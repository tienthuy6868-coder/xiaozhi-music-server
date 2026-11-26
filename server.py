import os
import re
import sys
import json
import uuid
import shutil
import asyncio
import logging
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse
from ytmusicapi import YTMusic
import yt_dlp

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("MusicServer")

CACHE_DIR = Path(os.environ.get("MUSIC_CACHE_DIR", "/tmp/music_cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)
MAX_CACHE_FILES = int(os.environ.get("MAX_CACHE_FILES", "8"))
MP3_BITRATE = os.environ.get("MP3_BITRATE", "96k")
COOKIES_FILE = os.environ.get("YTDLP_COOKIES", "")
FALLBACK_MUSIC_SERVER = os.environ.get(
    "FALLBACK_MUSIC_SERVER", "https://xiaozhi-music-server-y4qs.onrender.com"
).rstrip("/")

app = FastAPI(title="Xiaozhi Music Server")
ytmusic = YTMusic()
# Map audio filename -> original search query for seamless SoundCloud fallback
PENDING_QUERIES: dict[str, str] = {}
PENDING_SONG_ARTIST: dict[str, tuple[str, str]] = {}
PENDING_FALLBACK_URLS: dict[str, str] = {}
DOWNLOAD_TASKS: dict[str, asyncio.Task] = {}


def _get_cookies_path() -> Optional[str]:
    if not COOKIES_FILE or not os.path.exists(COOKIES_FILE):
        return None
    tmp_cookies = Path("/tmp/yt_cookies.txt")
    if not tmp_cookies.exists():
        shutil.copy2(COOKIES_FILE, tmp_cookies)
    return str(tmp_cookies)


def _cleanup_cache() -> None:
    """Keep cache directory bounded by removing oldest MP3 files."""
    files = sorted(CACHE_DIR.glob("*.mp3"), key=lambda p: p.stat().st_mtime)
    while len(files) > MAX_CACHE_FILES:
        oldest = files.pop(0)
        try:
            oldest.unlink()
        except OSError:
            pass


def _search_ytmusic(song: str, artist: str = "") -> Optional[dict]:
    query = f"{song} {artist}".strip()
    try:
        results = ytmusic.search(query, filter="songs", limit=3)
        if not results:
            results = ytmusic.search(query, filter="videos", limit=3)
        if results:
            top = results[0]
            artists = ", ".join(a.get("name", "") for a in top.get("artists", []) if a.get("name"))
            return {
                "video_id": top.get("videoId"),
                "title": top.get("title") or song,
                "artist": artists or artist,
                "duration": top.get("duration_seconds") or 0,
            }
    except Exception as e:
        logger.warning(f"ytmusicapi search failed, falling back to yt-dlp search: {e}")

    # Fallback via yt-dlp ytsearch1
    ydl_opts = {"quiet": True, "skip_download": True, "extract_flat": True}
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(f"ytsearch1:{query}", download=False)
        entries = info.get("entries") or []
        if not entries:
            return None
        entry = entries[0]
        return {
            "video_id": entry.get("id"),
            "title": entry.get("title") or song,
            "artist": entry.get("uploader") or artist,
            "duration": int(entry.get("duration") or 0),
        }


def _search_soundcloud_meta(query: str) -> Optional[dict]:
    """Fast metadata-only SoundCloud search fallback."""
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "extract_flat": True,
        "default_search": "scsearch1",
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(f"scsearch1:{query}", download=False)
        entries = info.get("entries") or []
        if not entries:
            return None
        entry = entries[0]
        sc_id = entry.get("id")
        if not sc_id:
            return None
        return {
            "filename": f"sc_{sc_id}.mp3",
            "title": entry.get("title") or query,
            "artist": entry.get("uploader") or "",
            "duration": int(entry.get("duration") or 0),
        }


def _extract_stream_url(url_or_query: str, use_cookies: bool = False, player_clients: Optional[list[str]] = None, max_duration: int = 600) -> Optional[dict]:
    """Fast in-process yt-dlp URL extraction (no subprocess Python startup)."""
    ydl_opts = {
        "format": "bestaudio*/best",
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "socket_timeout": 5,
    }
    if player_clients:
        ydl_opts["extractor_args"] = {"youtube": {"player_client": player_clients}}
    if use_cookies:
        cookies_path = _get_cookies_path()
        if cookies_path:
            ydl_opts["cookiefile"] = cookies_path

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url_or_query, download=False)
        if "entries" in info:
            entries = [e for e in (info.get("entries") or []) if e]
            if not entries:
                return None
            # Prefer a normal-length song (60s..max_duration) over 30s previews or 1-hour mixes
            preferred = [e for e in entries if 60 <= (e.get("duration") or 0) <= max_duration]
            if not preferred:
                preferred = [e for e in entries if (e.get("duration") or 0) <= max_duration]
            chosen = preferred[0] if preferred else entries[0]
            if not chosen.get("url") and chosen.get("webpage_url"):
                info = ydl.extract_info(chosen["webpage_url"], download=False)
            else:
                info = chosen
        stream_url = info.get("url")
        if not stream_url:
            return None
        return {
            "url": stream_url,
            "ext": (info.get("ext") or "").lower(),
            "protocol": (info.get("protocol") or "").lower(),
            "headers": info.get("http_headers") or {},
        }


async def _terminate_proc(proc: Optional[asyncio.subprocess.Process]) -> None:
    if proc and proc.returncode is None:
        try:
            proc.kill()
        except ProcessLookupError:
            pass
        try:
            await proc.wait()
        except Exception:
            pass


def _download_direct_mp3_sync(stream_url: str, headers: dict, part_path: Path, timeout: int = 15) -> int:
    req = urllib.request.Request(stream_url, headers=headers)
    total = 0
    with urllib.request.urlopen(req, timeout=timeout) as resp, open(part_path, "wb") as f:
        while True:
            chunk = resp.read(32768)
            if not chunk:
                break
            f.write(chunk)
            f.flush()
            total += len(chunk)
    return total


def _fetch_kho2_meta(song: str, artist: str = "") -> Optional[dict]:
    """Query fallback Kho 2 server (/stream_pcm) and normalize audio_url to an absolute URL."""
    if not FALLBACK_MUSIC_SERVER:
        return None
    params = {"song": song}
    if artist:
        params["artist"] = artist
    url = f"{FALLBACK_MUSIC_SERVER}/stream_pcm?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": "XiaozhiMusicMerger/1.0"})
    with urllib.request.urlopen(req, timeout=25) as resp:
        if resp.status != 200:
            return None
        data = json.loads(resp.read().decode("utf-8"))
    audio_url = data.get("audio_url") or ""
    if not audio_url:
        return None
    if not audio_url.startswith("http"):
        audio_url = f"{FALLBACK_MUSIC_SERVER}{audio_url if audio_url.startswith('/') else '/' + audio_url}"
    data["audio_url"] = audio_url
    return data


async def _ffmpeg_stream_to_part(stream_url: str, headers: dict, part_path: Path) -> int:
    ff_cmd = ["ffmpeg", "-loglevel", "error"]
    if stream_url.startswith("http"):
        ff_cmd += ["-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "3"]
    if headers:
        hdr_str = "".join(f"{k}: {v}\r\n" for k, v in headers.items())
        ff_cmd += ["-headers", hdr_str]
    ff_cmd += [
        "-i", stream_url,
        "-vn", "-acodec", "libmp3lame",
        "-b:a", MP3_BITRATE,
        "-compression_level", "7",
        "-threads", "1",
        "-ar", "44100", "-ac", "2",
        "-f", "mp3", "pipe:1",
    ]
    ff_proc = await asyncio.create_subprocess_exec(
        *ff_cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    total = 0
    try:
        with open(part_path, "wb") as f:
            while True:
                chunk = await ff_proc.stdout.read(32768)
                if not chunk:
                    break
                f.write(chunk)
                f.flush()
                total += len(chunk)
        rc = await ff_proc.wait()
        if rc != 0 or total <= 4096:
            err = (await ff_proc.stderr.read()).decode("utf-8", errors="ignore").strip()
            logger.warning(f"ffmpeg failed (rc={rc}, bytes={total}): {err[:200]}")
            return 0
        return total
    except Exception:
        await _terminate_proc(ff_proc)
        raise


def _search_soundcloud_urls(query: str, max_duration: int = 600) -> list[str]:
    """Fast flat SoundCloud search returning ordered candidate URLs (60s..600s first)."""
    ydl_opts = {"quiet": True, "no_warnings": True, "extract_flat": True, "socket_timeout": 5}
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(f"scsearch5:{query}", download=False)
        entries = [e for e in (info.get("entries") or []) if e]
        if not entries:
            return []
        preferred = [e for e in entries if 60 <= (e.get("duration") or 0) <= max_duration]
        short = [e for e in entries if (e.get("duration") or 0) < 60]
        long = [e for e in entries if (e.get("duration") or 0) > max_duration]
        ordered = preferred + short + long
        urls: list[str] = []
        for e in ordered:
            u = e.get("url") or e.get("webpage_url")
            if u and u not in urls:
                urls.append(u)
        return urls


async def _download_to_part(filename: str, target_mp3: Path, query: str) -> None:
    """Resolve stream URL (fast SoundCloud by default; optional YouTube via ENABLE_YTDLP_YOUTUBE=1) and write MP3."""
    part_path = CACHE_DIR / f"{filename}.part"
    part_path.unlink(missing_ok=True)
    try:
        candidates: list[tuple[str, dict]] = []
        if os.environ.get("ENABLE_YTDLP_YOUTUBE", "0") == "1" and not filename.startswith("sc_"):
            video_id = filename[:-4]
            yt_url = f"https://music.youtube.com/watch?v={video_id}"
            try:
                yt_info = await asyncio.to_thread(_extract_stream_url, yt_url, True, ["tv", "web"])
                if yt_info:
                    candidates.append(("ytmusic-cookies", yt_info))
            except Exception as e:
                logger.warning(f"[ytmusic-cookies] extract failed for {filename}: {str(e)[:160]}")

        if query:
            if "|" in query:
                song_only, full_q = query.split("|", 1)
                queries_to_try = [full_q, song_only]
            else:
                queries_to_try = [query]
            for q_try in queries_to_try:
                try:
                    sc_urls = await asyncio.to_thread(_search_soundcloud_urls, q_try, 600)
                    for sc_url in sc_urls:
                        try:
                            sc_info = await asyncio.to_thread(_extract_stream_url, sc_url, False, None)
                            if sc_info:
                                candidates.append(("soundcloud", sc_info))
                                break
                        except Exception as e:
                            logger.warning(f"[soundcloud] skip candidate {sc_url}: {str(e)[:140]}")
                    if candidates:
                        break
                except Exception as e:
                    logger.warning(f"[soundcloud] search failed for '{q_try}': {str(e)[:160]}")

        # If SoundCloud failed/DRM and we haven't tried YouTube yet, fall back to YouTube Music with cookies
        if not candidates and not filename.startswith("sc_"):
            video_id = filename[:-4]
            yt_url = f"https://music.youtube.com/watch?v={video_id}"
            try:
                yt_info = await asyncio.to_thread(_extract_stream_url, yt_url, True, ["tv", "web"])
                if yt_info:
                    candidates.append(("ytmusic-cookies-fallback", yt_info))
            except Exception as e:
                logger.warning(f"[ytmusic-cookies-fallback] extract failed for {filename}: {str(e)[:160]}")

        direct_k2_url = PENDING_FALLBACK_URLS.get(filename)
        if direct_k2_url:
            try:
                part_path.unlink(missing_ok=True)
                total = await asyncio.to_thread(
                    _download_direct_mp3_sync,
                    direct_k2_url,
                    {"User-Agent": "XiaozhiMusicMerger/1.0"},
                    part_path,
                    30,
                )
                if total > 4096 and part_path.exists():
                    os.replace(part_path, target_mp3)
                    _cleanup_cache()
                    logger.info(f"[kho2-direct] cached {filename} ({total} bytes)")
                    return
            except Exception as e:
                logger.warning(f"[kho2-direct] failed for {filename}: {e}")
                part_path.unlink(missing_ok=True)

        for label, info in candidates:
            try:
                part_path.unlink(missing_ok=True)
                if info["ext"] == "mp3" and info["protocol"] in ("http", "https"):
                    total = await asyncio.to_thread(_download_direct_mp3_sync, info["url"], info["headers"], part_path)
                else:
                    total = await _ffmpeg_stream_to_part(info["url"], info["headers"], part_path)

                if total > 4096 and part_path.exists():
                    os.replace(part_path, target_mp3)
                    _cleanup_cache()
                    logger.info(f"[{label}] cached {filename} ({total} bytes)")
                    return
            except Exception as e:
                logger.warning(f"[{label}] stream failed for {filename}: {e}")
                part_path.unlink(missing_ok=True)

        if FALLBACK_MUSIC_SERVER and not direct_k2_url:
            song_k2, artist_k2 = PENDING_SONG_ARTIST.get(
                filename, (query.split("|")[-1] if query else filename.removesuffix(".mp3"), "")
            )
            if song_k2:
                try:
                    k2_meta = await asyncio.to_thread(_fetch_kho2_meta, song_k2, artist_k2)
                    if k2_meta and k2_meta.get("audio_url"):
                        part_path.unlink(missing_ok=True)
                        total = await asyncio.to_thread(
                            _download_direct_mp3_sync,
                            k2_meta["audio_url"],
                            {"User-Agent": "XiaozhiMusicMerger/1.0"},
                            part_path,
                            30,
                        )
                        if total > 4096 and part_path.exists():
                            os.replace(part_path, target_mp3)
                            _cleanup_cache()
                            logger.info(f"[kho2-fallback] cached {filename} ({total} bytes)")
                            return
                except Exception as e:
                    logger.warning(f"[kho2-fallback] failed for {filename}: {e}")
                    part_path.unlink(missing_ok=True)
    finally:
        DOWNLOAD_TASKS.pop(filename, None)


def _ensure_download_task(filename: str, target_mp3: Path, query: str) -> Optional[asyncio.Task]:
    if target_mp3.exists() and target_mp3.stat().st_size > 1024:
        return None
    task = DOWNLOAD_TASKS.get(filename)
    if task is None or task.done():
        task = asyncio.create_task(_download_to_part(filename, target_mp3, query))
        DOWNLOAD_TASKS[filename] = task
    return task


async def _tail_mp3_stream(filename: str, target_mp3: Path, task: Optional[asyncio.Task]):
    """Tail .part (and final .mp3) as bytes arrive so ESP32-S3 starts playing ASAP."""
    part_path = CACHE_DIR / f"{filename}.part"
    offset = 0
    while True:
        src = target_mp3 if target_mp3.exists() else (part_path if part_path.exists() else None)
        if src is not None:
            try:
                with open(src, "rb") as f:
                    f.seek(offset)
                    data = f.read(32768)
                if data:
                    offset += len(data)
                    yield data
                    continue
            except FileNotFoundError:
                # Renamed from .part to .mp3 mid-read; retry on target_mp3
                continue

        if task is None or task.done():
            # Final flush check in case bytes landed right as task completed
            final_src = target_mp3 if target_mp3.exists() else (part_path if part_path.exists() else None)
            if final_src is not None:
                with open(final_src, "rb") as f:
                    f.seek(offset)
                    while True:
                        tail = f.read(32768)
                        if not tail:
                            break
                        offset += len(tail)
                        yield tail
            break

        await asyncio.sleep(0.1)


@app.api_route("/", methods=["GET", "HEAD"])
def health():
    cached = len(list(CACHE_DIR.glob("*.mp3")))
    return {
        "status": "ok",
        "service": "Xiaozhi Music Server (YouTube Music + SoundCloud + Kho 2 -> MP3)",
        "cached_tracks": cached,
        "fallback_server": FALLBACK_MUSIC_SERVER,
    }


@app.get("/stream_pcm")
async def stream_pcm(
    song: str = Query(..., min_length=1),
    artist: str = Query("", description="Optional artist name"),
):
    """Fast metadata lookup (<1.5s) + background pre-download compatible with Esp32Music::Download."""
    query = f"{song} {artist}".strip()
    meta = await asyncio.to_thread(_search_ytmusic, song, artist)
    if meta and meta.get("video_id"):
        filename = f"{meta['video_id']}.mp3"
        # Save song-only fallback separated by '|' if artist was provided
        PENDING_QUERIES[filename] = f"{song}|{query}" if artist else query
        PENDING_SONG_ARTIST[filename] = (song, artist)
        if len(PENDING_QUERIES) > 100:
            PENDING_QUERIES.pop(next(iter(PENDING_QUERIES)))
        if len(PENDING_SONG_ARTIST) > 100:
            PENDING_SONG_ARTIST.pop(next(iter(PENDING_SONG_ARTIST)))
        _ensure_download_task(filename, CACHE_DIR / filename, PENDING_QUERIES[filename])
        return {
            "title": meta["title"],
            "artist": meta["artist"],
            "duration": meta["duration"],
            "audio_url": f"/audio/{filename}",
            "lyric_url": "",
            "source": "ytmusic",
        }

    try:
        sc_meta = await asyncio.to_thread(_search_soundcloud_meta, query)
        if sc_meta:
            filename = sc_meta["filename"]
            PENDING_QUERIES[filename] = query
            PENDING_SONG_ARTIST[filename] = (song, artist)
            _ensure_download_task(filename, CACHE_DIR / filename, PENDING_QUERIES[filename])
            return {
                "title": sc_meta["title"],
                "artist": sc_meta["artist"],
                "duration": sc_meta["duration"],
                "audio_url": f"/audio/{filename}",
                "lyric_url": "",
                "source": "soundcloud",
            }
    except Exception as e:
        logger.error(f"SoundCloud search failed for '{query}': {e}")

    if FALLBACK_MUSIC_SERVER:
        try:
            k2_meta = await asyncio.to_thread(_fetch_kho2_meta, song, artist)
            if k2_meta and k2_meta.get("audio_url"):
                filename = f"k2_{uuid.uuid4().hex[:12]}.mp3"
                PENDING_FALLBACK_URLS[filename] = k2_meta["audio_url"]
                PENDING_SONG_ARTIST[filename] = (song, artist)
                if len(PENDING_FALLBACK_URLS) > 100:
                    PENDING_FALLBACK_URLS.pop(next(iter(PENDING_FALLBACK_URLS)))
                _ensure_download_task(filename, CACHE_DIR / filename, query)
                return {
                    "title": k2_meta.get("title") or song,
                    "artist": k2_meta.get("artist") or artist,
                    "duration": k2_meta.get("duration") or 0,
                    "audio_url": f"/audio/{filename}",
                    "lyric_url": k2_meta.get("lyric_url") or "",
                    "source": "kho2",
                }
        except Exception as e:
            logger.error(f"Kho 2 fallback failed for '{query}': {e}")

    raise HTTPException(status_code=404, detail="Song not found on Kho 1 (YouTube Music / SoundCloud) or Kho 2")


@app.get("/audio/{filename}")
async def get_audio(filename: str, q: str = Query("", description="Optional fallback query")):
    if not re.fullmatch(r"[A-Za-z0-9_-]+\.mp3", filename):
        raise HTTPException(status_code=400, detail="Invalid filename")
    path = CACHE_DIR / filename
    if path.exists() and path.stat().st_size > 1024:
        os.utime(path, None)
        return FileResponse(path, media_type="audio/mpeg")

    query = q or PENDING_QUERIES.get(filename, "")
    task = _ensure_download_task(filename, path, query)
    return StreamingResponse(
        _tail_mp3_stream(filename, path, task),
        media_type="audio/mpeg",
    )
