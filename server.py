import os
import re
import sys
import uuid
import shutil
import asyncio
import logging
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
MP3_BITRATE = os.environ.get("MP3_BITRATE", "128k")
COOKIES_FILE = os.environ.get("YTDLP_COOKIES", "")

app = FastAPI(title="Xiaozhi Music Server")
ytmusic = YTMusic()
# Map audio filename -> original search query for seamless SoundCloud fallback
PENDING_QUERIES: dict[str, str] = {}


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


def _build_ytdlp_attempts(filename: str, query: str) -> list[tuple[str, list[str]]]:
    base = [
        sys.executable, "-m", "yt_dlp",
        "--no-playlist", "--no-warnings", "--quiet",
        "--no-cache-dir", "-f", "bestaudio/best", "-o", "-",
    ]
    attempts: list[tuple[str, list[str]]] = []

    if filename.startswith("sc_"):
        if query:
            attempts.append(("soundcloud", base + [f"scsearch1:{query}"]))
        return attempts

    video_id = filename[:-4]
    if not re.fullmatch(r"[A-Za-z0-9_-]{6,20}", video_id):
        return attempts

    yt_url = f"https://music.youtube.com/watch?v={video_id}"
    cookies_path = _get_cookies_path()
    if cookies_path:
        attempts.append((
            "ytmusic-cookies",
            base + [
                "--cookies", cookies_path,
                "--js-runtimes", "node",
                "--remote-components", "ejs:github",
                "--extractor-args", "youtube:player_client=web_safari,mweb,web,tv",
                yt_url,
            ],
        ))

    attempts.append((
        "ytmusic-android_vr",
        base + [
            "--extractor-args", "youtube:player_client=android_vr",
            yt_url,
        ],
    ))

    if query:
        attempts.append(("soundcloud-fallback", base + [f"scsearch1:{query}"]))

    return attempts


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


async def _stream_pipe_mp3(filename: str, target_mp3: Path, query: str):
    """Stream MP3 in real time via yt-dlp -> ffmpeg pipe while caching to disk."""
    attempts = _build_ytdlp_attempts(filename, query)
    if not attempts:
        return

    for label, ytdlp_cmd in attempts:
        tmp_path = CACHE_DIR / f"{filename}.{uuid.uuid4().hex[:6]}.tmp"
        yt_proc = None
        ff_proc = None
        read_fd, write_fd = os.pipe()
        try:
            yt_proc = await asyncio.create_subprocess_exec(
                *ytdlp_cmd,
                stdout=write_fd,
                stderr=asyncio.subprocess.PIPE,
            )
            os.close(write_fd)
            write_fd = -1

            ff_cmd = [
                "ffmpeg", "-loglevel", "error",
                "-i", "pipe:0",
                "-vn", "-acodec", "libmp3lame",
                "-b:a", MP3_BITRATE,
                "-ar", "44100", "-ac", "2",
                "-f", "mp3", "pipe:1",
            ]
            ff_proc = await asyncio.create_subprocess_exec(
                *ff_cmd,
                stdin=read_fd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            os.close(read_fd)
            read_fd = -1

            first_chunk = await ff_proc.stdout.read(8192)
            if not first_chunk:
                yt_err = (await yt_proc.stderr.read()).decode("utf-8", errors="ignore").strip()
                ff_err = (await ff_proc.stderr.read()).decode("utf-8", errors="ignore").strip()
                logger.warning(f"[{label}] produced 0 bytes for {filename}: yt={yt_err[:200]} ff={ff_err[:200]}")
                await _terminate_proc(yt_proc)
                await _terminate_proc(ff_proc)
                continue

            total_bytes = 0
            with open(tmp_path, "wb") as f:
                f.write(first_chunk)
                total_bytes += len(first_chunk)
                yield first_chunk

                while True:
                    chunk = await ff_proc.stdout.read(32768)
                    if not chunk:
                        break
                    f.write(chunk)
                    total_bytes += len(chunk)
                    yield chunk

            await yt_proc.wait()
            ff_rc = await ff_proc.wait()
            if ff_rc == 0 and total_bytes > 4096:
                os.replace(tmp_path, target_mp3)
                _cleanup_cache()
                logger.info(f"[{label}] streamed & cached {filename} ({total_bytes} bytes)")
            else:
                tmp_path.unlink(missing_ok=True)
            return
        except asyncio.CancelledError:
            tmp_path.unlink(missing_ok=True)
            await _terminate_proc(yt_proc)
            await _terminate_proc(ff_proc)
            raise
        except Exception as e:
            logger.warning(f"[{label}] stream error for {filename}: {e}")
            tmp_path.unlink(missing_ok=True)
            await _terminate_proc(yt_proc)
            await _terminate_proc(ff_proc)
        finally:
            if write_fd != -1:
                os.close(write_fd)
            if read_fd != -1:
                os.close(read_fd)


@app.api_route("/", methods=["GET", "HEAD"])
def health():
    cached = len(list(CACHE_DIR.glob("*.mp3")))
    return {
        "status": "ok",
        "service": "Xiaozhi Music Server (YouTube Music -> MP3)",
        "cached_tracks": cached,
    }


@app.get("/stream_pcm")
async def stream_pcm(
    song: str = Query(..., min_length=1),
    artist: str = Query("", description="Optional artist name"),
):
    """Fast metadata lookup (<1.5s) compatible with Esp32Music::Download."""
    query = f"{song} {artist}".strip()
    meta = await asyncio.to_thread(_search_ytmusic, song, artist)
    if meta and meta.get("video_id"):
        filename = f"{meta['video_id']}.mp3"
        PENDING_QUERIES[filename] = query
        if len(PENDING_QUERIES) > 100:
            PENDING_QUERIES.pop(next(iter(PENDING_QUERIES)))
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

    raise HTTPException(status_code=404, detail="Song not found on YouTube Music or SoundCloud")


@app.get("/audio/{filename}")
async def get_audio(filename: str, q: str = Query("", description="Optional fallback query")):
    if not re.fullmatch(r"[A-Za-z0-9_-]+\.mp3", filename):
        raise HTTPException(status_code=400, detail="Invalid filename")
    path = CACHE_DIR / filename
    if path.exists() and path.stat().st_size > 1024:
        os.utime(path, None)
        return FileResponse(path, media_type="audio/mpeg")

    query = q or PENDING_QUERIES.get(filename, "")
    return StreamingResponse(
        _stream_pipe_mp3(filename, path, query),
        media_type="audio/mpeg",
    )
