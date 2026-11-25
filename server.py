import os
import re
import time
import asyncio
import logging
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
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


def _download_mp3(video_id: str) -> Path:
    if not re.fullmatch(r"[A-Za-z0-9_-]{6,20}", video_id):
        raise ValueError("Invalid video_id")

    target_mp3 = CACHE_DIR / f"{video_id}.mp3"
    if target_mp3.exists() and target_mp3.stat().st_size > 1024:
        os.utime(target_mp3, None)
        return target_mp3

    out_tmpl = str(CACHE_DIR / f"{video_id}.%(ext)s")
    ydl_opts = {
        "format": "bestaudio*/best",
        "outtmpl": out_tmpl,
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "cachedir": False,
        "concurrent_fragment_downloads": 1,
        "extractor_args": {
            "youtube": {
                "player_client": ["android_vr", "tv", "web"],
            }
        },
        "postprocessors": [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": MP3_BITRATE.rstrip("k"),
            }
        ],
        "postprocessor_args": ["-threads", "1", "-ar", "44100", "-ac", "2"],
    }
    if COOKIES_FILE and os.path.exists(COOKIES_FILE):
        tmp_cookies = Path("/tmp/yt_cookies.txt")
        if not tmp_cookies.exists():
            import shutil
            shutil.copy2(COOKIES_FILE, tmp_cookies)
        ydl_opts["cookiefile"] = str(tmp_cookies)

    url = f"https://music.youtube.com/watch?v={video_id}"
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])

    if not target_mp3.exists():
        raise FileNotFoundError(f"Failed to produce MP3 for {video_id}")

    _cleanup_cache()
    return target_mp3


def _download_soundcloud_mp3(query: str) -> Optional[dict]:
    """Fallback to SoundCloud (no cookies or JS challenge needed)."""
    ydl_opts = {
        "format": "bestaudio/best",
        "outtmpl": str(CACHE_DIR / "sc_%(id)s.%(ext)s"),
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "cachedir": False,
        "default_search": "scsearch1",
        "postprocessors": [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": MP3_BITRATE.rstrip("k"),
            }
        ],
        "postprocessor_args": ["-threads", "1", "-ar", "44100", "-ac", "2"],
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(f"scsearch1:{query}", download=True)
        entries = info.get("entries") or []
        if not entries:
            return None
        entry = entries[0]
        sc_id = entry.get("id")
        if not sc_id:
            return None
        mp3_path = CACHE_DIR / f"sc_{sc_id}.mp3"
        if not mp3_path.exists():
            return None
        _cleanup_cache()
        return {
            "title": entry.get("title") or query,
            "artist": entry.get("uploader") or "",
            "duration": int(entry.get("duration") or 0),
            "filename": f"sc_{sc_id}.mp3",
            "source": "soundcloud",
        }


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
    """Compatible with Esp32Music::Download (/stream_pcm?song=...&artist=...)."""
    query = f"{song} {artist}".strip()
    meta = await asyncio.to_thread(_search_ytmusic, song, artist)
    if meta and meta.get("video_id"):
        video_id = meta["video_id"]
        try:
            await asyncio.to_thread(_download_mp3, video_id)
            return {
                "title": meta["title"],
                "artist": meta["artist"],
                "duration": meta["duration"],
                "audio_url": f"/audio/{video_id}.mp3",
                "lyric_url": "",
                "source": "ytmusic",
            }
        except Exception as e:
            logger.warning(f"YouTube Music failed for {video_id}, falling back to SoundCloud: {e}")

    try:
        sc_res = await asyncio.to_thread(_download_soundcloud_mp3, query)
        if sc_res:
            return {
                "title": sc_res["title"],
                "artist": sc_res["artist"],
                "duration": sc_res["duration"],
                "audio_url": f"/audio/{sc_res['filename']}",
                "lyric_url": "",
                "source": "soundcloud",
            }
    except Exception as e:
        logger.error(f"SoundCloud fallback also failed for '{query}': {e}")
        raise HTTPException(status_code=500, detail=f"Failed to prepare MP3: {e}")

    raise HTTPException(status_code=404, detail="Song not found on YouTube Music or SoundCloud")


@app.get("/audio/{filename}")
def get_audio(filename: str):
    if not re.fullmatch(r"[A-Za-z0-9_-]+\.mp3", filename):
        raise HTTPException(status_code=400, detail="Invalid filename")
    path = CACHE_DIR / filename
    if not path.exists():
        raise HTTPException(status_code=404, detail="Audio file not found")
    return FileResponse(path, media_type="audio/mpeg")
