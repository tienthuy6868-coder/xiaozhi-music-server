# Xiaozhi YouTube Music -> MP3 Streaming Server

Server tìm nhạc từ **YouTube Music** (`ytmusicapi` + `yt-dlp`), chuyển đổi sang **MP3 128kbps / 44.1kHz** bằng `ffmpeg` và trả về đúng định dạng JSON cho firmware `Esp32Music` trên ESP32-S3.

## Endpoints
- `GET /`: Kiểm tra trạng thái server.
- `GET /stream_pcm?song=<ten_bai>&artist=<ca_si>`: Tìm bài hát, tải & chuyển sang MP3, trả về JSON:
  ```json
  {
    "title": "...",
    "artist": "...",
    "duration": 345,
    "audio_url": "/audio/<video_id>.mp3",
    "lyric_url": "",
    "source": "ytmusic"
  }
  ```
- `GET /audio/<video_id>.mp3`: Stream file MP3 (hỗ trợ `Accept-Ranges` cho ESP32-S3).

## Chạy bằng Docker (khuyên dùng vì có sẵn ffmpeg)
```bash
docker build -t xiaozhi-music-server .
docker run -p 8000:8000 xiaozhi-music-server
```

## Chạy trực tiếp bằng Python (cần cài sẵn ffmpeg trong PATH)
```bash
pip install -r requirements.txt
uvicorn server:app --host 0.0.0.0 --port 8000
```

## Biến môi trường tùy chọn
- `MP3_BITRATE`: mặc định `128k` (nhẹ, stream mượt cho ESP32-S3).
- `MAX_CACHE_FILES`: số file MP3 cache tối đa (mặc định `50`).
- `YTDLP_COOKIES`: đường dẫn file `cookies.txt` nếu triển khai trên IP cloud bị YouTube yêu cầu xác thực.
