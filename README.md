# YTGrab

Download audio (MP3) and video (MP4) from YouTube — single videos, playlists, or entire channels.  
Features live progress, an async download queue, per-job cancellation, and a folder-browser UI.

---

## Quick start — Docker (recommended)

All data (database, audio, video) persists in named Docker volumes across restarts.

### Build & start

```bash
docker compose up --build -d
```

App available at **http://localhost:8000**

### Stop (keeps volumes intact)

```bash
docker compose down
```

### Stop & delete all downloaded data

```bash
docker compose down -v   # -v removes the named volumes
```

### View logs

```bash
docker compose logs -f
```

### Rebuild after code changes

```bash
docker compose up --build -d
```

---

## Local dev — uv

### One-time setup

```bash
uv venv
uv pip install -e .
```

> Requires **ffmpeg** on your PATH (`brew install ffmpeg` on macOS).

### Start

```bash
uv run uvicorn main:app --host 127.0.0.1 --port 8765 --reload
```

App available at **http://127.0.0.1:8765**

### Stop — kill by port number

Find the process listening on the port and kill it:

```bash
# macOS / Linux
lsof -ti :8765 | xargs kill -9

# or step by step:
lsof -i :8765          # note the PID in the second column
kill -9 <PID>
```

On Windows (PowerShell):

```powershell
netstat -ano | findstr :8765   # note the PID in the last column
taskkill /PID <PID> /F
```

---

## Folder layout (Docker)

Because we use bind mounts in `docker-compose.yml`, the downloads go straight to your actual machine in the same directory as the project:

| Host folder | Container mount | Contents |
|---|---|---|
| `./data`            | `/app/data`   | `ytgrab.db` — SQLite database |
| `./downloads/audio` | `/app/audio`  | Downloaded MP3 files & subfolders |
| `./downloads/video` | `/app/video`  | Downloaded MP4 files & subfolders |

### How to set custom folders

If you want downloads to go to a specific folder on your Mac (for example, your system `Music` or `Movies` folders), you can change the left-hand side of the `volumes` section in `docker-compose.yml`:

```yaml
    volumes:
      - ./data:/app/data
      - /Users/yourusername/Music:/app/audio
      - /Users/yourusername/Movies:/app/video
```

After modifying the file, run `docker compose down` and `docker compose up -d` for the new paths to take effect.

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `DATA_DIR`       | `.`               | Directory for `ytgrab.db` |
| `AUDIO_DIR`      | `downloads/audio` | Audio download root |
| `VIDEO_DIR`      | `downloads/video` | Video download root |
| `MAX_CONCURRENT` | `3`               | Max simultaneous downloads |
