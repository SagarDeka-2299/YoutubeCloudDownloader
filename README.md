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



---



Your app actually already has the underlying code ready to bypass bot detection! 

I looked at your codebase, and `yt-dlp` is already configured to accept a **`cookies.txt`** file, and your `docker-compose.yml` has the setup commented out. 

YouTube's bot detection primarily blocks requests that come from unauthenticated, server-side data-center IPs. By providing cookies from a logged-in (and trusted) normal web browser, `yt-dlp` makes its requests look identical to you browsing YouTube normally.

Here is exactly how to enable it in your app:

### Step 1: Export your YouTube Cookies
You need to export your browser cookies while logged into YouTube.
1. Install a "Get cookies.txt locally" extension for your browser (e.g., [Get cookies.txt LOCALLY for Chrome](https://chrome.google.com/webstore/detail/get-cookiestxt-locally/cclelndahbckbenkjhflpdnnchhlhlbg)).
2. Go to [youtube.com](https://www.youtube.com/) and make sure you are logged in (you can use a burner account if you prefer, but a real account you actively use is less likely to be flagged).
3. Click the extension icon and export your cookies.
4. Save the downloaded file as `cookies.txt` into the root of your `youtube_video_downloader` folder (right next to your `docker-compose.yml`).

> [!CAUTION]
> Your `cookies.txt` contains your session data. Do not commit this file to GitHub or share it with anyone! (It is already safely ignored in your `.gitignore`, but just as a warning.)

### Step 2: Update your `docker-compose.yml`
Uncomment the cookie lines in your `docker-compose.yml` so that Docker passes the file into the container.

In your `docker-compose.yml`, change the `volumes:` section to mount the file:
```yaml
    volumes:
      - ./data:/app/data
      - ./downloads/audio:/app/audio
      - ./downloads/video:/app/video
      - ./cookies.txt:/app/cookies.txt:ro
```

And in the `environment:` section, uncomment the env var:
```yaml
    environment:
      DATA_DIR:       /app/data
      AUDIO_DIR:      /app/audio
      VIDEO_DIR:      /app/video
      MAX_CONCURRENT: "3"
      COOKIES_FILE:   /app/cookies.txt
```

### Step 3: Restart the container
Run the following to restart the container with the newly mapped cookies file:
```bash
docker-compose down
docker-compose up -d
```

### Why this works:
1. **Cookies:** Makes you appear as an authenticated user who has solved CAPTCHAs implicitly.
2. **Player Clients:** Notice in `main.py` lines 208-210, your code is already instructing `yt-dlp` to pretend to be an `android` and `web` player client. These clients combined with valid cookies are currently the most robust way to avoid the "Sign in to confirm you're not a bot" error.