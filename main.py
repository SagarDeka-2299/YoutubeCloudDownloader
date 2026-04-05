"""
main.py – YTGrab backend
"""
from __future__ import annotations

import asyncio
import os
import re
import shutil
import sys
import threading
import time
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

import yt_dlp
import db
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel


# ── Logging helper ─────────────────────────────────────────────────────────────
def log(emoji: str, msg: str) -> None:
    print(f"{emoji}  {msg}", flush=True)


# ── Environment config ─────────────────────────────────────────────────────────
AUDIO_DIR = Path(os.environ.get("AUDIO_DIR", "downloads/audio"))
VIDEO_DIR = Path(os.environ.get("VIDEO_DIR", "downloads/video"))
AUDIO_DIR.mkdir(parents=True, exist_ok=True)
VIDEO_DIR.mkdir(parents=True, exist_ok=True)

MAX_CONCURRENT   = int(os.environ.get("MAX_CONCURRENT", "3"))
_FFMPEG_LOCATION = shutil.which("ffmpeg")
_COOKIES_FILE    = os.environ.get("COOKIES_FILE", "")  # path to Netscape cookies.txt

log("⚙️", f"AUDIO_DIR={AUDIO_DIR.resolve()}")
log("⚙️", f"VIDEO_DIR={VIDEO_DIR.resolve()}")
log("⚙️", f"MAX_CONCURRENT={MAX_CONCURRENT}")
log("⚙️", f"ffmpeg={'found at ' + _FFMPEG_LOCATION if _FFMPEG_LOCATION else 'NOT FOUND'}")
if _COOKIES_FILE:
    _cp = Path(_COOKIES_FILE)
    log("🍪", f"COOKIES_FILE={_COOKIES_FILE} exists={_cp.exists()} size={_cp.stat().st_size if _cp.exists() else 0}B")
else:
    log("⚠️", "COOKIES_FILE not set — YouTube may block downloads (bot detection)")

app = FastAPI(title="YTGrab")
app.mount("/files/audio", StaticFiles(directory=str(AUDIO_DIR)), name="audio_files")
app.mount("/files/video", StaticFiles(directory=str(VIDEO_DIR)), name="video_files")

_dl_semaphore: asyncio.Semaphore


# ── Job registry ───────────────────────────────────────────────────────────────
class DownloadJob:
    def __init__(self, job_id: str) -> None:
        self.job_id        = job_id
        self.messages:  list[dict]          = []
        self.done                           = False
        self._waiters:  list[asyncio.Queue] = []
        self.cancel_event                   = threading.Event()
        self._current_file: str             = ""
        self._last_db_write: float          = 0.0

    def should_update_db(self) -> bool:
        now = time.monotonic()
        if now - self._last_db_write >= 0.5:
            self._last_db_write = now
            return True
        return False

    def push(self, msg: dict) -> None:
        self.messages.append(msg)
        for q in self._waiters:
            try:
                q.put_nowait(msg)
            except Exception:
                pass
        if msg.get("phase") in ("done", "error", "cancelled"):
            self.done = True

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue()
        for m in self.messages:
            q.put_nowait(m)
        if not self.done:
            self._waiters.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        try:
            self._waiters.remove(q)
        except ValueError:
            pass


_jobs: dict[str, DownloadJob] = {}


# ── yt-dlp post-processor: saves to DB after ffmpeg is done ───────────────────
class _SaveToDB(yt_dlp.postprocessor.PostProcessor):
    """
    Runs AFTER all yt-dlp built-in post-processors (including FFmpegExtractAudio).
    This guarantees the final file path (e.g. .mp3) is correct.
    """
    def __init__(self, job: DownloadJob, loop: asyncio.AbstractEventLoop,
                 mode: str, quality: str) -> None:
        super().__init__()
        self._job     = job
        self._loop    = loop
        self._mode    = mode
        self._quality = quality

    def run(self, info: dict) -> tuple[list, dict]:
        job_short = self._job.job_id[:8]
        log("💾", f"[{job_short}] _SaveToDB.run() called — title={info.get('title','?')!r}")

        # After FFmpegExtractAudio the real file path lives in requested_downloads
        downloads  = info.get("requested_downloads") or []
        final_path = ""
        final_size = 0

        if downloads:
            d0 = downloads[0]
            # 'filepath' is set by FFmpeg PP; 'filename' is the pre-PP path
            final_path = d0.get("filepath") or d0.get("filename") or ""
            final_size = int(d0.get("filesize") or d0.get("filesize_approx") or 0)
            log("💾", f"[{job_short}] requested_downloads[0] filepath={final_path!r} "
                      f"filesize={final_size}")

        if not final_path:
            final_path = (info.get("filepath") or info.get("filename")
                          or info.get("_filename") or "")
            log("💾", f"[{job_short}] fallback path={final_path!r}")

        # Get actual size from disk if not provided
        if not final_size and final_path and Path(final_path).exists():
            final_size = Path(final_path).stat().st_size
            log("💾", f"[{job_short}] disk size={final_size}")

        if not final_path:
            log("❌", f"[{job_short}] No file path found after download — skipping DB save!")
            return [], info

        if not Path(final_path).exists():
            log("❌", f"[{job_short}] File does not exist on disk: {final_path}")

        ud           = info.get("upload_date") or ""
        release_date = f"{ud[:4]}-{ud[4:6]}-{ud[6:]}" if len(ud) == 8 else None
        tags         = (info.get("tags") or [])[:15]

        channel_id   = info.get("channel_id") or ""
        channel_name = info.get("channel") or info.get("uploader") or ""
        channel_url  = info.get("channel_url") or info.get("uploader_url") or ""

        log("💾", f"[{job_short}] upserting channel {channel_id!r} → {channel_name!r}")
        ch_db_id = db.upsert_channel(channel_id, channel_name, channel_url) if channel_id else None

        media_data = {
            "youtube_id":    info.get("id"),
            "title":         info.get("title"),
            "channel_db_id": ch_db_id,
            "channel_name":  channel_name,
            "channel_url":   channel_url,
            "duration":      info.get("duration"),
            "view_count":    info.get("view_count"),
            "like_count":    info.get("like_count"),
            "tags":          tags,
            "release_date":  release_date,
            "thumbnail":     info.get("thumbnail"),
            "youtube_url":   info.get("webpage_url"),
            "file_path":     final_path,
            "file_name":     Path(final_path).name,
            "file_size":     final_size,
            "mode":          self._mode,
            "quality":       self._quality,
        }
        log("💾", f"[{job_short}] calling db.upsert_media — file={Path(final_path).name!r}")
        try:
            media_id = db.upsert_media(media_data)
            log("✅", f"[{job_short}] Saved to DB with id={media_id}")
        except Exception as exc:
            log("❌", f"[{job_short}] db.upsert_media FAILED: {exc}")

        title   = info.get("title", "")
        track   = info.get("playlist_index") or 1
        n_total = info.get("n_entries") or 1
        self._loop.call_soon_threadsafe(
            self._job.push,
            {"phase": "saving", "title": title, "track": track, "total": n_total},
        )
        return [], info


# ── yt-dlp opts ────────────────────────────────────────────────────────────────
def _build_opts(mode: str, quality: str, output_dir: Path) -> dict:
    base: dict = {
        # Do NOT use quiet=True — we need to see errors in logs
        "quiet":        False,
        "no_warnings":  False,
        "ignoreerrors": False,   # False so exceptions propagate correctly
        "outtmpl":      str(output_dir / "%(title)s.%(ext)s"),
        "noprogress":   True,    # Suppress the text progress bar (we use hooks)
        # Use Android + web player clients — bypasses the "Sign in" bot check
        "extractor_args": {"youtube": {
            "player_client": ["android", "web"],
        }},
        "http_headers": {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        },
    }
    if _FFMPEG_LOCATION:
        base["ffmpeg_location"] = _FFMPEG_LOCATION
    if _COOKIES_FILE and Path(_COOKIES_FILE).exists():
        base["cookiefile"] = _COOKIES_FILE
        log("🍪", f"Using cookies file: {_COOKIES_FILE}")

    if mode == "audio":
        return {
            **base,
            "format": "bestaudio/best",
            "postprocessors": [{"key": "FFmpegExtractAudio",
                                "preferredcodec": "mp3", "preferredquality": "192"}],
        }

    fmt = (
        "bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio/best[ext=mp4]/best"
        if quality == "best"
        else (
            f"bestvideo[height<={quality}][ext=mp4]+bestaudio[ext=m4a]/"
            f"bestvideo[height<={quality}]+bestaudio/"
            f"best[height<={quality}][ext=mp4]/best[height<={quality}]/best"
        )
    )
    return {**base, "format": fmt, "merge_output_format": "mp4"}


# ── Download worker ────────────────────────────────────────────────────────────
async def _run_job(job: DownloadJob, url: str, mode: str, quality: str,
                   target_path: str) -> None:
    global _dl_semaphore
    loop       = asyncio.get_event_loop()
    job_short  = job.job_id[:8]
    base_dir   = AUDIO_DIR if mode == "audio" else VIDEO_DIR
    output_dir = (base_dir / target_path).resolve() if target_path else base_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    opts       = _build_opts(mode, quality, output_dir)

    log("📋", f"[{job_short}] Job created — mode={mode} quality={quality} url={url[:60]}")
    log("📁", f"[{job_short}] Output dir: {output_dir}")

    db.queue_update(job.job_id, status="queued")
    job.push({"phase": "fetching", "message": "Waiting for slot…"})

    async with _dl_semaphore:
        log("🚦", f"[{job_short}] Semaphore acquired — starting download")
        db.queue_update(job.job_id, status="downloading")
        job.push({"phase": "fetching", "message": "Fetching metadata…"})

        _thumb_seeded = [False]
        _cancelled    = [False]

        def _progress_hook(d: dict) -> None:
            if job.cancel_event.is_set():
                _cancelled[0] = True
                raise Exception("__ytgrab_cancel__")

            status = d.get("status")
            info   = d.get("info_dict") or {}
            title  = info.get("title") or Path(d.get("filename", "")).stem
            track  = info.get("playlist_index") or 1
            n_tot  = info.get("n_entries") or 1

            job._current_file = d.get("filename", "") or d.get("tmpfilename", "")

            # Seed thumbnail + title on first hook call
            if not _thumb_seeded[0] and title:
                thumb = info.get("thumbnail") or ""
                log("🖼️", f"[{job_short}] Seeding title={title!r} thumb={'yes' if thumb else 'no'}")
                db.queue_update(job.job_id, title=title, thumbnail=thumb)
                _thumb_seeded[0] = True

            if status == "downloading":
                dl_bytes    = int(d.get("downloaded_bytes") or 0)
                total_bytes = int(d.get("total_bytes") or d.get("total_bytes_estimate") or 0)
                speed_bps   = float(d.get("speed") or 0)
                eta_sec     = int(d.get("eta") or 0)

                pct = f"{dl_bytes/total_bytes*100:.1f}%" if total_bytes else "?%"
                log("⬇️", f"[{job_short}] {pct} — {dl_bytes//1024//1024}MB/"
                          f"{total_bytes//1024//1024}MB @ {speed_bps//1024:.0f}KB/s ETA={eta_sec}s")

                loop.call_soon_threadsafe(job.push, {
                    "phase":            "downloading",
                    "title":            title,
                    "track":            track,
                    "total":            n_tot,
                    "downloaded_bytes": dl_bytes,
                    "total_bytes":      total_bytes,
                    "speed_bps":        speed_bps,
                    "eta_seconds":      eta_sec,
                    "mode":             mode,
                })
                if job.should_update_db():
                    db.queue_update(
                        job.job_id,
                        status="downloading",
                        title=title,
                        downloaded_bytes=dl_bytes,
                        total_bytes=total_bytes,
                        speed_bps=speed_bps,
                        eta_seconds=eta_sec,
                    )

            elif status == "finished":
                log("✔️", f"[{job_short}] Download chunk finished — entering ffmpeg/merge")
                loop.call_soon_threadsafe(job.push, {
                    "phase": "converting", "title": title,
                    "track": track, "total": n_tot, "mode": mode,
                })
                db.queue_update(job.job_id, status="converting", title=title)

            elif status == "error":
                log("❌", f"[{job_short}] yt-dlp reported error status in hook: {d}")

        opts["progress_hooks"] = [_progress_hook]

        def _run() -> str | None:
            """Returns None on success, '__cancelled__' on cancel, or error string."""
            log("▶️", f"[{job_short}] _run() entering yt_dlp.YoutubeDL")
            try:
                with yt_dlp.YoutubeDL(opts) as ydl:
                    # Register SaveToDB AFTER FFmpeg so it sees the final .mp3/.mp4 path
                    saver = _SaveToDB(job, loop, mode, quality)
                    ydl.add_post_processor(saver, when="post_process")

                    log("🔍", f"[{job_short}] Calling extract_info(download=True)")
                    info = ydl.extract_info(url, download=True)

                if info is None:
                    log("⚠️", f"[{job_short}] extract_info returned None")
                    return "yt-dlp returned no info (URL may be invalid or private)"

                log("✅", f"[{job_short}] extract_info complete — type={info.get('_type','video')!r} "
                          f"title={info.get('title','?')!r}")

            except Exception as exc:
                msg = str(exc)
                if "__ytgrab_cancel__" in msg or _cancelled[0] or job.cancel_event.is_set():
                    log("🚫", f"[{job_short}] Cancelled by user")
                    return "__cancelled__"
                log("❌", f"[{job_short}] Exception in _run: {exc}")
                import traceback
                traceback.print_exc()
                return msg

            if _cancelled[0] or job.cancel_event.is_set():
                return "__cancelled__"

            log("✅", f"[{job_short}] _run() completed successfully")
            return None

        error = await loop.run_in_executor(None, _run)
        log("🏁", f"[{job_short}] run_in_executor returned — error={error!r}")

    # ── Post-download cleanup ──────────────────────────────────────────────────
    if error == "__cancelled__":
        log("🚫", f"[{job_short}] Cleaning up partial files after cancel")
        for pattern in [job._current_file,
                        job._current_file + ".part",
                        job._current_file + ".ytdl"]:
            if pattern:
                try:
                    Path(pattern).unlink(missing_ok=True)
                    log("🗑️", f"[{job_short}] Removed partial: {pattern}")
                except Exception as e:
                    log("⚠️", f"[{job_short}] Could not remove {pattern}: {e}")
        job.push({"phase": "cancelled"})
        db.queue_update(job.job_id, status="cancelled")
        await asyncio.sleep(2)
        await loop.run_in_executor(None, db.queue_delete, job.job_id)
        _jobs.pop(job.job_id, None)
        log("🚫", f"[{job_short}] Cancel cleanup done")

    elif error:
        log("❌", f"[{job_short}] Job failed: {error}")
        job.push({"phase": "error", "message": error})
        db.queue_update(job.job_id, status="error", error_msg=error)
        # Keep in DB so user can dismiss; remove from memory
        _jobs.pop(job.job_id, None)

    else:
        log("🎉", f"[{job_short}] Job completed successfully!")
        job.push({"phase": "done"})
        db.queue_update(job.job_id, status="done")
        # Keep visible for 5 s so UI can flash green
        await asyncio.sleep(5)
        await loop.run_in_executor(None, db.queue_delete, job.job_id)
        _jobs.pop(job.job_id, None)
        log("✨", f"[{job_short}] Queue row cleaned up")


# ── App lifecycle ──────────────────────────────────────────────────────────────
@app.on_event("startup")
async def _startup() -> None:
    global _dl_semaphore
    _dl_semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    log("🚀", "App starting up — initialising DB")
    db.init_db()
    log("🗄️", "DB initialised")
    # Mark orphaned active rows as error (server was killed mid-download)
    rows, _ = db.queue_list(0, 9999)
    for row in rows:
        if row.get("status") in ("downloading", "converting", "saving", "queued"):
            log("⚠️", f"Orphaned queue row {row['job_id'][:8]} status={row['status']} → error")
            db.queue_update(row["job_id"], status="error",
                            error_msg="Server restarted — download was interrupted")
    log("✅", "Startup complete")


# ── Static / HTML ──────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    return HTMLResponse(content=Path("static/index.html").read_text())


# ── Folder helpers ─────────────────────────────────────────────────────────────
def _base_for(ftype: str) -> Path:
    if ftype == "audio":
        return AUDIO_DIR.resolve()
    elif ftype == "video":
        return VIDEO_DIR.resolve()
    raise ValueError(f"Unknown type: {ftype}")


def _safe_resolve(base: Path, rel: str) -> Path:
    target = (base / rel).resolve()
    if not str(target).startswith(str(base)):
        raise ValueError("Path escapes root")
    return target


def _has_media_files(directory: Path) -> bool:
    for item in directory.rglob("*"):
        if item.is_file():
            return True
    return False


@app.get("/api/folders")
async def list_folders(type: str = "audio", path: str = "") -> dict:
    loop = asyncio.get_event_loop()

    def _list() -> dict:
        base = _base_for(type)
        try:
            current = _safe_resolve(base, path)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        current.mkdir(parents=True, exist_ok=True)
        rel    = current.relative_to(base)
        parts  = list(rel.parts)
        crumbs = [{"name": "Root", "path": ""}]
        cumul  = ""
        for part in parts:
            cumul = f"{cumul}/{part}".lstrip("/")
            crumbs.append({"name": part, "path": cumul})
        dirs = []
        for item in sorted(current.iterdir()):
            if item.is_dir():
                rel_path = str(item.relative_to(base))
                dirs.append({"name": item.name, "path": rel_path,
                             "has_files": _has_media_files(item)})
        return {
            "type":        type,
            "current":     str(current.relative_to(base)) if current != base else "",
            "breadcrumbs": crumbs,
            "dirs":        dirs,
        }

    return await loop.run_in_executor(None, _list)


class FolderCreate(BaseModel):
    type: str
    path: str


@app.post("/api/folders")
async def create_folder(req: FolderCreate) -> dict:
    loop = asyncio.get_event_loop()

    def _create() -> dict:
        base = _base_for(req.type)
        try:
            target = _safe_resolve(base, req.path)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        target.mkdir(parents=True, exist_ok=True)
        return {"created": str(target.relative_to(base))}

    return await loop.run_in_executor(None, _create)


@app.delete("/api/folders")
async def delete_folder(type: str, path: str) -> dict:
    loop = asyncio.get_event_loop()

    def _delete() -> dict:
        base = _base_for(type)
        if not path or path in (".", "/", ""):
            raise HTTPException(status_code=400, detail="Cannot delete root folder")
        try:
            target = _safe_resolve(base, path)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        if not target.exists():
            raise HTTPException(status_code=404, detail="Folder not found")
        if _has_media_files(target):
            raise HTTPException(status_code=409,
                                detail="Folder contains files – move or delete files first")
        shutil.rmtree(target)
        return {"deleted": path}

    return await loop.run_in_executor(None, _delete)


# ── Jobs + Queue ───────────────────────────────────────────────────────────────
class JobRequest(BaseModel):
    url:         str
    mode:        Literal["audio", "video"] = "audio"
    quality:     str                       = "best"
    target_path: str                       = ""


def extract_youtube_id(url: str) -> str | None:
    match = re.search(r'(?:v=|\/)([0-9A-Za-z_-]{11})(?:[?&]|$)', url)
    return match.group(1) if match else None


@app.post("/api/jobs")
async def create_job(req: JobRequest) -> dict:
    loop = asyncio.get_event_loop()
    log("📥", f"POST /api/jobs — url={req.url[:60]!r} mode={req.mode}")

    y_id = extract_youtube_id(req.url)
    if y_id:
        existing = await loop.run_in_executor(None, db.get_media_by_youtube_id, y_id)
        if existing:
            log("⚠️", f"Duplicate detected: youtube_id={y_id}")
            raise HTTPException(status_code=409, detail={"duplicate": True, "media": existing})

    job_id = str(uuid4())
    job    = DownloadJob(job_id)
    _jobs[job_id] = job

    await loop.run_in_executor(
        None, db.queue_insert, job_id, req.url, req.mode, req.quality, req.target_path
    )
    log("✅", f"Job {job_id[:8]} inserted into queue")
    asyncio.create_task(
        _run_job(job, req.url, req.mode, req.quality, req.target_path)
    )
    return {"job_id": job_id}


@app.delete("/api/jobs/{job_id}")
async def cancel_job(job_id: str) -> dict:
    loop = asyncio.get_event_loop()
    log("🛑", f"DELETE /api/jobs/{job_id[:8]} — cancel/dismiss request")
    job = _jobs.get(job_id)
    if job:
        job.cancel_event.set()
        _jobs.pop(job_id, None)
        log("🛑", f"Job {job_id[:8]} cancel signal sent")
    else:
        # Job finished/errored — just nuke the DB row
        await loop.run_in_executor(None, db.queue_delete, job_id)
        log("🗑️", f"Job {job_id[:8]} not in memory — DB row deleted directly")
    return {"cancelled": job_id}


@app.get("/api/queue")
async def get_queue(page: int = 0, limit: int = 10) -> dict:
    loop = asyncio.get_event_loop()
    rows, total = await loop.run_in_executor(None, db.queue_list, page, limit)
    return {"jobs": rows, "total": total, "page": page, "limit": limit}


@app.websocket("/ws/queue")
async def ws_queue(ws: WebSocket) -> None:
    await ws.accept()
    log("🔌", "WS /ws/queue — client connected")
    page           = 0
    limit          = 5
    last_snapshot: dict[str, dict] = {}
    last_total     = -1

    async def _reader() -> None:
        nonlocal page, limit, last_snapshot, last_total
        try:
            while True:
                data = await ws.receive_json()
                if "page" in data:
                    page  = int(data["page"])
                    limit = int(data.get("limit", limit))
                    last_snapshot = {}
                    last_total    = -1
        except Exception:
            pass

    reader_task = asyncio.create_task(_reader())
    loop        = asyncio.get_event_loop()

    try:
        while True:
            rows, total = await loop.run_in_executor(None, db.queue_list, page, limit)
            current     = {r["job_id"]: r for r in rows}

            changed = [r for jid, r in current.items() if last_snapshot.get(jid) != r]
            removed = [jid for jid in last_snapshot if jid not in current]

            if changed or removed or total != last_total:
                await ws.send_json({
                    "type":    "update",
                    "total":   total,
                    "page":    page,
                    "limit":   limit,
                    "jobs":    changed,
                    "removed": removed,
                })
                last_snapshot = current
                last_total    = total

            await asyncio.sleep(0.4)

    except (WebSocketDisconnect, Exception):
        log("🔌", "WS /ws/queue — client disconnected")
    finally:
        reader_task.cancel()


# ── Info ───────────────────────────────────────────────────────────────────────
def _fetch_info(url: str) -> dict:
    log("🔍", f"_fetch_info url={url[:60]!r}")
    opts = {
        "quiet": True, "no_warnings": True,
        "extract_flat": "in_playlist", "ignoreerrors": True,
        "extractor_args": {"youtube": {
            "player_client": ["android", "web"],
        }},
        "http_headers": {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        },
    }
    if _FFMPEG_LOCATION:
        opts["ffmpeg_location"] = _FFMPEG_LOCATION
    if _COOKIES_FILE and Path(_COOKIES_FILE).exists():
        opts["cookiefile"] = _COOKIES_FILE
    with yt_dlp.YoutubeDL(opts) as ydl:
        result = ydl.extract_info(url, download=False) or {}
    log("✅", f"_fetch_info complete — type={result.get('_type','video')!r} title={result.get('title','?')!r}")
    return result


@app.get("/api/info")
async def video_info(url: str) -> dict:
    log("📡", f"GET /api/info url={url[:60]!r}")
    loop = asyncio.get_event_loop()
    try:
        info = await loop.run_in_executor(None, _fetch_info, url)
    except Exception as exc:
        log("❌", f"GET /api/info failed: {exc}")
        raise HTTPException(status_code=400, detail=str(exc))

    url_type = info.get("_type")
    if url_type in ("playlist", "channel"):
        entries = list(info.get("entries") or [])
        return {
            "type":     url_type,
            "title":    info.get("title"),
            "count":    len(entries),
            "uploader": info.get("uploader") or info.get("channel"),
        }
    return {
        "type":       "video",
        "title":      info.get("title"),
        "duration":   info.get("duration"),
        "thumbnail":  info.get("thumbnail"),
        "uploader":   info.get("uploader") or info.get("channel"),
        "view_count": info.get("view_count"),
    }


# ── Media library ──────────────────────────────────────────────────────────────
@app.get("/api/media")
async def list_media(
    mode:         Literal["audio", "video"] = "audio",
    channel_id:   int | None = None,
    min_views:    int | None = None,
    max_views:    int | None = None,
    min_likes:    int | None = None,
    min_duration: int | None = None,
    max_duration: int | None = None,
    tags:         str | None = None,
    sort_by:      str = "download_date",
    order:        str = "desc",
    limit:        int = 60,
    offset:       int = 0,
) -> list[dict]:
    loop     = asyncio.get_event_loop()
    tag_list = [t.strip() for t in tags.split(",")] if tags else None
    items    = await loop.run_in_executor(
        None, db.query_media,
        mode, channel_id, min_views, max_views, min_likes,
        min_duration, max_duration, tag_list,
        sort_by, order, limit, offset,
    )
    base_str = str(AUDIO_DIR.resolve()) if mode == "audio" else str(VIDEO_DIR.resolve())
    prefix   = "/files/audio/" if mode == "audio" else "/files/video/"
    for item in items:
        fp = item.get("file_path") or ""
        if fp.startswith(base_str):
            rel = fp[len(base_str):].lstrip("/\\").replace("\\", "/")
        else:
            rel = item.get("file_name") or ""
        item["download_url"] = prefix + "/".join(p for p in rel.split("/") if p)
    return items


@app.get("/api/channels")
async def list_channels() -> list[dict]:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, db.get_channels)


@app.get("/api/tags")
async def list_tags(mode: Literal["audio", "video"] = "audio") -> list[str]:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, db.get_all_tags, mode)


@app.delete("/api/media/{media_id}")
async def delete_media(media_id: int) -> dict:
    log("🗑️", f"DELETE /api/media/{media_id}")
    loop = asyncio.get_event_loop()

    def _delete():
        record = db.get_media_by_id(media_id)
        if not record:
            return False
        fp = record.get("file_path") or ""
        if fp:
            try:
                Path(fp).unlink(missing_ok=True)
                log("🗑️", f"Deleted file: {fp}")
            except Exception as e:
                log("⚠️", f"Could not delete file {fp}: {e}")
        db.delete_media_record(media_id)
        return True

    success = await loop.run_in_executor(None, _delete)
    if not success:
        raise HTTPException(status_code=404, detail="Not found")
    return {"deleted": media_id}


class MoveRequest(BaseModel):
    new_path: str


@app.post("/api/media/{media_id}/move")
async def move_media(media_id: int, req: MoveRequest) -> dict:
    loop   = asyncio.get_event_loop()
    record = await loop.run_in_executor(None, db.get_media_by_id, media_id)
    if not record:
        raise HTTPException(status_code=404, detail="Not found")
    fp = record.get("file_path") or ""
    if not fp or not Path(fp).exists():
        raise HTTPException(status_code=404, detail="Source file missing on disk")
    mode     = record.get("mode")
    base_dir = AUDIO_DIR if mode == "audio" else VIDEO_DIR
    target_dir = (base_dir / req.new_path.strip("/")).resolve()
    target_dir.mkdir(parents=True, exist_ok=True)
    new_fp = target_dir / Path(fp).name
    if str(Path(fp).resolve()) == str(new_fp.resolve()):
        return {"moved": False, "reason": "Already at target"}
    if new_fp.exists():
        raise HTTPException(status_code=400, detail="File already exists in target folder")

    def _move():
        shutil.move(fp, str(new_fp))
        db.update_media_path(media_id, str(new_fp))
        log("📦", f"Moved {fp} → {new_fp}")

    await loop.run_in_executor(None, _move)
    return {"moved": True, "new_path": str(new_fp)}
