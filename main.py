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
from datetime import datetime
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

import yt_dlp
import db
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from starlette.background import BackgroundTask
import tempfile
import zipfile
from threading import Lock
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

DOWNLOAD_RETRY_DELAYS = [5, 5, 5, 10, 10, 30, 30, 60, 60, 100, 200]


# ── Logging helper ─────────────────────────────────────────────────────────────
def log(emoji: str, msg: str) -> None:
    print(f"{emoji}  {msg}", flush=True)


# ── Environment config ─────────────────────────────────────────────────────────
AUDIO_DIR = Path(os.environ.get("AUDIO_DIR", "downloads/audio"))
VIDEO_DIR = Path(os.environ.get("VIDEO_DIR", "downloads/video"))
ZIPS_DIR  = Path(os.environ.get("ZIP_DIR", "downloads/zips"))
AUDIO_DIR.mkdir(parents=True, exist_ok=True)
VIDEO_DIR.mkdir(parents=True, exist_ok=True)
ZIPS_DIR.mkdir(parents=True, exist_ok=True)

MAX_CONCURRENT   = int(os.environ.get("MAX_CONCURRENT", "3"))
_FFMPEG_LOCATION = shutil.which("ffmpeg")
_COOKIES_FILE    = os.environ.get("COOKIES_FILE", "")  # path to Netscape cookies.txt

log("⚙️", f"AUDIO_DIR={AUDIO_DIR.resolve()}")
log("⚙️", f"VIDEO_DIR={VIDEO_DIR.resolve()}")
log("⚙️", f"ZIPS_DIR={ZIPS_DIR.resolve()}")
log("⚙️", f"MAX_CONCURRENT={MAX_CONCURRENT}")
log("⚙️", f"ffmpeg={'found at ' + _FFMPEG_LOCATION if _FFMPEG_LOCATION else 'NOT FOUND'}")
if _COOKIES_FILE:
    _cp = Path(_COOKIES_FILE)
    if _cp.exists():
        _tmp_cookies = Path(tempfile.gettempdir()) / "ytgrab_cookies.txt"
        shutil.copy(_cp, _tmp_cookies)
        _COOKIES_FILE = str(_tmp_cookies)
        log("🍪", f"COOKIES_FILE={_COOKIES_FILE} (copied) exists=True size={_tmp_cookies.stat().st_size}B")
    else:
        log("🍪", f"COOKIES_FILE={_cp} NOT FOUND")
else:
    log("⚠️", "COOKIES_FILE not set — YouTube may block downloads (bot detection)")

app = FastAPI(title="YTGrab")
app.mount("/files/audio", StaticFiles(directory=str(AUDIO_DIR)), name="audio_files")
app.mount("/files/video", StaticFiles(directory=str(VIDEO_DIR)), name="video_files")

_dl_semaphore: asyncio.Semaphore
_playlist_preview_status: dict[str, dict[str, Any]] = {}
_playlist_preview_lock = Lock()


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

        # --- Transcript Processing ---
        subs_info = info.get("requested_subtitles")
        if subs_info and media_id != -1:
            for lang, sub_data in subs_info.items():
                sub_file = sub_data.get("filepath")
                if sub_file and Path(sub_file).exists():
                    log("📝", f"[{job_short}] Found transcript for {lang}")
                    try:
                        vtt_text = Path(sub_file).read_text(encoding="utf-8")
                        clean_text = self._strip_vtt(vtt_text)
                        db.insert_transcript(media_id, lang, clean_text)
                        log("📝", f"[{job_short}] Saved transcript for {lang} to DB")
                    except Exception as e:
                        log("❌", f"[{job_short}] Error parsing transcript {lang}: {e}")
                    
                    try:
                        Path(sub_file).unlink(missing_ok=True)
                    except:
                        pass

        title   = info.get("title", "")
        track   = info.get("playlist_index") or 1
        n_total = info.get("n_entries") or 1
        self._loop.call_soon_threadsafe(
            self._job.push,
            {"phase": "saving", "title": title, "track": track, "total": n_total},
        )
        return [], info

    def _strip_vtt(self, vtt_text: str) -> str:
        lines = vtt_text.splitlines()
        clean_lines = []
        import re
        for line in lines:
            if "WEBVTT" in line or "-->" in line or "Kind: captions" in line or "Language:" in line:
                continue
            cleaned = line.strip()
            if cleaned or (clean_lines and clean_lines[-1]):
                 cleaned = re.sub(r'<[^>]+>', '', cleaned)
                 clean_lines.append(cleaned)
        
        res = []
        for line in clean_lines:
            if not line and (not res or not res[-1]):
                continue
            res.append(line)
        return "\n".join(res).strip()


# ── yt-dlp opts ────────────────────────────────────────────────────────────────
def _build_opts(mode: str, quality: str, output_dir: Path, subtitles: list[str] = None) -> dict:
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

    if subtitles:
        base["writesubtitles"] = True
        base["writeautomaticsub"] = True
        base["subtitleslangs"] = subtitles
        base["subtitlesformat"] = "vtt/best"

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
                   target_path: str, subtitles: list[str]) -> None:
    global _dl_semaphore
    loop       = asyncio.get_event_loop()
    job_short  = job.job_id[:8]
    base_dir   = AUDIO_DIR if mode == "audio" else VIDEO_DIR
    output_dir = (base_dir / target_path).resolve() if target_path else base_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    opts       = _build_opts(mode, quality, output_dir, subtitles)

    log("📋", f"[{job_short}] Job created — mode={mode} quality={quality} url={url[:60]}")
    log("📁", f"[{job_short}] Output dir: {output_dir}")

    db.queue_update(job.job_id, status="queued")
    job.push({"phase": "fetching", "message": "Waiting for slot…"})

    async with _dl_semaphore:
        log("🚦", f"[{job_short}] Semaphore acquired — starting download")

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

        error = None
        for attempt_index in range(len(DOWNLOAD_RETRY_DELAYS) + 1):
            db.queue_update(job.job_id, status="downloading")
            job.push({"phase": "fetching", "message": "Fetching metadata…"})
            error = await loop.run_in_executor(None, _run)
            log("🏁", f"[{job_short}] run_in_executor returned — error={error!r} attempt={attempt_index + 1}")
            if not error or error == "__cancelled__":
                break
            if attempt_index >= len(DOWNLOAD_RETRY_DELAYS):
                break
            delay = DOWNLOAD_RETRY_DELAYS[attempt_index]
            retry_msg = f"Retrying after {delay}s due to download error"
            log("🔁", f"[{job_short}] {retry_msg}: {error}")
            job.push({"phase": "queued", "message": retry_msg})
            db.queue_update(job.job_id, status="queued", error_msg=retry_msg)
            try:
                await asyncio.wait_for(asyncio.to_thread(job.cancel_event.wait), timeout=delay)
                error = "__cancelled__"
                break
            except asyncio.TimeoutError:
                continue

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
    subtitles:   list[str]                 = []


class PlaylistJobRequest(BaseModel):
    playlist_url:         str
    mode:                 Literal["audio", "video"] = "audio"
    quality:              str                       = "best"
    target_path:          str                       = ""
    excluded_urls:        list[str]                 = []
    global_subtitles:     list[str]                 = []
    individual_subtitles: dict[str, list[str]]      = {}


class InquiryRequest(BaseModel):
    url: str


def extract_youtube_id(url: str) -> str | None:
    match = re.search(r'(?:v=|\/)([0-9A-Za-z_-]{11})(?:[?&]|$)', url)
    return match.group(1) if match else None


def _normalize_subtitles(subtitles: list[str] | None) -> list[str]:
    cleaned: list[str] = []
    seen: set[str] = set()
    for lang in subtitles or []:
        value = str(lang or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        cleaned.append(value)
    return cleaned


def _sort_and_filter_preview_items(
    items: list[dict[str, Any]],
    *,
    sort_by: str,
    order: str,
    min_duration: int | None,
    max_duration: int | None,
    min_views: int | None,
    min_likes: int | None,
    max_likes: int | None,
    subtitle_state: str | None,
    page: int,
    limit: int,
) -> dict:
    def keep(item: dict) -> bool:
        duration = int(item.get("duration") or 0)
        views = int(item.get("view_count") or 0)
        likes = int(item.get("like_count") or 0)
        has_subs = bool(item.get("subtitles"))
        if min_duration is not None and duration < min_duration * 60:
            return False
        if max_duration is not None and duration > max_duration * 60:
            return False
        if min_views is not None and views < min_views:
            return False
        if min_likes is not None and likes < min_likes:
            return False
        if max_likes is not None and likes > max_likes:
            return False
        if subtitle_state == "has_subs" and not has_subs:
            return False
        if subtitle_state == "no_subs" and has_subs:
            return False
        return True

    filtered = [item for item in items if keep(item)]
    reverse = order.lower() == "desc"

    def sort_key(item: dict) -> Any:
        if sort_by == "title":
            return (str(item.get("title") or "").lower(), item.get("playlist_index") or 0)
        if sort_by == "uploader":
            return (str(item.get("uploader") or "").lower(), item.get("playlist_index") or 0)
        if sort_by == "duration":
            return (int(item.get("duration") or 0), item.get("playlist_index") or 0)
        if sort_by == "view_count":
            return (int(item.get("view_count") or 0), item.get("playlist_index") or 0)
        if sort_by == "like_count":
            return (int(item.get("like_count") or 0), item.get("playlist_index") or 0)
        return int(item.get("playlist_index") or 0)

    filtered.sort(key=sort_key, reverse=reverse)
    start = page * limit
    return {
        "items": filtered[start:start + limit],
        "total": len(filtered),
        "page": page,
        "limit": limit,
    }


def _run_inquiry_sync(inquiry_id: str, url: str) -> None:
    try:
        db.inquiry_update(inquiry_id, status="building", phase="checking_video_ids",
                          detail="Checking current playlist or channel video IDs")
        info = _yt_dlp_info(url, extract_flat="in_playlist")
        src_type = str(info.get("_type") or "video")

        if src_type not in ("playlist", "channel"):
            video_info = _yt_dlp_info(url)
            row = _sanitize_preview_entry(video_info, fallback_index=1)
            if not row:
                raise ValueError("Unsupported or unavailable video")
            db.upsert_preview_source(url, "video", video_info.get("title"),
                                     video_info.get("uploader") or video_info.get("channel"), 1)
            db.upsert_preview_item(url, row)
            db.delete_preview_items_not_in(url, [row["url"]])
            db.inquiry_update(
                inquiry_id,
                source_type="video",
                title=row.get("title"),
                uploader=row.get("uploader"),
                total_count=1,
                processed_count=1,
                status="done",
                phase="ready",
                detail="Preview ready",
                error_msg="",
            )
            threading.Timer(2.0, db.inquiry_delete, args=(inquiry_id,)).start()
            return

        entries = [e for e in list(info.get("entries") or []) if _is_supported_playlist_entry(e)]
        db.upsert_preview_source(url, src_type, info.get("title"), info.get("uploader") or info.get("channel"), len(entries))
        cached_items = {item["url"]: item for item in db.get_preview_items(url)}
        current_urls: list[str] = []
        db.inquiry_update(
            inquiry_id,
            source_type=src_type,
            title=info.get("title") or "",
            uploader=info.get("uploader") or info.get("channel") or "",
            total_count=len(entries),
            processed_count=0,
            status="building",
            phase="checking_video_ids",
            detail="Checking current playlist or channel video IDs",
            error_msg="",
        )

        processed = 0
        for idx, entry in enumerate(entries, start=1):
            entry_url = str(entry.get("url") or "").strip()
            if not entry_url:
                continue
            current_urls.append(entry_url)
            cached = cached_items.get(entry_url)
            if cached:
                cached["playlist_index"] = idx
                if _preview_item_is_fresh_today(cached):
                    db.upsert_preview_item(url, cached)
                else:
                    try:
                        item_info = _yt_dlp_info(entry_url)
                    except Exception:
                        item_info = None
                    row = _sanitize_preview_entry(item_info, fallback_index=idx)
                    if row:
                        db.upsert_preview_item(url, row)
                    else:
                        db.upsert_preview_item(url, cached)
            else:
                try:
                    item_info = _yt_dlp_info(entry_url)
                except Exception:
                    item_info = None
                row = _sanitize_preview_entry(item_info, fallback_index=idx)
                if row:
                    db.upsert_preview_item(url, row)
            processed += 1
            db.inquiry_update(
                inquiry_id,
                processed_count=processed,
                total_count=len(entries),
                status="building",
                phase="checking_video_ids",
                detail=f"Checked {processed} of {len(entries)} video IDs",
            )

        db.delete_preview_items_not_in(url, current_urls)
        db.inquiry_update(
            inquiry_id,
            processed_count=len(entries),
            total_count=len(entries),
            status="done",
            phase="ready",
            detail="Preview ready",
            error_msg="",
        )
        threading.Timer(2.0, db.inquiry_delete, args=(inquiry_id,)).start()
    except Exception as exc:
        db.inquiry_update(
            inquiry_id,
            status="error",
            phase="error",
            detail="Inquiry failed",
            error_msg=str(exc),
        )


def _preview_item_is_fresh_today(item: dict[str, Any] | None) -> bool:
    if not item:
        return False
    updated_at = str(item.get("updated_at") or "").strip()
    if not updated_at:
        return False
    try:
        return datetime.fromisoformat(updated_at.replace(" ", "T")).date() == datetime.now().date()
    except ValueError:
        return False


def _is_supported_playlist_entry(entry: dict | None) -> bool:
    if not entry:
        return False
    title = str(entry.get("title") or "").strip().lower()
    if title in {"[private video]", "[deleted video]"}:
        return False
    if "private video" in title or "deleted video" in title:
        return False
    availability = str(entry.get("availability") or "").strip().lower()
    if availability in {"private", "subscriber_only", "needs_auth", "premium_only"}:
        return False
    url = str(entry.get("url") or "").strip()
    if not url:
        return False
    return True


def _yt_dlp_info(url: str, *, extract_flat: str | None = None) -> dict:
    opts: dict[str, Any] = {
        "quiet": True,
        "no_warnings": True,
        "ignoreerrors": True,
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
    if extract_flat:
        opts["extract_flat"] = extract_flat
    if _FFMPEG_LOCATION:
        opts["ffmpeg_location"] = _FFMPEG_LOCATION
    if _COOKIES_FILE and Path(_COOKIES_FILE).exists():
        opts["cookiefile"] = _COOKIES_FILE
    with yt_dlp.YoutubeDL(opts) as ydl:
        return ydl.extract_info(url, download=False) or {}


def _sanitize_preview_entry(info: dict | None, *, fallback_index: int = 0) -> dict | None:
    if not info:
        return None
    title = str(info.get("title") or "").strip()
    if not title or "private video" in title.lower() or "deleted video" in title.lower():
        return None
    availability = str(info.get("availability") or "").strip().lower()
    if availability in {"private", "subscriber_only", "needs_auth", "premium_only"}:
        return None
    url = str(info.get("webpage_url") or info.get("url") or "").strip()
    if not url:
        return None
    subs = info.get("subtitles") or {}
    auto_subs = info.get("automatic_captions") or {}
    sub_langs = sorted(set(list(subs.keys()) + list(auto_subs.keys())))
    return {
        "url": url,
        "title": title,
        "thumbnail": info.get("thumbnail"),
        "uploader": info.get("uploader") or info.get("channel"),
        "duration": info.get("duration"),
        "view_count": info.get("view_count"),
        "like_count": info.get("like_count"),
        "subtitles": sub_langs,
        "playlist_index": int(info.get("playlist_index") or fallback_index),
    }


def _get_playlist_preview_entries(url: str) -> list[dict[str, Any]]:
    return db.get_preview_items(url)


def _ensure_playlist_preview_build(url: str, *, force_refresh: bool = False) -> None:
    with _playlist_preview_lock:
        cached = db.get_preview_items(url)
        status = _playlist_preview_status.get(url)
        if status and status.get("status") == "building":
            return
        if cached and not force_refresh:
            _playlist_preview_status[url] = {
                "status": "ready",
                "processed": len(cached),
                "total": len(cached),
                "phase": "ready",
                "detail": "Preview ready",
                "error": None,
            }
            return
        _playlist_preview_status[url] = {
            "status": "building",
            "processed": 0,
            "total": 0,
            "phase": "checking_video_ids",
            "detail": "Checking current playlist or channel video IDs",
            "error": None,
        }

    def _worker() -> None:
        try:
            flat_info = _yt_dlp_info(url, extract_flat="in_playlist")
            entries = [e for e in list(flat_info.get("entries") or []) if _is_supported_playlist_entry(e)]
            db.upsert_preview_source(
                url,
                str(flat_info.get("_type") or "playlist"),
                flat_info.get("title"),
                flat_info.get("uploader") or flat_info.get("channel"),
                len(entries),
            )
            cached_items = {item["url"]: item for item in db.get_preview_items(url)}
            with _playlist_preview_lock:
                _playlist_preview_status[url] = {
                    "status": "building",
                    "processed": 0,
                    "total": len(entries),
                    "phase": "checking_video_ids",
                    "detail": "Checking current playlist or channel video IDs",
                    "error": None,
                }

            current_urls: list[str] = []
            processed = 0
            for idx, entry in enumerate(entries, start=1):
                entry_url = str(entry.get("url") or "").strip()
                if entry_url:
                    current_urls.append(entry_url)
                    cached = cached_items.get(entry_url)
                    if cached:
                        cached["playlist_index"] = idx
                        db.upsert_preview_item(url, cached)
                    else:
                        try:
                            info = _yt_dlp_info(entry_url)
                        except Exception:
                            info = None
                        row = _sanitize_preview_entry(info, fallback_index=idx)
                        if row:
                            db.upsert_preview_item(url, row)
                processed += 1
                with _playlist_preview_lock:
                    _playlist_preview_status[url] = {
                        "status": "building",
                        "processed": processed,
                        "total": len(entries),
                        "phase": "syncing_cache",
                        "detail": "Checking cache, fetching missing metadata, and storing updates",
                        "error": None,
                    }

            with _playlist_preview_lock:
                _playlist_preview_status[url] = {
                    "status": "building",
                    "processed": len(entries),
                    "total": len(entries),
                    "phase": "finalizing",
                    "detail": "Removing videos no longer present and finalizing results",
                    "error": None,
                }
            db.delete_preview_items_not_in(url, current_urls)
            with _playlist_preview_lock:
                _playlist_preview_status[url] = {
                    "status": "ready",
                    "processed": len(entries),
                    "total": len(entries),
                    "phase": "ready",
                    "detail": "Preview ready",
                    "error": None,
                }
        except Exception as exc:
            with _playlist_preview_lock:
                _playlist_preview_status[url] = {
                    "status": "error",
                    "processed": 0,
                    "total": 0,
                    "phase": "error",
                    "detail": "Preview build failed",
                    "error": str(exc),
                }

    threading.Thread(target=_worker, daemon=True).start()


async def _enqueue_job(
    *,
    loop: asyncio.AbstractEventLoop,
    url: str,
    mode: str,
    quality: str,
    target_path: str,
    subtitles: list[str] | None,
) -> tuple[str | None, dict | None]:
    y_id = extract_youtube_id(url)
    if y_id:
        existing = await loop.run_in_executor(None, db.get_media_by_youtube_id, y_id)
        if existing:
            log("⚠️", f"Duplicate detected: youtube_id={y_id}")
            return None, existing

    job_id = str(uuid4())
    job = DownloadJob(job_id)
    _jobs[job_id] = job

    await loop.run_in_executor(
        None, db.queue_insert, job_id, url, mode, quality, target_path
    )
    log("✅", f"Job {job_id[:8]} inserted into queue")
    asyncio.create_task(
        _run_job(job, url, mode, quality, target_path, _normalize_subtitles(subtitles))
    )
    return job_id, None


@app.post("/api/jobs")
async def create_job(req: JobRequest) -> dict:
    loop = asyncio.get_event_loop()
    log("📥", f"POST /api/jobs — url={req.url[:60]!r} mode={req.mode}")
    job_id, existing = await _enqueue_job(
        loop=loop,
        url=req.url,
        mode=req.mode,
        quality=req.quality,
        target_path=req.target_path,
        subtitles=req.subtitles,
    )
    if existing:
        raise HTTPException(status_code=409, detail={"duplicate": True, "media": existing})
    return {"job_id": job_id}


@app.post("/api/playlist_jobs")
async def create_playlist_jobs(req: PlaylistJobRequest) -> dict:
    loop = asyncio.get_event_loop()
    log("📥", f"POST /api/playlist_jobs — url={req.playlist_url[:60]!r} mode={req.mode}")

    try:
        info = await loop.run_in_executor(None, _fetch_info, req.playlist_url)
    except Exception as exc:
        log("❌", f"POST /api/playlist_jobs failed while fetching playlist: {exc}")
        raise HTTPException(status_code=400, detail=str(exc))

    if info.get("_type") not in ("playlist", "channel"):
        raise HTTPException(status_code=400, detail="URL is not a playlist or channel")

    excluded_urls = {str(url or "").strip() for url in req.excluded_urls if str(url or "").strip()}
    global_subtitles = _normalize_subtitles(req.global_subtitles)
    individual_subtitles = {
        str(url).strip(): _normalize_subtitles(langs)
        for url, langs in req.individual_subtitles.items()
        if str(url).strip()
    }

    queued_job_ids: list[str] = []
    skipped_duplicates = 0
    skipped_excluded = 0

    for entry in info.get("entries") or []:
        if not _is_supported_playlist_entry(entry):
            continue
        entry_url = str((entry or {}).get("url") or "").strip()
        if not entry_url:
            continue
        if entry_url in excluded_urls:
            skipped_excluded += 1
            continue

        subtitles = individual_subtitles.get(entry_url, global_subtitles)
        job_id, existing = await _enqueue_job(
            loop=loop,
            url=entry_url,
            mode=req.mode,
            quality=req.quality,
            target_path=req.target_path,
            subtitles=subtitles,
        )
        if existing:
            skipped_duplicates += 1
            continue
        if job_id:
            queued_job_ids.append(job_id)

    return {
        "queued": len(queued_job_ids),
        "skipped_duplicates": skipped_duplicates,
        "skipped_excluded": skipped_excluded,
        "job_ids": queued_job_ids,
    }


@app.post("/api/inquiries")
async def create_inquiry(req: InquiryRequest) -> dict:
    loop = asyncio.get_event_loop()
    source_url = str(req.url or "").strip()
    existing = await loop.run_in_executor(None, db.inquiry_get_by_source_url, source_url)
    if existing and existing.get("status") in {"queued", "building", "done"}:
        return existing

    inquiry_id = f"iq_{uuid4().hex[:8]}"
    await loop.run_in_executor(None, db.inquiry_insert, inquiry_id, req.url)
    threading.Thread(target=_run_inquiry_sync, args=(inquiry_id, req.url), daemon=True).start()
    row = await loop.run_in_executor(None, db.inquiry_get, inquiry_id)
    return row or {"inquiry_id": inquiry_id, "source_url": req.url}


@app.get("/api/inquiries/{inquiry_id}")
async def get_inquiry(inquiry_id: str) -> dict:
    loop = asyncio.get_event_loop()
    row = await loop.run_in_executor(None, db.inquiry_get, inquiry_id)
    if not row:
        raise HTTPException(status_code=404, detail="Inquiry not found")
    return row


@app.delete("/api/inquiries/{inquiry_id}")
async def delete_inquiry(inquiry_id: str) -> dict:
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, db.inquiry_delete, inquiry_id)
    return {"deleted": inquiry_id}


@app.delete("/api/jobs/{job_id}")
async def cancel_job(job_id: str) -> dict:
    loop = asyncio.get_event_loop()
    log("🛑", f"DELETE /api/jobs/{job_id[:8]} — cancel/dismiss request")
    job = _jobs.get(job_id)
    if job:
        job.cancel_event.set()
        if getattr(job, 'zip_path', None) and os.path.exists(job.zip_path):
            try:
                os.remove(job.zip_path)
                log("🗑️", f"Deleted zip temp file for job {job_id[:8]}")
            except Exception as e:
                log("⚠️", f"Failed to delete zip file: {e}")
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
        except (WebSocketDisconnect, Exception):
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


@app.websocket("/ws/zip_queue")
async def ws_zip_queue(ws: WebSocket) -> None:
    await ws.accept()
    client_ip = ws.client.host if ws.client else "127.0.0.1"
    log("🔌", f"WS /ws/zip_queue — client connected from {client_ip}")
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
            rows, total = await loop.run_in_executor(None, db.zip_list_by_ip, client_ip, page, limit)
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
        log("🔌", "WS /ws/zip_queue — client disconnected")
    finally:
        reader_task.cancel()


@app.websocket("/ws/inquiries")
async def ws_inquiries(ws: WebSocket) -> None:
    await ws.accept()
    log("🔌", "WS /ws/inquiries — client connected")
    page = 0
    limit = 10
    last_snapshot: dict[str, dict] = {}
    last_total = -1

    async def _reader() -> None:
        nonlocal page, limit, last_snapshot, last_total
        try:
            while True:
                data = await ws.receive_json()
                if "page" in data:
                    page = int(data["page"])
                    limit = int(data.get("limit", limit))
                    last_snapshot = {}
                    last_total = -1
        except Exception:
            pass

    reader_task = asyncio.create_task(_reader())
    loop = asyncio.get_event_loop()
    try:
        while True:
            rows, total = await loop.run_in_executor(None, db.inquiry_list, page, limit)
            current = {r["inquiry_id"]: r for r in rows}
            changed = [r for jid, r in current.items() if last_snapshot.get(jid) != r]
            removed = [jid for jid in last_snapshot if jid not in current]
            if changed or removed or total != last_total:
                await ws.send_json({
                    "type": "update",
                    "total": total,
                    "page": page,
                    "limit": limit,
                    "inquiries": changed,
                    "removed": removed,
                })
                last_snapshot = current
                last_total = total
            await asyncio.sleep(0.4)
    except Exception:
        log("🔌", "WS /ws/inquiries — client disconnected")
    finally:
        reader_task.cancel()


# ── Info ───────────────────────────────────────────────────────────────────────
def _fetch_info(url: str) -> dict:
    log("🔍", f"_fetch_info url={url[:60]!r}")
    result = _yt_dlp_info(url, extract_flat="in_playlist")
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
        entries = [e for e in list(info.get("entries") or []) if _is_supported_playlist_entry(e)]
        return {
            "type":     url_type,
            "title":    info.get("title"),
            "count":    len(entries),
            "uploader": info.get("uploader") or info.get("channel"),
            "entries":  [{"url": e.get("url"), "title": e.get("title")} for e in entries if e.get("url")],
        }
    
    # For single videos, let's collect subtitle languages
    subs = info.get("subtitles") or {}
    auto_subs = info.get("automatic_captions") or {}
    sub_langs = list(set(list(subs.keys()) + list(auto_subs.keys())))
    sub_langs.sort()

    return {
        "type":       "video",
        "title":      info.get("title"),
        "duration":   info.get("duration"),
        "thumbnail":  info.get("thumbnail"),
        "uploader":   info.get("uploader") or info.get("channel"),
        "view_count": info.get("view_count"),
        "like_count": info.get("like_count"),
        "subtitles":  sub_langs,
    }


@app.get("/api/inquiries/{inquiry_id}/preview")
async def inquiry_preview(
    inquiry_id: str,
    page: int = 0,
    limit: int = 10,
    sort_by: str = "playlist_index",
    order: str = "asc",
    min_duration: int | None = None,
    max_duration: int | None = None,
    min_views: int | None = None,
    min_likes: int | None = None,
    max_likes: int | None = None,
    subtitle_state: str | None = None,
) -> dict:
    loop = asyncio.get_event_loop()
    req_page = max(0, int(page))
    req_limit = max(1, min(int(limit), 50))

    def _build() -> dict:
        inquiry = db.inquiry_get(inquiry_id)
        if not inquiry:
            return {"missing": True}
        if inquiry.get("status") == "building" or inquiry.get("status") == "queued":
            return {
                "status_only": True,
                "status": {
                    "status": inquiry.get("status"),
                    "processed": inquiry.get("processed_count") or 0,
                    "total": inquiry.get("total_count") or 0,
                    "phase": inquiry.get("phase") or "",
                    "detail": inquiry.get("detail") or "",
                    "error": inquiry.get("error_msg") or "",
                },
            }
        if inquiry.get("status") == "error":
            return {
                "status_only": True,
                "status": {
                    "status": "error",
                    "processed": inquiry.get("processed_count") or 0,
                    "total": inquiry.get("total_count") or 0,
                    "phase": inquiry.get("phase") or "",
                    "detail": inquiry.get("detail") or "",
                    "error": inquiry.get("error_msg") or "",
                },
            }
        cached = db.get_preview_items(inquiry["source_url"])
        if not cached:
            return {
                "status_only": False,
                "items": [],
                "total": 0,
                "page": req_page,
                "limit": req_limit,
            }

        items = list(cached)

        def keep(item: dict) -> bool:
            duration = int(item.get("duration") or 0)
            views = int(item.get("view_count") or 0)
            likes = int(item.get("like_count") or 0)
            has_subs = bool(item.get("subtitles"))
            if min_duration is not None and duration < min_duration * 60:
                return False
            if max_duration is not None and duration > max_duration * 60:
                return False
            if min_views is not None and views < min_views:
                return False
            if min_likes is not None and likes < min_likes:
                return False
            if max_likes is not None and likes > max_likes:
                return False
            if subtitle_state == "has_subs" and not has_subs:
                return False
            if subtitle_state == "no_subs" and has_subs:
                return False
            return True

        result = _sort_and_filter_preview_items(
            items,
            sort_by=sort_by,
            order=order,
            min_duration=min_duration,
            max_duration=max_duration,
            min_views=min_views,
            min_likes=min_likes,
            max_likes=max_likes,
            subtitle_state=subtitle_state,
            page=req_page,
            limit=req_limit,
        )
        return {
            "status_only": False,
            **result,
            "inquiry": inquiry,
        }
    result = await loop.run_in_executor(None, _build)
    if result.get("missing"):
        raise HTTPException(status_code=404, detail="Inquiry not found")
    if result.get("status_only"):
        status = result.get("status") or {}
        code = 202 if status.get("status") == "building" else 500 if status.get("status") == "error" else 202
        return JSONResponse(
            status_code=code,
            content={
                "status": status.get("status", "building"),
                "processed": status.get("processed", 0),
                "total": status.get("total", 0),
                "phase": status.get("phase", "building"),
                "detail": status.get("detail"),
                "error": status.get("error"),
            },
        )
    return result


@app.get("/api/preview")
async def preview_by_url(
    url: str,
    page: int = 0,
    limit: int = 10,
    sort_by: str = "playlist_index",
    order: str = "asc",
    min_duration: int | None = None,
    max_duration: int | None = None,
    min_views: int | None = None,
    min_likes: int | None = None,
    max_likes: int | None = None,
    subtitle_state: str | None = None,
) -> dict:
    loop = asyncio.get_event_loop()
    req_page = max(0, int(page))
    req_limit = max(1, min(int(limit), 50))

    def _build() -> dict:
        cached = db.get_preview_items(url)
        if not cached:
            return {
                "items": [],
                "total": 0,
                "page": req_page,
                "limit": req_limit,
            }
        return _sort_and_filter_preview_items(
            list(cached),
            sort_by=sort_by,
            order=order,
            min_duration=min_duration,
            max_duration=max_duration,
            min_views=min_views,
            min_likes=min_likes,
            max_likes=max_likes,
            subtitle_state=subtitle_state,
            page=req_page,
            limit=req_limit,
        )

    return await loop.run_in_executor(None, _build)


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
    sub_lang:     str | None = None,
    sort_by:      str = "download_date",
    order:        str = "desc",
    limit:        int = 60,
    offset:       int = 0,
) -> dict:
    loop     = asyncio.get_event_loop()
    tag_list = [t.strip() for t in tags.split(",")] if tags else None
    items, total = await loop.run_in_executor(
        None, db.query_media,
        mode, channel_id, min_views, max_views, min_likes,
        min_duration, max_duration, tag_list, sub_lang,
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
    return {"items": items, "total": total}


@app.get("/api/channels")
async def list_channels() -> list[dict]:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, db.get_channels)


@app.get("/api/sub_langs")
async def list_sub_langs(mode: str = "audio") -> list[str]:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, db.get_all_sub_langs, mode)


@app.get("/api/tags")
async def list_tags(mode: Literal["audio", "video"] = "audio") -> list[str]:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, db.get_all_tags, mode)

_active_zip_tasks: dict[str, threading.Event] = {}

class ZipJobRequest(BaseModel):
    mode:         Literal["audio", "video"] = "audio"
    channel_id:   int | None = None
    min_views:    int | None = None
    max_views:    int | None = None
    min_likes:    int | None = None
    min_duration: int | None = None
    max_duration: int | None = None
    tags:         str | None = None
    sub_lang:     str | None = None
    sort_by:      str = "download_date"
    order:        str = "desc"

@app.post("/api/zip_jobs")
async def create_zip_job(req: ZipJobRequest, request: Request) -> dict:
    loop = asyncio.get_event_loop()
    job_id = f"z_{uuid4().hex[:8]}"
    client_ip = request.client.host if request.client else "127.0.0.1"
    
    await loop.run_in_executor(None, db.zip_insert, job_id, client_ip, "queued")
    log("✅", f"Zip Job {job_id} inserted for IP {client_ip}")
    
    cancel_ev = threading.Event()
    _active_zip_tasks[job_id] = cancel_ev
    asyncio.create_task(_zip_worker(job_id, client_ip, req, cancel_ev))
    return {"job_id": job_id}

async def _zip_worker(job_id: str, client_ip: str, req: ZipJobRequest, cancel_ev: threading.Event):
    loop = asyncio.get_event_loop()
    
    try:
        tag_list = [t.strip() for t in req.tags.split(",")] if req.tags else None
        
        items = await loop.run_in_executor(
            None, db.query_media,
            req.mode, req.channel_id, req.min_views, req.max_views, req.min_likes,
            req.min_duration, req.max_duration, tag_list, req.sub_lang,
            req.sort_by, req.order, 10000, 0
        )
        
        if not items:
            await loop.run_in_executor(None, lambda: db.zip_update(job_id, status="error", error_msg="No matching files found to zip"))
            _active_zip_tasks.pop(job_id, None)
            return

        final_zip_path = str(ZIPS_DIR / f"{job_id}.zip")
        await loop.run_in_executor(None, lambda: db.zip_update(job_id, status="zipping", title="Exporting Library...", percent=0, zip_path=final_zip_path))
        
        def create_zip():
            with zipfile.ZipFile(final_zip_path, 'w', zipfile.ZIP_STORED) as zf:
                total = len(items)
                for i, it in enumerate(items):
                    if cancel_ev.is_set():
                        break
                    
                    if i % max(1, total // 50) == 0 or i == total - 1:
                        db.zip_update(job_id, percent=round((i / total) * 100, 1), title=f"Zipped {i}/{total}")
                    
                    fp = it.get("file_path")
                    if fp and os.path.exists(fp):
                        zf.write(fp, arcname=os.path.basename(fp))
                        
        await loop.run_in_executor(None, create_zip)
        
        if cancel_ev.is_set():
            if os.path.exists(final_zip_path):
                os.remove(final_zip_path)
            await loop.run_in_executor(None, db.zip_delete, job_id)
            _active_zip_tasks.pop(job_id, None)
            return
            
        await loop.run_in_executor(None, lambda: db.zip_update(job_id, status="done", percent=100, title=f"Complete ({len(items)} items)"))
        _active_zip_tasks.pop(job_id, None)
        
    except Exception as e:
        log("❌", f"[{job_id}] Zip Worker error: {e}")
        await loop.run_in_executor(None, lambda: db.zip_update(job_id, status="error", error_msg=str(e), percent=0))
        _active_zip_tasks.pop(job_id, None)


@app.get("/api/zip_jobs/{job_id}/download")
async def download_zip_job(job_id: str):
    loop = asyncio.get_event_loop()
    job = await loop.run_in_executor(None, db.zip_get, job_id)
    if not job or not job.get("zip_path") or not os.path.exists(job["zip_path"]):
        raise HTTPException(status_code=404, detail="Zip file not found or expired")
    
    return FileResponse(
        job["zip_path"],
        media_type="application/zip",
        filename=f"ytgrab_export_{job_id[:8]}.zip"
    )

@app.delete("/api/zip_jobs/{job_id}")
async def cancel_zip_job(job_id: str) -> dict:
    loop = asyncio.get_event_loop()
    log("🛑", f"DELETE /api/zip_jobs/{job_id} — cancel/dismiss request")
    
    cancel_ev = _active_zip_tasks.get(job_id)
    if cancel_ev:
        cancel_ev.set()
        log("🛑", f"Zip Job {job_id[:8]} cancel signal sent")
    else:
        # Job finished/errored — just nuke the DB row and file
        job = await loop.run_in_executor(None, db.zip_get, job_id)
        if job and job.get("zip_path") and os.path.exists(job["zip_path"]):
             try:
                 os.remove(job["zip_path"])
             except:
                 pass
        await loop.run_in_executor(None, db.zip_delete, job_id)
        log("🗑️", f"Zip Job {job_id[:8]} not running — DB row/file deleted directly")
    return {"cancelled": job_id}


@app.get("/api/media/{media_id}/transcripts")
async def get_media_transcripts(media_id: int) -> list[dict]:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, db.get_transcripts, media_id)


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
