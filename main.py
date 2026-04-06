"""
main.py – YTGrab backend
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
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
from urllib.parse import parse_qs, quote, urlparse, urlunparse
from urllib.request import Request as UrlRequest, urlopen

INQUIRY_RETRY_DELAY_SECONDS = 60
INQUIRY_RETRY_MAX_TRIALS = 5


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

_AUDIO_EXTS = {".mp3", ".m4a", ".aac", ".wav", ".flac", ".ogg", ".opus"}
_VIDEO_EXTS = {".mp4", ".mkv", ".webm", ".mov", ".avi", ".m4v"}

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
_download_executor: ThreadPoolExecutor | None = None
_default_executor: ThreadPoolExecutor | None = None
_playlist_preview_status: dict[str, dict[str, Any]] = {}
_playlist_preview_lock = Lock()


# ── Job registry ───────────────────────────────────────────────────────────────
class DownloadJob:
    def __init__(
        self,
        job_id: str,
        *,
        source_url: str = "",
        mode: str = "audio",
        quality: str = "best",
        subtitles: list[str] | None = None,
        inquiry_id: str | None = None,
        retry_attempt: int = 1,
    ) -> None:
        self.job_id        = job_id
        self.messages:  list[dict]          = []
        self.done                           = False
        self._waiters:  list[asyncio.Queue] = []
        self.cancel_event                   = threading.Event()
        self._current_file: str             = ""
        self._last_db_write: float          = 0.0
        self.source_url = source_url
        self.mode = mode
        self.quality = quality
        self.subtitles = subtitles or []
        self.inquiry_id = inquiry_id
        self.retry_attempt = retry_attempt

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
_inquiry_retry_lock = Lock()
_inquiry_retry_batches: dict[str, dict[str, dict[str, Any]]] = {}
_inquiry_retry_task: asyncio.Task | None = None


def _mode_for_extension(path: Path, fallback: str = "audio") -> str:
    suffix = path.suffix.lower()
    if suffix in _VIDEO_EXTS:
        return "video"
    if suffix in _AUDIO_EXTS:
        return "audio"
    return fallback


def _base_dir_for_mode(mode: str) -> Path:
    return AUDIO_DIR if mode == "audio" else VIDEO_DIR


def _as_relative_path(path: Path, base_dir: Path) -> str:
    rel = path.resolve().relative_to(base_dir.resolve())
    return str(rel).replace("\\", "/")


def _resolve_media_abspath(record: dict) -> Path:
    raw = str(record.get("file_path") or "").strip()
    mode = str(record.get("mode") or "audio")
    base = _base_dir_for_mode(mode)
    if raw:
        p = Path(raw)
        if p.is_absolute():
            return p
        candidate = (base / raw).resolve()
        if candidate.exists():
            return candidate
    name = str(record.get("file_name") or "").strip()
    if name:
        for root in (AUDIO_DIR, VIDEO_DIR):
            for hit in root.rglob(name):
                if hit.is_file():
                    return hit.resolve()
    if raw:
        return (base / raw).resolve()
    return (base / name).resolve()


def _find_variant_abspath(record: dict, desired_mode: str) -> Path | None:
    primary = _resolve_media_abspath(record)
    if primary.exists() and _mode_for_extension(primary, "") == desired_mode:
        return primary

    desired_root = _base_dir_for_mode(desired_mode)
    rel_raw = str(record.get("file_path") or "").replace("\\", "/").strip()
    file_name = str(record.get("file_name") or "").strip()
    stem = Path(file_name or primary.name).stem
    exts = _AUDIO_EXTS if desired_mode == "audio" else _VIDEO_EXTS

    candidates: list[Path] = []
    if rel_raw and not Path(rel_raw).is_absolute():
        rel_dir = Path(rel_raw).parent
        probe_dir = (desired_root / rel_dir).resolve()
        if probe_dir.exists():
            candidates.extend([p.resolve() for p in probe_dir.iterdir() if p.is_file() and p.stem == stem and p.suffix.lower() in exts])

    if primary.parent.exists():
        candidates.extend([p.resolve() for p in primary.parent.iterdir() if p.is_file() and p.stem == stem and p.suffix.lower() in exts])

    if file_name:
        exact = (desired_root / file_name).resolve()
        if exact.exists() and exact.is_file() and exact.suffix.lower() in exts:
            candidates.append(exact)

    if desired_root.exists():
        candidates.extend([p.resolve() for p in desired_root.rglob(f"{stem}.*") if p.is_file() and p.suffix.lower() in exts])

    seen: set[str] = set()
    ordered: list[Path] = []
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        ordered.append(candidate)
    return ordered[0] if ordered else None


def _record_has_media_on_disk(record: dict | None) -> bool:
    if not record:
        return False
    for desired_mode in ("audio", "video"):
        fp = _find_variant_abspath(record, desired_mode)
        if fp and fp.exists():
            return True
    primary = _resolve_media_abspath(record)
    return primary.exists()


def _canonical_job_key(url: str) -> str:
    return _canonical_video_url(url) or str(url or "").strip()


def _find_pending_duplicate(url: str, *, exclude_job_id: str | None = None) -> dict | None:
    key = _canonical_job_key(url)
    if not key:
        return None

    for job in list(_jobs.values()):
        if _canonical_job_key(job.source_url) == key:
            return {
                "duplicate": True,
                "source": "memory",
                "job_id": job.job_id,
                "url": job.source_url,
                "title": "",
                "status": "queued",
            }

    for row in db.queue_all():
        status = str(row.get("status") or "").strip().lower()
        if status in {"done", "cancelled"}:
            continue
        if exclude_job_id and str(row.get("job_id") or "") == exclude_job_id:
            continue
        if _canonical_job_key(str(row.get("url") or "")) == key:
            row["duplicate"] = True
            row["source"] = "queue"
            return row

    with _inquiry_retry_lock:
        for batch in _inquiry_retry_batches.values():
            item = batch.get(key)
            if item:
                return {
                    "duplicate": True,
                    "source": "retry",
                    "job_id": item.get("job_id") or "",
                    "url": item.get("source_url") or key,
                    "title": "",
                    "status": "retry_wait",
                }
    return None


def _cleanup_duplicate_download_outputs(existing: dict, primary_path: Path, downloads: list[dict]) -> None:
    keep: set[str] = set()
    for desired_mode in ("audio", "video"):
        fp = _find_variant_abspath(existing, desired_mode)
        if fp and fp.exists():
            keep.add(str(fp.resolve()))

    cleanup_paths: set[str] = set()
    if primary_path and primary_path.exists():
        cleanup_paths.add(str(primary_path.resolve()))
    for dl in downloads or []:
        for raw in (dl.get("filepath"), dl.get("filename")):
            if not raw:
                continue
            p = Path(raw).resolve()
            if p.exists():
                cleanup_paths.add(str(p))

    for raw in cleanup_paths:
        if raw in keep:
            continue
        try:
            Path(raw).unlink(missing_ok=True)
            log("🧹", f"Removed duplicate output: {raw}")
        except Exception as exc:
            log("⚠️", f"Could not remove duplicate output {raw}: {exc}")


def _ensure_file_in_mode_folder(path: Path, mode: str, target_subdir: str = "") -> Path:
    base = _base_dir_for_mode(mode).resolve()
    target_dir = (base / target_subdir.strip("/")).resolve() if target_subdir else base
    target_dir.mkdir(parents=True, exist_ok=True)
    src = path.resolve()
    if str(src).startswith(str(base) + os.sep):
        return src
    dst = (target_dir / src.name).resolve()
    if src == dst:
        return src
    if dst.exists():
        stem, suf = dst.stem, dst.suffix
        i = 1
        while True:
            candidate = dst.with_name(f"{stem}_{i}{suf}")
            if not candidate.exists():
                dst = candidate
                break
            i += 1
    shutil.move(str(src), str(dst))
    return dst


def _move_matching_variants(path: Path) -> int:
    if not path.exists() or not path.parent.exists():
        return 0
    moved = 0
    stem = path.stem
    for sibling in list(path.parent.iterdir()):
        if not sibling.is_file() or sibling == path or sibling.stem != stem:
            continue
        sibling_mode = _mode_for_extension(sibling, "")
        if sibling_mode not in {"audio", "video"}:
            continue
        target = _ensure_file_in_mode_folder(sibling, sibling_mode)
        if target != sibling:
            moved += 1
    return moved


def _migrate_media_storage_layout() -> None:
    moved = 0
    updated = 0
    records = db.list_all_media_records()
    for rec in records:
        media_id = int(rec.get("id"))
        current_abs = _resolve_media_abspath(rec)
        mode = _mode_for_extension(current_abs, fallback=str(rec.get("mode") or "audio"))
        if current_abs.exists():
            rel_hint = str(rec.get("file_path") or "").replace("\\", "/").strip()
            subdir = Path(rel_hint).parent.as_posix() if rel_hint and not Path(rel_hint).is_absolute() else ""
            target_abs = _ensure_file_in_mode_folder(current_abs, mode, subdir)
            if target_abs != current_abs:
                moved += 1
            rel = _as_relative_path(target_abs, _base_dir_for_mode(mode))
            size = target_abs.stat().st_size if target_abs.exists() else int(rec.get("file_size") or 0)
            db.update_media_file_record(
                media_id,
                mode=mode,
                file_path=rel,
                file_name=target_abs.name,
                file_size=size,
            )
            moved += _move_matching_variants(target_abs)
            updated += 1

    for root, dest_mode in ((AUDIO_DIR, "video"), (VIDEO_DIR, "audio")):
        for fp in root.rglob("*"):
            if not fp.is_file():
                continue
            target_mode = _mode_for_extension(fp, fallback=dest_mode)
            if (root == AUDIO_DIR and target_mode == "video") or (root == VIDEO_DIR and target_mode == "audio"):
                _ensure_file_in_mode_folder(fp, target_mode)
                moved += 1

    log("🧹", f"Storage migration complete — updated={updated} moved={moved}")


def _build_download_url(path: Path, mode: str) -> str:
    prefix = "/files/audio/" if mode == "audio" else "/files/video/"
    rel = _as_relative_path(path, _base_dir_for_mode(mode))
    return prefix + "/".join(p for p in rel.split("/") if p)


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

        # Normalize final file placement by extension and keep DB path relative.
        final_obj = Path(final_path).resolve() if final_path else Path()
        stored_mode = _mode_for_extension(final_obj, fallback=self._mode)
        target_subdir = ""
        if final_obj and final_obj.exists():
            final_obj = _ensure_file_in_mode_folder(final_obj, stored_mode, target_subdir)
            final_path = str(final_obj)
            _move_matching_variants(final_obj)

        # If yt-dlp kept a merged mp4 in the audio folder, move it under VIDEO_DIR.
        for dl in downloads:
            side_path = str(dl.get("filepath") or dl.get("filename") or "").strip()
            if not side_path:
                continue
            p = Path(side_path).resolve()
            if not p.exists() or p == final_obj:
                continue
            side_mode = _mode_for_extension(p, fallback=self._mode)
            if side_mode == "video":
                _ensure_file_in_mode_folder(p, "video", target_subdir)
            elif side_mode == "audio":
                _ensure_file_in_mode_folder(p, "audio", target_subdir)

        youtube_id = str(info.get("id") or "").strip()
        if youtube_id:
            existing = db.get_media_by_youtube_id(youtube_id)
            if existing and _record_has_media_on_disk(existing):
                existing_primary = _find_variant_abspath(existing, stored_mode) or _resolve_media_abspath(existing)
                if existing_primary.exists() and final_obj and final_obj.exists():
                    if str(existing_primary.resolve()) != str(final_obj.resolve()):
                        log("⚠️", f"[{job_short}] Duplicate completed download detected for youtube_id={youtube_id}; keeping existing media")
                        _cleanup_duplicate_download_outputs(existing, final_obj, downloads)
                        return [], info

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

        rel_path = _as_relative_path(Path(final_path), _base_dir_for_mode(stored_mode))
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
            "file_path":     rel_path,
            "file_name":     Path(final_path).name,
            "file_size":     final_size,
            "mode":          stored_mode,
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
            "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio/best[ext=mp4]/best",
            "merge_output_format": "mp4",
            "keepvideo": True,
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


def _queue_retry_item(job: DownloadJob, error_msg: str) -> None:
    if not job.inquiry_id:
        return
    next_attempt = job.retry_attempt + 1
    if next_attempt > INQUIRY_RETRY_MAX_TRIALS:
        return
    key = _canonical_video_url(job.source_url) or job.source_url
    if not key:
        return
    with _inquiry_retry_lock:
        batch = _inquiry_retry_batches.setdefault(job.inquiry_id, {})
        batch[key] = {
            "job_id": job.job_id,
            "source_url": key,
            "mode": job.mode,
            "quality": job.quality,
            "subtitles": list(job.subtitles or []),
            "inquiry_id": job.inquiry_id,
            "retry_attempt": next_attempt,
            "next_retry_at": time.time() + INQUIRY_RETRY_DELAY_SECONDS,
            "last_error": error_msg,
        }


async def _process_inquiry_retries() -> None:
    while True:
        try:
            await asyncio.sleep(2.0)
            if _jobs:
                continue
            loop = asyncio.get_event_loop()
            active_count = await loop.run_in_executor(None, db.queue_active_count)
            if active_count > 0:
                continue

            now = time.time()
            due_items: list[dict[str, Any]] = []
            with _inquiry_retry_lock:
                empty_inquiries: list[str] = []
                for inquiry_id, batch in _inquiry_retry_batches.items():
                    for key, item in list(batch.items()):
                        if float(item.get("next_retry_at") or 0) <= now:
                            due_items.append(item)
                            batch.pop(key, None)
                    if not batch:
                        empty_inquiries.append(inquiry_id)
                for inquiry_id in empty_inquiries:
                    _inquiry_retry_batches.pop(inquiry_id, None)

            for item in due_items:
                try:
                    job_id, existing = await _enqueue_job(
                        loop=loop,
                        url=item["source_url"],
                        mode=item.get("mode") or "audio",
                        quality=item.get("quality") or "best",
                        subtitles=item.get("subtitles") or [],
                        inquiry_id=item.get("inquiry_id"),
                        retry_attempt=int(item.get("retry_attempt") or 1),
                        existing_job_id=str(item.get("job_id") or "").strip() or None,
                    )
                    if existing and item.get("job_id"):
                        await loop.run_in_executor(None, db.queue_delete, item["job_id"])
                except Exception as exc:
                    log("❌", f"Retry enqueue failed for inquiry {item.get('inquiry_id')}: {exc}")
        except asyncio.CancelledError:
            break
        except Exception as exc:
            log("❌", f"Inquiry retry loop error: {exc}")


# ── Download worker ────────────────────────────────────────────────────────────
async def _run_job(job: DownloadJob, url: str, mode: str, quality: str,
                   subtitles: list[str]) -> None:
    global _dl_semaphore, _download_executor
    loop       = asyncio.get_event_loop()
    job_short  = job.job_id[:8]
    base_dir   = AUDIO_DIR if mode == "audio" else VIDEO_DIR
    output_dir = base_dir.resolve()
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

        db.queue_update(job.job_id, status="downloading")
        job.push({"phase": "fetching", "message": "Fetching metadata…"})
        error = await loop.run_in_executor(_download_executor, _run)
        log("🏁", f"[{job_short}] run_in_executor returned — error={error!r} attempt={job.retry_attempt}")

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
        await loop.run_in_executor(None, db.queue_delete, job.job_id)
        _jobs.pop(job.job_id, None)
        log("🚫", f"[{job_short}] Cancel cleanup done")

    elif error:
        log("❌", f"[{job_short}] Job failed: {error}")
        if job.inquiry_id and job.retry_attempt < INQUIRY_RETRY_MAX_TRIALS:
            retry_msg = (
                f"Retry queued for inquiry after {INQUIRY_RETRY_DELAY_SECONDS}s "
                f"when queue is empty ({job.retry_attempt}/{INQUIRY_RETRY_MAX_TRIALS})"
            )
            log("🔁", f"[{job_short}] {retry_msg}")
            _queue_retry_item(job, error)
            job.push({"phase": "error", "message": retry_msg})
            db.queue_update(job.job_id, status="retry_wait", error_msg=retry_msg)
            _jobs.pop(job.job_id, None)
        else:
            job.push({"phase": "error", "message": error})
            db.queue_update(job.job_id, status="error", error_msg=error)
            _jobs.pop(job.job_id, None)

    else:
        log("🎉", f"[{job_short}] Job completed successfully!")
        job.push({"phase": "done"})
        db.queue_update(job.job_id, status="done")
        await loop.run_in_executor(None, db.queue_delete, job.job_id)
        _jobs.pop(job.job_id, None)
        log("✨", f"[{job_short}] Queue row cleaned up")


# ── App lifecycle ──────────────────────────────────────────────────────────────
@app.on_event("startup")
async def _startup() -> None:
    global _dl_semaphore, _download_executor, _default_executor, _inquiry_retry_task
    _dl_semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    loop = asyncio.get_event_loop()
    # Keep request/DB operations responsive even when downloads are active.
    default_workers = max(16, MAX_CONCURRENT * 4)
    _default_executor = ThreadPoolExecutor(max_workers=default_workers, thread_name_prefix="app-default")
    loop.set_default_executor(_default_executor)
    # Isolate long yt-dlp operations from API/DB thread pool.
    _download_executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENT, thread_name_prefix="download")
    log("🚀", "App starting up — initialising DB")
    db.init_db()
    log("🗄️", "DB initialised")
    _migrate_media_storage_layout()
    # Mark orphaned active rows as error (server was killed mid-download)
    rows, _ = db.queue_list(0, 9999)
    for row in rows:
        if row.get("status") in ("downloading", "converting", "saving", "queued", "retry_wait"):
            log("⚠️", f"Orphaned queue row {row['job_id'][:8]} status={row['status']} → error")
            db.queue_update(row["job_id"], status="error",
                            error_msg="Server restarted — download was interrupted")
    _inquiry_retry_task = asyncio.create_task(_process_inquiry_retries())
    log("✅", "Startup complete")


@app.on_event("shutdown")
async def _shutdown() -> None:
    global _download_executor, _default_executor, _inquiry_retry_task
    if _inquiry_retry_task:
        _inquiry_retry_task.cancel()
        _inquiry_retry_task = None
    if _download_executor:
        _download_executor.shutdown(wait=False, cancel_futures=False)
        _download_executor = None
    if _default_executor:
        _default_executor.shutdown(wait=False, cancel_futures=False)
        _default_executor = None


# ── Static / HTML ──────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    return HTMLResponse(content=Path("static/index.html").read_text())


# ── Jobs + Queue ───────────────────────────────────────────────────────────────
class JobRequest(BaseModel):
    url:         str
    mode:        Literal["audio", "video"] = "audio"
    quality:     str                       = "best"
    subtitles:   list[str]                 = []
    inquiry_id:  str | None                = None


class PlaylistJobRequest(BaseModel):
    playlist_url:         str
    mode:                 Literal["audio", "video"] = "audio"
    quality:              str                       = "best"
    excluded_urls:        list[str]                 = []
    global_subtitles:     list[str]                 = []
    individual_subtitles: dict[str, list[str]]      = {}
    inquiry_id:           str | None                = None


class InquiryRequest(BaseModel):
    url: str


def extract_youtube_id(url: str) -> str | None:
    match = re.search(r'(?:v=|\/)([0-9A-Za-z_-]{11})(?:[?&]|$)', url)
    return match.group(1) if match else None


def _canonical_video_url(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    if not raw.startswith(("http://", "https://")):
        raw = f"https://{raw}"
    video_id = extract_youtube_id(raw)
    if video_id:
        return f"https://www.youtube.com/watch?v={video_id}"
    try:
        parsed = urlparse(raw)
    except Exception:
        return raw
    if not parsed.scheme or not parsed.netloc:
        return raw
    path = parsed.path.rstrip("/") or parsed.path or "/"
    cleaned = parsed._replace(path=path, params="", query="", fragment="")
    return urlunparse(cleaned)


def _canonical_source_url(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    if not raw.startswith(("http://", "https://")):
        raw = f"https://{raw}"
    try:
        parsed = urlparse(raw)
    except Exception:
        return raw
    host = (parsed.netloc or "").lower()
    host_base = host.removeprefix("www.").removeprefix("m.")
    path = (parsed.path or "/").rstrip("/") or "/"
    query = parse_qs(parsed.query or "")
    list_id = str((query.get("list") or [""])[0] or "").strip()
    video_id = extract_youtube_id(raw)
    if host_base in {"youtube.com", "youtu.be"} and video_id:
        return f"https://www.youtube.com/watch?v={video_id}"
    if host_base in {"youtube.com", "youtu.be"} and list_id:
        if path in {"/playlist", "/watch"} or path.startswith("/playlist"):
            return f"https://www.youtube.com/playlist?list={list_id}"
        return f"https://www.youtube.com/playlist?list={list_id}"
    if host_base == "youtube.com":
        cleaned = parsed._replace(netloc="www.youtube.com", path=path, params="", query="", fragment="")
        return urlunparse(cleaned)
    cleaned = parsed._replace(path=path, params="", fragment="")
    return urlunparse(cleaned)


def _playlist_id_from_url(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    try:
        parsed = urlparse(raw if raw.startswith(("http://", "https://")) else f"https://{raw}")
    except Exception:
        return ""
    return str((parse_qs(parsed.query or {}).get("list") or [""])[0] or "").strip()


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
        url = _canonical_source_url(url)
        db.inquiry_update(inquiry_id, status="building", phase="checking_video_ids",
                          detail="Checking current playlist or channel video IDs")
        cached_source = db.get_preview_source(url)
        cached_items = db.get_preview_items(url)
        if not cached_source or not cached_items:
            playlist_id = _playlist_id_from_url(url)
            if playlist_id:
                alias = db.find_preview_source_by_playlist_id(playlist_id, exclude_source_url=url)
                if alias:
                    alias_items = db.get_preview_items(alias["source_url"])
                    if alias_items and _is_updated_within_24h(alias.get("updated_at")):
                        db.upsert_preview_source(
                            url,
                            alias.get("source_type") or "playlist",
                            alias.get("title"),
                            alias.get("uploader"),
                            int(alias.get("total_count") or len(alias_items)),
                        )
                        for item in alias_items:
                            alias_url = _canonical_video_url(item.get("url") or "")
                            if not alias_url:
                                continue
                            item["url"] = alias_url
                            db.upsert_preview_item(url, item)
                        cached_source = db.get_preview_source(url)
                        cached_items = db.get_preview_items(url)
        cached_total = len(cached_items)
        source_total = int(cached_source.get("total_count") or 0) if cached_source else 0
        cache_consistent = (source_total <= 0) or (cached_total >= source_total)
        if cached_source and cached_items and cache_consistent and _is_updated_within_24h(cached_source.get("updated_at")):
            src_type = str(cached_source.get("source_type") or ("video" if len(cached_items) == 1 else "playlist"))
            total_count = int(cached_source.get("total_count") or len(cached_items))
            first = cached_items[0]
            db.inquiry_update(
                inquiry_id,
                source_type=src_type,
                title=cached_source.get("title") or first.get("title") or url,
                uploader=cached_source.get("uploader") or first.get("uploader") or "",
                total_count=total_count,
                processed_count=total_count,
                status="done",
                phase="ready",
                detail="Preview loaded from cache",
                error_msg="",
            )
            return

        try:
            info = _yt_dlp_info(url, extract_flat="in_playlist")
        except Exception:
            info = _yt_dlp_info(url)
        src_type = str(info.get("_type") or "video")

        if src_type not in ("playlist", "channel"):
            video_info = info
            row = _sanitize_preview_entry(video_info, fallback_index=1)
            if not row:
                try:
                    video_info = _yt_dlp_info(url)
                except Exception:
                    video_info = info
                row = _sanitize_preview_entry(video_info, fallback_index=1)
            if not row:
                row = _coerce_video_preview_entry(video_info, source_url=url)
            canonical_url = ""
            video_id = extract_youtube_id(url)
            if video_id:
                canonical_url = f"https://www.youtube.com/watch?v={video_id}"
            if not row and canonical_url and canonical_url != url:
                try:
                    canonical_info = _yt_dlp_info(canonical_url)
                except Exception:
                    canonical_info = None
                row = _sanitize_preview_entry(canonical_info, fallback_index=1)
                if not row:
                    row = _coerce_video_preview_entry(canonical_info, source_url=url)
            if not row:
                row = _oembed_video_preview_entry(url, source_url=url)
            if not row and canonical_url and canonical_url != url:
                row = _oembed_video_preview_entry(canonical_url, source_url=url)
            if not row:
                raise ValueError("Unsupported or unavailable video")
            db.upsert_preview_source(url, "video", row.get("title"),
                                     row.get("uploader"), 1)
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
            return

        entries = [e for e in list(info.get("entries") or []) if _is_supported_playlist_entry(e)]
        db.upsert_preview_source(url, src_type, info.get("title"), info.get("uploader") or info.get("channel"), len(entries))
        cached_items: dict[str, dict[str, Any]] = {}
        for item in db.get_preview_items(url):
            item_key = _canonical_video_url(item.get("url") or "")
            if not item_key:
                continue
            item["url"] = item_key
            cached_items[item_key] = item
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
            raw_entry_url = str(entry.get("url") or "").strip()
            entry_url = _canonical_video_url(raw_entry_url)
            if not entry_url:
                continue
            current_urls.append(entry_url)
            cached = cached_items.get(entry_url)
            if cached:
                cached["url"] = entry_url
                cached["playlist_index"] = idx
                if _preview_item_is_fresh_24h(cached):
                    db.upsert_preview_item(url, cached)
                else:
                    try:
                        item_info = _yt_dlp_info(raw_entry_url or entry_url)
                    except Exception:
                        item_info = None
                    row = _sanitize_preview_entry(item_info, fallback_index=idx)
                    if row:
                        db.upsert_preview_item(url, row)
                    else:
                        fallback_row = _fallback_preview_row_from_entry(entry, fallback_index=idx)
                        if fallback_row:
                            db.upsert_preview_item(url, fallback_row)
                        else:
                            db.upsert_preview_item(url, cached)
            else:
                try:
                    item_info = _yt_dlp_info(raw_entry_url or entry_url)
                except Exception:
                    item_info = None
                row = _sanitize_preview_entry(item_info, fallback_index=idx)
                if row:
                    db.upsert_preview_item(url, row)
                else:
                    fallback_row = _fallback_preview_row_from_entry(entry, fallback_index=idx)
                    if fallback_row:
                        db.upsert_preview_item(url, fallback_row)
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
    except Exception as exc:
        db.inquiry_update(
            inquiry_id,
            status="error",
            phase="error",
            detail=str(exc)[:220] or "Inquiry failed",
            error_msg=str(exc),
        )


def _preview_item_is_fresh_24h(item: dict[str, Any] | None) -> bool:
    if not item:
        return False
    updated_at = str(item.get("updated_at") or "").strip()
    if not updated_at:
        return False
    try:
        updated_dt = datetime.fromisoformat(updated_at.replace(" ", "T"))
        return (datetime.now() - updated_dt).total_seconds() <= 24 * 3600
    except ValueError:
        return False


def _is_updated_within_24h(updated_at: Any) -> bool:
    ts = str(updated_at or "").strip()
    if not ts:
        return False
    try:
        updated_dt = datetime.fromisoformat(ts.replace(" ", "T"))
        return (datetime.now() - updated_dt).total_seconds() <= 24 * 3600
    except ValueError:
        return False


def _coerce_video_preview_entry(info: dict | None, *, source_url: str) -> dict | None:
    if not info:
        return None
    title = str(info.get("title") or "").strip() or "Video"
    page_url = _canonical_video_url(str(info.get("webpage_url") or info.get("original_url") or source_url or "").strip())
    if not page_url:
        return None
    subs = info.get("subtitles") or {}
    auto_subs = info.get("automatic_captions") or {}
    sub_langs = sorted(set(list(subs.keys()) + list(auto_subs.keys())))
    return {
        "url": page_url,
        "title": title,
        "thumbnail": info.get("thumbnail"),
        "uploader": info.get("uploader") or info.get("channel"),
        "duration": info.get("duration"),
        "view_count": info.get("view_count"),
        "like_count": info.get("like_count"),
        "subtitles": sub_langs,
        "playlist_index": 1,
    }


def _oembed_video_preview_entry(video_url: str, *, source_url: str) -> dict | None:
    clean_url = _canonical_video_url(video_url)
    if not clean_url:
        return None
    endpoint = f"https://www.youtube.com/oembed?format=json&url={quote(clean_url, safe='')}"
    req = UrlRequest(endpoint, headers={
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
    })
    try:
        with urlopen(req, timeout=8) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None
    title = str(payload.get("title") or "").strip()
    if not title:
        return None
    author = str(payload.get("author_name") or "").strip()
    return {
        "url": clean_url or _canonical_video_url(source_url),
        "title": title,
        "thumbnail": payload.get("thumbnail_url"),
        "uploader": author,
        "duration": None,
        "view_count": None,
        "like_count": None,
        "subtitles": [],
        "playlist_index": 1,
    }


def _is_rate_limited_exception(exc: Exception, message: str) -> bool:
    # Prefer concrete HTTP/status codes from the exception chain.
    for obj in (exc, getattr(exc, "cause", None), getattr(exc, "exc", None), getattr(exc, "response", None)):
        if obj is None:
            continue
        for attr in ("status", "status_code", "code"):
            code = getattr(obj, attr, None)
            if isinstance(code, int) and code == 429:
                return True
    exc_info = getattr(exc, "exc_info", None)
    if isinstance(exc_info, tuple) and len(exc_info) >= 2 and exc_info[1] is not None:
        inner = exc_info[1]
        for attr in ("status", "status_code", "code"):
            code = getattr(inner, attr, None)
            if isinstance(code, int) and code == 429:
                return True

    # Fallback for yt-dlp paths that only provide text.
    msg = (message or "").lower()
    return (
        "rate-limited by youtube" in msg
        or "too many requests" in msg
        or "http error 429" in msg
    )


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
    url = _canonical_video_url(str(info.get("webpage_url") or info.get("url") or "").strip())
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


def _fallback_preview_row_from_entry(entry: dict | None, *, fallback_index: int) -> dict | None:
    if not entry:
        return None
    title = str(entry.get("title") or "").strip()
    if not title:
        return None
    title_l = title.lower()
    if "private video" in title_l or "deleted video" in title_l:
        return None
    raw_url = str(entry.get("webpage_url") or entry.get("url") or "").strip()
    url = _canonical_video_url(raw_url)
    if not url:
        return None
    return {
        "url": url,
        "title": title,
        "thumbnail": entry.get("thumbnail"),
        "uploader": entry.get("uploader") or entry.get("channel"),
        "duration": entry.get("duration"),
        "view_count": entry.get("view_count"),
        "like_count": entry.get("like_count"),
        "subtitles": [],
        "playlist_index": int(entry.get("playlist_index") or fallback_index),
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
    subtitles: list[str] | None,
    inquiry_id: str | None = None,
    retry_attempt: int = 1,
    existing_job_id: str | None = None,
) -> tuple[str | None, dict | None]:
    y_id = extract_youtube_id(url)
    if y_id:
        existing = await loop.run_in_executor(None, db.get_media_by_youtube_id, y_id)
        if _record_has_media_on_disk(existing):
            log("⚠️", f"Duplicate detected: youtube_id={y_id}")
            return None, existing

    pending = await loop.run_in_executor(None, lambda: _find_pending_duplicate(url, exclude_job_id=existing_job_id))
    if pending:
        log("⚠️", f"Duplicate detected in pending queue: {url[:80]}")
        return None, pending

    job_id = existing_job_id or str(uuid4())
    normalized_mode = "audio"
    normalized_subs = _normalize_subtitles(subtitles)
    job = DownloadJob(
        job_id,
        source_url=url,
        mode=normalized_mode,
        quality=quality,
        subtitles=normalized_subs,
        inquiry_id=inquiry_id,
        retry_attempt=retry_attempt,
    )
    _jobs[job_id] = job

    if existing_job_id:
        await loop.run_in_executor(
            None, db.queue_update, job_id,
            status="queued",
            downloaded_bytes=0,
            total_bytes=0,
            speed_bps=0,
            eta_seconds=0,
            error_msg="",
        )
    else:
        await loop.run_in_executor(
            None, db.queue_insert, job_id, url, normalized_mode, quality, ""
        )
    log("✅", f"Job {job_id[:8]} inserted into queue")
    asyncio.create_task(
        _run_job(job, url, normalized_mode, quality, normalized_subs)
    )
    return job_id, None


@app.post("/api/jobs")
async def create_job(req: JobRequest) -> dict:
    loop = asyncio.get_event_loop()
    log("📥", f"POST /api/jobs — url={req.url[:60]!r} mode=audio")
    job_id, existing = await _enqueue_job(
        loop=loop,
        url=req.url,
        mode="audio",
        quality=req.quality,
        subtitles=req.subtitles,
        inquiry_id=req.inquiry_id,
    )
    if existing:
        return {"skipped_duplicate": True, "media": existing}
    return {"job_id": job_id}


@app.post("/api/playlist_jobs")
async def create_playlist_jobs(req: PlaylistJobRequest) -> dict:
    loop = asyncio.get_event_loop()
    log("📥", f"POST /api/playlist_jobs — url={req.playlist_url[:60]!r} mode=audio")

    try:
        info = await loop.run_in_executor(None, _fetch_info, req.playlist_url)
    except Exception as exc:
        log("❌", f"POST /api/playlist_jobs failed while fetching playlist: {exc}")
        raise HTTPException(status_code=400, detail=str(exc))

    if info.get("_type") not in ("playlist", "channel"):
        raise HTTPException(status_code=400, detail="URL is not a playlist or channel")

    excluded_urls = {
        _canonical_video_url(str(url or "").strip())
        for url in req.excluded_urls
        if _canonical_video_url(str(url or "").strip())
    }
    global_subtitles = _normalize_subtitles(req.global_subtitles)
    individual_subtitles = {
        _canonical_video_url(str(url).strip()): _normalize_subtitles(langs)
        for url, langs in req.individual_subtitles.items()
        if _canonical_video_url(str(url).strip())
    }

    queued_job_ids: list[str] = []
    skipped_duplicates = 0
    skipped_excluded = 0

    for entry in info.get("entries") or []:
        if not _is_supported_playlist_entry(entry):
            continue
        entry_url = _canonical_video_url(str((entry or {}).get("url") or "").strip())
        if not entry_url:
            continue
        if entry_url in excluded_urls:
            skipped_excluded += 1
            continue

        subtitles = individual_subtitles.get(entry_url, global_subtitles)
        job_id, existing = await _enqueue_job(
            loop=loop,
            url=entry_url,
            mode="audio",
            quality=req.quality,
            subtitles=subtitles,
            inquiry_id=req.inquiry_id,
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
    source_url = _canonical_source_url(req.url)
    existing = await loop.run_in_executor(None, db.inquiry_get_by_source_url, source_url)
    if existing and existing.get("status") in {"queued", "building", "done"}:
        return existing

    inquiry_id = f"iq_{uuid4().hex[:8]}"
    await loop.run_in_executor(None, db.inquiry_insert, inquiry_id, source_url)
    threading.Thread(target=_run_inquiry_sync, args=(inquiry_id, source_url), daemon=True).start()
    row = await loop.run_in_executor(None, db.inquiry_get, inquiry_id)
    return row or {"inquiry_id": inquiry_id, "source_url": source_url}


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
    try:
        result = _yt_dlp_info(url, extract_flat="in_playlist")
    except Exception:
        result = _yt_dlp_info(url)
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
    source_url = _canonical_source_url(url)

    def _build() -> dict:
        cached = db.get_preview_items(source_url)
        if not cached:
            playlist_id = _playlist_id_from_url(source_url)
            if playlist_id:
                alias = db.find_preview_source_by_playlist_id(playlist_id, exclude_source_url=source_url)
                if alias:
                    alias_items = db.get_preview_items(alias["source_url"])
                    if alias_items and _is_updated_within_24h(alias.get("updated_at")):
                        db.upsert_preview_source(
                            source_url,
                            alias.get("source_type") or "playlist",
                            alias.get("title"),
                            alias.get("uploader"),
                            int(alias.get("total_count") or len(alias_items)),
                        )
                        for item in alias_items:
                            alias_url = _canonical_video_url(item.get("url") or "")
                            if not alias_url:
                                continue
                            item["url"] = alias_url
                            db.upsert_preview_item(source_url, item)
                        cached = db.get_preview_items(source_url)
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
    mode:         Literal["audio", "video", "all"] = "audio",
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
    stats = await loop.run_in_executor(
        None, db.query_media_stats,
        mode, channel_id, min_views, max_views, min_likes,
        min_duration, max_duration, tag_list, sub_lang,
    )
    for item in items:
        audio_fp = _find_variant_abspath(item, "audio")
        video_fp = _find_variant_abspath(item, "video")
        item["audio_download_url"] = _build_download_url(audio_fp, "audio") if audio_fp else ""
        item["video_download_url"] = _build_download_url(video_fp, "video") if video_fp else ""
        if mode == "video":
            item["download_url"] = item["video_download_url"] or item["audio_download_url"]
        else:
            item["download_url"] = item["audio_download_url"] or item["video_download_url"]
    return {"items": items, "total": total, "stats": stats}

@app.get("/api/library_summary")
async def library_summary() -> dict:
    loop = asyncio.get_event_loop()
    def _summary() -> dict[str, int]:
        audio_bytes = 0
        video_bytes = 0
        duration_seconds = 0
        media_count = 0
        seen_audio: set[str] = set()
        seen_video: set[str] = set()
        for record in db.list_all_media_records():
            media_count += 1
            duration_seconds += int(record.get("duration") or 0)
            audio_fp = _find_variant_abspath(record, "audio")
            if audio_fp and audio_fp.exists():
                key = str(audio_fp.resolve())
                if key not in seen_audio:
                    seen_audio.add(key)
                    audio_bytes += audio_fp.stat().st_size
            video_fp = _find_variant_abspath(record, "video")
            if video_fp and video_fp.exists():
                key = str(video_fp.resolve())
                if key not in seen_video:
                    seen_video.add(key)
                    video_bytes += video_fp.stat().st_size
        return {
            "media_count": media_count,
            "duration_seconds": duration_seconds,
            "audio_bytes": audio_bytes,
            "video_bytes": video_bytes,
            "total_bytes": audio_bytes + video_bytes,
        }
    return await loop.run_in_executor(None, _summary)


@app.get("/api/channels")
async def list_channels() -> list[dict]:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, db.get_channels)


@app.get("/api/sub_langs")
async def list_sub_langs(mode: Literal["audio", "video", "all"] = "audio") -> list[str]:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, db.get_all_sub_langs, mode)


@app.get("/api/tags")
async def list_tags(mode: Literal["audio", "video", "all"] = "audio") -> list[str]:
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
            "all", req.channel_id, req.min_views, req.max_views, req.min_likes,
            req.min_duration, req.max_duration, tag_list, req.sub_lang,
            req.sort_by, req.order, 10000, 0
        )
        
        if not items:
            await loop.run_in_executor(None, lambda: db.zip_update(job_id, status="error", error_msg="No matching files found to zip"))
            _active_zip_tasks.pop(job_id, None)
            return

        selected_items: list[tuple[dict, Path]] = []
        for item in items:
            fp = _find_variant_abspath(item, req.mode)
            if fp and fp.exists():
                selected_items.append((item, fp))

        if not selected_items:
            await loop.run_in_executor(None, lambda: db.zip_update(job_id, status="error", error_msg=f"No {req.mode} files found to zip"))
            _active_zip_tasks.pop(job_id, None)
            return

        final_zip_path = str(ZIPS_DIR / f"{job_id}.zip")
        await loop.run_in_executor(None, lambda: db.zip_update(job_id, status="zipping", title="Exporting Library...", percent=0, zip_path=final_zip_path))
        
        def create_zip():
            def _safe_piece(s: str) -> str:
                cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", str(s or "").strip())
                return cleaned.strip("._-") or "x"

            with zipfile.ZipFile(final_zip_path, 'w', zipfile.ZIP_STORED) as zf:
                total = len(selected_items)
                for i, (it, fp) in enumerate(selected_items):
                    if cancel_ev.is_set():
                        break
                    
                    if i % max(1, total // 50) == 0 or i == total - 1:
                        db.zip_update(job_id, percent=round((i / total) * 100, 1), title=f"Zipped {i}/{total}")

                    if fp.exists():
                        zf.write(str(fp), arcname=fp.name)
                    if req.mode == "audio":
                        media_id = int(it.get("id") or 0)
                        if media_id > 0:
                            transcripts = db.get_transcripts(media_id)
                            base_stem = _safe_piece(Path(str(it.get("file_name") or fp.name)).stem)
                            video_id = _safe_piece(str(it.get("youtube_id") or media_id))
                            for tr in transcripts:
                                lang = _safe_piece(str(tr.get("language") or "unknown"))
                                text = str(tr.get("text") or "").strip()
                                if not text:
                                    continue
                                arc = f"subtitles/{base_stem}__{video_id}.{lang}.txt"
                                zf.writestr(arc, text)
                        
        await loop.run_in_executor(None, create_zip)
        
        if cancel_ev.is_set():
            if os.path.exists(final_zip_path):
                os.remove(final_zip_path)
            await loop.run_in_executor(None, db.zip_delete, job_id)
            _active_zip_tasks.pop(job_id, None)
            return
            
        await loop.run_in_executor(None, lambda: db.zip_update(job_id, status="done", percent=100, title=f"Complete ({len(selected_items)} items)"))
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
        fp = _resolve_media_abspath(record)
        removed_counterpart = False
        if fp:
            try:
                fp.unlink(missing_ok=True)
                log("🗑️", f"Deleted file: {fp}")
            except Exception as e:
                log("⚠️", f"Could not delete file {fp}: {e}")
            counterpart_mode = "video" if str(record.get("mode") or "audio") == "audio" else "audio"
            counterpart = _find_variant_abspath(record, counterpart_mode)
            if counterpart and counterpart.exists():
                try:
                    counterpart.unlink(missing_ok=True)
                    removed_counterpart = True
                    log("🗑️", f"Deleted counterpart file: {counterpart}")
                except Exception as e:
                    log("⚠️", f"Could not delete counterpart {counterpart}: {e}")
        db.delete_media_record(media_id)
        return True

    success = await loop.run_in_executor(None, _delete)
    if not success:
        raise HTTPException(status_code=404, detail="Not found")
    return {"deleted": media_id}
