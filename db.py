"""
db.py – sync SQLite helpers (sqlite3, thread-safe).
Used directly from yt-dlp post-processor thread AND wrapped in
run_in_executor for async API routes.
"""
import json
import os
import sqlite3
from pathlib import Path
from typing import Any

_DATA_DIR = Path(os.environ.get("DATA_DIR", "data"))
DB_PATH   = str(_DATA_DIR / "ytgrab.db")

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS channels (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id  TEXT UNIQUE,
    name        TEXT NOT NULL,
    url         TEXT,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS media (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    youtube_id    TEXT UNIQUE,
    title         TEXT,
    channel_id    INTEGER REFERENCES channels(id),
    channel_name  TEXT,
    channel_url   TEXT,
    duration      INTEGER,
    view_count    INTEGER,
    like_count    INTEGER,
    tags          TEXT,
    release_date  TEXT,
    thumbnail     TEXT,
    youtube_url   TEXT,
    file_path     TEXT,
    file_name     TEXT,
    file_size     INTEGER,
    mode          TEXT,
    quality       TEXT,
    download_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS queue (
    job_id           TEXT PRIMARY KEY,
    url              TEXT NOT NULL,
    mode             TEXT NOT NULL,
    quality          TEXT NOT NULL,
    target_path      TEXT    DEFAULT '',
    status           TEXT    DEFAULT 'queued',
    title            TEXT,
    thumbnail        TEXT,
    downloaded_bytes INTEGER DEFAULT 0,
    total_bytes      INTEGER DEFAULT 0,
    speed_bps        REAL    DEFAULT 0,
    eta_seconds      INTEGER DEFAULT 0,
    error_msg        TEXT,
    created_at       DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at       DATETIME DEFAULT CURRENT_TIMESTAMP
);
"""


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c


def init_db() -> None:
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    with _conn() as c:
        c.executescript(SCHEMA)
        c.commit()


# ── Channel ────────────────────────────────────────────────────────────────────

def upsert_channel(channel_id: str, name: str, url: str | None) -> int | None:
    if not channel_id:
        return None
    with _conn() as c:
        c.execute(
            "INSERT INTO channels (channel_id, name, url) VALUES (?,?,?) "
            "ON CONFLICT(channel_id) DO UPDATE SET name=excluded.name, url=excluded.url",
            (channel_id, name or "", url),
        )
        c.commit()
        row = c.execute("SELECT id FROM channels WHERE channel_id=?", (channel_id,)).fetchone()
        return row[0] if row else None


# ── Media ──────────────────────────────────────────────────────────────────────

def upsert_media(data: dict) -> int:
    tags_str = json.dumps(data.get("tags") or [])
    with _conn() as c:
        c.execute(
            """
            INSERT INTO media
                (youtube_id, title, channel_id, channel_name, channel_url,
                 duration, view_count, like_count, tags, release_date,
                 thumbnail, youtube_url, file_path, file_name, file_size,
                 mode, quality)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(youtube_id) DO UPDATE SET
                title=excluded.title,
                channel_id=excluded.channel_id,
                channel_name=excluded.channel_name,
                channel_url=excluded.channel_url,
                duration=excluded.duration,
                view_count=excluded.view_count,
                like_count=excluded.like_count,
                tags=excluded.tags,
                release_date=excluded.release_date,
                thumbnail=excluded.thumbnail,
                youtube_url=excluded.youtube_url,
                file_path=excluded.file_path,
                file_name=excluded.file_name,
                file_size=excluded.file_size,
                mode=excluded.mode,
                quality=excluded.quality,
                download_date=CURRENT_TIMESTAMP
            """,
            (
                data.get("youtube_id"),
                data.get("title"),
                data.get("channel_db_id"),
                data.get("channel_name"),
                data.get("channel_url"),
                data.get("duration"),
                data.get("view_count"),
                data.get("like_count"),
                tags_str,
                data.get("release_date"),
                data.get("thumbnail"),
                data.get("youtube_url"),
                data.get("file_path"),
                data.get("file_name"),
                data.get("file_size"),
                data.get("mode"),
                data.get("quality"),
            ),
        )
        c.commit()
        row = c.execute(
            "SELECT id FROM media WHERE youtube_id=?", (data.get("youtube_id"),)
        ).fetchone()
        return row[0] if row else -1


def query_media(
    mode: str = "audio",
    channel_id: int | None = None,
    min_views: int | None = None,
    max_views: int | None = None,
    min_likes: int | None = None,
    min_duration: int | None = None,
    max_duration: int | None = None,
    tags: list[str] | None = None,
    sort_by: str = "download_date",
    order: str = "desc",
    limit: int = 60,
    offset: int = 0,
) -> list[dict]:
    _allowed = {"download_date", "release_date", "title", "channel_name",
                "view_count", "like_count", "duration", "file_size"}
    sb = sort_by if sort_by in _allowed else "download_date"
    od = "DESC" if order.lower() == "desc" else "ASC"

    cond: list[str] = ["mode=?"]
    params: list[Any] = [mode]

    if channel_id is not None:
        cond.append("channel_id=?"); params.append(channel_id)
    if min_views is not None:
        cond.append("COALESCE(view_count,0)>=?"); params.append(min_views)
    if max_views is not None:
        cond.append("COALESCE(view_count,0)<=?"); params.append(max_views)
    if min_likes is not None:
        cond.append("COALESCE(like_count,0)>=?"); params.append(min_likes)
    if min_duration is not None:
        cond.append("COALESCE(duration,0)>=?"); params.append(min_duration)
    if max_duration is not None:
        cond.append("COALESCE(duration,0)<=?"); params.append(max_duration)

    fetch = (limit * 4) if tags else limit
    sql = f"SELECT * FROM media WHERE {' AND '.join(cond)} ORDER BY {sb} {od} LIMIT ? OFFSET ?"
    params += [fetch, offset]

    with _conn() as c:
        rows = [dict(r) for r in c.execute(sql, params)]

    result: list[dict] = []
    for r in rows:
        r["tags"] = json.loads(r.get("tags") or "[]")
        r["size_mb"] = round((r.get("file_size") or 0) / (1024 * 1024), 2)
        if tags:
            rt = {t.lower() for t in r["tags"]}
            if not any(t.lower() in rt for t in tags):
                continue
        result.append(r)
        if len(result) >= limit:
            break

    return result


def get_channels() -> list[dict]:
    with _conn() as c:
        return [dict(r) for r in c.execute(
            "SELECT id, channel_id, name, url FROM channels ORDER BY name ASC"
        )]


def get_all_tags(mode: str = "audio") -> list[str]:
    with _conn() as c:
        rows = c.execute(
            "SELECT tags FROM media WHERE mode=? AND tags NOT IN ('[]','null','') AND tags IS NOT NULL",
            (mode,),
        ).fetchall()
    tag_set: set[str] = set()
    for (ts,) in rows:
        try:
            tag_set.update(json.loads(ts))
        except Exception:
            pass
    return sorted(tag_set)


def get_media_by_id(media_id: int) -> dict | None:
    with _conn() as c:
        row = c.execute("SELECT * FROM media WHERE id=?", (media_id,)).fetchone()
        if row:
            d = dict(row)
            d["tags"] = json.loads(d.get("tags") or "[]")
            return d
        return None


def get_media_by_youtube_id(y_id: str) -> dict | None:
    with _conn() as c:
        row = c.execute("SELECT * FROM media WHERE youtube_id=?", (y_id,)).fetchone()
        if row:
            d = dict(row)
            d["tags"] = json.loads(d.get("tags") or "[]")
            return d
        return None


def delete_media_record(media_id: int) -> None:
    with _conn() as c:
        c.execute("DELETE FROM media WHERE id=?", (media_id,))
        c.commit()


def update_media_path(media_id: int, new_path: str) -> None:
    with _conn() as c:
        c.execute("UPDATE media SET file_path=? WHERE id=?", (new_path, media_id))
        c.commit()


# ── Queue ──────────────────────────────────────────────────────────────────────

def queue_insert(job_id: str, url: str, mode: str, quality: str,
                 target_path: str = "") -> None:
    with _conn() as c:
        c.execute(
            "INSERT OR IGNORE INTO queue (job_id, url, mode, quality, target_path) "
            "VALUES (?,?,?,?,?)",
            (job_id, url, mode, quality, target_path),
        )
        c.commit()


def queue_update(job_id: str, **kwargs: Any) -> None:
    """Update any subset of queue columns."""
    _allowed = {
        "status", "title", "thumbnail",
        "downloaded_bytes", "total_bytes", "speed_bps", "eta_seconds", "error_msg",
    }
    sets, vals = [], []
    for k, v in kwargs.items():
        if k in _allowed:
            sets.append(f"{k}=?")
            vals.append(v)
    if not sets:
        return
    sets.append("updated_at=CURRENT_TIMESTAMP")
    vals.append(job_id)
    with _conn() as c:
        c.execute(f"UPDATE queue SET {', '.join(sets)} WHERE job_id=?", vals)
        c.commit()


def queue_get(job_id: str) -> dict | None:
    with _conn() as c:
        row = c.execute("SELECT * FROM queue WHERE job_id=?", (job_id,)).fetchone()
        return dict(row) if row else None


def queue_list(page: int = 0, limit: int = 10) -> tuple[list[dict], int]:
    with _conn() as c:
        total = c.execute("SELECT COUNT(*) FROM queue").fetchone()[0]
        offset = page * limit
        rows = c.execute(
            "SELECT * FROM queue ORDER BY created_at ASC LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()
        return [dict(r) for r in rows], total


def queue_delete(job_id: str) -> None:
    with _conn() as c:
        c.execute("DELETE FROM queue WHERE job_id=?", (job_id,))
        c.commit()
