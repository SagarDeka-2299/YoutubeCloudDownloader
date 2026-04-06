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

CREATE TABLE IF NOT EXISTS zip_queue (
    job_id           TEXT PRIMARY KEY,
    ip_address       TEXT NOT NULL,
    status           TEXT DEFAULT 'queued',
    title            TEXT,
    percent          REAL DEFAULT 0,
    zip_path         TEXT DEFAULT '',
    error_msg        TEXT,
    created_at       DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at       DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS inquiry_queue (
    inquiry_id       TEXT PRIMARY KEY,
    source_url       TEXT NOT NULL,
    source_type      TEXT DEFAULT '',
    title            TEXT DEFAULT '',
    uploader         TEXT DEFAULT '',
    total_count      INTEGER DEFAULT 0,
    processed_count  INTEGER DEFAULT 0,
    status           TEXT DEFAULT 'queued',
    phase            TEXT DEFAULT '',
    detail           TEXT DEFAULT '',
    error_msg        TEXT DEFAULT '',
    created_at       DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at       DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transcripts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    media_id    INTEGER REFERENCES media(id) ON DELETE CASCADE,
    language    TEXT,
    text        TEXT,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS preview_sources (
    source_url    TEXT PRIMARY KEY,
    source_type   TEXT,
    title         TEXT,
    uploader      TEXT,
    total_count   INTEGER DEFAULT 0,
    updated_at    DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS preview_items (
    source_url      TEXT NOT NULL REFERENCES preview_sources(source_url) ON DELETE CASCADE,
    video_url       TEXT NOT NULL,
    playlist_index  INTEGER DEFAULT 0,
    title           TEXT,
    thumbnail       TEXT,
    uploader        TEXT,
    duration        INTEGER,
    view_count      INTEGER,
    like_count      INTEGER,
    subtitles_json  TEXT DEFAULT '[]',
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_url, video_url)
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
    sub_lang: str | None = None,
    sort_by: str = "download_date",
    order: str = "desc",
    limit: int = 60,
    offset: int = 0,
) -> tuple[list[dict], int]:
    _allowed = {"download_date", "release_date", "title", "channel_name",
                "view_count", "like_count", "duration", "file_size"}
    sb = sort_by if sort_by in _allowed else "download_date"
    od = "DESC" if order.lower() == "desc" else "ASC"

    cond: list[str] = []
    params: list[Any] = []
    if mode != "all":
        cond.append("mode=?")
        params.append(mode)

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
    if sub_lang is not None and sub_lang.strip():
        cond.append("EXISTS (SELECT 1 FROM transcripts t WHERE t.media_id = media.id AND t.language = ?)")
        params.append(sub_lang.strip())

    where_sql = f"WHERE {' AND '.join(cond)}" if cond else ""
    fetch = (limit * 4) if tags else limit
    sql = f"SELECT * FROM media {where_sql} ORDER BY {sb} {od} LIMIT ? OFFSET ?"
    params += [fetch, offset]

    with _conn() as c:
        total = c.execute(f"SELECT COUNT(*) FROM media {where_sql}", params[:-2]).fetchone()[0]
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

    if tags:
        total = 0
        count_sql = f"SELECT tags FROM media {where_sql}"
        with _conn() as c:
            for row in c.execute(count_sql, params[:-2]):
                row_tags = json.loads((dict(row).get("tags")) or "[]")
                row_tag_set = {t.lower() for t in row_tags}
                if any(t.lower() in row_tag_set for t in tags):
                    total += 1

    return result, total


def query_media_stats(
    mode: str = "audio",
    channel_id: int | None = None,
    min_views: int | None = None,
    max_views: int | None = None,
    min_likes: int | None = None,
    min_duration: int | None = None,
    max_duration: int | None = None,
    tags: list[str] | None = None,
    sub_lang: str | None = None,
) -> dict[str, int]:
    cond: list[str] = []
    params: list[Any] = []
    if mode != "all":
        cond.append("mode=?")
        params.append(mode)

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
    if sub_lang is not None and sub_lang.strip():
        cond.append("EXISTS (SELECT 1 FROM transcripts t WHERE t.media_id = media.id AND t.language = ?)")
        params.append(sub_lang.strip())

    where_sql = f"WHERE {' AND '.join(cond)}" if cond else ""
    with _conn() as c:
        rows = [dict(r) for r in c.execute(
            f"SELECT file_size, duration, tags FROM media {where_sql}",
            params,
        )]

    def _tag_match(row_tags: list[str]) -> bool:
        if not tags:
            return True
        row_set = {str(t).lower() for t in row_tags}
        return any(str(t).lower() in row_set for t in tags)

    total_count = 0
    total_bytes = 0
    total_duration = 0
    for row in rows:
        row_tags = json.loads(row.get("tags") or "[]")
        if not _tag_match(row_tags):
            continue
        total_count += 1
        total_bytes += int(row.get("file_size") or 0)
        total_duration += int(row.get("duration") or 0)

    return {
        "total_count": total_count,
        "total_bytes": total_bytes,
        "total_duration_seconds": total_duration,
    }


def get_channels() -> list[dict]:
    with _conn() as c:
        return [dict(r) for r in c.execute(
            "SELECT id, channel_id, name, url FROM channels ORDER BY name ASC"
        )]


def get_all_tags(mode: str = "audio") -> list[str]:
    with _conn() as c:
        if mode == "all":
            rows = c.execute(
                "SELECT tags FROM media WHERE tags NOT IN ('[]','null','') AND tags IS NOT NULL"
            ).fetchall()
        else:
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


def get_all_sub_langs(mode: str = "audio") -> list[str]:
    with _conn() as c:
        if mode == "all":
            rows = c.execute(
                "SELECT DISTINCT t.language FROM transcripts t "
                "JOIN media m ON t.media_id = m.id "
                "ORDER BY t.language ASC"
            ).fetchall()
        else:
            rows = c.execute(
                "SELECT DISTINCT t.language FROM transcripts t "
                "JOIN media m ON t.media_id = m.id "
                "WHERE m.mode=? ORDER BY t.language ASC",
                (mode,)
            ).fetchall()
    return [r[0] for r in rows if r[0]]


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


def update_media_file_record(
    media_id: int,
    *,
    mode: str,
    file_path: str,
    file_name: str,
    file_size: int | None = None,
) -> None:
    with _conn() as c:
        if file_size is None:
            c.execute(
                "UPDATE media SET mode=?, file_path=?, file_name=? WHERE id=?",
                (mode, file_path, file_name, media_id),
            )
        else:
            c.execute(
                "UPDATE media SET mode=?, file_path=?, file_name=?, file_size=? WHERE id=?",
                (mode, file_path, file_name, file_size, media_id),
            )
        c.commit()


def list_all_media_records() -> list[dict]:
    with _conn() as c:
        rows = c.execute("SELECT * FROM media ORDER BY id ASC").fetchall()
        return [dict(r) for r in rows]


# ── Transcripts ──────────────────────────────────────────────────────────────────

def insert_transcript(media_id: int, lang: str, text: str) -> None:
    with _conn() as c:
        # Avoid inserting empty transcripts
        if not text.strip():
            return
        c.execute(
            "INSERT INTO transcripts (media_id, language, text) VALUES (?,?,?)",
            (media_id, lang, text)
        )
        c.commit()


def get_transcripts(media_id: int) -> list[dict]:
    with _conn() as c:
        rows = c.execute(
            "SELECT language, text, created_at FROM transcripts WHERE media_id=? ORDER BY language ASC",
            (media_id,)
        ).fetchall()
        return [dict(r) for r in rows]


# ── Preview Cache ─────────────────────────────────────────────────────────────

def upsert_preview_source(source_url: str, source_type: str, title: str | None,
                          uploader: str | None, total_count: int) -> None:
    with _conn() as c:
        c.execute(
            """
            INSERT INTO preview_sources (source_url, source_type, title, uploader, total_count)
            VALUES (?,?,?,?,?)
            ON CONFLICT(source_url) DO UPDATE SET
                source_type=excluded.source_type,
                title=excluded.title,
                uploader=excluded.uploader,
                total_count=excluded.total_count,
                updated_at=CURRENT_TIMESTAMP
            """,
            (source_url, source_type, title, uploader, total_count),
        )
        c.commit()


def get_preview_source(source_url: str) -> dict | None:
    with _conn() as c:
        row = c.execute(
            """
            SELECT source_url, source_type, title, uploader, total_count, updated_at
            FROM preview_sources
            WHERE source_url=?
            """,
            (source_url,),
        ).fetchone()
        return dict(row) if row else None


def find_preview_source_by_playlist_id(playlist_id: str, exclude_source_url: str | None = None) -> dict | None:
    pid = str(playlist_id or "").strip()
    if not pid:
        return None
    with _conn() as c:
        if exclude_source_url:
            row = c.execute(
                """
                SELECT source_url, source_type, title, uploader, total_count, updated_at
                FROM preview_sources
                WHERE instr(source_url, ?) > 0 AND source_url <> ?
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (f"list={pid}", exclude_source_url),
            ).fetchone()
        else:
            row = c.execute(
                """
                SELECT source_url, source_type, title, uploader, total_count, updated_at
                FROM preview_sources
                WHERE instr(source_url, ?) > 0
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (f"list={pid}",),
            ).fetchone()
        return dict(row) if row else None


def get_preview_items(source_url: str) -> list[dict]:
    with _conn() as c:
        rows = c.execute(
            """
            SELECT video_url, playlist_index, title, thumbnail, uploader,
                   duration, view_count, like_count, subtitles_json, updated_at
            FROM preview_items
            WHERE source_url=?
            ORDER BY playlist_index ASC, video_url ASC
            """,
            (source_url,),
        ).fetchall()
    items = []
    for row in rows:
        item = dict(row)
        item["url"] = item.pop("video_url")
        item["subtitles"] = json.loads(item.pop("subtitles_json") or "[]")
        items.append(item)
    return items


def upsert_preview_item(source_url: str, item: dict) -> None:
    with _conn() as c:
        c.execute(
            """
            INSERT INTO preview_items
                (source_url, video_url, playlist_index, title, thumbnail, uploader,
                 duration, view_count, like_count, subtitles_json)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(source_url, video_url) DO UPDATE SET
                playlist_index=excluded.playlist_index,
                title=excluded.title,
                thumbnail=excluded.thumbnail,
                uploader=excluded.uploader,
                duration=excluded.duration,
                view_count=excluded.view_count,
                like_count=excluded.like_count,
                subtitles_json=excluded.subtitles_json,
                updated_at=CURRENT_TIMESTAMP
            """,
            (
                source_url,
                item.get("url"),
                item.get("playlist_index") or 0,
                item.get("title"),
                item.get("thumbnail"),
                item.get("uploader"),
                item.get("duration"),
                item.get("view_count"),
                item.get("like_count"),
                json.dumps(item.get("subtitles") or []),
            ),
        )
        c.commit()


def delete_preview_items_not_in(source_url: str, video_urls: list[str]) -> None:
    with _conn() as c:
        if video_urls:
            placeholders = ",".join("?" for _ in video_urls)
            c.execute(
                f"DELETE FROM preview_items WHERE source_url=? AND video_url NOT IN ({placeholders})",
                [source_url, *video_urls],
            )
        else:
            c.execute("DELETE FROM preview_items WHERE source_url=?", (source_url,))
        c.commit()


# ── Inquiry Queue ─────────────────────────────────────────────────────────────

def inquiry_insert(inquiry_id: str, source_url: str) -> None:
    with _conn() as c:
        c.execute(
            "INSERT OR IGNORE INTO inquiry_queue (inquiry_id, source_url, status, phase, detail) VALUES (?,?,?,?,?)",
            (inquiry_id, source_url, "queued", "queued", "Waiting to start"),
        )
        c.commit()


def inquiry_update(inquiry_id: str, **kwargs: Any) -> None:
    allowed = {
        "source_type", "title", "uploader", "total_count", "processed_count",
        "status", "phase", "detail", "error_msg",
    }
    sets, vals = [], []
    for key, value in kwargs.items():
        if key in allowed:
            sets.append(f"{key}=?")
            vals.append(value)
    if not sets:
        return
    sets.append("updated_at=CURRENT_TIMESTAMP")
    vals.append(inquiry_id)
    with _conn() as c:
        c.execute(f"UPDATE inquiry_queue SET {', '.join(sets)} WHERE inquiry_id=?", vals)
        c.commit()


def inquiry_get(inquiry_id: str) -> dict | None:
    with _conn() as c:
        row = c.execute("SELECT * FROM inquiry_queue WHERE inquiry_id=?", (inquiry_id,)).fetchone()
        return dict(row) if row else None


def inquiry_get_by_source_url(source_url: str) -> dict | None:
    with _conn() as c:
        row = c.execute(
            """
            SELECT * FROM inquiry_queue
            WHERE source_url=?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (source_url,),
        ).fetchone()
        return dict(row) if row else None


def inquiry_list(page: int = 0, limit: int = 10) -> tuple[list[dict], int]:
    with _conn() as c:
        total = c.execute("SELECT COUNT(*) FROM inquiry_queue").fetchone()[0]
        offset = page * limit
        rows = c.execute(
            "SELECT * FROM inquiry_queue ORDER BY created_at DESC LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()
        return [dict(r) for r in rows], total


def inquiry_delete(inquiry_id: str) -> None:
    with _conn() as c:
        c.execute("DELETE FROM inquiry_queue WHERE inquiry_id=?", (inquiry_id,))
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


def queue_active_count() -> int:
    with _conn() as c:
        row = c.execute(
            "SELECT COUNT(*) FROM queue WHERE status IN ('queued','downloading','converting','saving')"
        ).fetchone()
        return int(row[0] if row else 0)


def queue_delete(job_id: str) -> None:
    with _conn() as c:
        c.execute("DELETE FROM queue WHERE job_id=?", (job_id,))
        c.commit()


# ── Zip Queue ──────────────────────────────────────────────────────────────────

def zip_insert(job_id: str, ip_address: str, status: str = 'queued') -> None:
    with _conn() as c:
        c.execute(
            "INSERT OR IGNORE INTO zip_queue (job_id, ip_address, status) VALUES (?,?,?)",
            (job_id, ip_address, status)
        )
        c.commit()

def zip_update(job_id: str, **kwargs: Any) -> None:
    _allowed = {"status", "title", "percent", "zip_path", "error_msg"}
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
        c.execute(f"UPDATE zip_queue SET {', '.join(sets)} WHERE job_id=?", vals)
        c.commit()

def zip_get(job_id: str) -> dict | None:
    with _conn() as c:
        row = c.execute("SELECT * FROM zip_queue WHERE job_id=?", (job_id,)).fetchone()
        return dict(row) if row else None

def zip_list_by_ip(ip_address: str, page: int = 0, limit: int = 10) -> tuple[list[dict], int]:
    with _conn() as c:
        total = c.execute("SELECT COUNT(*) FROM zip_queue WHERE ip_address=?", (ip_address,)).fetchone()[0]
        offset = page * limit
        rows = c.execute(
            "SELECT * FROM zip_queue WHERE ip_address=? ORDER BY created_at ASC LIMIT ? OFFSET ?",
            (ip_address, limit, offset),
        ).fetchall()
        return [dict(r) for r in rows], total

def zip_delete(job_id: str) -> None:
    with _conn() as c:
        c.execute("DELETE FROM zip_queue WHERE job_id=?", (job_id,))
        c.commit()
