# persistent_cached_minio.py
import sqlite3
import json
import time
import os
import threading
from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional, Dict, Any, Iterator, List
from minio import Minio
from minio.datatypes import Object
from minio.commonconfig import CopySource
from datetime import datetime


# -----------------------
# Public types
# -----------------------
@dataclass
class CachedObject:
    obj: Optional[Object]           # MinIO Object-like (may be None for CACHE_ONLY misses)
    bucket: str
    key: str
    metadata: Optional[Dict[str, Any]]
    tags: Optional[Dict[str, str]]
    cached: bool                    # True if value came from cache
    cached_at: Optional[float]      # epoch seconds when cached (None if not cached)
    age_seconds: Optional[float]    # now - cached_at
    stale: bool                     # True if cached and stale (based on TTL)
    source: str                     # 'cache' or 's3' or 'none'


class CacheMode(Enum):
    DEFAULT = auto()       # Use cache if fresh, otherwise fetch & refresh
    BYPASS = auto()        # Always fetch from S3, don't touch cache
    CACHE_ONLY = auto()    # Use only cache; if not present return CachedObject with obj=None
    FORCE_REFRESH = auto() # Always fetch from S3 and update cache


# -----------------------
# Helper Utilities
# -----------------------
def _now() -> float:
    return time.time()


def _isoformat(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat() if dt else None


def _obj_from_row(row: sqlite3.Row) -> Object:
    """
    Recreate a minio.datatypes.Object (or similar object) from DB row.
    We construct the Object with the fields we need (bucket, object_name, last_modified, etag, size, metadata).
    """
    last_modified = None
    if row["last_modified"]:
        try:
            last_modified = datetime.fromisoformat(row["last_modified"])
        except Exception:
            last_modified = None

    # Construct a minio Object-like instance:
    return Object(
        bucket_name=row["bucket"],
        object_name=row["key"],
        last_modified=last_modified,
        etag=row["etag"],
        size=row["size"],
        metadata=json.loads(row["metadata"]) if row["metadata"] else None
    )


# -----------------------
# PersistentCachedMinio
# -----------------------
class PersistentCachedMinio:
    """
    Thread-safe persistent cache wrapper around a Minio client.

    - Backed by SQLite (db_path)
    - TTL controls staleness
    - max_db_size_mb controls eviction by file size (evict oldest cached rows)
    - Methods return CachedObject instances for list/stat/get_tags
    """
    def __init__(
        self,
        client: Minio,
        db_path: str = "s3cache.db",
        ttl: int = 3600,
        max_db_size_mb: int = 0  # 0 means unlimited
    ):
        self._client = client
        self.db_path = db_path
        self.ttl = ttl
        self.max_db_size_mb = max_db_size_mb
        self._lock = threading.RLock()

        # Ensure directory exists
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)

        # Single connection guarded by lock
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        # Use WAL for better concurrency
        with self._conn:
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._init_db()

    # -------------------------
    # DB initialization
    # -------------------------
    def _init_db(self):
        with self._lock, self._conn:
            cur = self._conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS buckets (
                    name TEXT PRIMARY KEY
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS objects (
                    bucket TEXT NOT NULL,
                    key TEXT NOT NULL,
                    etag TEXT,
                    size INTEGER,
                    last_modified TEXT,
                    metadata TEXT,
                    tags TEXT,
                    cached_at REAL,
                    PRIMARY KEY (bucket, key)
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_objects_bucket_key ON objects(bucket, key)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_objects_cached_at ON objects(cached_at)")
            self._conn.commit()

    # -------------------------
    # Internal cache helpers
    # -------------------------
    def _get_row(self, bucket: str, key: str) -> Optional[sqlite3.Row]:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("SELECT * FROM objects WHERE bucket=? AND key=?", (bucket, key))
            return cur.fetchone()

    def _is_row_stale(self, row: sqlite3.Row) -> bool:
        if row is None:
            return True
        if self.ttl <= 0:
            return False
        cached_at = row["cached_at"]
        if cached_at is None:
            return True
        return (_now() - cached_at) > self.ttl

    def _write_object_row(
        self,
        bucket: str,
        key: str,
        etag: Optional[str],
        size: Optional[int],
        last_modified: Optional[datetime],
        metadata: Optional[Dict[str, Any]],
        tags: Optional[Dict[str, str]]
    ):
        with self._lock, self._conn:
            self._conn.execute("""
                INSERT OR REPLACE INTO objects
                (bucket, key, etag, size, last_modified, metadata, tags, cached_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                bucket,
                key,
                etag,
                size,
                _isoformat(last_modified),
                json.dumps(metadata) if metadata else None,
                json.dumps(tags) if tags else None,
                _now()
            ))
            self._conn.commit()
            if self.max_db_size_mb and self.max_db_size_mb > 0:
                self._enforce_db_size_limit()

    def _delete_object_row(self, bucket: str, key: str):
        with self._lock, self._conn:
            self._conn.execute("DELETE FROM objects WHERE bucket=? AND key=?", (bucket, key))
            self._conn.commit()

    def _update_tags_row(self, bucket: str, key: str, tags: Dict[str, str]):
        with self._lock, self._conn:
            self._conn.execute("UPDATE objects SET tags=?, cached_at=? WHERE bucket=? AND key=?",
                               (json.dumps(tags), _now(), bucket, key))
            self._conn.commit()
            if self.max_db_size_mb and self.max_db_size_mb > 0:
                self._enforce_db_size_limit()

    def _list_cached_keys_for_prefix(self, bucket: str, prefix: str) -> List[str]:
        with self._lock:
            cur = self._conn.cursor()
            # keys ordered lexicographically by key
            cur.execute("SELECT key FROM objects WHERE bucket=? AND key LIKE ? ORDER BY key",
                        (bucket, f"{prefix}%"))
            return [r["key"] for r in cur.fetchall()]

    def _rows_to_cached_objects(self, rows: List[sqlite3.Row]) -> List[CachedObject]:
        out: List[CachedObject] = []
        for row in rows:
            obj = _obj_from_row(row)
            cached_at = row["cached_at"]
            age = _now() - cached_at if cached_at else None
            stale = False
            if cached_at is None:
                stale = True
            elif self.ttl > 0 and age is not None:
                stale = age > self.ttl
            tags = json.loads(row["tags"]) if row["tags"] else None
            metadata = json.loads(row["metadata"]) if row["metadata"] else None
            out.append(CachedObject(
                obj=obj,
                bucket=row["bucket"],
                key=row["key"],
                metadata=metadata,
                tags=tags,
                cached=True,
                cached_at=cached_at,
                age_seconds=age,
                stale=stale,
                source="cache"
            ))
        return out

    def _enforce_db_size_limit(self):
        """
        If DB file size exceeds max_db_size_mb, evict oldest cached rows until under limit.
        Eviction is done by deleting oldest `cached_at` rows.
        """
        try:
            size_bytes = os.path.getsize(self.db_path)
        except OSError:
            return
        limit_bytes = int(self.max_db_size_mb * 1024 * 1024)
        if size_bytes <= limit_bytes:
            return

        with self._lock, self._conn:
            cur = self._conn.cursor()
            # Count rows and iteratively delete in batches until size reduced
            # We'll delete oldest rows first
            while True:
                try:
                    size_bytes = os.path.getsize(self.db_path)
                except OSError:
                    break
                if size_bytes <= limit_bytes:
                    break
                # Find N oldest rows (by cached_at)
                cur.execute("SELECT bucket, key FROM objects ORDER BY cached_at ASC LIMIT 100")
                rows = cur.fetchall()
                if not rows:
                    break
                keys_to_delete = [(r["bucket"], r["key"]) for r in rows]
                cur.executemany("DELETE FROM objects WHERE bucket=? AND key=?", keys_to_delete)
                self._conn.commit()

    # -------------------------
    # Bucket ops
    # -------------------------
    def list_buckets(self) -> List[Any]:
        """
        Return list of buckets. If we have buckets cached, return them, otherwise fetch and cache.
        """
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("SELECT name FROM buckets")
            rows = cur.fetchall()
            if rows:
                return [type("Bucket", (), {"name": r["name"]}) for r in rows]

        # not in cache -> fetch from s3
        buckets = list(self._client.list_buckets())
        with self._lock, self._conn:
            cur = self._conn.cursor()
            cur.executemany("INSERT OR IGNORE INTO buckets (name) VALUES (?)",
                            [(b.name,) for b in buckets])
            self._conn.commit()
        return buckets

    def make_bucket(self, bucket_name: str, **kwargs):
        res = self._client.make_bucket(bucket_name, **kwargs)
        with self._lock, self._conn:
            self._conn.execute("INSERT OR IGNORE INTO buckets (name) VALUES (?)", (bucket_name,))
            self._conn.commit()
        return res

    # -------------------------
    # list_objects with pagination & cache fill
    # -------------------------
    def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        recursive: bool = False,
        include_user_meta: bool = False,
        limit: Optional[int] = None,
        cache_mode: CacheMode = CacheMode.DEFAULT
    ) -> Iterator[CachedObject]:
        """
        Cache-aware list_objects supporting a `limit` parameter.
        Strategy:
          - If cache contains >= limit objects for prefix and mode allows, return from cache.
          - Otherwise iterate S3 list_objects in order and yield merged results (caching missing entries)
            until we reach limit (or S3 exhausted).
        Note: If cache_mode == BYPASS, we iterate S3 and do NOT use cache hits (but will cache new items).
              If cache_mode == CACHE_ONLY, we only return items from cache (no S3 calls).
        """
        # If caller requested CACHE_ONLY and no limit specified -> return all cached entries for prefix
        if cache_mode == CacheMode.CACHE_ONLY:
            with self._lock:
                cur = self._conn.cursor()
                if prefix:
                    cur.execute("SELECT * FROM objects WHERE bucket=? AND key LIKE ? ORDER BY key", (bucket, f"{prefix}%"))
                else:
                    cur.execute("SELECT * FROM objects WHERE bucket=? ORDER BY key", (bucket,))
                rows = cur.fetchall()
            for co in self._rows_to_cached_objects(rows[:limit] if limit else rows):
                yield co
            return

        # If not CACHE_ONLY, attempt to serve from cache if it's sufficient
        cached_keys = self._list_cached_keys_for_prefix(bucket, prefix)
        if limit is not None and len(cached_keys) >= limit and cache_mode == CacheMode.DEFAULT:
            # return first limit cached
            with self._lock:
                cur = self._conn.cursor()
                cur.execute("SELECT * FROM objects WHERE bucket=? AND key LIKE ? ORDER BY key LIMIT ?",
                            (bucket, f"{prefix}%", limit))
                rows = cur.fetchall()
            for co in self._rows_to_cached_objects(rows):
                yield co
            return

        # Otherwise we will iterate S3 and produce up to `limit` results merging cache and S3.
        # We'll iterate S3 in lexicographic order (minio.list_objects does this) and for each object:
        #  - if it's in cache and cache_mode != BYPASS, yield cached entry (if not stale under DEFAULT),
        #  - otherwise fetch stat and tags as needed, cache and yield.
        count = 0
        seen_keys = set()

        # If cache contains some entries, we still want the correct ordering.
        # So iterate S3 and for each key encountered, decide whether it's cached or not.
        for s3obj in self._client.list_objects(bucket, prefix=prefix, recursive=recursive):
            key = s3obj.object_name
            if key in seen_keys:
                continue
            seen_keys.add(key)

            row = self._get_row(bucket, key)
            # Decide behavior based on cache_mode and staleness
            if row and cache_mode != CacheMode.BYPASS:
                stale = self._is_row_stale(row)
                if cache_mode == CacheMode.DEFAULT and not stale:
                    # return cached
                    for co in self._rows_to_cached_objects([row]):
                        yield co
                        count += 1
                        if limit is not None and count >= limit:
                            return
                    continue
                if cache_mode == CacheMode.FORCE_REFRESH:
                    # treat as not cached, fall through to fetch live
                    pass
                if cache_mode == CacheMode.DEFAULT and stale:
                    # fetch fresh and update cache
                    pass

            # If we reach here, we need to fetch latest from S3
            try:
                # Use stat_object for full metadata
                stat = self._client.stat_object(bucket, key)
                # tags may be fetched lazily only when asked, but here we'll cache tags=None to avoid extra call
                # (user can call get_object_tags explicitly to fetch tags)
                self._write_object_row(bucket, key, getattr(stat, "etag", None),
                                       getattr(stat, "size", None),
                                       getattr(stat, "last_modified", None),
                                       getattr(stat, "metadata", None),
                                       None)
                # create CachedObject from freshly fetched stat
                co = CachedObject(
                    obj=stat,
                    bucket=bucket,
                    key=key,
                    metadata=getattr(stat, "metadata", None),
                    tags=None,
                    cached=True,
                    cached_at=_now(),
                    age_seconds=0.0,
                    stale=False,
                    source="s3"
                )
                yield co
                count += 1
                if limit is not None and count >= limit:
                    return
            except Exception:
                # If stat fails, skip object
                continue

        # End of S3 list. If there are cached rows for prefix that were not present in S3 listing
        # (unlikely) or we want to include items that might be only in cache (e.g., previously cached but S3 removed),
        # we could append them here. For now, do not add extra items.
        return

    # -------------------------
    # stat_object
    # -------------------------
    def stat_object(self, bucket: str, key: str, cache_mode: CacheMode = CacheMode.DEFAULT) -> CachedObject:
        """
        Return a CachedObject for the stat of an object.
        """
        # Handle CACHE_ONLY quickly
        if cache_mode == CacheMode.CACHE_ONLY:
            row = self._get_row(bucket, key)
            if not row:
                return CachedObject(obj=None, bucket=bucket, key=key, metadata=None,
                                    tags=None, cached=False, cached_at=None, age_seconds=None,
                                    stale=True, source="none")
            co = self._rows_to_cached_objects([row])[0]
            return co

        # Check cache
        row = self._get_row(bucket, key)
        if row and cache_mode != CacheMode.BYPASS:
            stale = self._is_row_stale(row)
            if cache_mode == CacheMode.DEFAULT and not stale:
                co = self._rows_to_cached_objects([row])[0]
                return co
            if cache_mode == CacheMode.FORCE_REFRESH:
                pass  # fallthrough to fetch
            if cache_mode == CacheMode.DEFAULT and stale:
                pass  # fallthrough to fetch fresh

        # Fetch from S3
        try:
            stat = self._client.stat_object(bucket, key)
        except Exception:
            # If we have cached row, but S3 failed and cache exists, return cached (even if stale)
            if row:
                co = self._rows_to_cached_objects([row])[0]
                return co
            # otherwise propagate or return empty CachedObject
            raise

        # Persist row
        self._write_object_row(bucket, key, getattr(stat, "etag", None),
                               getattr(stat, "size", None),
                               getattr(stat, "last_modified", None),
                               getattr(stat, "metadata", None),
                               None)  # tags left None until fetched
        return CachedObject(
            obj=stat,
            bucket=bucket,
            key=key,
            metadata=getattr(stat, "metadata", None),
            tags=None,
            cached=True,
            cached_at=_now(),
            age_seconds=0.0,
            stale=False,
            source="s3"
        )

    # -------------------------
    # get_object_tags
    # -------------------------
    def get_object_tags(self, bucket: str, key: str, cache_mode: CacheMode = CacheMode.DEFAULT) -> CachedObject:
        """
        Return CachedObject where tags field is populated. If the object is in cache and tags are present
        and fresh, return that. Otherwise fetch tags from S3 (unless CACHE_ONLY).
        """
        row = self._get_row(bucket, key)
        if row:
            tags = json.loads(row["tags"]) if row["tags"] else None
            cached_at = row["cached_at"]
            age = (_now() - cached_at) if cached_at else None
            stale = self._is_row_stale(row)
            if tags is not None and cache_mode != CacheMode.BYPASS:
                if cache_mode == CacheMode.DEFAULT and not stale:
                    # return cached tags
                    return CachedObject(
                        obj=_obj_from_row(row),
                        bucket=bucket,
                        key=key,
                        metadata=json.loads(row["metadata"]) if row["metadata"] else None,
                        tags=tags,
                        cached=True,
                        cached_at=cached_at,
                        age_seconds=age,
                        stale=stale,
                        source="cache"
                    )
                # else fallthrough to fetch fresh tags
            if cache_mode == CacheMode.CACHE_ONLY:
                return CachedObject(
                    obj=_obj_from_row(row),
                    bucket=bucket,
                    key=key,
                    metadata=json.loads(row["metadata"]) if row["metadata"] else None,
                    tags=tags,
                    cached=True if tags is not None else False,
                    cached_at=cached_at,
                    age_seconds=age,
                    stale=stale,
                    source="cache" if tags is not None else "none"
                )

        if cache_mode == CacheMode.CACHE_ONLY:
            # no row -> return miss
            return CachedObject(obj=None, bucket=bucket, key=key, metadata=None, tags=None,
                                cached=False, cached_at=None, age_seconds=None, stale=True, source="none")

        # Fetch tags from S3
        try:
            tags_obj = self._client.get_object_tags(bucket, key)
            tags = tags_obj.to_dict() if hasattr(tags_obj, "to_dict") else dict(tags_obj)
            # Update DB tags
            # If object isn't in DB yet, we may stat it to fill base row
            if not row:
                try:
                    stat = self._client.stat_object(bucket, key)
                    self._write_object_row(bucket, key, getattr(stat, "etag", None),
                                           getattr(stat, "size", None),
                                           getattr(stat, "last_modified", None),
                                           getattr(stat, "metadata", None),
                                           tags)
                except Exception:
                    # If stat fails, still insert tags entry with minimal info
                    self._write_object_row(bucket, key, None, None, None, None, tags)
            else:
                self._update_tags_row(bucket, key, tags)
            # return entry
            row = self._get_row(bucket, key)
            return CachedObject(
                obj=_obj_from_row(row) if row else None,
                bucket=bucket,
                key=key,
                metadata=json.loads(row["metadata"]) if row and row["metadata"] else None,
                tags=tags,
                cached=True,
                cached_at=row["cached_at"] if row else _now(),
                age_seconds=0.0,
                stale=False,
                source="s3"
            )
        except Exception:
            # On failure, if cached exists return cached (maybe tags None)
            if row:
                tags = json.loads(row["tags"]) if row["tags"] else None
                cached_at = row["cached_at"]
                age = (_now() - cached_at) if cached_at else None
                stale = self._is_row_stale(row)
                return CachedObject(
                    obj=_obj_from_row(row),
                    bucket=bucket,
                    key=key,
                    metadata=json.loads(row["metadata"]) if row["metadata"] else None,
                    tags=tags,
                    cached=True if tags else False,
                    cached_at=cached_at,
                    age_seconds=age,
                    stale=stale,
                    source="cache" if tags else "none"
                )
            raise

    # -------------------------
    # Write operations (invalidate/update cache)
    # -------------------------
    def remove_object(self, bucket: str, key: str):
        res = self._client.remove_object(bucket, key)
        self._delete_object_row(bucket, key)
        return res

    def remove_objects(self, bucket: str, delete_list):
        # delete_list is an iterable of ObjectIdentifier-like items
        results = []
        for err in self._client.remove_objects(bucket, delete_list):
            results.append(err)
        # Remove from cache
        with self._lock, self._conn:
            cur = self._conn.cursor()
            for d in delete_list:
                # d may be a minio.datatypes.Object or object with object_name
                name = getattr(d, "object_name", None) or getattr(d, "object_name", None) or d
                cur.execute("DELETE FROM objects WHERE bucket=? AND key=?", (bucket, name))
            self._conn.commit()
        return results

    def copy_object(self, bucket: str, object_name: str, src: CopySource):
        res = self._client.copy_object(bucket, object_name, src)
        # Invalidate destination entry (we will re-cache on access)
        self._delete_object_row(bucket, object_name)
        return res

    def set_object_tags(self, bucket: str, key: str, tags: Dict[str, str]):
        res = self._client.set_object_tags(bucket, key, tags)
        # Update cache tags (and cached_at)
        self._update_tags_row(bucket, key, tags)
        return res

    # Optional: expose convenience method to force eviction / clear cache
    def clear_cache(self):
        with self._lock, self._conn:
            self._conn.execute("DELETE FROM objects")
            self._conn.execute("DELETE FROM buckets")
            self._conn.commit()

    # -------------------------
    # Pass-through for other methods not explicitly wrapped
    # -------------------------
    def __getattr__(self, name):
        """
        Forward any other attribute access to the underlying Minio client.
        """
        return getattr(self._client, name)