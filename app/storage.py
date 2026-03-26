"""SQLite storage: stores, products, chats, user state, analytics."""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger("wb_chat_bot")

# ── Table definitions ────────────────────────────────────────────────

TABLES = [
    """
    CREATE TABLE IF NOT EXISTS stores (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_chat_id TEXT NOT NULL,
        store_name TEXT NOT NULL,
        wb_api_token TEXT NOT NULL,
        wb_content_token TEXT NOT NULL DEFAULT '',
        message_text TEXT NOT NULL DEFAULT '',
        product_whitelist TEXT NOT NULL DEFAULT '',
        app_mode TEXT NOT NULL DEFAULT 'dry-run',
        is_active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        UNIQUE(user_chat_id, store_name)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS store_products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        store_id INTEGER NOT NULL,
        nm_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(store_id, nm_id),
        FOREIGN KEY(store_id) REFERENCES stores(id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS user_state (
        chat_id TEXT PRIMARY KEY,
        active_store_id INTEGER,
        onboarding_step TEXT,
        onboarding_data TEXT,
        input_waiting TEXT,
        menu_message_id INTEGER
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS processed_chats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        store_id INTEGER,
        chat_id TEXT NOT NULL,
        first_event_id TEXT,
        reply_sign TEXT,
        status TEXT NOT NULL DEFAULT 'pending',
        sent_message_text TEXT,
        processed_at TEXT NOT NULL,
        wb_response TEXT,
        error_text TEXT,
        nm_id INTEGER,
        product_name TEXT,
        client_name TEXT,
        client_message TEXT,
        complaint_category TEXT,
        rating INTEGER,
        UNIQUE(store_id, chat_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS key_value (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS notified_events (
        event_id TEXT NOT NULL,
        store_id INTEGER NOT NULL,
        notified_at TEXT NOT NULL,
        UNIQUE(event_id, store_id)
    )
    """,
]


class Storage:
    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._init_tables()
        logger.info("Storage initialized: %s", db_path)

    def _init_tables(self) -> None:
        with self._conn:
            for sql in TABLES:
                self._conn.execute(sql)
            self._migrate()

    def _migrate(self) -> None:
        """Run lightweight migrations for schema changes."""
        # Add store_id to processed_chats if missing
        cols = {
            r[1]
            for r in self._conn.execute("PRAGMA table_info(processed_chats)").fetchall()
        }
        if "store_id" not in cols:
            self._conn.execute(
                "ALTER TABLE processed_chats ADD COLUMN store_id INTEGER DEFAULT 1"
            )
            logger.info("Migration: added store_id to processed_chats")

        # Add notification_group_id to stores if missing
        store_cols = {
            r[1]
            for r in self._conn.execute("PRAGMA table_info(stores)").fetchall()
        }
        if "notification_group_id" not in store_cols:
            self._conn.execute(
                "ALTER TABLE stores ADD COLUMN notification_group_id TEXT DEFAULT ''"
            )
            logger.info("Migration: added notification_group_id to stores")
        if "notification_thread_id" not in store_cols:
            self._conn.execute(
                "ALTER TABLE stores ADD COLUMN notification_thread_id TEXT DEFAULT ''"
            )
            logger.info("Migration: added notification_thread_id to stores")

    # ── Stores ───────────────────────────────────────────────────

    def create_store(
        self,
        user_chat_id: str,
        store_name: str,
        wb_api_token: str,
        wb_content_token: str = "",
        message_text: str = "",
        product_whitelist: str = "",
        app_mode: str = "dry-run",
    ) -> int:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn:
            cursor = self._conn.execute(
                """
                INSERT INTO stores
                    (user_chat_id, store_name, wb_api_token, wb_content_token,
                     message_text, product_whitelist, app_mode, is_active, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)
                """,
                (user_chat_id, store_name, wb_api_token, wb_content_token,
                 message_text, product_whitelist, app_mode, now),
            )
            return cursor.lastrowid  # type: ignore

    def get_store(self, store_id: int) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT * FROM stores WHERE id = ?", (store_id,)
        ).fetchone()
        return dict(row) if row else None

    def get_stores_for_user(self, user_chat_id: str) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM stores WHERE user_chat_id = ? ORDER BY id",
            (user_chat_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_all_active_stores(self) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM stores WHERE is_active = 1"
        ).fetchall()
        return [dict(r) for r in rows]

    def update_store(self, store_id: int, **fields: Any) -> None:
        if not fields:
            return
        sets = ", ".join(f"{k} = ?" for k in fields)
        vals = list(fields.values()) + [store_id]
        with self._conn:
            self._conn.execute(f"UPDATE stores SET {sets} WHERE id = ?", vals)

    def delete_store(self, store_id: int) -> None:
        with self._conn:
            self._conn.execute("DELETE FROM store_products WHERE store_id = ?", (store_id,))
            self._conn.execute("DELETE FROM processed_chats WHERE store_id = ?", (store_id,))
            self._conn.execute("DELETE FROM key_value WHERE key = ?", (f"cursor:{store_id}",))
            self._conn.execute("DELETE FROM stores WHERE id = ?", (store_id,))

    def count_stores_for_user(self, user_chat_id: str) -> int:
        row = self._conn.execute(
            "SELECT COUNT(*) as cnt FROM stores WHERE user_chat_id = ?",
            (user_chat_id,),
        ).fetchone()
        return row["cnt"] if row else 0

    # ── Store Products ───────────────────────────────────────────

    def save_store_products(self, store_id: int, products: list[dict[str, Any]]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn:
            for p in products:
                self._conn.execute(
                    """
                    INSERT INTO store_products (store_id, nm_id, name, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(store_id, nm_id) DO UPDATE SET
                        name = excluded.name, updated_at = excluded.updated_at
                    """,
                    (store_id, p["nm_id"], p["name"], now),
                )

    def get_store_products(self, store_id: int) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT nm_id, name FROM store_products WHERE store_id = ? ORDER BY name",
            (store_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_store_product_name(self, store_id: int, nm_id: int) -> str | None:
        row = self._conn.execute(
            "SELECT name FROM store_products WHERE store_id = ? AND nm_id = ?",
            (store_id, nm_id),
        ).fetchone()
        return row["name"] if row else None

    # ── User State ───────────────────────────────────────────────

    def get_user_state(self, chat_id: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT * FROM user_state WHERE chat_id = ?", (chat_id,)
        ).fetchone()
        return dict(row) if row else None

    def set_user_state(self, chat_id: str, **fields: Any) -> None:
        existing = self.get_user_state(chat_id)
        if existing:
            if not fields:
                return
            sets = ", ".join(f"{k} = ?" for k in fields)
            vals = list(fields.values()) + [chat_id]
            with self._conn:
                self._conn.execute(f"UPDATE user_state SET {sets} WHERE chat_id = ?", vals)
        else:
            fields["chat_id"] = chat_id
            cols = ", ".join(fields.keys())
            placeholders = ", ".join("?" for _ in fields)
            with self._conn:
                self._conn.execute(
                    f"INSERT INTO user_state ({cols}) VALUES ({placeholders})",
                    list(fields.values()),
                )

    def get_active_store_id(self, chat_id: str) -> int | None:
        state = self.get_user_state(chat_id)
        return state["active_store_id"] if state else None

    def set_active_store(self, chat_id: str, store_id: int) -> None:
        self.set_user_state(chat_id, active_store_id=store_id)

    # ── Cursors (per store) ──────────────────────────────────────

    def get_cursor_for_store(self, store_id: int) -> int | None:
        row = self._conn.execute(
            "SELECT value FROM key_value WHERE key = ?",
            (f"cursor:{store_id}",),
        ).fetchone()
        if row is None:
            return None
        try:
            return int(row["value"])
        except (ValueError, TypeError):
            return None

    def save_cursor_for_store(self, store_id: int, cursor: int) -> None:
        now = datetime.now(timezone.utc).isoformat()
        key = f"cursor:{store_id}"
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO key_value (key, value, updated_at) VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
                """,
                (key, str(cursor), now),
            )

    # ── Generic key-value ────────────────────────────────────────

    def get_kv(self, key: str) -> str | None:
        row = self._conn.execute(
            "SELECT value FROM key_value WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row else None

    def set_kv(self, key: str, value: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO key_value (key, value, updated_at) VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
                """,
                (key, value, now),
            )

    # ── Notified Events ─────────────────────────────────────────

    def is_event_notified(self, event_id: str, store_id: int) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM notified_events WHERE event_id = ? AND store_id = ?",
            (event_id, store_id),
        ).fetchone()
        return row is not None

    def mark_event_notified(self, event_id: str, store_id: int) -> None:
        now = datetime.now(timezone.utc).isoformat()
        try:
            with self._conn:
                self._conn.execute(
                    "INSERT OR IGNORE INTO notified_events (event_id, store_id, notified_at) VALUES (?, ?, ?)",
                    (event_id, store_id, now),
                )
        except Exception:
            pass

    # ── Processed Chats ──────────────────────────────────────────

    def is_chat_processed(self, chat_id: str, store_id: int) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM processed_chats WHERE chat_id = ? AND store_id = ?",
            (chat_id, store_id),
        ).fetchone()
        return row is not None

    def reserve_chat(self, chat_id: str, store_id: int, **kwargs: Any) -> bool:
        """Atomically reserve a chat for processing. Returns True if reserved, False if already exists."""
        now = datetime.now(timezone.utc).isoformat()
        try:
            with self._conn:
                self._conn.execute(
                    """INSERT INTO processed_chats
                        (store_id, chat_id, first_event_id, reply_sign, status, processed_at,
                         nm_id, product_name, client_name, client_message, rating)
                    VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?)""",
                    (store_id, chat_id, kwargs.get("first_event_id"),
                     kwargs.get("reply_sign"), now,
                     kwargs.get("nm_id"), kwargs.get("product_name"),
                     kwargs.get("client_name"), kwargs.get("client_message"),
                     kwargs.get("rating")),
                )
            return True
        except sqlite3.IntegrityError:
            return False

    def save_chat(
        self,
        chat_id: str,
        store_id: int,
        first_event_id: str | None = None,
        reply_sign: str | None = None,
        status: str = "pending",
        sent_message_text: str | None = None,
        wb_response: dict[str, Any] | None = None,
        error_text: str | None = None,
        nm_id: int | None = None,
        product_name: str | None = None,
        client_name: str | None = None,
        client_message: str | None = None,
        complaint_category: str | None = None,
        rating: int | None = None,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        wb_resp_str = json.dumps(wb_response, ensure_ascii=False) if wb_response else None
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO processed_chats
                    (store_id, chat_id, first_event_id, reply_sign, status,
                     sent_message_text, processed_at, wb_response, error_text,
                     nm_id, product_name, client_name, client_message,
                     complaint_category, rating)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(store_id, chat_id) DO UPDATE SET
                    status = excluded.status,
                    sent_message_text = excluded.sent_message_text,
                    wb_response = excluded.wb_response,
                    error_text = excluded.error_text,
                    processed_at = excluded.processed_at,
                    nm_id = COALESCE(excluded.nm_id, nm_id),
                    product_name = COALESCE(excluded.product_name, product_name),
                    client_name = COALESCE(excluded.client_name, client_name),
                    client_message = COALESCE(excluded.client_message, client_message),
                    complaint_category = COALESCE(excluded.complaint_category, complaint_category),
                    rating = COALESCE(excluded.rating, rating)
                """,
                (store_id, chat_id, first_event_id, reply_sign, status,
                 sent_message_text, now, wb_resp_str, error_text,
                 nm_id, product_name, client_name, client_message,
                 complaint_category, rating),
            )

    def get_last_error(self, store_id: int) -> str | None:
        """Get last error text for a store, or None."""
        row = self._conn.execute(
            "SELECT error_text FROM processed_chats WHERE store_id = ? AND error_text IS NOT NULL "
            "ORDER BY processed_at DESC LIMIT 1",
            (store_id,),
        ).fetchone()
        return row["error_text"] if row else None

    # ── Analytics ────────────────────────────────────────────────

    def get_stats(self, store_id: int) -> dict[str, int]:
        rows = self._conn.execute(
            "SELECT status, COUNT(*) as cnt FROM processed_chats WHERE store_id = ? GROUP BY status",
            (store_id,),
        ).fetchall()
        stats = {row["status"]: row["cnt"] for row in rows}
        stats["total"] = sum(stats.values())
        return stats

    def get_complaints_by_product(self, store_id: int) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT nm_id, product_name, COUNT(*) as cnt, AVG(rating) as avg_rating
            FROM processed_chats
            WHERE store_id = ? AND nm_id IS NOT NULL
            GROUP BY nm_id ORDER BY cnt DESC LIMIT 10
            """,
            (store_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_complaints_by_category(self, store_id: int) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """
            SELECT complaint_category, COUNT(*) as cnt
            FROM processed_chats
            WHERE store_id = ? AND complaint_category IS NOT NULL AND complaint_category != ''
            GROUP BY complaint_category ORDER BY cnt DESC
            """,
            (store_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_last_chats(self, store_id: int, limit: int = 5) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """SELECT chat_id, status, sent_message_text, processed_at, error_text,
                      nm_id, product_name, client_name, client_message, complaint_category, rating
            FROM processed_chats WHERE store_id = ? ORDER BY id DESC LIMIT ?""",
            (store_id, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    # ── Legacy Migration ─────────────────────────────────────────

    def run_legacy_migration(
        self,
        legacy_chat_id: str,
        legacy_wb_api_token: str,
        legacy_wb_content_token: str,
        legacy_message_text: str,
        legacy_product_whitelist: str,
        legacy_app_mode: str,
    ) -> None:
        """Migrate single-tenant data to multi-store schema (runs once)."""
        # Only migrate if no stores exist yet and we have legacy data
        if not legacy_wb_api_token or not legacy_chat_id:
            return

        existing = self.get_stores_for_user(legacy_chat_id)
        if existing:
            return  # already migrated

        logger.info("Running legacy migration for chat_id=%s", legacy_chat_id)

        # Check for overrides in old settings keys
        msg = self.get_kv("settings:message_text") or legacy_message_text
        wl = self.get_kv("settings:product_whitelist") or legacy_product_whitelist
        mode = self.get_kv("settings:app_mode") or legacy_app_mode

        store_id = self.create_store(
            user_chat_id=legacy_chat_id,
            store_name="Мой магазин",
            wb_api_token=legacy_wb_api_token,
            wb_content_token=legacy_wb_content_token,
            message_text=msg,
            product_whitelist=wl,
            app_mode=mode,
        )

        # Migrate products
        try:
            rows = self._conn.execute("SELECT nm_id, name FROM products").fetchall()
            if rows:
                now = datetime.now(timezone.utc).isoformat()
                for r in rows:
                    self._conn.execute(
                        """
                        INSERT OR IGNORE INTO store_products (store_id, nm_id, name, updated_at)
                        VALUES (?, ?, ?, ?)
                        """,
                        (store_id, r["nm_id"], r["name"], now),
                    )
                self._conn.commit()
        except sqlite3.OperationalError:
            pass  # old products table doesn't exist

        # Migrate cursor
        old_cursor = self.get_kv("next_cursor")
        if old_cursor:
            self.save_cursor_for_store(store_id, int(old_cursor))

        # Set active store for user
        self.set_active_store(legacy_chat_id, store_id)

        # Update old processed_chats with store_id
        try:
            with self._conn:
                self._conn.execute(
                    "UPDATE processed_chats SET store_id = ? WHERE store_id IS NULL",
                    (store_id,),
                )
        except sqlite3.OperationalError:
            pass

        logger.info("Legacy migration complete: store_id=%d", store_id)

    def close(self) -> None:
        self._conn.close()
