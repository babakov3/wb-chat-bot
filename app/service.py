"""Core service: multi-store polling of WB events, processing new chats, sending messages."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from app.categorizer import categorize_complaint
from app.config import Config
from app.storage import Storage
from app.telegram_client import TelegramClient
from app.wb_client import WBApiError, WBClient, WBClientPool

logger = logging.getLogger("wb_chat_bot")


class ChatService:
    def __init__(
        self,
        config: Config,
        storage: Storage,
        telegram: TelegramClient,
        wb_pool: WBClientPool,
    ) -> None:
        self.config = config
        self.storage = storage
        self.telegram = telegram
        self.wb_pool = wb_pool
        self.started_at: datetime = datetime.now(timezone.utc)
        self._running: bool = False
        self._poll_count: int = 0
        self._last_heartbeat: datetime = datetime.now(timezone.utc)

    async def run(self) -> None:
        """Main loop: run Telegram polling and WB polling in parallel."""
        self._running = True
        logger.info("Service starting (multi-store mode)")
        await asyncio.gather(
            self._telegram_loop(),
            self._wb_loop(),
        )

    async def _telegram_loop(self) -> None:
        """Fast loop for Telegram updates — 1 second interval for snappy UI."""
        while self._running:
            try:
                await self.telegram.poll_updates()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Telegram poll error: %s", exc)
            await asyncio.sleep(1)

    async def _wb_loop(self) -> None:
        """WB events polling loop — separate from Telegram for speed."""
        while self._running:
            try:
                await self._heartbeat()
                active_stores = self.storage.get_all_active_stores()
                for store in active_stores:
                    try:
                        await self._poll_store_events(store)
                    except Exception as exc:
                        logger.error(
                            "Error polling store %d (%s): %s",
                            store["id"], store["store_name"], exc,
                            exc_info=True,
                        )
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("WB poll error: %s", exc, exc_info=True)
                await asyncio.sleep(self.config.poll_interval_seconds * 2)

            await asyncio.sleep(self.config.poll_interval_seconds)

    async def stop(self) -> None:
        """Graceful shutdown."""
        self._running = False
        logger.info("Service stopping...")

    # ── Per-store event polling ──────────────────────────────────────────

    async def _poll_store_events(self, store: dict[str, Any]) -> None:
        """Fetch new events from WB for a single store and process them."""
        store_id = store["id"]
        chat_id = store["user_chat_id"]
        api_token = store["wb_api_token"]

        wb = self.wb_pool.get(store_id, api_token)
        cursor = self.storage.get_cursor_for_store(store_id)

        # Initialize cursor on first poll
        if cursor is None:
            await self._init_cursor_for_store(store, wb)
            return

        try:
            data = await wb.get_chat_events(next_cursor=cursor)
        except WBApiError as exc:
            if exc.status_code in (401, 403):
                await self.telegram.notify(
                    chat_id,
                    f"[{store['store_name']}] WB токен невалиден ({exc.status_code}). "
                    "Магазин приостановлен. Обновите токен в настройках.",
                )
                self.storage.update_store(store_id, is_active=0)
                await self.wb_pool.remove(store_id)
            elif exc.status_code == 429:
                logger.warning("Store %d rate limited, will retry next cycle", store_id)
            else:
                logger.error("WB API error for store %d: %s", store_id, exc)
            return
        except Exception as exc:
            logger.error("WB request failed for store %d: %s", store_id, exc)
            return

        events = data.get("events", [])
        next_cursor = data.get("next")

        if next_cursor is not None:
            next_cursor_int = int(next_cursor)
            if cursor is None or next_cursor_int != cursor:
                self.storage.save_cursor_for_store(store_id, next_cursor_int)

        self._poll_count += 1
        if events:
            logger.debug("Store %d: %d events", store_id, len(events))

        # Process new chat events
        new_chat_tasks = []
        for event in events:
            if self._is_new_chat_event(event):
                wb_chat_id = self._extract_chat_id(event)
                if wb_chat_id and not self.storage.is_chat_processed(wb_chat_id, store_id):
                    if not self._passes_filters(event, wb_chat_id, store):
                        continue
                    new_chat_tasks.append(
                        self._handle_new_chat(event, wb_chat_id, store, wb)
                    )

        if new_chat_tasks:
            await asyncio.gather(*new_chat_tasks, return_exceptions=True)

    async def _init_cursor_for_store(self, store: dict[str, Any], wb: WBClient) -> None:
        """Initialize cursor for a store — ALWAYS set to current timestamp.

        CRITICAL: Never read old events on first init. This prevents
        sending messages to old/historical chats. Only future events
        (after this moment) will be processed.
        """
        store_id = store["id"]
        ts = WBClient.current_timestamp_ms()
        self.storage.save_cursor_for_store(store_id, ts)
        logger.info(
            "Store %d: cursor initialized to NOW (%d). Only future events will be processed.",
            store_id, ts,
        )
        # No user-facing message — the onboarding completion message is enough

    # ── Event field extraction ──────────────────────────────────────────

    def _is_new_chat_event(self, event: dict[str, Any]) -> bool:
        if event.get("isNewChat"):
            return True
        payload = event.get("payload", event.get("data", {}))
        if isinstance(payload, dict) and payload.get("isNewChat"):
            return True
        return False

    def _extract_chat_id(self, event: dict[str, Any]) -> str | None:
        chat_id = event.get("chatID") or event.get("chatId") or event.get("chat_id")
        if chat_id:
            return str(chat_id)
        payload = event.get("payload", event.get("data", {}))
        if isinstance(payload, dict):
            cid = payload.get("chatID") or payload.get("chatId") or payload.get("chat_id")
            if cid:
                return str(cid)
        return None

    def _extract_reply_sign(self, event: dict[str, Any]) -> str | None:
        rs = event.get("replySign") or event.get("reply_sign")
        if rs:
            return str(rs)
        payload = event.get("payload", event.get("data", {}))
        if isinstance(payload, dict):
            rs = payload.get("replySign") or payload.get("reply_sign")
            if rs:
                return str(rs)
        return None

    def _extract_event_id(self, event: dict[str, Any]) -> str | None:
        eid = (
            event.get("eventID")
            or event.get("eventId")
            or event.get("id")
            or event.get("event_id")
        )
        return str(eid) if eid else None

    def _extract_nm_id(self, event: dict[str, Any]) -> int | None:
        for key in ("nmID", "nmId", "nm_id"):
            val = event.get(key)
            if val is not None:
                try:
                    return int(val)
                except (ValueError, TypeError):
                    pass
        payload = event.get("payload", event.get("data", {}))
        if isinstance(payload, dict):
            for key in ("nmID", "nmId", "nm_id"):
                val = payload.get(key)
                if val is not None:
                    try:
                        return int(val)
                    except (ValueError, TypeError):
                        pass
        return None

    def _extract_rating(self, event: dict[str, Any]) -> int | None:
        for key in ("rating", "valuation", "grade"):
            val = event.get(key)
            if val is not None:
                try:
                    return int(val)
                except (ValueError, TypeError):
                    pass
        payload = event.get("payload", event.get("data", {}))
        if isinstance(payload, dict):
            for key in ("rating", "valuation", "grade"):
                val = payload.get(key)
                if val is not None:
                    try:
                        return int(val)
                    except (ValueError, TypeError):
                        pass
        return None

    def _extract_client_name(self, event: dict[str, Any]) -> str | None:
        name = event.get("clientName") or event.get("client_name")
        if name:
            return str(name)
        payload = event.get("payload", event.get("data", {}))
        if isinstance(payload, dict):
            name = payload.get("clientName") or payload.get("client_name")
            if name:
                return str(name)
        return None

    def _extract_client_message(self, event: dict[str, Any]) -> str | None:
        msg = event.get("message", {})
        if isinstance(msg, dict):
            text = msg.get("text")
            if text:
                return str(text)
        payload = event.get("payload", event.get("data", {}))
        if isinstance(payload, dict):
            msg = payload.get("message", {})
            if isinstance(msg, dict):
                text = msg.get("text")
                if text:
                    return str(text)
        return None

    def _extract_product_name(self, event: dict[str, Any]) -> str | None:
        msg = event.get("message", {})
        if isinstance(msg, dict):
            att = msg.get("attachments", {})
            if isinstance(att, dict):
                gc = att.get("goodCard", {})
                if isinstance(gc, dict):
                    name = gc.get("name")
                    if name:
                        return str(name)
        return None

    # ── Filtering (per store) ───────────────────────────────────────────

    def _passes_filters(self, event: dict[str, Any], wb_chat_id: str, store: dict[str, Any]) -> bool:
        """Check if event passes the store's product whitelist filter."""
        wl_str = store.get("product_whitelist", "")
        if not wl_str:
            return True  # no filter = process all

        whitelist: set[int] = set()
        for part in wl_str.split(","):
            part = part.strip()
            if part:
                try:
                    whitelist.add(int(part))
                except ValueError:
                    pass

        if not whitelist:
            return True

        nm_id = self._extract_nm_id(event)
        if nm_id is None:
            logger.info("Chat %s (store %d): skipped — product filter active but no nmID", wb_chat_id, store["id"])
            return False
        if nm_id not in whitelist:
            logger.info("Chat %s (store %d): skipped — product %d not in whitelist", wb_chat_id, store["id"], nm_id)
            return False

        return True

    # ── New chat handling ───────────────────────────────────────────────

    async def _handle_new_chat(
        self, event: dict[str, Any], wb_chat_id: str,
        store: dict[str, Any], wb: WBClient,
    ) -> None:
        """Process a single new chat for a store."""
        store_id = store["id"]
        user_chat_id = store["user_chat_id"]

        event_id = self._extract_event_id(event)
        reply_sign = self._extract_reply_sign(event)
        nm_id = self._extract_nm_id(event)
        rating = self._extract_rating(event)
        client_name = self._extract_client_name(event)
        client_message = self._extract_client_message(event)
        product_name = self._extract_product_name(event)

        # Try to get product name from store_products if not in event
        if not product_name and nm_id:
            product_name = self.storage.get_store_product_name(store_id, nm_id)

        complaint_category = categorize_complaint(client_message)

        extra = {
            "nm_id": nm_id,
            "product_name": product_name,
            "client_name": client_name,
            "client_message": client_message,
            "complaint_category": complaint_category,
            "rating": rating,
        }

        logger.info(
            "Store %d: new chat %s | %s | nmID=%s | rating=%s | category=%s",
            store_id, wb_chat_id, client_name, nm_id, rating, complaint_category,
        )

        # Apply delay
        await asyncio.sleep(self.config.new_chat_delay_seconds)

        # Double-check dedup
        if self.storage.is_chat_processed(wb_chat_id, store_id):
            logger.info("Chat %s (store %d) already processed during delay", wb_chat_id, store_id)
            return

        is_dry_run = store["app_mode"] != "production"
        message_text = store["message_text"]

        if is_dry_run:
            self.storage.save_chat(
                chat_id=wb_chat_id,
                store_id=store_id,
                first_event_id=event_id,
                reply_sign=reply_sign,
                status="dry-run",
                sent_message_text=message_text,
                **extra,
            )
            name = extra.get("client_name") or "?"
            cat = extra.get("complaint_category") or ""
            nm = extra.get("nm_id") or ""
            await self.telegram.notify(
                user_chat_id,
                f"[{store['store_name']}] <b>[ТЕСТ] Новый негатив</b>\n"
                f"Клиент: {name} | Артикул: {nm}\n"
                f"Категория: {cat}\n"
                f"Отправили бы: <code>{message_text[:50]}</code>"
            )
        else:
            await self._send_production_message(
                wb_chat_id, event_id, reply_sign, message_text,
                store, wb, extra,
            )

    async def _send_production_message(
        self,
        wb_chat_id: str,
        event_id: str | None,
        reply_sign: str | None,
        message_text: str,
        store: dict[str, Any],
        wb: WBClient,
        extra: dict[str, Any],
    ) -> None:
        """Send message in production mode."""
        store_id = store["id"]
        user_chat_id = store["user_chat_id"]
        store_name = store["store_name"]

        if not reply_sign:
            logger.warning(
                "Store %d: no replySign for chat %s, fetching from chats list",
                store_id, wb_chat_id,
            )
            reply_sign = await self._fetch_reply_sign(wb, wb_chat_id)

        if not reply_sign:
            error_msg = f"[{store_name}] Не удалось отправить в чат: нет replySign"
            logger.error(error_msg)
            self.storage.save_chat(
                chat_id=wb_chat_id,
                store_id=store_id,
                first_event_id=event_id,
                reply_sign=None,
                status="error",
                error_text="no replySign",
                **extra,
            )
            await self.telegram.notify(user_chat_id, error_msg)
            return

        try:
            wb_response = await wb.send_message(
                chat_id=wb_chat_id,
                reply_sign=reply_sign,
                message_text=message_text,
            )
            logger.info("Store %d: message sent to chat %s", store_id, wb_chat_id)
            self.storage.save_chat(
                chat_id=wb_chat_id,
                store_id=store_id,
                first_event_id=event_id,
                reply_sign=reply_sign,
                status="sent",
                sent_message_text=message_text,
                wb_response=wb_response,
                **extra,
            )
            name = extra.get("client_name") or "?"
            cat = extra.get("complaint_category") or ""
            nm = extra.get("nm_id") or ""
            await self.telegram.notify(
                user_chat_id,
                f"[{store_name}] <b>Сообщение отправлено</b>\n"
                f"Клиент: {name} | Артикул: {nm}\n"
                f"Категория: {cat}"
            )
        except WBApiError as exc:
            logger.error("Store %d: send failed for chat %s: %s", store_id, wb_chat_id, exc)
            self.storage.save_chat(
                chat_id=wb_chat_id,
                store_id=store_id,
                first_event_id=event_id,
                reply_sign=reply_sign,
                status="error",
                sent_message_text=message_text,
                error_text=str(exc),
                **extra,
            )
            await self.telegram.notify(
                user_chat_id,
                f"[{store_name}] Ошибка отправки: {exc}"
            )
        except Exception as exc:
            logger.error("Store %d: unexpected send error for %s: %s", store_id, wb_chat_id, exc)
            self.storage.save_chat(
                chat_id=wb_chat_id,
                store_id=store_id,
                first_event_id=event_id,
                reply_sign=reply_sign,
                status="error",
                error_text=str(exc),
                **extra,
            )
            await self.telegram.notify(
                user_chat_id,
                f"[{store_name}] Непредвиденная ошибка: {exc}"
            )

    async def _fetch_reply_sign(self, wb: WBClient, wb_chat_id: str) -> str | None:
        """Try to get replySign from the chats list endpoint."""
        try:
            chats = await wb.get_chats_list()
            for chat in chats:
                cid = str(chat.get("chatID") or chat.get("chatId") or chat.get("chat_id", ""))
                if cid == wb_chat_id:
                    return chat.get("replySign") or chat.get("reply_sign")
        except Exception as exc:
            logger.error("Failed to fetch replySign for %s: %s", wb_chat_id, exc)
        return None

    async def _heartbeat(self) -> None:
        """Log heartbeat periodically."""
        now = datetime.now(timezone.utc)
        delta = (now - self._last_heartbeat).total_seconds()
        interval = self.config.heartbeat_interval_minutes * 60
        if delta >= interval:
            active_count = len(self.storage.get_all_active_stores())
            logger.info(
                "Heartbeat | active_stores=%d | polls=%d",
                active_count, self._poll_count,
            )
            self._last_heartbeat = now
