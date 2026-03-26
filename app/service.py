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
        self._store_semaphores: dict[int, asyncio.Semaphore] = {}

    def _get_semaphore(self, store_id: int) -> asyncio.Semaphore:
        if store_id not in self._store_semaphores:
            self._store_semaphores[store_id] = asyncio.Semaphore(3)
        return self._store_semaphores[store_id]

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

    async def _notify(self, store: dict[str, Any], text: str, group: bool = True) -> None:
        """Send notification to user + group (if linked and group=True)."""
        await self.telegram.notify(store["user_chat_id"], text)
        if group:
            group_id = store.get("notification_group_id") or ""
            if group_id:
                thread_id_str = store.get("notification_thread_id") or ""
                thread_id = int(thread_id_str) if thread_id_str else None
                try:
                    await self.telegram.notify(str(group_id), text, message_thread_id=thread_id)
                except Exception as exc:
                    logger.warning("Failed to notify group %s: %s", group_id, exc)

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

        # Process new chat events with atomic reservation
        sem = self._get_semaphore(store_id)
        new_chat_tasks = []
        for event in events:
            if self._is_new_chat_event(event):
                wb_chat_id = self._extract_chat_id(event)
                if not wb_chat_id:
                    continue
                if not self._passes_filters(event, wb_chat_id, store):
                    continue
                # Atomic reservation — prevents race conditions
                event_id = self._extract_event_id(event)
                reply_sign = self._extract_reply_sign(event)
                nm_id = self._extract_nm_id(event)
                reserved = self.storage.reserve_chat(
                    wb_chat_id, store_id,
                    first_event_id=event_id, reply_sign=reply_sign, nm_id=nm_id,
                    product_name=self._extract_product_name(event),
                    client_name=self._extract_client_name(event),
                    client_message=self._extract_client_message(event),
                    rating=self._extract_rating(event),
                )
                if not reserved:
                    continue  # already being processed or done
                new_chat_tasks.append(
                    self._handle_reserved_chat(event, wb_chat_id, store, wb, sem)
                )

        if new_chat_tasks:
            await asyncio.gather(*new_chat_tasks, return_exceptions=True)

        # Notify about client messages (new inquiries + replies to our messages)
        for event in events:
            if self._is_new_chat_event(event):
                continue  # already handled above
            sender = event.get("sender", "")
            if sender != "client":
                continue
            wb_chat_id = self._extract_chat_id(event)
            if not wb_chat_id:
                continue
            event_id = self._extract_event_id(event)
            if not event_id:
                continue
            # Deduplicate notifications by event_id
            if self.storage.is_event_notified(event_id, store_id):
                continue
            self.storage.mark_event_notified(event_id, store_id)

            client_name = self._extract_client_name(event) or "?"
            client_message = self._extract_client_message(event) or ""
            msg_preview = client_message[:100] + "..." if len(client_message) > 100 else client_message
            nm_id = self._extract_nm_id(event)
            product_name = self._extract_product_name(event)
            if not product_name and nm_id:
                product_name = self.storage.get_store_product_name(store_id, nm_id)

            # Check if this chat was already processed (reply to our message)
            is_reply = self.storage.is_chat_processed(wb_chat_id, store_id)

            if is_reply:
                await self._notify(
                    store,
                    f"[{store['store_name']}] 💬 <b>Клиент ответил</b>\n"
                    f"👤 {client_name}\n"
                    f"📝 <i>{msg_preview}</i>"
                )
            else:
                prod_label = f"📦 {nm_id} — {product_name}" if nm_id else ""
                await self._notify(
                    store,
                    f"[{store['store_name']}] 📩 <b>Новое сообщение</b>\n"
                    f"👤 {client_name}\n"
                    f"{prod_label}\n"
                    f"📝 <i>{msg_preview}</i>"
                )

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
        # Check top-level
        for key in ("nmID", "nmId", "nm_id"):
            val = event.get(key)
            if val is not None:
                try:
                    return int(val)
                except (ValueError, TypeError):
                    pass
        # Check inside message.attachments.goodCard (WB actual format)
        msg = event.get("message", {})
        if isinstance(msg, dict):
            att = msg.get("attachments", {})
            if isinstance(att, dict):
                gc = att.get("goodCard", {})
                if isinstance(gc, dict):
                    val = gc.get("nmID") or gc.get("nmId")
                    if val is not None:
                        try:
                            return int(val)
                        except (ValueError, TypeError):
                            pass
        # Check payload fallback
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

    # ── Review chat detection ────────────────────────────────────────────

    # Keywords that indicate WB auto-message about a review
    _REVIEW_KEYWORDS = (
        "отзыв", "оценк", "низкой оценк", "оставили отзыв",
        "негативн", "оценили", "поставили оценку",
    )

    async def _is_review_chat(
        self, wb_chat_id: str, store: dict[str, Any], wb: WBClient
    ) -> bool:
        """Check if a chat was triggered by a negative review.

        Verifies WB's auto-message about a review exists.
        Checks both chats list (lastMessage) and recent events for seller messages
        containing review keywords. Only returns True if confirmed.
        """
        try:
            # Method 1: Check chats list — lastMessage
            chats = await wb.get_chats_list()
            for chat in chats:
                cid = chat.get("chatID") or chat.get("chatId")
                if cid != wb_chat_id:
                    continue

                last_msg = chat.get("lastMessage", {})
                last_text = (last_msg.get("text") or "").lower()
                for kw in self._REVIEW_KEYWORDS:
                    if kw in last_text:
                        logger.info("Chat %s: review confirmed via lastMessage", wb_chat_id)
                        return True
                break  # found our chat, no keyword match

            # Method 2: Check recent events — look for seller message with review keywords
            cursor = self.storage.get_cursor_for_store(store["id"])
            if cursor:
                # Go back 60 seconds to catch WB auto-message
                check_cursor = max(0, cursor - 60000)
                try:
                    data = await wb.get_chat_events(next_cursor=check_cursor)
                    events = data.get("events", [])
                    for ev in events:
                        ev_chat = ev.get("chatID") or ev.get("chatId")
                        if ev_chat != wb_chat_id:
                            continue
                        ev_sender = ev.get("sender", "")
                        if ev_sender in ("seller", "system", "auto"):
                            ev_text = ""
                            msg = ev.get("message", {})
                            if isinstance(msg, dict):
                                ev_text = (msg.get("text") or "").lower()
                            for kw in self._REVIEW_KEYWORDS:
                                if kw in ev_text:
                                    logger.info("Chat %s: review confirmed via seller event", wb_chat_id)
                                    return True
                except Exception as exc:
                    logger.warning("Events check failed for chat %s: %s", wb_chat_id, exc)

        except Exception as exc:
            logger.warning("Could not verify review chat %s: %s", wb_chat_id, exc)

        logger.info("Chat %s: NOT a review chat — no WB auto-message found", wb_chat_id)
        return False

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

    async def _handle_reserved_chat(
        self, event: dict[str, Any], wb_chat_id: str,
        store: dict[str, Any], wb: WBClient,
        sem: asyncio.Semaphore,
    ) -> None:
        """Process a reserved chat with semaphore limiting."""
        async with sem:
            await self._process_chat(event, wb_chat_id, store, wb)

    async def _process_chat(
        self, event: dict[str, Any], wb_chat_id: str,
        store: dict[str, Any], wb: WBClient,
    ) -> None:
        """Process a single new chat for a store (already reserved in DB)."""
        store_id = store["id"]
        user_chat_id = store["user_chat_id"]

        event_id = self._extract_event_id(event)
        reply_sign = self._extract_reply_sign(event)
        nm_id = self._extract_nm_id(event)
        rating = self._extract_rating(event)
        client_name = self._extract_client_name(event)
        client_message = self._extract_client_message(event)
        product_name = self._extract_product_name(event)

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

        # Quick check: if sender is "client" and message has no review keywords,
        # this is likely a regular customer inquiry, not a review chat
        sender = event.get("sender", "")
        event_text = (client_message or "").lower()
        if sender == "client":
            has_review_keyword = any(kw in event_text for kw in self._REVIEW_KEYWORDS)
            if not has_review_keyword:
                logger.info(
                    "Store %d: chat %s — client-initiated, no review keywords in message. Checking WB auto-message...",
                    store_id, wb_chat_id,
                )

        # Wait for WB auto-message to arrive
        await asyncio.sleep(self.config.new_chat_delay_seconds)

        # Verify WB sent its auto-message about a review
        if not await self._is_review_chat(wb_chat_id, store, wb):
            logger.info(
                "Store %d: chat %s skipped — not a review chat (no WB auto-message found)",
                store_id, wb_chat_id,
            )
            # Update status to skipped so we don't reprocess
            self.storage.save_chat(
                chat_id=wb_chat_id, store_id=store_id,
                status="skipped", **extra,
            )
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
            name = client_name or "?"
            cat = complaint_category or "💬 Другое"
            nm = nm_id or ""
            prod = product_name or ""
            await self._notify(
                store,
                f"[{store['store_name']}] 🟡 <b>[ТЕСТ] Негатив</b>\n"
                f"👤 {name}\n"
                f"📦 {nm} — {prod}\n"
                f"{cat}",
                group=False,
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
            nm = extra.get("nm_id") or ""
            prod = extra.get("product_name") or ""
            cat = extra.get("complaint_category") or "💬 Другое"
            await self._notify(
                store,
                f"[{store_name}] ✅ <b>Отправлено</b>\n"
                f"👤 {name}\n"
                f"📦 {nm} — {prod}\n"
                f"{cat}",
                group=False,
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
            await self._notify(store, f"[{store_name}] ❌ Ошибка отправки: {exc}", group=False)
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
            await self._notify(store, f"[{store_name}] ❌ Ошибка: {exc}", group=False)

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
