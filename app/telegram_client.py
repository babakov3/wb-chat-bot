"""Telegram Bot client — multi-user, inline keyboards, callback queries."""

from __future__ import annotations

import logging
from typing import Any, Callable, Coroutine

import httpx

logger = logging.getLogger("wb_chat_bot")

TG_API = "https://api.telegram.org"

UpdateHandler = Callable[[dict[str, Any]], Coroutine[Any, Any, None]]


class TelegramClient:
    """Telegram Bot API client. Accepts messages from any chat_id."""

    def __init__(self, bot_token: str) -> None:
        self._bot_token = bot_token
        self._base = f"{TG_API}/bot{bot_token}"
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(15.0))
        self._last_update_id: int = 0
        self._update_handler: UpdateHandler | None = None

    def set_update_handler(self, handler: UpdateHandler) -> None:
        self._update_handler = handler

    # ── Send methods (explicit chat_id) ──────────────────────────

    async def send_message(
        self,
        chat_id: str,
        text: str,
        parse_mode: str = "HTML",
        reply_markup: dict[str, Any] | None = None,
        message_thread_id: int | None = None,
    ) -> int | None:
        try:
            payload: dict[str, Any] = {
                "chat_id": chat_id,
                "text": text[:4096],
                "parse_mode": parse_mode,
            }
            if reply_markup:
                payload["reply_markup"] = reply_markup
            if message_thread_id:
                payload["message_thread_id"] = message_thread_id
            resp = await self._client.post(f"{self._base}/sendMessage", json=payload)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("ok"):
                    return data["result"]["message_id"]
            else:
                logger.error("TG send failed: %s %s", resp.status_code, resp.text[:300])
        except Exception as exc:
            logger.error("TG send error: %s", exc)
        return None

    async def edit_message_text(
        self,
        chat_id: str,
        message_id: int,
        text: str,
        parse_mode: str = "HTML",
        reply_markup: dict[str, Any] | None = None,
    ) -> None:
        try:
            payload: dict[str, Any] = {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": text[:4096],
                "parse_mode": parse_mode,
            }
            if reply_markup:
                payload["reply_markup"] = reply_markup
            resp = await self._client.post(f"{self._base}/editMessageText", json=payload)
            if resp.status_code != 200 and "message is not modified" not in resp.text:
                logger.debug("TG edit failed: %s", resp.text[:300])
        except Exception as exc:
            logger.error("TG edit error: %s", exc)

    async def delete_message(self, chat_id: str, message_id: int) -> None:
        try:
            await self._client.post(
                f"{self._base}/deleteMessage",
                json={"chat_id": chat_id, "message_id": message_id},
            )
        except Exception as exc:
            logger.debug("TG delete error: %s", exc)

    async def answer_callback_query(
        self, callback_query_id: str, text: str | None = None
    ) -> None:
        try:
            payload: dict[str, Any] = {"callback_query_id": callback_query_id}
            if text:
                payload["text"] = text
            await self._client.post(f"{self._base}/answerCallbackQuery", json=payload)
        except Exception as exc:
            logger.debug("TG answer_cq error: %s", exc)

    async def notify(self, chat_id: str, text: str, message_thread_id: int | None = None) -> None:
        await self.send_message(chat_id, text, message_thread_id=message_thread_id)

    # ── Polling ──────────────────────────────────────────────────

    async def poll_updates(self) -> None:
        if not self._update_handler:
            return
        try:
            resp = await self._client.get(
                f"{self._base}/getUpdates",
                params={
                    "offset": self._last_update_id + 1,
                    "timeout": 1,
                    "allowed_updates": '["message","callback_query"]',
                },
                timeout=httpx.Timeout(10.0),
            )
            if resp.status_code != 200:
                return
            data = resp.json()
            if not data.get("ok"):
                return
            for update in data.get("result", []):
                update_id = update.get("update_id", 0)
                if update_id > self._last_update_id:
                    self._last_update_id = update_id
                try:
                    await self._update_handler(update)
                except Exception as exc:
                    logger.error("Update handler error: %s", exc, exc_info=True)
        except (httpx.TimeoutException, httpx.ConnectError):
            pass
        except Exception as exc:
            logger.debug("TG poll error: %s", exc)

    async def close(self) -> None:
        await self._client.aclose()
