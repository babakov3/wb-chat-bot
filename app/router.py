"""Message routing: dispatches Telegram updates to the correct handler
based on user context (commands, buttons, callbacks, text input)."""

from __future__ import annotations

import logging
from typing import Any, Callable, Coroutine

from app.storage import Storage
from app.telegram_client import TelegramClient

logger = logging.getLogger("wb_chat_bot")

# Type aliases for handler signatures
CommandHandler = Callable[[str], Coroutine[Any, Any, None]]
ButtonHandler = Callable[[str], Coroutine[Any, Any, None]]
CallbackHandler = Callable[[str, str, int], Coroutine[Any, Any, None]]
TextInputHandler = Callable[[str, str], Coroutine[Any, Any, None]]


class MessageRouter:
    """Routes incoming Telegram updates to registered handlers.

    Handlers receive chat_id as their first argument so they can
    operate in a multi-user context.
    """

    def __init__(self, storage: Storage, telegram: TelegramClient) -> None:
        self._storage = storage
        self._tg = telegram
        self._command_handlers: dict[str, CommandHandler] = {}
        self._button_handlers: dict[str, ButtonHandler] = {}
        self._callback_handler: CallbackHandler | None = None
        self._text_input_handler: TextInputHandler | None = None

    def register_command(self, cmd: str, handler: CommandHandler) -> None:
        """Register a /command handler. cmd without leading slash."""
        self._command_handlers[cmd.lower()] = handler

    def register_button(self, label: str, handler: ButtonHandler) -> None:
        """Register a reply keyboard button handler by exact label."""
        self._button_handlers[label] = handler

    def set_callback_handler(self, handler: CallbackHandler) -> None:
        """Set the callback query handler.

        Signature: async fn(chat_id: str, data: str, message_id: int)
        """
        self._callback_handler = handler

    def set_text_input_handler(self, handler: TextInputHandler) -> None:
        """Set the free-text input handler.

        Signature: async fn(chat_id: str, text: str)
        """
        self._text_input_handler = handler

    async def handle_update(self, update: dict[str, Any]) -> None:
        """Main entry point: called by TelegramClient for each update."""
        # ── Callback query ────────────────────────────────────────
        cq = update.get("callback_query")
        if cq:
            cq_id = cq.get("id", "")
            data = cq.get("data", "")
            msg = cq.get("message", {})
            message_id = msg.get("message_id", 0)
            chat = msg.get("chat", {})
            chat_id = str(chat.get("id", ""))

            # Always answer the callback to remove the loading spinner
            await self._tg.answer_callback_query(cq_id)

            if self._callback_handler and chat_id and data:
                try:
                    await self._callback_handler(chat_id, data, message_id)
                except Exception as exc:
                    logger.error("Callback handler error: %s", exc, exc_info=True)
            return

        # ── Message ───────────────────────────────────────────────
        msg = update.get("message")
        if not msg:
            return

        chat = msg.get("chat", {})
        chat_id = str(chat.get("id", ""))
        if not chat_id:
            return

        text = (msg.get("text") or "").strip()
        if not text:
            return

        # 1. Check if user has input_waiting -> dispatch to text_input_handler
        user_state = self._storage.get_user_state(chat_id)
        if user_state and user_state.get("input_waiting"):
            if self._text_input_handler:
                try:
                    await self._text_input_handler(chat_id, text)
                except Exception as exc:
                    logger.error("Text input handler error: %s", exc, exc_info=True)
            return

        # 2. Check /command
        if text.startswith("/"):
            cmd = text.split()[0][1:].split("@")[0].lower()
            handler = self._command_handlers.get(cmd)
            if handler:
                try:
                    await handler(chat_id)
                except Exception as exc:
                    logger.error("Command handler error for /%s: %s", cmd, exc, exc_info=True)
            return

        # 3. Check reply keyboard button
        handler = self._button_handlers.get(text)
        if handler:
            try:
                await handler(chat_id)
            except Exception as exc:
                logger.error("Button handler error for '%s': %s", text, exc, exc_info=True)
            return

        # 4. Unrecognized text — ignore silently
        logger.debug("Unrecognized message from %s: %s", chat_id, text[:50])
