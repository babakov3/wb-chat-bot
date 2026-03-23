"""Entrypoint: wire everything together, handle graceful shutdown."""

from __future__ import annotations

import asyncio
import signal
import sys

from app.commands import register_all
from app.config import load_config
from app.logger import setup_logging
from app.router import MessageRouter
from app.service import ChatService
from app.storage import Storage
from app.telegram_client import TelegramClient
from app.wb_client import WBClientPool


async def main() -> None:
    config = load_config()
    logger = setup_logging(config.log_level)
    logger.info("=" * 50)
    logger.info("WB Chat Bot starting (multi-store)")
    logger.info("=" * 50)

    # Core components
    storage = Storage(config.db_path)

    # Legacy migration (single-tenant -> multi-store, runs once)
    storage.run_legacy_migration(
        legacy_chat_id=config.legacy_chat_id,
        legacy_wb_api_token=config.legacy_wb_api_token,
        legacy_wb_content_token=config.legacy_wb_content_token,
        legacy_message_text=config.legacy_message_text,
        legacy_product_whitelist=config.legacy_product_whitelist,
        legacy_app_mode=config.legacy_app_mode,
    )

    telegram = TelegramClient(config.telegram_bot_token)
    wb_pool = WBClientPool()

    # Router: dispatches Telegram updates to handlers
    router = MessageRouter(storage, telegram)
    telegram.set_update_handler(router.handle_update)

    # Service: main polling loop
    service = ChatService(config, storage, telegram, wb_pool)

    # Register all commands, buttons, callbacks
    register_all(router, service, storage, telegram, wb_pool)

    # Graceful shutdown on SIGINT/SIGTERM
    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def _signal_handler() -> None:
        logger.info("Shutdown signal received")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    # Run service in background task
    service_task = asyncio.create_task(service.run())

    # Wait for shutdown signal
    await shutdown_event.wait()

    # Stop service gracefully
    await service.stop()
    service_task.cancel()
    try:
        await service_task
    except asyncio.CancelledError:
        pass

    # Cleanup
    await wb_pool.close_all()
    await telegram.close()
    storage.close()
    logger.info("WB Chat Bot stopped cleanly")


def run() -> None:
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    run()
