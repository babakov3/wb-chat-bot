"""Bot-level configuration loaded from environment variables.

Only bot-level fields needed at startup. Per-store settings
(tokens, messages, products) live in the stores table.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class Config:
    telegram_bot_token: str
    log_level: str
    db_path: str
    poll_interval_seconds: int
    new_chat_delay_seconds: int
    heartbeat_interval_minutes: int

    # Legacy fields for one-time migration from single-tenant .env
    legacy_wb_api_token: str
    legacy_wb_content_token: str
    legacy_chat_id: str
    legacy_message_text: str
    legacy_product_whitelist: str
    legacy_app_mode: str


def load_config(env_path: str | None = None) -> Config:
    """Load and validate configuration from .env file."""
    if env_path:
        load_dotenv(env_path)
    else:
        load_dotenv()

    telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

    if not telegram_bot_token:
        print("CONFIG ERROR: TELEGRAM_BOT_TOKEN is required", file=sys.stderr)
        sys.exit(1)

    db_path = os.getenv("DB_PATH", "./data/app.db").strip()
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    Path("logs").mkdir(parents=True, exist_ok=True)

    return Config(
        telegram_bot_token=telegram_bot_token,
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        db_path=db_path,
        poll_interval_seconds=int(os.getenv("POLL_INTERVAL_SECONDS", "5")),
        new_chat_delay_seconds=int(os.getenv("NEW_CHAT_DELAY_SECONDS", "5")),
        heartbeat_interval_minutes=int(os.getenv("HEARTBEAT_INTERVAL_MINUTES", "30")),
        # Legacy fields — read from .env but not required
        legacy_wb_api_token=os.getenv("WB_API_TOKEN", "").strip(),
        legacy_wb_content_token=os.getenv("WB_CONTENT_TOKEN", "").strip(),
        legacy_chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip(),
        legacy_message_text=os.getenv("MESSAGE_TEXT", "").strip(),
        legacy_product_whitelist=os.getenv("PRODUCT_WHITELIST", "").strip(),
        legacy_app_mode=os.getenv("APP_MODE", "dry-run").strip().lower(),
    )
