"""Unit tests for core service logic: dedup, dry-run, pause, new chat handling,
WB response parsing, field names, multipart send, cursor logic."""

from __future__ import annotations

import asyncio
import os
import tempfile
import unittest
from unittest.mock import AsyncMock, MagicMock, patch, call

from app.config import Config
from app.service import ChatService
from app.storage import Storage
from app.wb_client import WBClient


def make_config(**overrides) -> Config:
    defaults = dict(
        wb_api_token="test-token",
        telegram_bot_token="test-tg-token",
        telegram_chat_id="123456",
        app_mode="dry-run",
        poll_interval_seconds=1,
        new_chat_delay_seconds=0,  # no delay in tests
        message_text="тест",
        log_level="DEBUG",
        db_path=":memory:",
        heartbeat_interval_minutes=60,
        reply_to_ratings=set(),
        product_whitelist=set(),
        product_blacklist=set(),
    )
    defaults.update(overrides)
    return Config(**defaults)


def make_service(config: Config | None = None, db_path: str | None = None) -> ChatService:
    cfg = config or make_config()
    storage_path = db_path or cfg.db_path
    if storage_path == ":memory:":
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        storage_path = tmp.name
        tmp.close()

    storage = Storage(storage_path)
    wb = AsyncMock()
    telegram = AsyncMock()
    telegram.notify = AsyncMock()
    telegram.send_message = AsyncMock()

    service = ChatService(config=cfg, storage=storage, wb=wb, telegram=telegram)
    service._db_tmp_path = storage_path
    return service


def _cleanup_service(service: ChatService) -> None:
    service.storage.close()
    if hasattr(service, "_db_tmp_path"):
        try:
            os.unlink(service._db_tmp_path)
        except FileNotFoundError:
            pass


# ── Deduplication ───────────────────────────────────────────────────────

class TestDeduplication(unittest.TestCase):
    def setUp(self):
        self.service = make_service()

    def tearDown(self):
        _cleanup_service(self.service)

    def test_chat_not_processed_initially(self):
        self.assertFalse(self.service.storage.is_chat_processed("chat-001"))

    def test_chat_marked_processed(self):
        self.service.storage.save_chat(
            chat_id="chat-001",
            first_event_id="evt-1",
            reply_sign="sign-1",
            status="sent",
        )
        self.assertTrue(self.service.storage.is_chat_processed("chat-001"))

    def test_duplicate_chat_not_saved_twice(self):
        self.service.storage.save_chat(
            chat_id="chat-001",
            first_event_id="evt-1",
            reply_sign="sign-1",
            status="sent",
            sent_message_text="тест",
        )
        self.service.storage.save_chat(
            chat_id="chat-001",
            first_event_id="evt-1",
            reply_sign="sign-1",
            status="error",
            error_text="some error",
        )
        stats = self.service.storage.get_stats()
        self.assertEqual(stats.get("total", 0), 1)
        self.assertEqual(stats.get("error", 0), 1)

    def test_stats_count(self):
        for i in range(3):
            self.service.storage.save_chat(
                chat_id=f"chat-{i}",
                first_event_id=f"evt-{i}",
                reply_sign=f"sign-{i}",
                status="sent",
            )
        self.service.storage.save_chat(
            chat_id="chat-99",
            first_event_id="evt-99",
            reply_sign="sign-99",
            status="dry-run",
        )
        stats = self.service.storage.get_stats()
        self.assertEqual(stats["sent"], 3)
        self.assertEqual(stats["dry-run"], 1)
        self.assertEqual(stats["total"], 4)


# ── Dry-Run ─────────────────────────────────────────────────────────────

class TestDryRun(unittest.TestCase):
    def setUp(self):
        self.service = make_service(make_config(app_mode="dry-run"))

    def tearDown(self):
        _cleanup_service(self.service)

    def test_dry_run_does_not_call_send(self):
        event = {"chatID": "chat-dry-1", "isNewChat": True, "replySign": "sign-1", "eventID": "evt-1"}
        asyncio.run(self.service._handle_new_chat(event, "chat-dry-1"))
        self.service.wb.send_message.assert_not_called()

    def test_dry_run_saves_to_storage(self):
        event = {"chatID": "chat-dry-2", "isNewChat": True, "replySign": "sign-2", "eventID": "evt-2"}
        asyncio.run(self.service._handle_new_chat(event, "chat-dry-2"))
        self.assertTrue(self.service.storage.is_chat_processed("chat-dry-2"))
        chats = self.service.storage.get_last_chats(1)
        self.assertEqual(chats[0]["status"], "dry-run")

    def test_dry_run_sends_telegram_notification(self):
        event = {"chatID": "chat-dry-3", "isNewChat": True, "replySign": "sign-3", "eventID": "evt-3"}
        asyncio.run(self.service._handle_new_chat(event, "chat-dry-3"))
        self.service.telegram.notify.assert_called_once()
        call_text = self.service.telegram.notify.call_args[0][0]
        self.assertIn("DRY-RUN", call_text)


# ── Pause ───────────────────────────────────────────────────────────────

class TestPause(unittest.TestCase):
    def setUp(self):
        self.service = make_service()

    def tearDown(self):
        _cleanup_service(self.service)

    def test_pause_flag_starts_false(self):
        self.assertFalse(self.service.paused)

    def test_pause_and_resume(self):
        self.service.paused = True
        self.assertTrue(self.service.paused)
        self.service.paused = False
        self.assertFalse(self.service.paused)


# ── New Chat Detection with official WB field names ─────────────────────

class TestNewChatDetection(unittest.TestCase):
    def setUp(self):
        self.service = make_service()

    def tearDown(self):
        _cleanup_service(self.service)

    def test_is_new_chat_event_direct(self):
        event = {"isNewChat": True, "chatID": "c1"}
        self.assertTrue(self.service._is_new_chat_event(event))

    def test_is_new_chat_event_nested(self):
        event = {"payload": {"isNewChat": True, "chatID": "c2"}}
        self.assertTrue(self.service._is_new_chat_event(event))

    def test_not_new_chat_event(self):
        event = {"chatID": "c3", "type": "message"}
        self.assertFalse(self.service._is_new_chat_event(event))

    def test_extract_chat_id_official_chatID(self):
        """Official WB field is 'chatID' (capital ID)."""
        self.assertEqual(self.service._extract_chat_id({"chatID": "abc"}), "abc")

    def test_extract_chat_id_fallback_chatId(self):
        """Fallback: camelCase 'chatId'."""
        self.assertEqual(self.service._extract_chat_id({"chatId": "def"}), "def")

    def test_extract_chat_id_priority(self):
        """chatID takes priority over chatId."""
        event = {"chatID": "official", "chatId": "fallback"}
        self.assertEqual(self.service._extract_chat_id(event), "official")

    def test_extract_chat_id_nested(self):
        self.assertEqual(
            self.service._extract_chat_id({"payload": {"chatID": "xyz"}}), "xyz"
        )

    def test_extract_event_id_official_eventID(self):
        """Official WB field is 'eventID'."""
        event = {"eventID": "e-100", "chatID": "c1"}
        self.assertEqual(self.service._extract_event_id(event), "e-100")

    def test_extract_event_id_fallback(self):
        """Fallback: 'id' field."""
        event = {"id": "e-200", "chatID": "c1"}
        self.assertEqual(self.service._extract_event_id(event), "e-200")

    def test_extract_reply_sign(self):
        event = {"replySign": "rs-123", "chatID": "c1"}
        self.assertEqual(self.service._extract_reply_sign(event), "rs-123")


# ── Production Send ─────────────────────────────────────────────────────

class TestProductionSend(unittest.TestCase):
    def setUp(self):
        self.service = make_service(make_config(app_mode="production"))

    def tearDown(self):
        _cleanup_service(self.service)

    def test_production_sends_message(self):
        self.service.wb.send_message = AsyncMock(return_value={"status": 200})
        event = {"chatID": "chat-prod-1", "isNewChat": True, "replySign": "sign-1", "eventID": "evt-1"}
        asyncio.run(self.service._handle_new_chat(event, "chat-prod-1"))
        self.service.wb.send_message.assert_called_once_with(
            chat_id="chat-prod-1",
            reply_sign="sign-1",
            message_text="тест",
        )

    def test_production_saves_sent_status(self):
        self.service.wb.send_message = AsyncMock(return_value={"status": 200})
        event = {"chatID": "chat-prod-2", "isNewChat": True, "replySign": "sign-2", "eventID": "evt-2"}
        asyncio.run(self.service._handle_new_chat(event, "chat-prod-2"))
        chats = self.service.storage.get_last_chats(1)
        self.assertEqual(chats[0]["status"], "sent")

    def test_production_no_reply_sign_errors(self):
        """If no replySign and can't fetch it, should save error."""
        self.service.wb.get_chats_list = AsyncMock(return_value=[])
        event = {"chatID": "chat-prod-3", "isNewChat": True, "eventID": "evt-3"}
        asyncio.run(self.service._handle_new_chat(event, "chat-prod-3"))
        self.service.wb.send_message.assert_not_called()
        chats = self.service.storage.get_last_chats(1)
        self.assertEqual(chats[0]["status"], "error")

    def test_production_fetches_reply_sign_from_chats_list(self):
        """When event has no replySign, fetch from chats list using chatID."""
        self.service.wb.get_chats_list = AsyncMock(return_value=[
            {"chatID": "chat-prod-4", "replySign": "fetched-sign"},
            {"chatID": "other-chat", "replySign": "other-sign"},
        ])
        self.service.wb.send_message = AsyncMock(return_value={"status": 200})
        event = {"chatID": "chat-prod-4", "isNewChat": True, "eventID": "evt-4"}
        asyncio.run(self.service._handle_new_chat(event, "chat-prod-4"))
        self.service.wb.send_message.assert_called_once_with(
            chat_id="chat-prod-4",
            reply_sign="fetched-sign",
            message_text="тест",
        )


# ── Cursor Persistence (integer timestamps) ────────────────────────────

class TestCursorPersistence(unittest.TestCase):
    def setUp(self):
        self.service = make_service()

    def tearDown(self):
        _cleanup_service(self.service)

    def test_cursor_none_initially(self):
        self.assertIsNone(self.service.storage.get_cursor())

    def test_cursor_saved_as_integer(self):
        self.service.storage.save_cursor(1700000000000)
        self.assertEqual(self.service.storage.get_cursor(), 1700000000000)

    def test_cursor_updated(self):
        self.service.storage.save_cursor(1700000000000)
        self.service.storage.save_cursor(1700000001000)
        self.assertEqual(self.service.storage.get_cursor(), 1700000001000)

    def test_cursor_returns_none_for_invalid_data(self):
        """If somehow non-numeric data got saved, get_cursor returns None."""
        # Directly insert invalid data
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        with self.service.storage._conn:
            self.service.storage._conn.execute(
                "INSERT INTO key_value (key, value, updated_at) VALUES ('next_cursor', 'garbage', ?)",
                (now,),
            )
        self.assertIsNone(self.service.storage.get_cursor())

    def test_no_initialized_marker(self):
        """Cursor must never be '__initialized__' — should be integer or None."""
        self.service.storage.save_cursor(0)
        cursor = self.service.storage.get_cursor()
        self.assertIsInstance(cursor, int)
        self.assertNotEqual(cursor, "__initialized__")


# ── WB Response Parsing ─────────────────────────────────────────────────

class TestWBResponseParsing(unittest.TestCase):
    """Test that service correctly parses WB envelope: { result: { ... } }."""

    def setUp(self):
        self.service = make_service()

    def tearDown(self):
        _cleanup_service(self.service)

    def test_parse_events_from_result_wrapper(self):
        """WB returns { result: { next, totalEvents, events } }.
        wb_client.get_chat_events unwraps to the 'result' dict.
        _poll_events then reads events and next from that."""
        wb_result = {
            "next": 1700000005000,
            "totalEvents": 2,
            "events": [
                {"chatID": "c1", "eventID": "e1", "isNewChat": True, "replySign": "rs1"},
                {"chatID": "c2", "eventID": "e2", "isNewChat": False},
            ],
        }
        self.service.wb.get_chat_events = AsyncMock(return_value=wb_result)

        # Set an initial cursor so _poll_events runs
        self.service.storage.save_cursor(1700000000000)

        asyncio.run(self.service._poll_events())

        # Cursor should be updated to new value
        self.assertEqual(self.service.storage.get_cursor(), 1700000005000)
        # Only one new chat (c1 has isNewChat=True)
        self.assertTrue(self.service.storage.is_chat_processed("c1"))
        self.assertFalse(self.service.storage.is_chat_processed("c2"))

    def test_parse_events_empty(self):
        """Empty events list should not crash."""
        wb_result = {"next": 1700000010000, "totalEvents": 0, "events": []}
        self.service.wb.get_chat_events = AsyncMock(return_value=wb_result)
        self.service.storage.save_cursor(1700000000000)
        asyncio.run(self.service._poll_events())
        self.assertEqual(self.service.storage.get_cursor(), 1700000010000)


# ── chatID / eventID field handling ────────────────────────────────────

class TestOfficialFieldNames(unittest.TestCase):
    """Verify service handles official WB field names (chatID, eventID) correctly."""

    def setUp(self):
        self.service = make_service()

    def tearDown(self):
        _cleanup_service(self.service)

    def test_full_event_with_official_fields(self):
        """End-to-end: event with official chatID/eventID/replySign is processed."""
        event = {
            "chatID": "official-chat-1",
            "eventID": "official-evt-1",
            "isNewChat": True,
            "replySign": "official-rs-1",
        }
        asyncio.run(self.service._handle_new_chat(event, "official-chat-1"))
        self.assertTrue(self.service.storage.is_chat_processed("official-chat-1"))
        chats = self.service.storage.get_last_chats(1)
        self.assertEqual(chats[0]["chat_id"], "official-chat-1")
        self.assertEqual(chats[0]["status"], "dry-run")


# ── Multipart send_message ──────────────────────────────────────────────

class TestMultipartSend(unittest.TestCase):
    """Test that WBClient.send_message uses multipart/form-data correctly."""

    def test_send_message_uses_files_param(self):
        """Verify send_message passes 'files' kwarg for multipart encoding."""
        import httpx
        from unittest.mock import patch, AsyncMock

        client = WBClient("test-token")

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"result": "ok"}
        mock_response.raise_for_status = MagicMock()

        # Patch _request_with_retry to capture the kwargs
        captured_kwargs = {}

        async def capture_request(method, path, **kwargs):
            captured_kwargs.update(kwargs)
            captured_kwargs["method"] = method
            captured_kwargs["path"] = path
            return mock_response

        client._request_with_retry = capture_request

        result = asyncio.run(client.send_message("chat-1", "rs-1", "hello"))

        # Verify it posted to correct endpoint
        self.assertEqual(captured_kwargs["method"], "POST")
        self.assertEqual(captured_kwargs["path"], "/api/v1/seller/message")

        # Verify 'files' param is used (multipart)
        self.assertIn("files", captured_kwargs)
        files = captured_kwargs["files"]
        # Should have 3 fields: chatID, replySign, message
        field_names = [f[0] for f in files]
        self.assertIn("chatID", field_names)
        self.assertIn("replySign", field_names)
        self.assertIn("message", field_names)

        # Verify retryable=False
        self.assertFalse(captured_kwargs.get("retryable", True))

    def test_send_message_field_values(self):
        """Verify the actual values sent in multipart fields."""
        client = WBClient("test-token")

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}
        mock_response.raise_for_status = MagicMock()

        captured = {}

        async def capture(method, path, **kwargs):
            captured.update(kwargs)
            return mock_response

        client._request_with_retry = capture

        asyncio.run(client.send_message("my-chat", "my-sign", "тест"))

        files = captured["files"]
        values = {f[0]: f[1][1] for f in files}
        self.assertEqual(values["chatID"], "my-chat")
        self.assertEqual(values["replySign"], "my-sign")
        self.assertEqual(values["message"], "тест")


# ── Cursor Initialization (no __initialized__) ─────────────────────────

class TestCursorInit(unittest.TestCase):
    """Test that _init_cursor uses integer timestamps, never __initialized__."""

    def setUp(self):
        self.service = make_service()

    def tearDown(self):
        _cleanup_service(self.service)

    def test_init_cursor_from_wb_response(self):
        """When WB returns a next cursor, use it."""
        self.service.wb.get_chat_events = AsyncMock(return_value={
            "next": 1700000099000,
            "totalEvents": 5,
            "events": [],
        })
        asyncio.run(self.service._init_cursor())
        self.assertEqual(self.service.storage.get_cursor(), 1700000099000)

    def test_init_cursor_fallback_to_timestamp(self):
        """When WB returns no next cursor, use current timestamp in ms."""
        self.service.wb.get_chat_events = AsyncMock(return_value={
            "next": None,
            "totalEvents": 0,
            "events": [],
        })
        asyncio.run(self.service._init_cursor())
        cursor = self.service.storage.get_cursor()
        self.assertIsNotNone(cursor)
        self.assertIsInstance(cursor, int)
        # Should be a reasonable recent timestamp (after 2024)
        self.assertGreater(cursor, 1700000000000)

    def test_init_cursor_on_api_error_uses_timestamp(self):
        """On API error, fallback to current timestamp."""
        from app.wb_client import WBApiError
        self.service.wb.get_chat_events = AsyncMock(
            side_effect=WBApiError(500, "server error")
        )
        asyncio.run(self.service._init_cursor())
        cursor = self.service.storage.get_cursor()
        self.assertIsNotNone(cursor)
        self.assertIsInstance(cursor, int)
        self.assertGreater(cursor, 1700000000000)


# ── Rating Filter ───────────────────────────────────────────────────────

class TestRatingFilter(unittest.TestCase):
    def tearDown(self):
        _cleanup_service(self.service)

    def test_no_filter_passes_all(self):
        """No rating filter configured — all events pass."""
        self.service = make_service()
        event = {"chatID": "c1", "isNewChat": True, "rating": 5}
        self.assertTrue(self.service._passes_filters(event, "c1"))

    def test_rating_filter_passes_matching(self):
        """Rating 2 passes when filter is {1,2,3}."""
        self.service = make_service(make_config(reply_to_ratings={1, 2, 3}))
        event = {"chatID": "c1", "isNewChat": True, "rating": 2}
        self.assertTrue(self.service._passes_filters(event, "c1"))

    def test_rating_filter_blocks_non_matching(self):
        """Rating 5 blocked when filter is {1,2,3}."""
        self.service = make_service(make_config(reply_to_ratings={1, 2, 3}))
        event = {"chatID": "c1", "isNewChat": True, "rating": 5}
        self.assertFalse(self.service._passes_filters(event, "c1"))

    def test_rating_filter_blocks_missing_rating(self):
        """No rating in event — skipped when filter is active."""
        self.service = make_service(make_config(reply_to_ratings={1, 2, 3}))
        event = {"chatID": "c1", "isNewChat": True}
        self.assertFalse(self.service._passes_filters(event, "c1"))

    def test_rating_extracted_from_nested(self):
        """Rating in payload is also recognized."""
        self.service = make_service(make_config(reply_to_ratings={4, 5}))
        event = {"chatID": "c1", "isNewChat": True, "payload": {"rating": 4}}
        self.assertTrue(self.service._passes_filters(event, "c1"))


# ── Product Filter ──────────────────────────────────────────────────────

class TestProductFilter(unittest.TestCase):
    def tearDown(self):
        _cleanup_service(self.service)

    def test_whitelist_passes_matching(self):
        """Product in whitelist — passes."""
        self.service = make_service(make_config(product_whitelist={100, 200}))
        event = {"chatID": "c1", "isNewChat": True, "nmID": 100}
        self.assertTrue(self.service._passes_filters(event, "c1"))

    def test_whitelist_blocks_non_matching(self):
        """Product not in whitelist — blocked."""
        self.service = make_service(make_config(product_whitelist={100, 200}))
        event = {"chatID": "c1", "isNewChat": True, "nmID": 999}
        self.assertFalse(self.service._passes_filters(event, "c1"))

    def test_whitelist_blocks_missing_nmid(self):
        """No nmID in event — skipped when whitelist is active."""
        self.service = make_service(make_config(product_whitelist={100}))
        event = {"chatID": "c1", "isNewChat": True}
        self.assertFalse(self.service._passes_filters(event, "c1"))

    def test_blacklist_blocks_matching(self):
        """Product in blacklist — blocked."""
        self.service = make_service(make_config(product_blacklist={999}))
        event = {"chatID": "c1", "isNewChat": True, "nmID": 999}
        self.assertFalse(self.service._passes_filters(event, "c1"))

    def test_blacklist_passes_non_matching(self):
        """Product not in blacklist — passes."""
        self.service = make_service(make_config(product_blacklist={999}))
        event = {"chatID": "c1", "isNewChat": True, "nmID": 100}
        self.assertTrue(self.service._passes_filters(event, "c1"))

    def test_whitelist_and_blacklist_combined(self):
        """Product in whitelist but also in blacklist — blacklist wins."""
        self.service = make_service(make_config(
            product_whitelist={100, 200, 300},
            product_blacklist={200},
        ))
        event_ok = {"chatID": "c1", "isNewChat": True, "nmID": 100}
        event_blocked = {"chatID": "c2", "isNewChat": True, "nmID": 200}
        self.assertTrue(self.service._passes_filters(event_ok, "c1"))
        self.assertFalse(self.service._passes_filters(event_blocked, "c2"))


# ── Combined Rating + Product Filter ────────────────────────────────────

class TestCombinedFilters(unittest.TestCase):
    def tearDown(self):
        _cleanup_service(self.service)

    def test_both_filters_pass(self):
        """Both rating and product match — passes."""
        self.service = make_service(make_config(
            reply_to_ratings={1, 2, 3},
            product_whitelist={100},
        ))
        event = {"chatID": "c1", "isNewChat": True, "nmID": 100, "rating": 2}
        self.assertTrue(self.service._passes_filters(event, "c1"))

    def test_rating_passes_product_fails(self):
        """Rating OK but product not in whitelist — blocked."""
        self.service = make_service(make_config(
            reply_to_ratings={1, 2, 3},
            product_whitelist={100},
        ))
        event = {"chatID": "c1", "isNewChat": True, "nmID": 999, "rating": 2}
        self.assertFalse(self.service._passes_filters(event, "c1"))

    def test_product_passes_rating_fails(self):
        """Product OK but rating not in allowed — blocked."""
        self.service = make_service(make_config(
            reply_to_ratings={1, 2, 3},
            product_whitelist={100},
        ))
        event = {"chatID": "c1", "isNewChat": True, "nmID": 100, "rating": 5}
        self.assertFalse(self.service._passes_filters(event, "c1"))

    def test_no_filters_always_passes(self):
        """No filters at all — everything passes."""
        self.service = make_service()
        event = {"chatID": "c1", "isNewChat": True}
        self.assertTrue(self.service._passes_filters(event, "c1"))

    def test_filter_skipped_event_not_processed(self):
        """End-to-end: filtered event should not be processed."""
        self.service = make_service(make_config(reply_to_ratings={1, 2}))
        # Rating 5 should be filtered out
        wb_result = {
            "next": 1700000005000,
            "totalEvents": 1,
            "events": [
                {"chatID": "c1", "eventID": "e1", "isNewChat": True,
                 "replySign": "rs1", "rating": 5},
            ],
        }
        self.service.wb.get_chat_events = AsyncMock(return_value=wb_result)
        self.service.storage.save_cursor(1700000000000)
        asyncio.run(self.service._poll_events())
        self.assertFalse(self.service.storage.is_chat_processed("c1"))

    def test_filter_matching_event_is_processed(self):
        """End-to-end: matching event should be processed."""
        self.service = make_service(make_config(reply_to_ratings={1, 2}))
        wb_result = {
            "next": 1700000005000,
            "totalEvents": 1,
            "events": [
                {"chatID": "c1", "eventID": "e1", "isNewChat": True,
                 "replySign": "rs1", "rating": 1},
            ],
        }
        self.service.wb.get_chat_events = AsyncMock(return_value=wb_result)
        self.service.storage.save_cursor(1700000000000)
        asyncio.run(self.service._poll_events())
        self.assertTrue(self.service.storage.is_chat_processed("c1"))


if __name__ == "__main__":
    unittest.main()
