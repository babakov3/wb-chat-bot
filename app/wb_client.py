"""Wildberries Buyers Chat API client with retry and error handling.

Official endpoints (Buyers Chat API):
  GET  /api/v1/seller/events   — chat events stream
  GET  /api/v1/seller/chats    — list of chats
  POST /api/v1/seller/message  — send message (multipart/form-data)

Response envelope:
  { "result": { ... }, "errors": [...] }
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import httpx

logger = logging.getLogger("wb_chat_bot")

BASE_URL = "https://buyer-chat-api.wildberries.ru"

# Retry settings
MAX_RETRIES = 3
RETRY_BASE_DELAY = 2  # seconds


class WBApiError(Exception):
    """Raised on non-recoverable WB API errors."""

    def __init__(self, status_code: int, detail: str = "") -> None:
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"WB API error {status_code}: {detail}")


class WBClient:
    def __init__(self, api_token: str) -> None:
        self._token = api_token
        # No global Content-Type — GET requests don't need it,
        # and POST /message requires multipart which sets its own.
        self._client = httpx.AsyncClient(
            base_url=BASE_URL,
            headers={"Authorization": self._token},
            timeout=httpx.Timeout(30.0, connect=10.0),
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def _request_with_retry(
        self,
        method: str,
        path: str,
        retryable: bool = True,
        **kwargs: Any,
    ) -> httpx.Response:
        """Execute HTTP request with exponential backoff retry for transient errors."""
        last_exc: Exception | None = None

        attempts = MAX_RETRIES if retryable else 1
        for attempt in range(1, attempts + 1):
            try:
                resp = await self._client.request(method, path, **kwargs)

                if resp.status_code in (401, 402, 403):
                    raise WBApiError(resp.status_code, resp.text[:500])

                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", "5"))
                    logger.warning("Rate limited (429), sleeping %ds", retry_after)
                    await asyncio.sleep(retry_after)
                    continue

                if resp.status_code >= 500:
                    logger.warning(
                        "WB server error %d on attempt %d/%d",
                        resp.status_code, attempt, attempts,
                    )
                    if attempt < attempts:
                        delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                        await asyncio.sleep(delay)
                        continue
                    raise WBApiError(resp.status_code, resp.text[:500])

                return resp

            except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError) as exc:
                last_exc = exc
                logger.warning(
                    "Network error on attempt %d/%d: %s", attempt, attempts, exc
                )
                if attempt < attempts:
                    delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                    await asyncio.sleep(delay)
                    continue

        raise last_exc or RuntimeError("Request failed after retries")

    # ── Events ──────────────────────────────────────────────────────────

    async def get_chat_events(self, next_cursor: int | None = None) -> dict[str, Any]:
        """Fetch chat events from GET /api/v1/seller/events.

        WB response shape:
            {
              "result": {
                "next": <int>,           # timestamp ms — next cursor
                "totalEvents": <int>,
                "events": [...]
              },
              "errors": [...]
            }

        Returns the inner 'result' dict with keys: next, totalEvents, events.
        If parsing fails, returns a safe default.
        """
        params: dict[str, Any] = {}
        if next_cursor is not None:
            params["next"] = next_cursor

        resp = await self._request_with_retry("GET", "/api/v1/seller/events", params=params)
        resp.raise_for_status()

        body = resp.json()
        if not isinstance(body, dict):
            logger.error("Unexpected events response type: %s", type(body))
            return {"events": [], "next": next_cursor, "totalEvents": 0}

        result = body.get("result")
        if isinstance(result, dict):
            return result

        # Fallback: maybe WB returned flat structure (future-proofing)
        logger.warning("Events response has no 'result' wrapper, using body as-is")
        return body

    # ── Send message ────────────────────────────────────────────────────

    async def send_message(
        self,
        chat_id: str,
        reply_sign: str,
        message_text: str,
    ) -> dict[str, Any]:
        """Send a text message via POST /api/v1/seller/message (multipart/form-data).

        WB requires multipart/form-data even for text-only messages.
        httpx 'files' param produces proper multipart with boundary.
        """
        # Using 'files' to force genuine multipart/form-data encoding.
        # Each field is a tuple: (field_name, (None, value, content_type)).
        # None filename tells httpx this is a form field, not a file upload.
        resp = await self._request_with_retry(
            "POST",
            "/api/v1/seller/message",
            files=[
                ("chatID", (None, chat_id)),
                ("replySign", (None, reply_sign)),
                ("message", (None, message_text)),
            ],
            retryable=False,  # don't retry sends to avoid duplicate messages
        )
        resp.raise_for_status()
        try:
            return resp.json()
        except Exception:
            return {"status": resp.status_code, "body": resp.text[:500]}

    # ── Chats list ──────────────────────────────────────────────────────

    async def get_chats_list(self) -> list[dict[str, Any]]:
        """Get list of chats from GET /api/v1/seller/chats.

        WB response shape:
            {
              "result": [ { "chatID": ..., "replySign": ..., ... }, ... ],
              "errors": [...]
            }

        Returns the inner 'result' list directly.
        """
        resp = await self._request_with_retry("GET", "/api/v1/seller/chats")
        resp.raise_for_status()

        body = resp.json()
        if isinstance(body, dict):
            result = body.get("result")
            if isinstance(result, list):
                return result
            # Fallback: maybe 'chats' key
            if isinstance(body.get("chats"), list):
                return body["chats"]
        if isinstance(body, list):
            return body

        logger.warning("Unexpected chats response shape: %s", type(body))
        return []

    # ── Token check ─────────────────────────────────────────────────────

    async def check_token(self) -> bool:
        """Quick token validity check by fetching chats list."""
        try:
            await self.get_chats_list()
            return True
        except WBApiError as exc:
            if exc.status_code in (401, 403):
                return False
            raise
        except Exception:
            return False

    # ── Content API (product catalog) ───────────────────────────────────

    async def get_product_cards(self, content_token: str | None = None) -> list[dict[str, Any]]:
        """Fetch all product cards from WB Content API.

        POST https://content-api.wildberries.ru/content/v2/get/cards/list
        Returns: [{"nm_id": int, "name": str}, ...]

        Uses content_token if provided, otherwise falls back to main token.
        Handles pagination via cursor.
        """
        token = content_token or self._token
        all_cards: list[dict[str, Any]] = []

        cursor: dict[str, Any] = {"limit": 100}

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, connect=10.0),
        ) as client:
            for _ in range(50):  # safety limit: max 5000 products
                resp = await client.post(
                    "https://content-api.wildberries.ru/content/v2/get/cards/list",
                    headers={"Authorization": token},
                    json={
                        "settings": {
                            "cursor": cursor,
                            "filter": {"withPhoto": -1},
                        }
                    },
                )

                if resp.status_code in (401, 403):
                    raise WBApiError(resp.status_code, "Content API token invalid")
                resp.raise_for_status()

                body = resp.json()
                cards = body.get("cards", body.get("data", {}).get("cards", []))
                if not cards:
                    break

                for card in cards:
                    nm_id = card.get("nmID")
                    # Title can be in different fields depending on API version
                    name = card.get("title") or ""
                    if not name:
                        subject = card.get("subjectName", "")
                        vendor = card.get("vendorCode", "")
                        name = f"{subject} {vendor}".strip()
                    if not name:
                        name = str(nm_id)
                    if nm_id:
                        all_cards.append({"nm_id": int(nm_id), "name": name})

                # Pagination: check if there are more
                resp_cursor = body.get("cursor", {})
                total = resp_cursor.get("total", len(cards))
                if total < cursor["limit"]:
                    break  # last page

                cursor = {
                    "limit": 100,
                    "updatedAt": resp_cursor.get("updatedAt", ""),
                    "nmID": resp_cursor.get("nmID", 0),
                }

        logger.info("Loaded %d product cards from WB Content API", len(all_cards))
        return all_cards

    # ── Utility ─────────────────────────────────────────────────────────

    @staticmethod
    def current_timestamp_ms() -> int:
        """Return current UTC timestamp in milliseconds (for cursor init)."""
        return int(time.time() * 1000)


# ── Client Pool (multi-store) ──────────────────────────────────────────


class WBClientPool:
    """Maintains one WBClient per store, keyed by store_id."""

    def __init__(self) -> None:
        self._clients: dict[int, WBClient] = {}

    def get(self, store_id: int, api_token: str) -> WBClient:
        """Get or create a WBClient for the given store."""
        if store_id not in self._clients:
            self._clients[store_id] = WBClient(api_token)
        return self._clients[store_id]

    async def remove(self, store_id: int) -> None:
        """Remove and close a specific store's client."""
        client = self._clients.pop(store_id, None)
        if client:
            await client.close()

    async def close_all(self) -> None:
        """Close all clients and clear the pool."""
        for c in self._clients.values():
            await c.close()
        self._clients.clear()
