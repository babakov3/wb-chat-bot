"""Review monitoring: periodic snapshots of review counts, alerts on anomalies."""

from __future__ import annotations

import logging
from typing import Any

from app.storage import Storage
from app.telegram_client import TelegramClient
from app.wb_client import WBClient, WBClientPool

logger = logging.getLogger("wb_chat_bot")

# Minimum drop to trigger an alert (absolute number)
ALERT_THRESHOLD = 5


class ReviewMonitor:
    def __init__(
        self,
        storage: Storage,
        telegram: TelegramClient,
        wb_pool: WBClientPool,
    ) -> None:
        self.storage = storage
        self.telegram = telegram
        self.wb_pool = wb_pool

    async def run_snapshot(self, store: dict[str, Any]) -> None:
        """Take a snapshot of all review counts for a store and alert on drops."""
        store_id = store["id"]
        store_name = store["store_name"]
        api_token = store["wb_api_token"]
        user_chat_id = store["user_chat_id"]

        if not api_token:
            return

        try:
            current_counts = await self._fetch_review_counts(api_token)
        except Exception as exc:
            logger.warning("Review snapshot failed for store %d: %s", store_id, exc)
            return

        if not current_counts:
            logger.info("Store %d: no reviews found for snapshot", store_id)
            return

        # Get previous snapshots for comparison
        prev_snapshots = {
            s["nm_id"]: s for s in self.storage.get_all_latest_snapshots(store_id)
        }

        # Compare and detect drops
        drops: list[dict[str, Any]] = []
        total_before = 0
        total_after = 0

        for nm_id, info in current_counts.items():
            count_now = info["count"]
            name = info["name"]

            # Save new snapshot
            self.storage.save_review_snapshot(store_id, nm_id, name, count_now)

            prev = prev_snapshots.get(nm_id)
            if prev:
                count_before = prev["review_count"]
                total_before += count_before
                total_after += count_now
                diff = count_before - count_now
                if diff > 0:
                    drops.append({
                        "nm_id": nm_id,
                        "name": name,
                        "before": count_before,
                        "after": count_now,
                        "diff": diff,
                    })
            else:
                total_after += count_now
                # First snapshot for this product — just save, no comparison

        # Alert if significant drops detected
        total_dropped = sum(d["diff"] for d in drops)
        if total_dropped >= ALERT_THRESHOLD:
            drops.sort(key=lambda x: x["diff"], reverse=True)
            lines = [f"⚠️ <b>[{store_name}] Удаление отзывов</b>\n"]
            lines.append(f"Пропало: <b>{total_dropped}</b> отзывов\n")

            for d in drops[:10]:  # top 10
                lines.append(
                    f"📦 {d['nm_id']} — {d['name'][:35]}: "
                    f"<b>-{d['diff']}</b> ({d['before']}→{d['after']})"
                )

            text = "\n".join(lines)

            # Send to user
            await self.telegram.notify(user_chat_id, text)

            # Send to group if linked
            group_id = store.get("notification_group_id") or ""
            if group_id:
                thread_id_str = store.get("notification_thread_id") or ""
                thread_id = int(thread_id_str) if thread_id_str else None
                try:
                    await self.telegram.notify(str(group_id), text, message_thread_id=thread_id)
                except Exception as exc:
                    logger.warning("Failed to send review alert to group: %s", exc)

            logger.warning(
                "Store %d: review drop detected — %d reviews removed across %d products",
                store_id, total_dropped, len(drops),
            )
        else:
            logger.info(
                "Store %d: review snapshot OK — %d products, %d total reviews",
                store_id, len(current_counts), total_after,
            )

    async def _fetch_review_counts(self, token: str) -> dict[int, dict[str, Any]]:
        """Fetch all reviews and count per product. Handles pagination."""
        import httpx

        counts: dict[int, dict[str, Any]] = {}
        take = 5000
        skip = 0
        max_pages = 20  # safety limit

        async with httpx.AsyncClient(timeout=30) as client:
            for page in range(max_pages):
                resp = await client.get(
                    "https://feedbacks-api.wildberries.ru/api/v1/feedbacks",
                    headers={"Authorization": token},
                    params={
                        "isAnswered": "true",
                        "take": take,
                        "skip": skip,
                        "order": "dateDesc",
                    },
                )
                if resp.status_code != 200:
                    logger.warning("Feedbacks API returned %d", resp.status_code)
                    break

                data = resp.json().get("data", {})
                feedbacks = data.get("feedbacks", [])

                for fb in feedbacks:
                    pd = fb.get("productDetails", {})
                    nm_id = pd.get("nmId")
                    if not nm_id:
                        continue
                    if nm_id not in counts:
                        counts[nm_id] = {
                            "name": pd.get("productName", "?"),
                            "count": 0,
                        }
                    counts[nm_id]["count"] += 1

                if len(feedbacks) < take:
                    break  # last page
                skip += take

            # Also count unanswered
            resp2 = await client.get(
                "https://feedbacks-api.wildberries.ru/api/v1/feedbacks",
                headers={"Authorization": token},
                params={
                    "isAnswered": "false",
                    "take": take,
                    "skip": 0,
                    "order": "dateDesc",
                },
            )
            if resp2.status_code == 200:
                data2 = resp2.json().get("data", {})
                for fb in data2.get("feedbacks", []):
                    pd = fb.get("productDetails", {})
                    nm_id = pd.get("nmId")
                    if not nm_id:
                        continue
                    if nm_id not in counts:
                        counts[nm_id] = {
                            "name": pd.get("productName", "?"),
                            "count": 0,
                        }
                    counts[nm_id]["count"] += 1

        return counts
