"""Telegram command and button handlers — multi-store aware.

All handlers receive chat_id as first argument and look up
the user's active store from user_state.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, TYPE_CHECKING

from app.onboarding import OnboardingWizard
from app.router import MessageRouter
from app.settings import SettingsUI, _kb
from app.storage import Storage
from app.telegram_client import TelegramClient
from app.wb_client import WBClientPool

if TYPE_CHECKING:
    from app.service import ChatService

logger = logging.getLogger("wb_chat_bot")

# Button labels — bottom menu
BTN_STATUS = "📊 Статус"
BTN_STOP = "🛑 СТОП"
BTN_STORES = "🏪 Магазины"
BTN_SETTINGS = "⚙️ Настройки"
BTN_ANALYTICS = "📈 Аналитика"


def _build_keyboard(store: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build reply keyboard with emergency stop."""
    return {
        "keyboard": [
            [{"text": BTN_STATUS}, {"text": BTN_STOP}],
            [{"text": BTN_SETTINGS}, {"text": BTN_ANALYTICS}],
            [{"text": BTN_STORES}],
        ],
        "resize_keyboard": True,
        "is_persistent": True,
    }


def register_all(
    router: MessageRouter,
    service: ChatService,
    storage: Storage,
    telegram: TelegramClient,
    wb_pool: WBClientPool,
) -> None:
    """Register all commands, buttons, and handlers on the router."""

    settings_ui = SettingsUI(storage, telegram, wb_pool)
    onboarding = OnboardingWizard(storage, telegram)

    # ── /start ────────────────────────────────────────────────────

    async def cmd_start(chat_id: str) -> None:
        stores = storage.get_stores_for_user(chat_id)
        if not stores:
            await onboarding.start(chat_id)
            return

        # Ensure active store is set
        active_id = storage.get_active_store_id(chat_id)
        if active_id is None:
            storage.set_active_store(chat_id, stores[0]["id"])
            active_id = stores[0]["id"]

        store = storage.get_store(active_id)
        if not store:
            await onboarding.start(chat_id)
            return

        mode_icon = "🟢" if store["app_mode"] == "production" else "🟡"
        mode_label = "БОЕВОЙ" if store["app_mode"] == "production" else "ТЕСТ"
        state = "работает" if store["is_active"] else "на паузе"

        text = (
            "<b>WB Негатив Бот</b>\n\n"
            f"{mode_icon} Магазин: <b>{store['store_name']}</b>\n"
            f"Режим: <b>{mode_label}</b> | {state}\n"
            f"Подключено магазинов: {len(stores)}"
        )
        await telegram.send_message(chat_id, text, reply_markup=_build_keyboard(store))

    # ── Status ────────────────────────────────────────────────────

    async def cmd_status(chat_id: str) -> None:
        store = _get_active_store(chat_id)
        if not store:
            await telegram.send_message(chat_id, "Нет магазинов. Нажмите /start")
            return

        mode_icon = "🟢" if store["app_mode"] == "production" else "🟡"
        mode_label = "БОЕВОЙ" if store["app_mode"] == "production" else "ТЕСТ"
        stats = storage.get_stats(store["id"])

        wl = store.get("product_whitelist", "")
        all_products = storage.get_store_products(store["id"])
        total_products = len(all_products) if all_products else 0
        if wl:
            from app.settings import _parse_int_set
            wl_count = len(_parse_int_set(wl))
            prod_label = f"{wl_count} из {total_products}"
        else:
            prod_label = f"все ({total_products})"

        # Token health
        token_ok = bool(store.get("wb_api_token"))
        token_label = "✅ Токен: активен" if token_ok else "❌ Токен: не задан"

        # Active/paused
        active_label = "▶️ Работает" if store["is_active"] else "⏸ На паузе"

        # Last processed
        last = storage.get_last_chats(store["id"], 1)
        if last:
            last_row = last[0]
            last_label = f"📅 Посл. обработка: {last_row['processed_at'][:16].replace('T', ' ')}"
        else:
            last_label = "📅 Посл. обработка: ещё не было"

        # Last error
        last_err = storage.get_last_error(store["id"])
        if last_err:
            err_label = f"⚠️ Посл. ошибка: {last_err[:50]}"
        else:
            err_label = "✅ Ошибок нет"

        text = (
            f"{mode_icon} <b>{store['store_name']}</b> — {mode_label}\n\n"
            f"{active_label}\n"
            f"{token_label}\n"
            f"📦 Товаров: <b>{prod_label}</b>\n"
            f"📨 Обработано: <b>{stats.get('total', 0)}</b>\n"
            f"{last_label}\n"
            f"{err_label}"
        )
        await telegram.send_message(chat_id, text, reply_markup=_build_keyboard(store))

    # ── Emergency STOP ─────────────────────────────────────────────

    async def cmd_stop_all(chat_id: str) -> None:
        """Emergency stop — deactivate ALL stores for this user."""
        stores = storage.get_stores_for_user(chat_id)
        if not stores:
            await telegram.send_message(chat_id, "Нет магазинов.")
            return
        count = 0
        for s in stores:
            if s["is_active"]:
                storage.update_store(s["id"], is_active=0)
                count += 1
        await telegram.send_message(
            chat_id,
            f"🛑 <b>ВСЕ магазины остановлены ({count} шт.)</b>\n\n"
            "Бот больше не отправляет сообщения.\n"
            "Для запуска: ⚙️ Настройки → переключить режим.",
            reply_markup=_build_keyboard(None),
        )

    # ── Analytics ─────────────────────────────────────────────────

    async def cmd_analytics(chat_id: str) -> None:
        store = _get_active_store(chat_id)
        if not store:
            await telegram.send_message(chat_id, "Нет магазинов. Нажмите /start")
            return

        store_id = store["id"]
        stats = storage.get_stats(store_id)
        total = stats.get("total", 0)

        if total == 0:
            await telegram.send_message(
                chat_id,
                f"📈 <b>Аналитика — {store['store_name']}</b>\n\n"
                "Данных пока нет. Бот ещё не обработал ни одного чата.",
            )
            return

        lines = [f"📈 <b>Аналитика — {store['store_name']}</b>\n"]
        sent = stats.get("sent", 0)
        dry = stats.get("dry-run", 0)
        err = stats.get("error", 0)
        lines.append(
            f"Всего: <b>{total}</b>\n"
            f"✅ Отправлено: {sent}\n"
            f"🟡 Тест: {dry}\n"
            f"❌ Ошибки: {err}\n"
        )

        categories = storage.get_complaints_by_category(store_id)
        if categories:
            lines.append("<b>По категориям:</b>")
            for c in categories:
                cat = c["complaint_category"] or "Другое"
                lines.append(f"  • {cat}: <b>{c['cnt']}</b>")
            lines.append("")

        products = storage.get_complaints_by_product(store_id)
        if products:
            lines.append("<b>По товарам:</b>")
            for p in products:
                nm = p["nm_id"]
                name = p["product_name"]
                if name and len(name) > 25:
                    name = name[:25] + "..."
                label = f"{nm} — {name}" if name else str(nm)
                avg_r = p["avg_rating"]
                avg_str = f" (ср. {avg_r:.1f}⭐)" if avg_r else ""
                lines.append(f"  • {label}: <b>{p['cnt']}</b>{avg_str}")

        await telegram.send_message(chat_id, "\n".join(lines))

    # ── Settings ──────────────────────────────────────────────────

    async def cmd_settings(chat_id: str) -> None:
        store = _get_active_store(chat_id)
        if not store:
            await telegram.send_message(chat_id, "Нет магазинов. Нажмите /start")
            return
        await settings_ui.show_settings(chat_id)

    # ── Stores ────────────────────────────────────────────────────

    async def cmd_stores(chat_id: str) -> None:
        stores = storage.get_stores_for_user(chat_id)
        active_id = storage.get_active_store_id(chat_id)

        if not stores:
            await telegram.send_message(chat_id, "Нет магазинов. Нажмите /start")
            return

        lines = ["🏪 <b>Ваши магазины</b>\n"]
        for s in stores:
            is_current = s["id"] == active_id
            marker = "▸ " if is_current else "  "
            mode = "ТЕСТ" if s["app_mode"] == "dry-run" else "БОЕВОЙ"
            state_icon = "✅" if s["is_active"] else "⏸"
            lines.append(
                f"{marker}{state_icon} <b>{s['store_name']}</b> [{mode}]"
            )

        rows: list[list[tuple[str, str]]] = []
        for s in stores:
            if s["id"] != active_id:
                rows.append([(f"Переключить на: {s['store_name']}", f"st:switch:{s['id']}")])
        rows.append([("➕ Добавить магазин", "st:add")])
        markup = _kb(rows)

        await telegram.send_message(chat_id, "\n".join(lines), reply_markup=markup)

    # ── Callback router ──────────────────────────────────────────

    async def on_callback(chat_id: str, data: str, message_id: int) -> None:
        """Route callbacks by prefix."""
        if data.startswith("s:") or data.startswith("st:"):
            if data == "st:add":
                await onboarding.start(chat_id)
                return
            await settings_ui.handle_callback(chat_id, data, message_id)
        elif data.startswith("ob:"):
            await onboarding.handle_callback(chat_id, data, message_id)
        elif data.startswith("grp:link:"):
            # grp:link:<store_id>:<group_id>
            parts = data.split(":")
            if len(parts) >= 4:
                store_id = int(parts[2])
                group_id = parts[3]
                store = storage.get_store(store_id)
                if store:
                    storage.update_store(store_id, notification_group_id=group_id)
                    await telegram.send_message(
                        group_id,
                        f"✅ Группа привязана к <b>{store['store_name']}</b>\n"
                        f"Уведомления будут приходить сюда."
                    )

    # ── Text input router ────────────────────────────────────────

    async def on_text_input(chat_id: str, text: str, message_id: int) -> None:
        """Route text input based on input_waiting prefix."""
        user_state = storage.get_user_state(chat_id)
        if not user_state:
            return
        waiting = user_state.get("input_waiting") or ""

        if waiting.startswith("ob:"):
            await onboarding.handle_text(chat_id, text, message_id)
        elif waiting.startswith("s:"):
            await settings_ui.handle_text_input(chat_id, text)

    # ── Helper ────────────────────────────────────────────────────

    def _get_active_store(chat_id: str) -> dict[str, Any] | None:
        store_id = storage.get_active_store_id(chat_id)
        if store_id is None:
            stores = storage.get_stores_for_user(chat_id)
            if stores:
                storage.set_active_store(chat_id, stores[0]["id"])
                store_id = stores[0]["id"]
            else:
                return None
        return storage.get_store(store_id)

    # ── /connect — link group to store ─────────────────────────────

    async def cmd_connect(chat_id: str) -> None:
        """Link a Telegram group (and topic) to ALL stores at once."""
        group_id = chat_id
        thread_id = getattr(router, '_last_thread_id', None)

        all_stores = storage._conn.execute(
            "SELECT id, store_name FROM stores"
        ).fetchall()

        if not all_stores:
            await telegram.send_message(group_id, "Нет подключённых магазинов.", message_thread_id=thread_id)
            return

        names = []
        for s in all_stores:
            storage.update_store(
                s["id"],
                notification_group_id=group_id,
                notification_thread_id=str(thread_id) if thread_id else "",
            )
            names.append(s["store_name"])

        names_str = "\n".join(f"  ✅ {n}" for n in names)
        topic_note = " (топик привязан)" if thread_id else ""
        await telegram.send_message(
            group_id,
            f"<b>Группа подключена{topic_note}</b>\n\n"
            f"{names_str}\n\n"
            f"Сюда будут приходить уведомления о новых обращениях и ответах клиентов.",
            message_thread_id=thread_id,
        )

    async def cmd_disconnect(chat_id: str) -> None:
        """Unlink group from store."""
        stores = storage._conn.execute(
            "SELECT id, store_name FROM stores WHERE notification_group_id = ?",
            (chat_id,),
        ).fetchall()
        if not stores:
            await telegram.send_message(chat_id, "Эта группа не привязана ни к одному магазину.")
            return
        for s in stores:
            storage.update_store(s["id"], notification_group_id="")
        names = ", ".join(s["store_name"] for s in stores)
        await telegram.send_message(chat_id, f"✅ Группа отвязана от: {names}")

    # ── Register everything ──────────────────────────────────────

    router.register_command("start", cmd_start)
    router.register_command("status", cmd_status)
    router.register_command("analytics", cmd_analytics)
    router.register_command("settings", cmd_settings)
    router.register_command("connect", cmd_connect)
    router.register_command("disconnect", cmd_disconnect)

    router.register_button(BTN_STATUS, cmd_status)
    router.register_button(BTN_STOP, cmd_stop_all)
    router.register_button(BTN_SETTINGS, cmd_settings)
    router.register_button(BTN_ANALYTICS, cmd_analytics)
    router.register_button(BTN_STORES, cmd_stores)

    router.set_callback_handler(on_callback)
    router.set_text_input_handler(on_text_input)
