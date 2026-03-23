"""4-step onboarding wizard for adding a new store.

Steps:
  1/4 — Store name
  2/4 — WB API token (validated)
  3/4 — Message template (preview full text, choose, enter contact)
  4/4 — Product selection

State is stored in user_state: onboarding_step + onboarding_data (JSON).
Callback prefix: ob:
"""

from __future__ import annotations

import json
import logging
from typing import Any

from app.settings import TEMPLATES, _kb
from app.storage import Storage
from app.telegram_client import TelegramClient
from app.wb_client import WBApiError, WBClient

logger = logging.getLogger("wb_chat_bot")

PAGE_SIZE = 8


class OnboardingWizard:
    """Multi-step setup wizard: Name -> Token -> Message -> Products -> Done."""

    def __init__(
        self,
        storage: Storage,
        telegram: TelegramClient,
    ) -> None:
        self._storage = storage
        self._tg = telegram

    # ── State helpers ─────────────────────────────────────────────

    def _get_data(self, chat_id: str) -> dict[str, Any]:
        state = self._storage.get_user_state(chat_id)
        if state and state.get("onboarding_data"):
            try:
                return json.loads(state["onboarding_data"])
            except (json.JSONDecodeError, TypeError):
                pass
        return {}

    def _save_data(self, chat_id: str, data: dict[str, Any]) -> None:
        self._storage.set_user_state(
            chat_id, onboarding_data=json.dumps(data, ensure_ascii=False)
        )

    def _set_step(self, chat_id: str, step: str) -> None:
        self._storage.set_user_state(chat_id, onboarding_step=step)

    def _clear(self, chat_id: str) -> None:
        self._storage.set_user_state(
            chat_id,
            onboarding_step=None,
            onboarding_data=None,
            input_waiting=None,
        )

    # ── Entry point ──────────────────────────────────────────────

    async def start(self, chat_id: str) -> None:
        """Begin onboarding from step 1."""
        self._save_data(chat_id, {})
        await self._step_1_name(chat_id)

    # ── Step 1: Store name ───────────────────────────────────────

    async def _step_1_name(self, chat_id: str) -> None:
        self._set_step(chat_id, "1")
        self._storage.set_user_state(chat_id, input_waiting="ob:name")
        text = (
            "<b>Шаг 1/4 — Название магазина</b>\n\n"
            "Введите название вашего магазина на Wildberries.\n"
            "Это имя будет отображаться в боте для удобства."
        )
        markup = _kb([[("Отмена", "ob:cancel")]])
        await self._tg.send_message(chat_id, text, reply_markup=markup)

    # ── Step 2: API token ────────────────────────────────────────

    async def _step_2_token(self, chat_id: str) -> None:
        self._set_step(chat_id, "2")
        self._storage.set_user_state(chat_id, input_waiting="ob:token")
        data = self._get_data(chat_id)
        store_name = data.get("store_name", "")
        text = (
            f"<b>Шаг 2/4 — API токен</b>\n"
            f"Магазин: <b>{store_name}</b>\n\n"
            "Вставьте API-токен Wildberries с правами:\n"
            "• <b>Чат с покупателями</b>\n"
            "• <b>Контент</b>\n\n"
            "Получить токен: seller.wildberries.ru → Настройки → Доступ к API"
        )
        markup = _kb([[("Назад", "ob:back:1")], [("Отмена", "ob:cancel")]])
        await self._tg.send_message(chat_id, text, reply_markup=markup)

    # ── Step 3: Message ──────────────────────────────────────────

    async def _step_3_message(self, chat_id: str) -> None:
        self._set_step(chat_id, "3")
        self._storage.set_user_state(chat_id, input_waiting=None)
        text = (
            "<b>Шаг 3/4 — Сообщение</b>\n\n"
            "Настройте текст, который будет автоматически "
            "отправляться клиентам с негативными отзывами.\n\n"
            "Выберите готовый шаблон или напишите своё:"
        )
        rows: list[list[tuple[str, str]]] = []
        for i, tpl in enumerate(TEMPLATES):
            rows.append([(tpl["name"], f"ob:tpl:{i}")])
        rows.append([("Написать своё", "ob:msg:custom")])
        rows.append([("Назад", "ob:back:2")])
        await self._tg.send_message(chat_id, text, reply_markup=_kb(rows))

    async def _show_template_preview(self, chat_id: str, idx: int) -> None:
        """Show full template text with Use / Back buttons."""
        if idx < 0 or idx >= len(TEMPLATES):
            return
        tpl = TEMPLATES[idx]
        data = self._get_data(chat_id)
        data["tpl_idx"] = idx
        self._save_data(chat_id, data)

        text = (
            f"<b>Шаблон: {tpl['name']}</b>\n\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"{tpl['text']}\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
            "Если вас устраивает этот шаблон, нажмите «Использовать»."
        )
        markup = _kb([
            [("Использовать", "ob:tpl:use")],
            [("Назад к шаблонам", "ob:back:3")],
        ])
        await self._tg.send_message(chat_id, text, reply_markup=markup)

    async def _ask_contact(self, chat_id: str) -> None:
        """Ask user for their contact (for {contact} placeholder)."""
        self._storage.set_user_state(chat_id, input_waiting="ob:contact")
        data = self._get_data(chat_id)
        tpl_idx = data.get("tpl_idx", 0)
        tpl_name = TEMPLATES[tpl_idx]["name"] if 0 <= tpl_idx < len(TEMPLATES) else "Шаблон"
        text = (
            f"<b>{tpl_name}</b>\n\n"
            "Введите ваш контакт для связи с клиентами.\n"
            "Например: <code>@my_support</code> или ссылку на TG/WhatsApp"
        )
        markup = _kb([[("Назад к шаблонам", "ob:back:3")]])
        await self._tg.send_message(chat_id, text, reply_markup=markup)

    async def _prompt_custom_message(self, chat_id: str) -> None:
        """Ask user to type their own message."""
        self._storage.set_user_state(chat_id, input_waiting="ob:msg")
        text = (
            "<b>Напишите сообщение</b>\n\n"
            "Отправьте текст следующим сообщением.\n"
            "Это сообщение будет автоматически отправляться "
            "клиентам с негативными отзывами.\n\n"
            "Вы можете использовать переносы строк."
        )
        markup = _kb([[("Назад к шаблонам", "ob:back:3")]])
        await self._tg.send_message(chat_id, text, reply_markup=markup)

    # ── Step 4: Products ─────────────────────────────────────────

    async def _step_4_products(self, chat_id: str) -> None:
        self._set_step(chat_id, "4")
        self._storage.set_user_state(chat_id, input_waiting=None)

        # Try to load products from WB Content API
        data = self._get_data(chat_id)
        api_token = data.get("api_token", "")

        text = (
            "<b>Шаг 4/4 — Товары</b>\n\n"
            "Выберите, по каким товарам отслеживать негатив."
        )
        markup = _kb([
            [("Все товары", "ob:prod:all")],
            [("Выбрать из каталога", "ob:prod:pick")],
            [("Назад", "ob:back:3")],
        ])
        await self._tg.send_message(chat_id, text, reply_markup=markup)

    async def _load_and_show_products(self, chat_id: str, page: int = 0) -> None:
        """Load products from WB and show picker."""
        data = self._get_data(chat_id)
        api_token = data.get("api_token", "")
        products = data.get("products", [])

        if not products:
            # Try loading from WB
            await self._tg.send_message(chat_id, "Загружаю каталог товаров из WB...")
            try:
                wb = WBClient(api_token)
                try:
                    cards = await wb.get_product_cards()
                    products = [{"nm_id": c["nm_id"], "name": c["name"]} for c in cards]
                    data["products"] = products
                    self._save_data(chat_id, data)
                finally:
                    await wb.close()
            except WBApiError as exc:
                logger.error("Failed to load products: %s", exc)
            except Exception as exc:
                logger.error("Failed to load products: %s", exc)

        if not products:
            text = (
                "<b>Каталог товаров</b>\n\n"
                "Не удалось загрузить товары из WB.\n"
                "Проверьте, что токен имеет права на «Контент».\n\n"
                "Пока будем отслеживать все товары."
            )
            markup = _kb([[("Продолжить (все товары)", "ob:prod:all")]])
            await self._tg.send_message(chat_id, text, reply_markup=markup)
            return

        selected = set(data.get("selected_products", []))
        data["product_page"] = page
        self._save_data(chat_id, data)

        start = page * PAGE_SIZE
        page_products = products[start : start + PAGE_SIZE]
        total_pages = (len(products) + PAGE_SIZE - 1) // PAGE_SIZE

        all_selected = len(selected) == len(products)
        rows: list[list[tuple[str, str]]] = []

        # "Select all" / "Deselect all" button
        if all_selected:
            rows.append([("Снять все", "ob:prod:none")])
        else:
            rows.append([("Выбрать все", "ob:prod:selall")])

        for p in page_products:
            nm = p["nm_id"]
            name = p["name"]
            if len(name) > 25:
                name = name[:25] + "…"
            icon = "✅" if nm in selected else "⬜"
            rows.append([(f"{icon} {nm} — {name}", f"ob:pt:{nm}")])

        # Navigation
        nav: list[tuple[str, str]] = []
        if page > 0:
            nav.append(("< Назад", f"ob:pp:{page - 1}"))
        if page < total_pages - 1:
            nav.append(("Далее >", f"ob:pp:{page + 1}"))
        if nav:
            rows.append(nav)

        selected_count = len(selected)
        rows.append([(f"Готово ({selected_count} выбрано)", "ob:prod:done")])

        text = (
            f"<b>Выберите товары</b> (стр. {page + 1}/{total_pages})\n\n"
            f"Всего товаров: {len(products)}\n"
            f"Выбрано: {selected_count}\n\n"
            "Нажмите на товар чтобы добавить/убрать:"
        )
        await self._tg.send_message(chat_id, text, reply_markup=_kb(rows))

    # ── Done ─────────────────────────────────────────────────────

    async def _finish(self, chat_id: str) -> None:
        """Create store in DB, set as active, show summary."""
        data = self._get_data(chat_id)

        store_name = data.get("store_name", "Магазин")
        api_token = data.get("api_token", "")
        message_text = data.get("message_text", "")
        selected = data.get("selected_products", [])
        products = data.get("products", [])

        # Build whitelist string
        wl_str = ",".join(str(nm) for nm in sorted(selected)) if selected else ""

        # Create store
        store_id = self._storage.create_store(
            user_chat_id=chat_id,
            store_name=store_name,
            wb_api_token=api_token,
            message_text=message_text,
            product_whitelist=wl_str,
            app_mode="dry-run",
        )

        # Save products to store_products table
        if products:
            self._storage.save_store_products(store_id, products)

        # Set as active store
        self._storage.set_active_store(chat_id, store_id)

        # Clear onboarding state
        self._clear(chat_id)

        # Build summary
        if selected:
            product_names = []
            for nm in sorted(selected):
                found = next((p["name"] for p in products if p["nm_id"] == nm), str(nm))
                product_names.append(found)
            products_text = "\n".join(f"  + {n}" for n in product_names)
        else:
            products_text = "  Все товары"

        msg_preview = message_text[:60]
        if len(message_text) > 60:
            msg_preview += "..."

        msg_type = "индивидуальное" if message_text else "не задано"
        text = (
            f"✅ <b>Магазин «{store_name}» подключён!</b>\n\n"
            f"Режим: ТЕСТ\n"
            f"Сообщение: {msg_type}\n"
            f"Товары: {products_text.strip()}\n\n"
            "Для запуска → ⚙️ Настройки → переключить на БОЕВОЙ"
        )
        from app.commands import _build_keyboard
        await self._tg.send_message(chat_id, text, reply_markup=_build_keyboard(None))

    # ── Callback handler ─────────────────────────────────────────

    async def handle_callback(self, chat_id: str, data: str, message_id: int) -> None:
        """Handle all ob: callbacks."""
        parts = data.split(":")

        # Cancel
        if data == "ob:cancel":
            self._clear(chat_id)
            from app.commands import _build_keyboard
            await self._tg.send_message(
                chat_id, "Настройка отменена.", reply_markup=_build_keyboard(None)
            )
            return

        # Back navigation
        if data == "ob:back:1":
            await self._step_1_name(chat_id)
            return
        if data == "ob:back:2":
            await self._step_2_token(chat_id)
            return
        if data == "ob:back:3":
            await self._step_3_message(chat_id)
            return

        # Step 3: Templates
        if data == "ob:msg:custom":
            await self._prompt_custom_message(chat_id)
            return
        if parts[0] == "ob" and parts[1] == "tpl":
            if len(parts) == 3 and parts[2] not in ("use",):
                await self._show_template_preview(chat_id, int(parts[2]))
                return
            if data == "ob:tpl:use":
                ob_data = self._get_data(chat_id)
                tpl_idx = ob_data.get("tpl_idx", -1)
                if 0 <= tpl_idx < len(TEMPLATES):
                    tpl = TEMPLATES[tpl_idx]
                    if "{contact}" in tpl["text"]:
                        await self._ask_contact(chat_id)
                    else:
                        ob_data["message_text"] = tpl["text"]
                        self._save_data(chat_id, ob_data)
                        await self._step_4_products(chat_id)
                return

        # Step 4: Products
        if data == "ob:prod:all":
            d = self._get_data(chat_id)
            d["selected_products"] = []
            self._save_data(chat_id, d)
            await self._finish(chat_id)
            return
        if data == "ob:prod:pick":
            d = self._get_data(chat_id)
            d["selected_products"] = []
            self._save_data(chat_id, d)
            await self._load_and_show_products(chat_id, 0)
            return
        if data == "ob:prod:selall":
            d = self._get_data(chat_id)
            all_nms = [p["nm_id"] for p in d.get("products", [])]
            d["selected_products"] = all_nms
            self._save_data(chat_id, d)
            page = d.get("product_page", 0)
            await self._load_and_show_products(chat_id, page)
            return
        if data == "ob:prod:none":
            d = self._get_data(chat_id)
            d["selected_products"] = []
            self._save_data(chat_id, d)
            page = d.get("product_page", 0)
            await self._load_and_show_products(chat_id, page)
            return
        if parts[0] == "ob" and parts[1] == "pt" and len(parts) == 3:
            nm = int(parts[2])
            d = self._get_data(chat_id)
            selected = set(d.get("selected_products", []))
            if nm in selected:
                selected.discard(nm)
            else:
                selected.add(nm)
            d["selected_products"] = list(selected)
            self._save_data(chat_id, d)
            page = d.get("product_page", 0)
            await self._load_and_show_products(chat_id, page)
            return
        if parts[0] == "ob" and parts[1] == "pp" and len(parts) == 3:
            await self._load_and_show_products(chat_id, int(parts[2]))
            return
        if data == "ob:prod:done":
            await self._finish(chat_id)
            return

    # ── Text input handler ───────────────────────────────────────

    async def handle_text(self, chat_id: str, text: str) -> None:
        """Handle free-text input during onboarding."""
        user_state = self._storage.get_user_state(chat_id)
        if not user_state:
            return
        waiting = user_state.get("input_waiting")
        if not waiting or not waiting.startswith("ob:"):
            return

        self._storage.set_user_state(chat_id, input_waiting=None)
        data = self._get_data(chat_id)

        if waiting == "ob:name":
            # Step 1: store name entered
            data["store_name"] = text.strip()
            self._save_data(chat_id, data)
            await self._step_2_token(chat_id)

        elif waiting == "ob:token":
            # Step 2: API token entered — validate
            token = text.strip()
            await self._tg.send_message(chat_id, "Проверяю токен...")
            wb = WBClient(token)
            try:
                valid = await wb.check_token()
            except Exception:
                valid = False
            finally:
                await wb.close()

            if valid:
                data["api_token"] = token
                self._save_data(chat_id, data)
                await self._tg.send_message(chat_id, "Токен валиден!")
                await self._step_3_message(chat_id)
            else:
                self._storage.set_user_state(chat_id, input_waiting="ob:token")
                await self._tg.send_message(
                    chat_id,
                    "Токен невалиден или не имеет прав на чаты.\n"
                    "Проверьте токен и отправьте ещё раз."
                )

        elif waiting == "ob:contact":
            # Step 3: contact entered for template
            tpl_idx = data.get("tpl_idx", -1)
            if 0 <= tpl_idx < len(TEMPLATES):
                tpl = TEMPLATES[tpl_idx]
                final = tpl["text"].replace("{contact}", text.strip())
                data["message_text"] = final
                self._save_data(chat_id, data)
                await self._tg.send_message(
                    chat_id,
                    f"Шаблон «{tpl['name']}» применён с контактом: {text.strip()}"
                )
            await self._step_4_products(chat_id)

        elif waiting == "ob:msg":
            # Step 3: custom message entered
            data["message_text"] = text
            self._save_data(chat_id, data)
            await self._tg.send_message(chat_id, "Сообщение сохранено.")
            await self._step_4_products(chat_id)
