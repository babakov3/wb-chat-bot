"""Пошаговый мастер подключения нового магазина (онбординг).

Шаги:
  1/4 — Название магазина
  2/4 — WB API токен (с валидацией через WB API)
  3/4 — Шаблон сообщения (превью, выбор, ввод контакта)
  4/4 — Выбор товаров для отслеживания

Состояние хранится в user_state:
  - onboarding_step — текущий шаг ("1", "2", "3", "4")
  - onboarding_data — JSON с накопленными данными (имя, токен, товары и т.д.)
  - input_waiting — тип ожидаемого текстового ввода (ob:name, ob:token и т.д.)

Соглашение о префиксах callback_data:
  ``ob:`` — все callback-кнопки мастера онбординга используют этот префикс.
  Примеры: ob:cancel, ob:back:2, ob:tpl:0, ob:prod:all, ob:pt:<nm_id>, ob:pp:<page>.

Для минимизации сообщений в чате бот создаёт одно сообщение при старте
и редактирует его на каждом шаге. ID сообщения хранится в onboarding_data["msg_id"].
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

# Количество товаров на одной странице при выборе на шаге 4.
# Ограничено из-за лимита Telegram на количество inline-кнопок в сообщении.
PAGE_SIZE = 8


class OnboardingWizard:
    """Multi-step setup wizard: Name -> Token -> Message -> Products -> Done."""

    def __init__(
        self,
        storage: Storage,
        telegram: TelegramClient,
    ) -> None:
        """Инициализация мастера онбординга.

        Args:
            storage: Хранилище данных (SQLite) для сохранения состояния и создания магазина.
            telegram: Клиент Telegram Bot API для отправки/редактирования сообщений.
        """
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

    def _get_msg_id(self, chat_id: str) -> int | None:
        data = self._get_data(chat_id)
        return data.get("msg_id")

    async def _edit(
        self,
        chat_id: str,
        text: str,
        reply_markup: dict[str, Any] | None = None,
    ) -> None:
        """Edit the single onboarding message.  Falls back to send if missing."""
        msg_id = self._get_msg_id(chat_id)
        if msg_id:
            await self._tg.edit_message_text(
                chat_id, msg_id, text, reply_markup=reply_markup
            )
        else:
            # Fallback: send a new message and persist its id
            new_id = await self._tg.send_message(
                chat_id, text, reply_markup=reply_markup
            )
            if new_id:
                data = self._get_data(chat_id)
                data["msg_id"] = new_id
                self._save_data(chat_id, data)

    # ── Entry point ──────────────────────────────────────────────

    async def start(self, chat_id: str) -> None:
        """Begin onboarding from step 1. Send the initial message."""
        self._set_step(chat_id, "1")
        self._storage.set_user_state(chat_id, input_waiting="ob:name")

        text = (
            "<b>Шаг 1/4 — Название магазина</b>\n\n"
            "Введите название вашего магазина на Wildberries.\n"
            "Это имя будет отображаться в боте для удобства."
        )
        markup = _kb([[("Отмена", "ob:cancel")]])
        new_id = await self._tg.send_message(chat_id, text, reply_markup=markup)
        # Persist the message id for future edits
        self._save_data(chat_id, {"msg_id": new_id})

    # ── Step 1: Store name ───────────────────────────────────────

    async def _step_1_name(self, chat_id: str) -> None:
        """Шаг 1/4: Запрашивает название магазина.

        Устанавливает input_waiting="ob:name" и редактирует сообщение
        с инструкцией для пользователя.
        """
        self._set_step(chat_id, "1")
        self._storage.set_user_state(chat_id, input_waiting="ob:name")
        text = (
            "<b>Шаг 1/4 — Название магазина</b>\n\n"
            "Введите название вашего магазина на Wildberries.\n"
            "Это имя будет отображаться в боте для удобства."
        )
        markup = _kb([[("Отмена", "ob:cancel")]])
        await self._edit(chat_id, text, reply_markup=markup)

    # ── Step 2: API token ────────────────────────────────────────

    async def _step_2_token(self, chat_id: str) -> None:
        """Шаг 2/4: Запрашивает WB API токен.

        Показывает инструкцию по получению токена и устанавливает
        input_waiting="ob:token". Валидация происходит в handle_text.
        """
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
        await self._edit(chat_id, text, reply_markup=markup)

    # ── Step 3: Message ──────────────────────────────────────────

    async def _step_3_message(self, chat_id: str) -> None:
        """Шаг 3/4: Выбор шаблона сообщения или ввод своего.

        Показывает список готовых шаблонов из TEMPLATES и кнопку «Написать своё».
        При выборе шаблона — показывается превью, при нажатии «Использовать» —
        запрашивается контакт (если в шаблоне есть {contact}).
        """
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
        await self._edit(chat_id, text, reply_markup=_kb(rows))

    async def _show_template_preview(self, chat_id: str, idx: int) -> None:
        """Показывает полный текст шаблона с кнопками «Использовать» / «Назад».

        Сохраняет индекс выбранного шаблона в onboarding_data["tpl_idx"]
        для последующего применения.

        Args:
            chat_id: Идентификатор чата Telegram.
            idx: Индекс шаблона в массиве TEMPLATES.
        """
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
        await self._edit(chat_id, text, reply_markup=markup)

    async def _ask_contact(self, chat_id: str) -> None:
        """Запрашивает контакт пользователя для подстановки в плейсхолдер {contact}.

        Устанавливает input_waiting="ob:contact". Контакт обрабатывается
        в handle_text и подставляется в текст выбранного шаблона.
        """
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
        await self._edit(chat_id, text, reply_markup=markup)

    async def _prompt_custom_message(self, chat_id: str) -> None:
        """Запрашивает у пользователя произвольный текст сообщения.

        Устанавливает input_waiting="ob:msg". Текст сохраняется
        в onboarding_data["message_text"] через handle_text.
        """
        self._storage.set_user_state(chat_id, input_waiting="ob:msg")
        text = (
            "<b>Напишите сообщение</b>\n\n"
            "Отправьте текст следующим сообщением.\n"
            "Это сообщение будет автоматически отправляться "
            "клиентам с негативными отзывами.\n\n"
            "Вы можете использовать переносы строк."
        )
        markup = _kb([[("Назад к шаблонам", "ob:back:3")]])
        await self._edit(chat_id, text, reply_markup=markup)

    # ── Step 4: Products ─────────────────────────────────────────

    async def _step_4_products(self, chat_id: str) -> None:
        """Шаг 4/4: Выбор товаров для отслеживания.

        Предлагает два варианта: отслеживать все товары или выбрать
        конкретные из каталога WB. При выборе «из каталога» загружаются
        карточки товаров через WB Content API.
        """
        self._set_step(chat_id, "4")
        self._storage.set_user_state(chat_id, input_waiting=None)

        text = (
            "<b>Шаг 4/4 — Товары</b>\n\n"
            "Выберите, по каким товарам отслеживать негатив."
        )
        markup = _kb([
            [("Все товары", "ob:prod:all")],
            [("Выбрать из каталога", "ob:prod:pick")],
            [("Назад", "ob:back:3")],
        ])
        await self._edit(chat_id, text, reply_markup=markup)

    async def _load_and_show_products(self, chat_id: str, page: int = 0) -> None:
        """Загружает каталог товаров из WB Content API и показывает выбор с пагинацией.

        При первом вызове загружает карточки товаров через WBClient.get_product_cards()
        и кэширует их в onboarding_data["products"]. На последующих вызовах
        (переключение страниц, выбор товара) использует кэш.

        Каждая страница содержит PAGE_SIZE товаров с чекбоксами и навигацией.

        Args:
            chat_id: Идентификатор чата Telegram.
            page: Номер страницы (с нуля).
        """
        data = self._get_data(chat_id)
        api_token = data.get("api_token", "")
        products = data.get("products", [])

        if not products:
            # Show loading state in the main message
            await self._edit(chat_id, "Загружаю каталог товаров из WB...")
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
            await self._edit(chat_id, text, reply_markup=markup)
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
        await self._edit(chat_id, text, reply_markup=_kb(rows))

    # ── Done ─────────────────────────────────────────────────────

    async def _finish(self, chat_id: str) -> None:
        """Create store in DB, set as active, show summary."""
        data = self._get_data(chat_id)

        store_name = data.get("store_name", "Магазин")
        api_token = data.get("api_token", "")
        message_text = data.get("message_text", "")
        selected = data.get("selected_products", [])
        products = data.get("products", [])
        msg_id = data.get("msg_id")

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
        markup = _build_keyboard(None)
        # Edit the main onboarding message with the final summary
        if msg_id:
            await self._tg.edit_message_text(
                chat_id, msg_id, text, reply_markup=markup
            )
        else:
            await self._tg.send_message(chat_id, text, reply_markup=markup)

    # ── Callback handler ─────────────────────────────────────────

    async def handle_callback(self, chat_id: str, data: str, message_id: int) -> None:
        """Диспетчер всех callback-запросов с префиксом ``ob:``.

        Обрабатывает навигацию (ob:back:N, ob:cancel), выбор шаблонов
        (ob:tpl:N, ob:tpl:use, ob:msg:custom), управление товарами
        (ob:prod:all, ob:prod:pick, ob:prod:selall, ob:prod:none,
        ob:pt:<nm_id>, ob:pp:<page>, ob:prod:done).
        """
        parts = data.split(":")

        # Cancel
        if data == "ob:cancel":
            self._clear(chat_id)
            from app.commands import _build_keyboard
            await self._tg.edit_message_text(
                chat_id, message_id, "Настройка отменена.",
                reply_markup=_build_keyboard(None),
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

    async def handle_text(self, chat_id: str, text: str, user_msg_id: int = 0) -> None:
        """Обрабатывает текстовый ввод пользователя во время онбординга.

        Определяет тип ожидаемого ввода по полю input_waiting:
          - ``ob:name``    — название магазина (шаг 1) -> переход к шагу 2
          - ``ob:token``   — API токен (шаг 2) -> валидация и переход к шагу 3
          - ``ob:contact`` — контакт для шаблона (шаг 3) -> переход к шагу 4
          - ``ob:msg``     — произвольное сообщение (шаг 3) -> переход к шагу 4

        Сообщение пользователя удаляется для чистоты чата (если user_msg_id задан).

        Args:
            chat_id: Идентификатор чата Telegram.
            text: Текст, отправленный пользователем.
            user_msg_id: ID сообщения пользователя для удаления (0 = не удалять).
        """
        user_state = self._storage.get_user_state(chat_id)
        if not user_state:
            return
        waiting = user_state.get("input_waiting")
        if not waiting or not waiting.startswith("ob:"):
            return

        self._storage.set_user_state(chat_id, input_waiting=None)
        data = self._get_data(chat_id)

        # Delete the user's text message to keep the chat tidy
        if user_msg_id:
            await self._tg.delete_message(chat_id, user_msg_id)

        if waiting == "ob:name":
            # Step 1: store name entered
            data["store_name"] = text.strip()
            self._save_data(chat_id, data)
            await self._step_2_token(chat_id)

        elif waiting == "ob:token":
            # Step 2: API token entered — validate
            token = text.strip()
            await self._edit(chat_id, "Проверяю токен...")
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
                await self._edit(chat_id, "Токен валиден!")
                await self._step_3_message(chat_id)
            else:
                self._storage.set_user_state(chat_id, input_waiting="ob:token")
                await self._edit(
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
            await self._step_4_products(chat_id)

        elif waiting == "ob:msg":
            # Step 3: custom message entered
            data["message_text"] = text
            self._save_data(chat_id, data)
            await self._step_4_products(chat_id)
