"""Inline-интерфейс настроек магазина через Telegram-клавиатуры.

Читает и записывает данные в таблицу stores через Storage.
Показывает настройки активного магазина пользователя (из user_state.active_store_id).

Соглашение о префиксах callback_data:
  - ``s:``  — действия внутри настроек (переключение режима, товары, шаблоны и т.д.)
  - ``st:`` — переключение между магазинами и добавление нового
"""

from __future__ import annotations

import logging
from typing import Any

from app.storage import Storage
from app.telegram_client import TelegramClient
from app.wb_client import WBClientPool

logger = logging.getLogger("wb_chat_bot")


def _kb(rows: list[list[tuple[str, str]]]) -> dict[str, Any]:
    """Build InlineKeyboardMarkup from list of rows of (label, callback_data)."""
    return {
        "inline_keyboard": [
            [{"text": label, "callback_data": data} for label, data in row]
            for row in rows
        ]
    }


# ── Шаблоны сообщений ────────────────────────────────────────────────
# Готовые шаблоны ответов клиентам с негативными отзывами.
# Плейсхолдер {contact} при применении заменяется на контакт пользователя
# (например, @my_support или ссылку на Telegram/WhatsApp).
# Каждый шаблон содержит:
#   - name: короткое название для отображения в меню
#   - text: полный текст сообщения с {contact} плейсхолдером
TEMPLATES = [
    {
        "name": "Компенсация + контакт",
        "text": (
            "Здравствуйте, уважаемый клиент!\n\n"
            "Мы видим, что Вы оставили негативный отзыв о нашем товаре и хотели бы это исправить. "
            "Нам очень жаль, что произошла такая ситуация с товаром. "
            "Сейчас у нас действует специальное предложение для тех клиентов, у кого попался товар с дефектом.\n\n"
            "Брак и неисправность у нашего товара - не частая ситуация, но, к сожалению, иногда случается, "
            "и нам бы хотелось, чтоб вы НЕ остались с бракованным товаром.\n\n"
            "Пожалуйста свяжитесь с нами в нашем официальном канале поддержки - "
            "{contact} для решения любого вопроса по товару, "
            "а также за денежной компенсацией по причине брака\n\n"
            "!ПРОСИМ НЕ ОТВЕЧАТЬ В ЭТОМ ЧАТЕ, ОН АВТОМАТИЧЕСКИЙ, МЫ НЕ УВИДИМ ВАШ ОТВЕТ.\n\n"
            "ПИШИТЕ СРАЗУ НАМ!"
        ),
    },
    {
        "name": "Короткий + контакт",
        "text": (
            "Здравствуйте! Нам жаль, что товар не оправдал ожиданий.\n\n"
            "Мы готовы решить вопрос — напишите нам: {contact}\n\n"
            "В этом чате мы не видим ответы, пишите нам напрямую!"
        ),
    },
    {
        "name": "Замена товара",
        "text": (
            "Здравствуйте! Мы видим ваш отзыв и хотим помочь.\n\n"
            "Если товар оказался с дефектом — мы готовы отправить замену или вернуть деньги.\n\n"
            "Для решения вопроса напишите нам: {contact}\n\n"
            "Этот чат автоматический — пишите нам напрямую!"
        ),
    },
]


def _parse_int_set(raw: str) -> set[int]:
    """Разбирает строку с целыми числами через запятую в множество int.

    Используется для десериализации product_whitelist из БД.
    Некорректные значения (не числа) молча пропускаются.

    Args:
        raw: Строка вида "123,456,789".

    Returns:
        Множество целых чисел, например {123, 456, 789}.
    """
    result: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if part:
            try:
                result.add(int(part))
            except ValueError:
                pass
    return result


def _int_set_to_str(s: set[int]) -> str:
    """Сериализует множество int обратно в строку через запятую.

    Обратная операция к _parse_int_set. Числа сортируются для стабильного вывода.

    Args:
        s: Множество целых чисел.

    Returns:
        Строка вида "123,456,789" или пустая строка, если множество пустое.
    """
    return ",".join(str(x) for x in sorted(s)) if s else ""


class SettingsUI:
    """Inline keyboard UI for per-store settings."""

    def __init__(
        self,
        storage: Storage,
        telegram: TelegramClient,
        wb_pool: WBClientPool,
    ) -> None:
        """Инициализация UI настроек.

        Args:
            storage: Хранилище данных (SQLite) для чтения/записи настроек магазина.
            telegram: Клиент Telegram Bot API для отправки и редактирования сообщений.
            wb_pool: Пул WB-клиентов — используется для сброса клиента при смене токена.
        """
        self._storage = storage
        self._tg = telegram
        self._wb_pool = wb_pool

    # ── Helpers ──────────────────────────────────────────────────

    def _get_active_store(self, chat_id: str) -> dict[str, Any] | None:
        store_id = self._storage.get_active_store_id(chat_id)
        if store_id is None:
            return None
        return self._storage.get_store(store_id)

    def _product_label(self, store_id: int, nm_id: int) -> str:
        name = self._storage.get_store_product_name(store_id, nm_id)
        if name:
            if len(name) > 30:
                name = name[:30] + "..."
            return f"{name} ({nm_id})"
        return str(nm_id)

    # ── Settings main entry ─────────────────────────────────────

    async def show_settings(self, chat_id: str) -> None:
        """Точка входа: отправляет главное меню настроек активного магазина.

        Создаёт новое сообщение с inline-клавиатурой и сохраняет его ID
        в user_state для последующего редактирования.
        """
        store = self._get_active_store(chat_id)
        if not store:
            await self._tg.send_message(chat_id, "Нет активного магазина. Добавьте через /start")
            return
        text, markup = self._main_menu(store)
        msg_id = await self._tg.send_message(chat_id, text, reply_markup=markup)
        if msg_id:
            self._storage.set_user_state(chat_id, menu_message_id=msg_id)

    # ── Callback dispatcher ─────────────────────────────────────

    async def handle_callback(self, chat_id: str, data: str, message_id: int) -> None:
        """Диспетчер всех callback-запросов с префиксами ``s:`` и ``st:``.

        Разбирает callback_data и вызывает соответствующий метод:
        переключение магазина, смена режима, управление товарами,
        шаблонами сообщений, токеном, переименование и удаление.
        """
        parts = data.split(":")

        # ── Store switching (st:) ────────────────────────────────
        if data == "st:list":
            await self._show_store_list(chat_id, message_id)
            return
        if data == "st:add":
            # Trigger onboarding — handled externally
            from app.onboarding import OnboardingWizard
            # The caller (commands.py register_all) should handle st:add
            return
        if parts[0] == "st" and parts[1] == "switch" and len(parts) == 3:
            store_id = int(parts[2])
            store = self._storage.get_store(store_id)
            if store and store["user_chat_id"] == chat_id:
                self._storage.set_active_store(chat_id, store_id)
                await self._tg.edit_message_text(
                    chat_id, message_id,
                    f"Активный магазин: <b>{store['store_name']}</b>",
                )
                # Show settings for new active store
                await self.show_settings(chat_id)
            return

        # ── Settings (s:) ────────────────────────────────────────
        store = self._get_active_store(chat_id)
        if not store:
            await self._tg.edit_message_text(
                chat_id, message_id,
                "Нет активного магазина.",
            )
            return

        store_id = store["id"]

        if data == "s:main":
            await self._show_main(chat_id, store, message_id)

        elif data == "s:mode":
            await self._toggle_mode(chat_id, store, message_id)

        # Message submenu
        elif data == "s:msg":
            await self._show_message_menu(chat_id, store, message_id)
        elif data == "s:msg:view":
            await self._show_message_full(chat_id, store, message_id)
        elif data == "s:msg:edit":
            await self._prompt_message_edit(chat_id, store, message_id)
        elif data == "s:msg:tpl":
            await self._show_templates(chat_id, message_id)
        elif parts[0] == "s" and parts[1] == "tpl" and len(parts) == 3:
            await self._preview_template(chat_id, int(parts[2]), message_id)
        elif data == "s:tpl:use":
            await self._apply_previewed_template(chat_id, message_id)

        # Update API token
        elif data == "s:token":
            await self._prompt_token_update(chat_id, store, message_id)

        # Rename store
        elif data == "s:rename":
            await self._prompt_rename(chat_id, store, message_id)

        # Delete store
        elif data == "s:delete":
            await self._confirm_delete(chat_id, store, message_id)
        elif data == "s:delete:yes":
            await self._delete_store(chat_id, store, message_id)
        elif data == "s:delete:no":
            await self._show_main(chat_id, store, message_id)

        # Products
        elif data == "s:prod":
            await self._show_products(chat_id, store, message_id)
        elif data == "s:prod:selall":
            await self._select_all_products(chat_id, store, message_id)
        elif data == "s:prod:none":
            await self._deselect_all_products(chat_id, store, message_id)
        elif parts[0] == "s" and parts[1] == "prm" and len(parts) == 3:
            await self._toggle_product_in_whitelist(chat_id, store, int(parts[2]), message_id)

    # ── Text input dispatcher ───────────────────────────────────

    async def handle_text_input(self, chat_id: str, text: str) -> None:
        """Обрабатывает текстовый ввод для настроек.

        Вызывается когда пользователь отправляет текст, а в user_state
        стоит input_waiting с префиксом ``s:``. Поддерживает:
          - ``s:rename``      — переименование магазина
          - ``s:token``       — обновление API-токена (с валидацией)
          - ``s:msg_text``    — ввод произвольного текста сообщения
          - ``s:tpl_contact`` — ввод контакта для шаблона с {contact}
        """
        user_state = self._storage.get_user_state(chat_id)
        if not user_state:
            return
        waiting = user_state.get("input_waiting")
        self._storage.set_user_state(chat_id, input_waiting=None)

        store = self._get_active_store(chat_id)
        if not store:
            return

        store_id = store["id"]
        menu_msg = user_state.get("menu_message_id")

        if waiting == "s:rename":
            self._storage.update_store(store_id, store_name=text.strip())
            await self._tg.send_message(chat_id, f"✅ Магазин переименован: <b>{text.strip()}</b>")
            if menu_msg:
                store = self._storage.get_store(store_id)
                await self._show_main(chat_id, store, menu_msg)
            return

        if waiting == "s:token":
            new_token = text.strip()
            # Validate token
            from app.wb_client import WBClient
            try:
                temp_client = WBClient(new_token)
                chats = await temp_client.get_chats_list()
                await temp_client.close()
            except Exception as exc:
                await self._tg.send_message(
                    chat_id,
                    f"❌ Токен невалиден: {exc}\n\nПопробуйте ещё раз или нажмите ⚙️ Настройки.",
                )
                return

            self._storage.update_store(store_id, wb_api_token=new_token, is_active=1)
            # Remove old client from pool so it picks up new token
            await self._wb_pool.remove(store_id)
            await self._tg.send_message(
                chat_id,
                f"✅ API токен обновлён для <b>{store['store_name']}</b>\n"
                "Данные и история сохранены. Магазин активирован.",
            )
            if menu_msg:
                store = self._storage.get_store(store_id)
                await self._show_main(chat_id, store, menu_msg)
            return

        if waiting == "s:msg_text":
            self._storage.update_store(store_id, message_text=text)
            await self._tg.send_message(chat_id, "Сообщение сохранено.")
            if menu_msg:
                store = self._storage.get_store(store_id)
                await self._show_message_menu(chat_id, store, menu_msg)

        elif waiting == "s:tpl_contact":
            import json
            od = user_state.get("onboarding_data")
            if od:
                try:
                    extra = json.loads(od)
                except (json.JSONDecodeError, TypeError):
                    extra = {}
            else:
                extra = {}
            tpl_idx = extra.get("tpl_idx", -1)
            if 0 <= tpl_idx < len(TEMPLATES):
                tpl = TEMPLATES[tpl_idx]
                final_text = tpl["text"].replace("{contact}", text.strip())
                self._storage.update_store(store_id, message_text=final_text)
                await self._tg.send_message(
                    chat_id,
                    f"Шаблон «{tpl['name']}» применён с контактом: {text.strip()}"
                )
            self._storage.set_user_state(chat_id, onboarding_data=None)
            if menu_msg:
                store = self._storage.get_store(store_id)
                await self._show_message_menu(chat_id, store, menu_msg)

    # ── Main menu ────────────────────────────────────────────────

    def _main_menu(self, store: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        """Формирует текст и inline-клавиатуру главного меню настроек.

        Отображает: режим работы, количество отслеживаемых товаров, тип сообщения.
        Кнопки: переключение режима, сообщение, товары, токен, переименование, удаление.

        Args:
            store: Словарь магазина из БД.

        Returns:
            Кортеж (текст сообщения, inline_keyboard markup).
        """
        mode_label = "БОЕВОЙ" if store["app_mode"] == "production" else "ТЕСТ"
        mode_icon = "🟢" if store["app_mode"] == "production" else "🟡"
        wl = store.get("product_whitelist", "")
        wl_items = _parse_int_set(wl) if wl else set()
        wl_count = len(wl_items)

        # Get total products count
        all_products = self._storage.get_store_products(store["id"])
        total_products = len(all_products) if all_products else 0

        # Product label
        if wl_count:
            prod_label = f"{wl_count} из {total_products}" if total_products else str(wl_count)
        else:
            prod_label = f"все ({total_products})" if total_products else "все"

        # Message type
        msg = store.get("message_text", "")
        msg_type = "индивидуальное" if msg else "не задано"

        text = (
            f"{mode_icon} <b>{store['store_name']}</b> — {mode_label}\n\n"
            f"📦 Товаров под контролем: <b>{prod_label}</b>\n"
            f"💬 Сообщение: <b>{msg_type}</b>"
        )

        # Mode toggle button
        if store["app_mode"] == "dry-run":
            mode_btn = ("🟡 ТЕСТ → переключить на БОЕВОЙ", "s:mode")
        else:
            mode_btn = ("🟢 БОЕВОЙ → переключить на ТЕСТ", "s:mode")

        markup = _kb([
            [mode_btn],
            [("✏️ Сообщение", "s:msg")],
            [(f"📦 Товары ({prod_label})", "s:prod")],
            [("🔑 Обновить API токен", "s:token")],
            [("✏️ Переименовать магазин", "s:rename")],
            [("🗑 Удалить магазин", "s:delete")],
        ])
        return text, markup

    async def _show_main(self, chat_id: str, store: dict[str, Any], message_id: int) -> None:
        """Редактирует существующее сообщение, показывая главное меню настроек.

        Сбрасывает input_waiting, чтобы бот не ожидал текстового ввода.
        """
        self._storage.set_user_state(chat_id, input_waiting=None)
        text, markup = self._main_menu(store)
        await self._tg.edit_message_text(chat_id, message_id, text, reply_markup=markup)

    # ── Token update ────────────────────────────────────────────

    async def _prompt_token_update(self, chat_id: str, store: dict[str, Any], message_id: int) -> None:
        """Запрашивает у пользователя новый WB API токен.

        Устанавливает input_waiting="s:token" и показывает инструкцию.
        Сам токен обрабатывается в handle_text_input.
        """
        self._storage.set_user_state(chat_id, input_waiting="s:token")
        await self._tg.edit_message_text(
            chat_id, message_id,
            f"🔑 <b>Обновление API токена</b>\n"
            f"Магазин: {store['store_name']}\n\n"
            "Отправьте новый WB API токен (Чат с покупателями).\n\n"
            "⚠️ Данные и история магазина сохранятся.",
        )

    # ── Mode toggle ──────────────────────────────────────────────

    async def _toggle_mode(self, chat_id: str, store: dict[str, Any], message_id: int) -> None:
        """Переключает режим магазина между ТЕСТ (dry-run) и БОЕВОЙ (production).

        После переключения обновляет главное меню настроек.
        """
        new_mode = "production" if store["app_mode"] == "dry-run" else "dry-run"
        self._storage.update_store(store["id"], app_mode=new_mode)
        logger.info("Store %d mode changed to %s", store["id"], new_mode)
        store = self._storage.get_store(store["id"])
        await self._show_main(chat_id, store, message_id)

    # ── Message submenu ──────────────────────────────────────────

    async def _show_message_menu(self, chat_id: str, store: dict[str, Any], message_id: int) -> None:
        """Показывает подменю управления сообщением для клиентов.

        Отображает превью текущего сообщения и кнопки: посмотреть полностью,
        изменить текст, выбрать шаблон.
        """
        self._storage.set_user_state(chat_id, input_waiting=None)
        msg = store["message_text"]
        preview = msg[:200] + "..." if len(msg) > 200 else msg
        text = (
            f"<b>Сообщение для клиента</b>\n"
            f"Магазин: {store['store_name']}\n\n"
            f"<code>{preview}</code>\n\n"
            f"Длина: {len(msg)} символов"
        )
        markup = _kb([
            [("Посмотреть полностью", "s:msg:view")],
            [("Изменить текст", "s:msg:edit")],
            [("Выбрать шаблон", "s:msg:tpl")],
            [("Назад", "s:main")],
        ])
        await self._tg.edit_message_text(chat_id, message_id, text, reply_markup=markup)

    async def _show_message_full(self, chat_id: str, store: dict[str, Any], message_id: int) -> None:
        """Показывает полный текст текущего сообщения (до 3500 символов)."""
        msg = store["message_text"]
        text = f"<b>Текущее сообщение:</b>\n\n{msg[:3500]}"
        markup = _kb([
            [("Изменить", "s:msg:edit"), ("Шаблоны", "s:msg:tpl")],
            [("Назад", "s:msg")],
        ])
        await self._tg.edit_message_text(chat_id, message_id, text, reply_markup=markup)

    async def _prompt_message_edit(self, chat_id: str, store: dict[str, Any], message_id: int) -> None:
        """Запрашивает у пользователя новый текст сообщения.

        Устанавливает input_waiting="s:msg_text". Текст обрабатывается в handle_text_input.
        """
        self._storage.set_user_state(
            chat_id, input_waiting="s:msg_text", menu_message_id=message_id
        )
        text = (
            "<b>Введите новый текст сообщения</b>\n\n"
            "Просто отправьте текст следующим сообщением.\n"
            "Поддерживаются переносы строк."
        )
        markup = _kb([[("Отмена", "s:msg")]])
        await self._tg.edit_message_text(chat_id, message_id, text, reply_markup=markup)

    async def _show_templates(self, chat_id: str, message_id: int) -> None:
        """Показывает список доступных шаблонов сообщений для выбора."""
        text = (
            "<b>Готовые шаблоны</b>\n\n"
            "Выберите шаблон для просмотра.\n"
            "После выбора вы увидите полный текст."
        )
        rows: list[list[tuple[str, str]]] = []
        for i, tpl in enumerate(TEMPLATES):
            rows.append([(tpl["name"], f"s:tpl:{i}")])
        rows.append([("Назад", "s:msg")])
        markup = _kb(rows)
        await self._tg.edit_message_text(chat_id, message_id, text, reply_markup=markup)

    async def _preview_template(self, chat_id: str, idx: int, message_id: int) -> None:
        """Показывает полный текст выбранного шаблона с кнопками «Использовать» / «Назад».

        Сохраняет индекс шаблона в onboarding_data для последующего применения.
        """
        if idx < 0 or idx >= len(TEMPLATES):
            return
        tpl = TEMPLATES[idx]
        text = (
            f"<b>Шаблон: {tpl['name']}</b>\n\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"{tpl['text']}\n"
            "━━━━━━━━━━━━━━━━━━"
        )
        # Store template index in onboarding_data for later use
        import json
        self._storage.set_user_state(
            chat_id, onboarding_data=json.dumps({"tpl_idx": idx})
        )
        markup = _kb([
            [("Использовать", "s:tpl:use")],
            [("Назад к шаблонам", "s:msg:tpl")],
        ])
        await self._tg.edit_message_text(chat_id, message_id, text, reply_markup=markup)

    async def _apply_previewed_template(self, chat_id: str, message_id: int) -> None:
        """Применяет ранее просмотренный шаблон к магазину.

        Если шаблон содержит плейсхолдер {contact} — запрашивает контакт
        у пользователя. Иначе — сохраняет текст шаблона как message_text магазина.
        """
        import json
        user_state = self._storage.get_user_state(chat_id)
        if not user_state:
            return
        od = user_state.get("onboarding_data")
        if not od:
            return
        try:
            extra = json.loads(od)
        except (json.JSONDecodeError, TypeError):
            return
        tpl_idx = extra.get("tpl_idx", -1)
        if tpl_idx < 0 or tpl_idx >= len(TEMPLATES):
            return
        tpl = TEMPLATES[tpl_idx]

        if "{contact}" in tpl["text"]:
            # Need contact info
            self._storage.set_user_state(
                chat_id,
                input_waiting="s:tpl_contact",
                menu_message_id=message_id,
            )
            text = (
                f"<b>Шаблон: {tpl['name']}</b>\n\n"
                "Введите ваш контакт для связи с клиентами.\n"
                "Например: <code>@my_support</code> или ссылку на TG/WhatsApp"
            )
            markup = _kb([[("Отмена", "s:msg:tpl")]])
            await self._tg.edit_message_text(chat_id, message_id, text, reply_markup=markup)
        else:
            store = self._get_active_store(chat_id)
            if store:
                self._storage.update_store(store["id"], message_text=tpl["text"])
                await self._tg.send_message(
                    chat_id, f"Шаблон «{tpl['name']}» применён."
                )
                store = self._storage.get_store(store["id"])
                await self._show_message_menu(chat_id, store, message_id)

    # ── Products ─────────────────────────────────────────────────

    async def _show_products(self, chat_id: str, store: dict[str, Any], message_id: int) -> None:
        """Показывает экран управления товарами с чекбоксами.

        Если whitelist пуст — отслеживаются все товары. Пользователь может
        включать/выключать конкретные товары или выбрать/снять все.
        Отображается до 15 товаров (ограничение Telegram на кнопки).
        """
        store_id = store["id"]
        wl = _parse_int_set(store.get("product_whitelist", ""))
        products = self._storage.get_store_products(store_id)

        if not products:
            text = (
                f"<b>Товары — {store['store_name']}</b>\n\n"
                "Каталог товаров пуст.\n"
                "Товары загрузятся автоматически при первом опросе."
            )
            markup = _kb([[("Назад", "s:main")]])
            await self._tg.edit_message_text(chat_id, message_id, text, reply_markup=markup)
            return

        if wl:
            text_items = []
            for nm in sorted(wl):
                name = self._storage.get_store_product_name(store_id, nm)
                label = name if name else str(nm)
                text_items.append(f"  + {label}")
            items_str = "\n".join(text_items)
            text = (
                f"<b>Товары — {store['store_name']}</b>\n\n"
                f"Отслеживаемые ({len(wl)}):\n{items_str}\n\n"
                "Нажмите на товар чтобы убрать из отслеживания:"
            )
        else:
            text = (
                f"<b>Товары — {store['store_name']}</b>\n\n"
                "Отслеживаются все товары.\n"
                "Нажмите на товар чтобы включить выборочное отслеживание:"
            )

        all_selected = wl and len(wl) == len(products)
        rows: list[list[tuple[str, str]]] = []

        # "Select all" / "Deselect all"
        if all_selected:
            rows.append([("Снять все", "s:prod:none")])
        else:
            rows.append([("Выбрать все", "s:prod:selall")])

        for p in products[:15]:
            nm = p["nm_id"]
            name = p["name"]
            if len(name) > 25:
                name = name[:25] + "…"
            icon = "✅" if nm in wl else "⬜"
            rows.append([(f"{icon} {nm} — {name}", f"s:prm:{nm}")])

        rows.append([("Назад", "s:main")])
        await self._tg.edit_message_text(chat_id, message_id, text, reply_markup=_kb(rows))

    # ── Rename store ─────────────────────────────────────────────

    async def _prompt_rename(self, chat_id: str, store: dict[str, Any], message_id: int) -> None:
        """Запрашивает новое название магазина. Устанавливает input_waiting="s:rename"."""
        self._storage.set_user_state(
            chat_id, input_waiting="s:rename", menu_message_id=message_id
        )
        text = (
            f"<b>Переименовать «{store['store_name']}»</b>\n\n"
            "Введите новое название магазина:"
        )
        markup = _kb([[("Отмена", "s:main")]])
        await self._tg.edit_message_text(chat_id, message_id, text, reply_markup=markup)

    # ── Delete store ────────────────────────────────────────────

    async def _confirm_delete(self, chat_id: str, store: dict[str, Any], message_id: int) -> None:
        """Показывает подтверждение удаления магазина с предупреждением о необратимости."""
        text = (
            f"⚠️ <b>Удалить магазин «{store['store_name']}»?</b>\n\n"
            "Все настройки, товары и история обработки будут удалены.\n"
            "Это действие нельзя отменить."
        )
        markup = _kb([
            [("❌ Да, удалить", "s:delete:yes")],
            [("Отмена", "s:main")],
        ])
        await self._tg.edit_message_text(chat_id, message_id, text, reply_markup=markup)

    async def _delete_store(self, chat_id: str, store: dict[str, Any], message_id: int) -> None:
        """Удаляет магазин из БД и переключает на следующий (если есть).

        Если других магазинов не осталось — сбрасывает active_store_id
        и предлагает добавить новый через /start.
        """
        store_name = store["store_name"]
        store_id = store["id"]
        self._storage.delete_store(store_id)

        # Switch to another store or clear
        remaining = self._storage.get_stores_for_user(chat_id)
        if remaining:
            self._storage.set_active_store(chat_id, remaining[0]["id"])
            await self._tg.edit_message_text(
                chat_id, message_id,
                f"🗑 Магазин «{store_name}» удалён.\n"
                f"Активный: <b>{remaining[0]['store_name']}</b>",
            )
        else:
            self._storage.set_active_store(chat_id, None)
            await self._tg.edit_message_text(
                chat_id, message_id,
                f"🗑 Магазин «{store_name}» удалён.\n"
                "Нажмите /start чтобы добавить новый.",
            )

    # ── Select/Deselect all products ─────────────────────────────

    async def _select_all_products(
        self, chat_id: str, store: dict[str, Any], message_id: int
    ) -> None:
        """Добавляет все товары магазина в whitelist (выборочное отслеживание всех)."""
        products = self._storage.get_store_products(store["id"])
        all_nms = {p["nm_id"] for p in products}
        self._storage.update_store(store["id"], product_whitelist=_int_set_to_str(all_nms))
        store = self._storage.get_store(store["id"])
        await self._show_products(chat_id, store, message_id)

    async def _deselect_all_products(
        self, chat_id: str, store: dict[str, Any], message_id: int
    ) -> None:
        """Очищает whitelist — бот будет отслеживать все товары без ограничений."""
        self._storage.update_store(store["id"], product_whitelist="")
        store = self._storage.get_store(store["id"])
        await self._show_products(chat_id, store, message_id)

    async def _toggle_product_in_whitelist(
        self, chat_id: str, store: dict[str, Any], nm_id: int, message_id: int
    ) -> None:
        """Переключает товар в whitelist: добавляет если нет, убирает если есть."""
        wl = _parse_int_set(store.get("product_whitelist", ""))
        if nm_id in wl:
            wl.discard(nm_id)
        else:
            wl.add(nm_id)
        self._storage.update_store(store["id"], product_whitelist=_int_set_to_str(wl))
        store = self._storage.get_store(store["id"])
        await self._show_products(chat_id, store, message_id)

    # ── Store switching ──────────────────────────────────────────

    async def _show_store_list(self, chat_id: str, message_id: int) -> None:
        """Показывает список магазинов с кнопками переключения и добавления."""
        stores = self._storage.get_stores_for_user(chat_id)
        active_id = self._storage.get_active_store_id(chat_id)

        if not stores:
            text = "У вас нет магазинов. Нажмите «Добавить» чтобы подключить."
        else:
            lines = ["<b>Ваши магазины</b>\n"]
            for s in stores:
                icon = ">" if s["id"] == active_id else " "
                mode = "ТЕСТ" if s["app_mode"] == "dry-run" else "БОЕВОЙ"
                status = "работает" if s["is_active"] else "на паузе"
                lines.append(f"{icon} <b>{s['store_name']}</b> [{mode}] — {status}")
            text = "\n".join(lines)

        rows: list[list[tuple[str, str]]] = []
        for s in stores:
            if s["id"] != active_id:
                rows.append([(f"Переключить на: {s['store_name']}", f"st:switch:{s['id']}")])
        rows.append([("Добавить магазин", "st:add")])
        rows.append([("Назад", "s:main")])

        await self._tg.edit_message_text(chat_id, message_id, text, reply_markup=_kb(rows))
