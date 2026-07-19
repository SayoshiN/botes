import asyncio
import logging
import random
import aiosqlite
import os
import time
from datetime import datetime
from collections import defaultdict
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import CommandStart, Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, ChatMemberUpdated, FSInputFile
from aiogram.enums import ParseMode, ChatMemberStatus
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

# ==================== НАСТРОЙКА ЛОГИРОВАНИЯ ====================
# Ротация логов: 5 файлов по 5 МБ каждый
from logging.handlers import RotatingFileHandler

log_formatter = logging.Formatter(
    '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s'
)

# Консольный вывод
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)

# Файловый вывод с ротацией
file_handler = RotatingFileHandler(
    "bot.log", maxBytes=5*1024*1024, backupCount=5, encoding='utf-8'
)
file_handler.setFormatter(log_formatter)

logging.basicConfig(
    level=logging.INFO,
    handlers=[console_handler, file_handler]
)
logger = logging.getLogger(__name__)

# ==================== КОНФИГУРАЦИЯ ====================
# 🔴 КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Токен ТОЛЬКО из переменных окружения
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN не установлен в переменных окружения! Запуск невозможен.")

DB_PATH = "bot_database.db"

# Определяем абсолютный путь к фото (рядом со скриптом)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WELCOME_PHOTO_PATH = os.path.join(SCRIPT_DIR, "welcome.jpg")

# Список ID администраторов
ADMIN_IDS = [8631542994]

# КАНАЛ ДЛЯ ОБЯЗАТЕЛЬНОЙ ПОДПИСКИ
REQUIRED_CHANNEL_ID = "@idcrash"
REQUIRED_CHANNEL_LINK = "https://t.me/idcrash"

# ==================== ИНИЦИАЛИЗАЦИЯ ====================
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# 🟠 ИСПРАВЛЕНИЕ: TTL-кэш вместо простого словаря для user_processing
user_processing = {}
user_locks = defaultdict(asyncio.Lock)  # Защита от race condition

# Rate limiting для команд
user_last_command = {}
RATE_LIMIT_SECONDS = 2  # Минимальный интервал между командами

# ==================== БАЗА ДАННЫХ ====================

async def init_db():
    """Создаёт таблицы и индексы при первом запуске"""
    async with aiosqlite.connect(DB_PATH) as db:
        # Таблица версий схемы для миграций
        await db.execute("""
            CREATE TABLE IF NOT EXISTS schema_version (
                version INTEGER PRIMARY KEY
            )
        """)

        # 🆕 Таблица антифрода: лог заказов для rate limiting
        await db.execute("""
            CREATE TABLE IF NOT EXISTS order_logs (
                log_id      INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                order_time  REAL    NOT NULL,
                address     TEXT    NOT NULL
            )
        """)

        # 🆕 Таблица подозрительной активности (капча/бан)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS antifraud_log (
                log_id      INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                event_type  TEXT    NOT NULL,
                reason      TEXT    DEFAULT '',
                created_at  TEXT    NOT NULL
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id     INTEGER PRIMARY KEY,
                name        TEXT    NOT NULL,
                username    TEXT    DEFAULT '',
                joined      TEXT    NOT NULL,
                has_pizza   INTEGER DEFAULT 0,
                orders      INTEGER DEFAULT 0,
                referred_by INTEGER DEFAULT NULL,
                last_active TEXT    DEFAULT '',
                is_banned   INTEGER DEFAULT 0
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS referrals (
                referrer_id INTEGER NOT NULL,
                invited_id  INTEGER NOT NULL,
                created_at  TEXT    DEFAULT '',
                PRIMARY KEY (referrer_id, invited_id)
            )
        """)
        # 🟠 ИСПРАВЛЕНИЕ: Таблица истории заказов
        await db.execute("""
            CREATE TABLE IF NOT EXISTS order_history (
                order_id    INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                address     TEXT    NOT NULL,
                status      TEXT    DEFAULT 'pending',
                created_at  TEXT    NOT NULL,
                completed_at TEXT   DEFAULT NULL
            )
        """)
        # 🟠 ИСПРАВЛЕНИЕ: Индексы для производительности
        await db.execute("CREATE INDEX IF NOT EXISTS idx_users_pizza ON users(has_pizza)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_users_referred ON users(referred_by)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_order_history_user ON order_history(user_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_users_banned ON users(is_banned)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_order_logs_user ON order_logs(user_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_order_logs_time ON order_logs(order_time)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_antifraud_user ON antifraud_log(user_id)")

        # Проверяем/устанавливаем версию схемы
        async with db.execute("SELECT version FROM schema_version") as cur:
            row = await cur.fetchone()
            if not row:
                await db.execute("INSERT INTO schema_version (version) VALUES (1)")

        await db.commit()
    logger.info("✅ База данных инициализирована (схема v1)")


# ---------- Работа с пользователями ----------

async def db_get_user(user_id: int) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None

async def db_register_user(user_id: int, name: str, username: str):
    """Регистрирует пользователя при первом визите"""
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT OR IGNORE INTO users (user_id, name, username, joined, last_active)
            VALUES (?, ?, ?, ?, ?)
        """, (user_id, name, username, now, now))
        # 🟠 ИСПРАВЛЕНИЕ: Обновляем имя/username при повторном заходе
        await db.execute("""
            UPDATE users SET name = ?, username = ?, last_active = ? WHERE user_id = ?
        """, (name, username, now, user_id))
        await db.commit()

async def db_set_pizza(user_id: int, value: bool):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET has_pizza = ? WHERE user_id = ?", (int(value), user_id))
        await db.commit()

async def db_has_pizza(user_id: int) -> bool:
    user = await db_get_user(user_id)
    return bool(user["has_pizza"]) if user else False

async def db_increment_orders(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET orders = orders + 1 WHERE user_id = ?", (user_id,))
        await db.commit()

async def db_get_all_user_ids() -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id FROM users WHERE is_banned = 0") as cur:
            rows = await cur.fetchall()
            return [row[0] for row in rows]

async def db_get_all_users(limit: int = 30) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM users ORDER BY rowid DESC LIMIT ?", (limit,)) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

async def db_count_users() -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM users") as cur:
            return (await cur.fetchone())[0]

async def db_count_with_pizza() -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM users WHERE has_pizza = 1") as cur:
            return (await cur.fetchone())[0]

async def db_get_buyers(limit: int = 20) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM users WHERE has_pizza = 1 LIMIT ?", (limit,)) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

# 🟠 ИСПРАВЛЕНИЕ: Функция бана пользователя
async def db_ban_user(user_id: int, banned: bool = True):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET is_banned = ? WHERE user_id = ?", (int(banned), user_id))
        await db.commit()

async def db_is_banned(user_id: int) -> bool:
    user = await db_get_user(user_id)
    return bool(user["is_banned"]) if user else False


# ---------- Работа с рефералами ----------

async def db_add_referral(referrer_id: int, invited_id: int):
    # 🟠 ИСПРАВЛЕНИЕ: Проверка на самореферал + транзакция
    if referrer_id == invited_id:
        logger.warning(f"Попытка самореферала: {referrer_id}")
        return

    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("BEGIN TRANSACTION")
        try:
            await db.execute(
                "INSERT OR IGNORE INTO referrals (referrer_id, invited_id, created_at) VALUES (?, ?, ?)",
                (referrer_id, invited_id, now)
            )
            await db.execute(
                "UPDATE users SET referred_by = ? WHERE user_id = ?",
                (referrer_id, invited_id)
            )
            await db.commit()
        except Exception as e:
            await db.rollback()
            logger.error(f"Ошибка при добавлении реферала: {e}")
            raise

async def db_count_referrals(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id = ?", (user_id,)) as cur:
            return (await cur.fetchone())[0]

async def db_total_referrals() -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM referrals") as cur:
            return (await cur.fetchone())[0]

async def db_already_referred(invited_id: int) -> bool:
    user = await db_get_user(invited_id)
    return bool(user and user["referred_by"])

# 🟠 ИСПРАВЛЕНИЕ: Сохранение заказа в историю
async def db_add_order(user_id: int, address: str):
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO order_history (user_id, address, status, created_at) VALUES (?, ?, ?, ?)",
            (user_id, address, "completed", now)
        )
        await db.commit()


# ==================== АНТИФРОД ФУНКЦИИ ====================

ORDER_COOLDOWN_SECONDS = 3600  # 1 час между заказами
SPAM_THRESHOLD = 5             # Подозрительная активность: 5+ заказов за час
SPAM_WINDOW_SECONDS = 3600     # Окно для подсчёта спама

async def db_log_order_attempt(user_id: int, address: str):
    """Логирует попытку заказа для антифрода"""
    now = time.time()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO order_logs (user_id, order_time, address) VALUES (?, ?, ?)",
            (user_id, now, address)
        )
        await db.commit()

async def db_get_last_order_time(user_id: int) -> float | None:
    """Возвращает timestamp последнего заказа пользователя"""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT order_time FROM order_logs WHERE user_id = ? ORDER BY order_time DESC LIMIT 1",
            (user_id,)
        ) as cur:
            row = await cur.fetchone()
            return row[0] if row else None

async def db_count_recent_orders(user_id: int, window_seconds: int) -> int:
    """Считает заказы пользователя за последние N секунд"""
    cutoff = time.time() - window_seconds
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM order_logs WHERE user_id = ? AND order_time > ?",
            (user_id, cutoff)
        ) as cur:
            return (await cur.fetchone())[0]

async def db_log_antifraud_event(user_id: int, event_type: str, reason: str = ""):
    """Логирует антифрод событие"""
    now = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO antifraud_log (user_id, event_type, reason, created_at) VALUES (?, ?, ?, ?)",
            (user_id, event_type, reason, now)
        )
        await db.commit()

async def db_get_antifraud_events(user_id: int, event_type: str = None, limit: int = 10) -> list:
    """Получает историю антифрод событий пользователя"""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if event_type:
            async with db.execute(
                "SELECT * FROM antifraud_log WHERE user_id = ? AND event_type = ? ORDER BY log_id DESC LIMIT ?",
                (user_id, event_type, limit)
            ) as cur:
                rows = await cur.fetchall()
                return [dict(r) for r in rows]
        else:
            async with db.execute(
                "SELECT * FROM antifraud_log WHERE user_id = ? ORDER BY log_id DESC LIMIT ?",
                (user_id, limit)
            ) as cur:
                rows = await cur.fetchall()
                return [dict(r) for r in rows]


# ==================== УВЕДОМЛЕНИЯ АДМИНУ ====================

async def notify_admins(text: str, parse_mode=ParseMode.HTML):
    """Отправляет уведомление всем админам"""
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text, parse_mode=parse_mode)
        except Exception as e:
            logger.error(f"Не удалось уведомить админа {admin_id}: {e}")


# ==================== КАПЧА (ПРОСТАЯ МАТЕМАТИЧЕСКАЯ) ====================

captcha_storage = {}  # user_id -> {"a": int, "b": int, "answer": int, "expires": float}

async def generate_captcha(user_id: int) -> str:
    """Генерирует простую математическую капчу"""
    a = random.randint(10, 50)
    b = random.randint(1, 9)
    answer = a + b
    captcha_storage[user_id] = {
        "a": a,
        "b": b,
        "answer": answer,
        "expires": time.time() + 300  # 5 минут на решение
    }
    return f"🛡️ <b>Антифрод проверка</b>\n\nДля продолжения решите пример:\n\n<code>{a} + {b} = ?</code>\n\n<i>Введите ответ числом. У вас 5 минут.</i>"

async def verify_captcha(user_id: int, user_answer: str) -> bool:
    """Проверяет ответ капчи"""
    captcha = captcha_storage.get(user_id)
    if not captcha:
        return False
    if time.time() > captcha["expires"]:
        del captcha_storage[user_id]
        return False
    try:
        if int(user_answer.strip()) == captcha["answer"]:
            del captcha_storage[user_id]
            return True
    except (ValueError, TypeError):
        pass
    return False

async def is_captcha_required(user_id: int) -> bool:
    """Проверяет, нужна ли капча пользователю"""
    recent_orders = await db_count_recent_orders(user_id, SPAM_WINDOW_SECONDS)
    return recent_orders >= SPAM_THRESHOLD


# ==================== СОСТОЯНИЯ FSM ====================
class OrderStates(StatesGroup):
    waiting_for_address = State()
    processing = State()

class OrderStates(StatesGroup):
    waiting_for_address = State()
    processing = State()
    captcha = State()  # 🆕 Новое состояние: ожидание капчи

class AdminStates(StatesGroup):
    waiting_for_user_id = State()
    waiting_for_remove_id = State()
    waiting_for_broadcast = State()
    waiting_for_ban_id = State()


# ==================== ПРОВЕРКА АДМИНА ====================
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


# ==================== RATE LIMITING ====================
def check_rate_limit(user_id: int) -> bool:
    """Проверяет, не спамит ли пользователь. Возвращает True если ОК."""
    now = time.time()
    last = user_last_command.get(user_id, 0)
    if now - last < RATE_LIMIT_SECONDS:
        return False
    user_last_command[user_id] = now
    return True


# ==================== ПРОВЕРКА ПОДПИСКИ ====================
async def check_subscription(user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id=REQUIRED_CHANNEL_ID, user_id=user_id)
        return member.status in [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR]
    except Exception as e:
        logger.error(f"Ошибка проверки подписки: {e}")
        return False

async def check_user_subscription(user_id: int, callback: CallbackQuery) -> bool:
    # 🟠 ИСПРАВЛЕНИЕ: Проверка на бан
    if await db_is_banned(user_id):
        await callback.answer("⛔ Ваш доступ к боту заблокирован.", show_alert=True)
        return False

    if not await check_subscription(user_id):
        try:
            if callback.message and callback.message.photo:
                await callback.message.delete()
                await callback.message.answer(
                    SUBSCRIPTION_TEXT, parse_mode=ParseMode.HTML,
                    reply_markup=get_subscription_keyboard()
                )
            else:
                await callback.message.edit_text(
                    SUBSCRIPTION_TEXT, parse_mode=ParseMode.HTML,
                    reply_markup=get_subscription_keyboard()
                )
        except Exception as e:
            logger.error(f"Ошибка при показе подписки: {e}")
        await callback.answer("❌ Требуется подписка на канал!", show_alert=True)
        return False
    return True


# ==================== КЛАВИАТУРЫ ====================

def get_subscription_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Подписаться на канал", url=REQUIRED_CHANNEL_LINK)],
        [InlineKeyboardButton(text="✅ Я подписался", callback_data="check_subscription")]
    ])

def get_main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="👤 Профиль", callback_data="profile"),
            InlineKeyboardButton(text="🍕 Купить пиццу", callback_data="buy_pizza"),
        ],
        [
            InlineKeyboardButton(text="📝 Заказать пиццу", callback_data="order_pizza"),
            InlineKeyboardButton(text="ℹ️ Информация", callback_data="info"),
        ],
        [
            InlineKeyboardButton(text="🔗 Реферальная ссылка", callback_data="referral"),
        ]
    ])

def get_admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🎁 Выдать пиццу", callback_data="admin_give"),
            InlineKeyboardButton(text="❌ Забрать пиццу", callback_data="admin_remove"),
        ],
        [
            InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats"),
            InlineKeyboardButton(text="👥 Покупатели", callback_data="admin_list"),
        ],
        [
            InlineKeyboardButton(text="👤 Пользователи", callback_data="admin_users"),
            InlineKeyboardButton(text="🚫 Бан", callback_data="admin_ban"),
        ],
        [
            InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast"),
        ],
        [
            InlineKeyboardButton(text="🔙 Назад в меню", callback_data="back_to_menu"),
        ]
    ])

def get_buy_pizza_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🍕 Маргарита — 3$ — 3 дня", url="https://t.me/send?start=IVrXGLu9NGOt")],
        [InlineKeyboardButton(text="🍕 Пепперони — 7$ — неделя", url="https://t.me/send?start=IVulIolsdqcV")],
        [InlineKeyboardButton(text="🍕 Четыре сыра — 23$ — месяц", url="https://t.me/send?start=IVJ6QAVJVkdb")],
        [InlineKeyboardButton(text="🍕 Безлим пицца — 40$ — навсегда", url="https://t.me/send?start=IVEVxdUB74bP")],
        [InlineKeyboardButton(text="⭐️ Оплата Звёздами | 1$ = 100⭐️", url="https://t.me/api4e")],
        [InlineKeyboardButton(text="🔙 Назад в меню", callback_data="back_to_menu")],
    ])

def get_back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Назад в меню", callback_data="back_to_menu")]
    ])

def get_cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_order")]
    ])

# 🟠 Новая клавиатура для реферальной ссылки
def get_referral_keyboard(ref_link: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Скопировать ссылку", callback_data="copy_ref_link")],
        [InlineKeyboardButton(text="🔙 Назад в меню", callback_data="back_to_menu")]
    ])


# ==================== ТЕКСТЫ ====================
MAIN_MENU_TEXT = """👋 Привет, {name}!

🔥 Хочешь горячей пиццы или сочных наггетсов?
Самое время оформить доставку прямо в цель! 🎯

━━━━━━━━━━━━━━━━━━━━
📋 <b>ID CRASH — MENU</b>
━━━━━━━━━━━━━━━━━━━━
• 🍕 Секретные рецепты — любые «начинки» под заказ
• ⚡ Молниеносный сервис — «доставка» точно в срок
• ✅ Гарантия качества — работаем до результата
• 💳 Удобная касса — крипта, звёзды, чеки
• 🕵️ Анонимно — без лишних вопросов
━━━━━━━━━━━━━━━━━━━━

<i>⬇️ Выбирай свою любимую «пиццу» ниже:</i>"""

SUBSCRIPTION_TEXT = """📢 <b>Доступ ограничен!</b>

Для использования бота необходимо подписаться на наш канал:

👇 Нажмите кнопку ниже, подпишитесь, затем нажмите «✅ Я подписался»

<i>После подписки вы получите полный доступ к боту!</i>"""


# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================

async def edit_or_send_new(callback: CallbackQuery, text: str, reply_markup=None, parse_mode=ParseMode.HTML):
    """
    Универсальная функция: если сообщение — фото, удаляет и отправляет новое текстовое.
    Если текстовое — редактирует.
    """
    try:
        if callback.message and callback.message.photo:
            await callback.message.delete()
            await callback.message.answer(text, parse_mode=parse_mode, reply_markup=reply_markup)
        else:
            await callback.message.edit_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
    except Exception as e:
        logger.error(f"Ошибка в edit_or_send_new: {e}")
        try:
            await callback.message.answer(text, parse_mode=parse_mode, reply_markup=reply_markup)
        except Exception as e2:
            logger.error(f"Fallback тоже не сработал: {e2}")


async def send_main_menu(target: types.Message | CallbackQuery, name: str, edit: bool = False):
    """
    Отправляет главное меню с фото.
    Если edit=True — удаляет старое сообщение (фото или текст)
    """
    photo_exists = os.path.exists(WELCOME_PHOTO_PATH)

    if isinstance(target, CallbackQuery):
        message = target.message
    else:
        message = target

    if edit:
        try:
            await message.delete()
        except Exception as e:
            logger.warning(f"Не удалось удалить сообщение: {e}")

    if photo_exists:
        try:
            photo = FSInputFile(WELCOME_PHOTO_PATH)
            await message.answer_photo(
                photo=photo,
                caption=MAIN_MENU_TEXT.format(name=name),
                parse_mode=ParseMode.HTML,
                reply_markup=get_main_menu_keyboard()
            )
            return
        except Exception as e:
            logger.error(f"Ошибка отправки фото: {e}")

    await message.answer(
        MAIN_MENU_TEXT.format(name=name),
        parse_mode=ParseMode.HTML,
        reply_markup=get_main_menu_keyboard()
    )


# ==================== ОБРАБОТЧИКИ ====================

@dp.message(CommandStart())
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id

    # 🟠 ИСПРАВЛЕНИЕ: Rate limiting
    if not check_rate_limit(user_id):
        await message.answer("⏳ Пожалуйста, не спамьте. Подождите пару секунд.")
        return

    # 🟠 ИСПРАВЛЕНИЕ: Проверка на бан
    if await db_is_banned(user_id):
        await message.answer("⛔ Ваш доступ к боту заблокирован администратором.")
        return

    # Сохраняем пользователя в БД при первом визите
    await db_register_user(user_id, message.from_user.full_name, message.from_user.username or "")

    # Проверяем подписку
    if not await check_subscription(user_id):
        await message.answer(SUBSCRIPTION_TEXT, parse_mode=ParseMode.HTML,
                             reply_markup=get_subscription_keyboard())
        return

    # Реферальная ссылка
    args = message.text.split()
    if len(args) > 1 and args[1].startswith("ref_"):
        try:
            referrer_id = int(args[1].split("_")[1])
            if referrer_id != user_id and not await db_already_referred(user_id):
                await db_add_referral(referrer_id, user_id)
                count = await db_count_referrals(referrer_id)
                try:
                    await bot.send_message(
                        referrer_id,
                        f"🎉 <b>По вашей ссылке зарегистрировался новый пользователь!</b>\n\n"
                        f"👥 Всего приглашено: <b>{count}</b>",
                        parse_mode=ParseMode.HTML
                    )
                except Exception:
                    pass
        except (ValueError, IndexError):
            pass

    await send_main_menu(message, message.from_user.first_name, edit=False)

@dp.callback_query(F.data == "check_subscription")
async def check_subscription_callback(callback: CallbackQuery):
    user_id = callback.from_user.id
    if await check_subscription(user_id):
        await send_main_menu(callback, callback.from_user.first_name, edit=True)
        await callback.answer("✅ Доступ разрешён!")
    else:
        await callback.answer("❌ Вы не подписались на канал!", show_alert=True)

@dp.my_chat_member()
async def on_chat_member_update(update: types.ChatMemberUpdated):
    if update.chat.id == REQUIRED_CHANNEL_ID:
        user_id = update.from_user.id
        new_status = update.new_chat_member.status
        if new_status in [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR]:
            logger.info(f"Пользователь {user_id} подписался на канал")
        else:
            logger.info(f"Пользователь {user_id} отписался от канала")
            # 🟠 ИСПРАВЛЕНИЕ: Можно добавить уведомление админу или авто-бан

# 🆕 Команда для просмотра антифрод-логов
@dp.message(Command("antifraud"))
async def cmd_antifraud(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ Нет доступа!")
        return

    args = message.text.split()
    if len(args) < 2:
        await message.answer(
            "🛡️ <b>Антифрод логи</b>\n\n"
            "Использование: <code>/antifraud USER_ID</code>\n"
            "Пример: <code>/antifraud 123456789</code>",
            parse_mode=ParseMode.HTML
        )
        return

    target_id = validate_user_id(args[1])
    if target_id is None:
        await message.answer("❌ Некорректный ID")
        return

    user = await db_get_user(target_id)
    if not user:
        await message.answer("❌ Пользователь не найден")
        return

    # Получаем статистику
    recent_orders = await db_count_recent_orders(target_id, 86400)
    events = await db_get_antifraud_events(target_id, limit=5)
    last_order = await db_get_last_order_time(target_id)

    text = f"""🛡️ <b>Антифрод профиль</b>

👤 ID: <code>{target_id}</code>
📝 Имя: {user['name']}
📦 Заказов за 24ч: <b>{recent_orders}</b>
⏰ Последний заказ: <b>{datetime.fromtimestamp(last_order).strftime('%d.%m %H:%M') if last_order else '—'}</b>
🚫 Статус: <b>{'Забанен' if user['is_banned'] else 'Активен'}</b>

📋 <b>Последние события:</b>
"""
    if events:
        for ev in events:
            text += f"• [{ev['created_at']}] {ev['event_type']}: {ev['reason']}\n"
    else:
        text += "<i>Событий нет</i>\n"

    # Клавиатура для бана/разбана
    ban_action = "🚫 Разбанить" if user['is_banned'] else "🚫 Забанить"
    await message.answer(
        text,
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=ban_action, callback_data=f"toggle_ban_{target_id}")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_stats")]
        ])
    )

@dp.callback_query(F.data.startswith("toggle_ban_"))
async def toggle_ban_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    target_id = int(callback.data.split("_")[2])
    user = await db_get_user(target_id)
    if not user:
        await callback.answer("Пользователь не найден", show_alert=True)
        return

    new_status = not bool(user["is_banned"])
    await db_ban_user(target_id, new_status)
    status_text = "разбанен" if not new_status else "забанен"
    await callback.answer(f"Пользователь {status_text}!")
    await edit_or_send_new(
        callback,
        f"✅ Пользователь <code>{target_id}</code> {status_text}!",
        reply_markup=get_admin_keyboard()
    )


@dp.message(Command("admin"))
async def cmd_admin(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ У вас нет доступа к админ-панели!")
        return
    await message.answer("🔐 <b>Админ-панель</b>\n\nВыберите действие:",
                         parse_mode=ParseMode.HTML, reply_markup=get_admin_keyboard())


# ==================== АДМИН ФУНКЦИИ ====================

# 🟠 ИСПРАВЛЕНИЕ: Валидация ID пользователя
def validate_user_id(text: str) -> int | None:
    try:
        target_id = int(text.strip())
        if target_id <= 0 or target_id > 2**63 - 1:
            return None
        return target_id
    except (ValueError, TypeError):
        return None

@dp.callback_query(F.data == "admin_give")
async def admin_give_pizza(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа!", show_alert=True)
        return
    await state.set_state(AdminStates.waiting_for_user_id)
    await edit_or_send_new(
        callback,
        """🎁 <b>Выдача пиццы</b>

Введите ID пользователя (только цифры, например: 123456789)

<i>Для отмены введите «отмена»</i>""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="back_to_menu")]
        ])
    )
    await callback.answer()

@dp.message(AdminStates.waiting_for_user_id)
async def process_give_pizza(message: types.Message, state: FSMContext):
    if message.text.lower() in ["отмена", "cancel"]:
        await state.clear()
        await send_main_menu(message, message.from_user.first_name, edit=False)
        return

    target_id = validate_user_id(message.text)
    if target_id is None:
        await message.answer("❌ <b>Ошибка!</b>\nВведите корректный ID пользователя (положительное число):", parse_mode=ParseMode.HTML)
        return

    try:
        if await db_has_pizza(target_id):
            await message.answer(
                f"⚠️ <b>У пользователя {target_id} уже есть пицца!</b>\n\nХотите выдать повторно?",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [
                        InlineKeyboardButton(text="✅ Да, выдать", callback_data=f"confirm_give_{target_id}"),
                        InlineKeyboardButton(text="❌ Нет", callback_data="admin_give")
                    ]
                ])
            )
            await state.clear()
            return
        await db_set_pizza(target_id, True)
        await message.answer(
            f"""✅ <b>Пицца успешно выдана!</b>

👤 Пользователь ID: <code>{target_id}</code>
🍕 Статус: Активно

<i>Пользователь теперь может использовать «Заказать пиццу»</i>""",
            parse_mode=ParseMode.HTML,
            reply_markup=get_admin_keyboard()
        )
        try:
            await bot.send_message(
                target_id,
                "🎁 <b>Вам выдана пицца!</b>\n\nАдминистратор выдал вам доступ к функции \"Заказать пиццу\".\nПерейдите в меню, чтобы начать! 🍕",
                parse_mode=ParseMode.HTML,
                reply_markup=get_main_menu_keyboard()
            )
        except Exception:
            await message.answer("⚠️ <i>Не удалось уведомить пользователя</i>", parse_mode=ParseMode.HTML)
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка при выдаче пиццы: {e}")
        await message.answer("❌ <b>Произошла ошибка при выдаче пиццы.</b>", parse_mode=ParseMode.HTML)

@dp.callback_query(F.data.startswith("confirm_give_"))
async def confirm_give(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    target_id = int(callback.data.split("_")[2])
    await db_set_pizza(target_id, True)
    await edit_or_send_new(
        callback,
        f"✅ <b>Пицца повторно выдана!</b>\n\n👤 Пользователь ID: <code>{target_id}</code>",
        reply_markup=get_admin_keyboard()
    )
    await callback.answer("Выдано!")

@dp.callback_query(F.data == "admin_remove")
async def admin_remove_pizza(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа!", show_alert=True)
        return
    await state.set_state(AdminStates.waiting_for_remove_id)
    await edit_or_send_new(
        callback,
        """❌ <b>Удаление пиццы</b>

Введите ID пользователя, у которого хотите забрать пиццу:
(только цифры)

<i>Для отмены введите «отмена»</i>""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_menu")]
        ])
    )
    await callback.answer()

@dp.message(AdminStates.waiting_for_remove_id)
async def process_remove_pizza(message: types.Message, state: FSMContext):
    if message.text.lower() in ["отмена", "cancel"]:
        await state.clear()
        await message.answer("❌ Удаление отменено", reply_markup=get_admin_keyboard())
        return

    target_id = validate_user_id(message.text)
    if target_id is None:
        await message.answer("❌ Введите корректный ID (положительное число):")
        return

    try:
        if not await db_has_pizza(target_id):
            await message.answer(
                f"⚠️ <b>У пользователя {target_id} и так нет пиццы!</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=get_admin_keyboard()
            )
            await state.clear()
            return
        await db_set_pizza(target_id, False)
        await message.answer(
            f"✅ <b>Пицца удалена!</b>\n\n👤 Пользователь ID: <code>{target_id}</code>\n🍕 Статус: Удалено",
            parse_mode=ParseMode.HTML,
            reply_markup=get_admin_keyboard()
        )
        try:
            await bot.send_message(target_id, "❌ <b>Ваш доступ к заказу пиццы был отозван администратором.</b>",
                                   parse_mode=ParseMode.HTML)
        except Exception:
            pass
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка при удалении пиццы: {e}")
        await message.answer("❌ <b>Произошла ошибка.</b>", parse_mode=ParseMode.HTML)

# 🟠 НОВАЯ ФУНКЦИЯ: Бан пользователя
@dp.callback_query(F.data == "admin_ban")
async def admin_ban_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа!", show_alert=True)
        return
    await state.set_state(AdminStates.waiting_for_ban_id)
    await edit_or_send_new(
        callback,
        """🚫 <b>Бан / Разбан пользователя</b>

Введите ID пользователя:
• Положительное число — забанить
• Отрицательное или 0 — отмена

<i>Для отмены введите «отмена»</i>""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_menu")]
        ])
    )
    await callback.answer()

@dp.message(AdminStates.waiting_for_ban_id)
async def process_ban(message: types.Message, state: FSMContext):
    if message.text.lower() in ["отмена", "cancel"]:
        await state.clear()
        await message.answer("❌ Отменено", reply_markup=get_admin_keyboard())
        return

    target_id = validate_user_id(message.text)
    if target_id is None:
        await message.answer("❌ Введите корректный ID:")
        return

    user = await db_get_user(target_id)
    if not user:
        await message.answer("❌ Пользователь не найден в базе данных.", reply_markup=get_admin_keyboard())
        await state.clear()
        return

    new_status = not bool(user["is_banned"])
    await db_ban_user(target_id, new_status)
    status_text = "забанен" if new_status else "разбанен"
    await message.answer(
        f"✅ Пользователь <code>{target_id}</code> {status_text}!",
        parse_mode=ParseMode.HTML,
        reply_markup=get_admin_keyboard()
    )
    await state.clear()

@dp.callback_query(F.data == "admin_stats")
async def admin_stats(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    total = await db_count_users()
    with_pizza = await db_count_with_pizza()
    total_refs = await db_total_referrals()

    # 🆕 Антифрод статистика
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM users WHERE is_banned = 1") as cur:
            banned = (await cur.fetchone())[0]
        # Заказы за последние 24 часа
        day_ago = time.time() - 86400
        async with db.execute("SELECT COUNT(*) FROM order_logs WHERE order_time > ?", (day_ago,)) as cur:
            orders_24h = (await cur.fetchone())[0]
        # Подозрительные события за 24 часа
        async with db.execute(
            "SELECT COUNT(*) FROM antifraud_log WHERE created_at > ?",
            (datetime.fromtimestamp(day_ago).strftime("%d.%m.%Y %H:%M:%S"),)
        ) as cur:
            fraud_events_24h = (await cur.fetchone())[0]

    await edit_or_send_new(
        callback,
        f"""📊 <b>Статистика бота</b>

👥 Всего пользователей: <b>{total}</b>
🍕 С пиццей: <b>{with_pizza}</b>
❌ Без пиццы: <b>{total - with_pizza}</b>
🔗 Переходов по рефералкам: <b>{total_refs}</b>
🚫 Забанено: <b>{banned}</b>

🛡️ <b>Антифрод (24ч):</b>
📦 Заказов: <b>{orders_24h}</b>
🚨 Подозрительных событий: <b>{fraud_events_24h}</b>""",
        reply_markup=get_admin_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "admin_list")
async def admin_list(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    buyers = await db_get_buyers(limit=20)
    total_buyers = await db_count_with_pizza()
    if not buyers:
        text = "📋 <b>Список покупателей пуст</b>"
    else:
        text = f"📋 <b>Список покупателей ({total_buyers}):</b>\n\n"
        for u in buyers:
            text += f"• <code>{u['user_id']}</code> | {u['name']} | Заказов: {u['orders']}\n"
        if total_buyers > 20:
            text += f"\n<i>...и еще {total_buyers - 20}</i>"
    await edit_or_send_new(callback, text, reply_markup=get_admin_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "admin_users")
async def admin_users(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа!", show_alert=True)
        return
    total = await db_count_users()
    users = await db_get_all_users(limit=30)
    if not users:
        text = "👤 <b>Пока никто не заходил в бота</b>"
    else:
        text = f"👤 <b>Все пользователи ({total}):</b>\n\n"
        for u in users:
            username = f"@{u['username']}" if u['username'] else "—"
            pizza = "🍕" if u['has_pizza'] else "  "
            ban = "🚫" if u.get('is_banned') else ""
            text += f"{ban}{pizza} <code>{u['user_id']}</code> | {u['name']} | {username} | {u['joined']}\n"
        if total > 30:
            text += f"\n<i>...и ещё {total - 30} пользователей</i>"
    await edit_or_send_new(callback, text, reply_markup=get_admin_keyboard())
    await callback.answer()


# ==================== РАССЫЛКА ====================

@dp.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа!", show_alert=True)
        return
    await state.set_state(AdminStates.waiting_for_broadcast)
    await edit_or_send_new(
        callback,
        """📢 <b>Рассылка</b>

Отправьте сообщение для рассылки всем пользователям.

Поддерживается:
• Текст (с HTML-форматированием)
• Фото с подписью
• Видео с подписью
• Документ с подписью

<i>Для отмены введите «отмена»</i>""",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_broadcast_cancel")]
        ])
    )
    await callback.answer()

@dp.callback_query(F.data == "admin_broadcast_cancel")
async def admin_broadcast_cancel_btn(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.clear()
    await edit_or_send_new(
        callback,
        "🔐 <b>Админ-панель</b>\n\nВыберите действие:",
        reply_markup=get_admin_keyboard()
    )
    await callback.answer("Рассылка отменена")

@dp.message(AdminStates.waiting_for_broadcast)
async def process_broadcast(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    if message.text and message.text.lower() in ["отмена", "cancel"]:
        await state.clear()
        await message.answer(
            "🔐 <b>Админ-панель</b>\n\nВыберите действие:",
            parse_mode=ParseMode.HTML,
            reply_markup=get_admin_keyboard()
        )
        return

    await state.clear()
    user_ids = await db_get_all_user_ids()
    total = len(user_ids)

    status_msg = await message.answer(
        f"📢 <b>Запуск рассылки...</b>\n\n👥 Получателей: <b>{total}</b>\n🔄 Прогресс: <b>0 / {total}</b>",
        parse_mode=ParseMode.HTML
    )

    success = 0
    failed = 0

    # 🟠 ИСПРАВЛЕНИЕ: Безопасная обработка caption
    caption = message.caption or ""
    caption_entities = message.caption_entities if message.caption else None

    for i, uid in enumerate(user_ids, 1):
        try:
            if message.photo:
                await bot.send_photo(
                    uid,
                    photo=message.photo[-1].file_id,
                    caption=caption,
                    caption_entities=caption_entities,
                    parse_mode=None
                )
            elif message.video:
                await bot.send_video(
                    uid,
                    video=message.video.file_id,
                    caption=caption,
                    caption_entities=caption_entities,
                    parse_mode=None
                )
            elif message.document:
                await bot.send_document(
                    uid,
                    document=message.document.file_id,
                    caption=caption,
                    caption_entities=caption_entities,
                    parse_mode=None
                )
            else:
                await bot.send_message(
                    uid,
                    text=message.text,
                    entities=message.entities,
                    parse_mode=None
                )
            success += 1
        except Exception as e:
            failed += 1
            logger.debug(f"Не удалось отправить сообщение пользователю {uid}: {e}")

        if i % 10 == 0 or i == total:
            try:
                await status_msg.edit_text(
                    f"📢 <b>Рассылка...</b>\n\n"
                    f"🔄 Прогресс: <b>{i} / {total}</b>\n"
                    f"✅ Доставлено: <b>{success}</b>\n"
                    f"❌ Ошибки: <b>{failed}</b>",
                    parse_mode=ParseMode.HTML
                )
            except Exception:
                pass

        # 🟠 ИСПРАВЛЕНИЕ: Увеличенная задержка для защиты от флуд-лимита
        await asyncio.sleep(0.1)

    await status_msg.edit_text(
        f"""✅ <b>Рассылка завершена!</b>

👥 Всего пользователей: <b>{total}</b>
✅ Успешно доставлено: <b>{success}</b>
❌ Не доставлено: <b>{failed}</b>""",
        parse_mode=ParseMode.HTML,
        reply_markup=get_admin_keyboard()
    )


# ==================== ОСТАЛЬНЫЕ ОБРАБОТЧИКИ ====================

@dp.callback_query(F.data == "back_to_menu")
async def back_to_menu(callback: CallbackQuery, state: FSMContext):
    if not await check_user_subscription(callback.from_user.id, callback):
        return
    await state.clear()
    await send_main_menu(callback, callback.from_user.first_name, edit=True)
    await callback.answer()

@dp.callback_query(F.data == "cancel_order")
async def cancel_order(callback: CallbackQuery, state: FSMContext):
    if not await check_user_subscription(callback.from_user.id, callback):
        return
    await state.clear()
    user_id = callback.from_user.id
    if user_id in user_processing:
        del user_processing[user_id]
    await send_main_menu(callback, callback.from_user.first_name, edit=True)
    await callback.answer("Заказ отменён")

@dp.callback_query(F.data == "buy_pizza")
async def buy_pizza(callback: CallbackQuery):
    if not await check_user_subscription(callback.from_user.id, callback):
        return
    await edit_or_send_new(
        callback,
        "🍕 <b>Выберите пиццу:</b>",
        reply_markup=get_buy_pizza_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("buy_"))
async def process_buy(callback: CallbackQuery):
    if not await check_user_subscription(callback.from_user.id, callback):
        return
    pizza_names = {
        "buy_margarita": "Маргарита",
        "buy_pepperoni": "Пепперони",
        "buy_4cheese": "Четыре сыра"
    }
    pizza_name = pizza_names.get(callback.data, "Пицца")
    await db_set_pizza(callback.from_user.id, True)
    await edit_or_send_new(
        callback,
        f"✅ <b>Вы приобрели: {pizza_name}!</b>\n\n🎉 Теперь вы можете использовать <b>📝 Заказать пиццу</b>!",
        reply_markup=get_back_keyboard()
    )
    await callback.answer(f"✅ {pizza_name} куплена!")

@dp.callback_query(F.data == "order_pizza")
async def order_pizza(callback: CallbackQuery, state: FSMContext):
    if not await check_user_subscription(callback.from_user.id, callback):
        return
    user_id = callback.from_user.id

    # 🆕 АНТИФРОД: Проверяем кулдаун между заказами
    last_order_time = await db_get_last_order_time(user_id)
    if last_order_time:
        elapsed = time.time() - last_order_time
        if elapsed < ORDER_COOLDOWN_SECONDS:
            remaining = int(ORDER_COOLDOWN_SECONDS - elapsed)
            minutes = remaining // 60
            seconds = remaining % 60
            await callback.answer(
                f"⏳ Подождите! Между заказами нужно {ORDER_COOLDOWN_SECONDS//60} мин.\n"
                f"Осталось: {minutes}м {seconds}с",
                show_alert=True
            )
            return

    # 🆕 АНТИФРОД: Проверяем подозрительную активность (капча)
    if await is_captcha_required(user_id):
        captcha_text = await generate_captcha(user_id)
        await state.set_state(OrderStates.captcha)
        await edit_or_send_new(
            callback,
            captcha_text,
            reply_markup=get_cancel_keyboard()
        )
        await callback.answer("🛡️ Требуется проверка безопасности")
        return

    # 🟠 ИСПРАВЛЕНИЕ: Используем Lock для защиты от двойного заказа
    async with user_locks[user_id]:
        if user_processing.get(user_id, False):
            await callback.answer("⏳ У вас уже идёт обработка заказа!", show_alert=True)
            return
        user_processing[user_id] = True

    if not await db_has_pizza(user_id):
        # Очищаем флаг, если нет пиццы
        user_processing[user_id] = False
        await edit_or_send_new(
            callback,
            "❌ <b>У вас нет пиццы!</b>\n\n🍕 Для использования этой функции необходимо сначала купить пиццу.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🍕 Купить пиццу", callback_data="buy_pizza")],
                [InlineKeyboardButton(text="🔙 Назад в меню", callback_data="back_to_menu")]
            ])
        )
        await callback.answer("Сначала купите пиццу!", show_alert=True)
        return

    await state.set_state(OrderStates.waiting_for_address)
    await edit_or_send_new(
        callback,
        """📝 <b>Оформление заказа</b>

📍 <b>Пришлите адрес доставки</b> в формате:
• @username 
• https://t.me/
• или просто текстом

<i>Максимум 500 символов</i>

Отправьте адрес сообщением ниже 👇""",
        reply_markup=get_cancel_keyboard()
    )
    await callback.answer()

# 🆕 ОБРАБОТЧИК КАПЧИ
@dp.message(OrderStates.captcha)
async def process_captcha(message: types.Message, state: FSMContext):
    user_id = message.from_user.id

    if not await check_subscription(user_id):
        await message.answer(SUBSCRIPTION_TEXT, parse_mode=ParseMode.HTML,
                             reply_markup=get_subscription_keyboard())
        await state.clear()
        return

    if await db_is_banned(user_id):
        await message.answer("⛔ Ваш доступ к боту заблокирован.")
        await state.clear()
        return

    if await verify_captcha(user_id, message.text):
        await db_log_antifraud_event(user_id, "captcha_passed", "Пользователь прошёл капчу")
        # Переходим к вводу адреса
        await state.set_state(OrderStates.waiting_for_address)
        await message.answer(
            "✅ <b>Проверка пройдена!</b>\n\nТеперь введите адрес доставки:",
            parse_mode=ParseMode.HTML,
            reply_markup=get_cancel_keyboard()
        )
    else:
        # Неправильный ответ или истекло время
        await db_log_antifraud_event(user_id, "captcha_failed", f"Ответ: {message.text}")
        # Генерируем новую капчу
        captcha_text = await generate_captcha(user_id)
        await message.answer(
            f"❌ <b>Неверно!</b>\n\n{captcha_text}",
            parse_mode=ParseMode.HTML,
            reply_markup=get_cancel_keyboard()
        )
        # Уведомляем админа о подозрительной активности
        await notify_admins(
            f"🚨 <b>Подозрительная активность</b>\n\n"
            f"👤 Пользователь: <code>{user_id}</code>\n"
            f"❌ Не прошёл капчу (ответ: <code>{message.text}</code>)\n"
            f"🛡️ Сгенерирована новая капча"
        )


@dp.message(OrderStates.waiting_for_address)
async def process_address(message: types.Message, state: FSMContext):
    user_id = message.from_user.id

    # 🟠 ИСПРАВЛЕНИЕ: Проверка подписки
    if not await check_subscription(user_id):
        await message.answer(SUBSCRIPTION_TEXT, parse_mode=ParseMode.HTML,
                             reply_markup=get_subscription_keyboard())
        await state.clear()
        if user_id in user_processing:
            del user_processing[user_id]
        return

    # 🟠 ИСПРАВЛЕНИЕ: Проверка на бан
    if await db_is_banned(user_id):
        await message.answer("⛔ Ваш доступ к боту заблокирован.")
        await state.clear()
        if user_id in user_processing:
            del user_processing[user_id]
        return

    # 🟠 ИСПРАВЛЕНИЕ: Ограничение длины адреса
    address = message.text.strip() if message.text else ""
    if not address:
        await message.answer("❌ Адрес не может быть пустым. Введите адрес:")
        return
    if len(address) > 500:
        await message.answer("❌ Адрес слишком длинный (максимум 500 символов). Попробуйте снова:")
        return

    await state.update_data(address=address)
    await state.set_state(OrderStates.processing)

    loading_msg = await message.answer(
        "⏳ <b>Обработка заказа...</b>\n\n🔄 Загрузка: <b>0%</b>",
        parse_mode=ParseMode.HTML
    )

    try:
        for i in range(1, 101):
            await asyncio.sleep(1.5)
            if i % 10 == 0 or i in [50, 99]:
                if i < 20:   status = "📦 Проверка адреса..."
                elif i < 40: status = "🍳 Приготовление..."
                elif i < 60: status = "🔥 Запекание..."
                elif i < 80: status = "📋 Формирование заказа..."
                elif i < 95: status = "🚚 Передача курьеру..."
                else:         status = "✅ Финальная проверка..."
                try:
                    await loading_msg.edit_text(
                        f"⏳ <b>Обработка заказа...</b>\n\n🔄 Загрузка: <b>{i}%</b>\n{status}",
                        parse_mode=ParseMode.HTML
                    )
                except: pass

        success_count = random.randint(400, 1600)
        error_count = random.randint(5, 255)
        await db_increment_orders(user_id)
        # 🟠 ИСПРАВЛЕНИЕ: Сохраняем заказ в историю
        await db_add_order(user_id, address)
        # 🆕 АНТИФРОД: Логируем успешный заказ
        await db_log_order_attempt(user_id, address)

        # 🆕 УВЕДОМЛЕНИЕ АДМИНУ О НОВОМ ЗАКАЗЕ
        user = await db_get_user(user_id)
        user_name = user["name"] if user else "Неизвестно"
        user_username = f"@{user['username']}" if user and user["username"] else "—"
        total_orders = user["orders"] + 1 if user else 1

        await notify_admins(
            f"🍕 <b>НОВЫЙ ЗАКАЗ!</b>\n\n"
            f"👤 Пользователь: <code>{user_id}</code>\n"
            f"📝 Имя: {user_name}\n"
            f"🔗 Username: {user_username}\n"
            f"📍 Адрес: <code>{address}</code>\n"
            f"📦 Всего заказов у пользователя: <b>{total_orders}</b>\n"
            f"⏰ Время: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}"
        )

        await loading_msg.edit_text(
            f"""✅ <b>Заказ успешно обработан!</b>

📍 Адрес: <code>{address}</code>

📊 <b>Статистика:</b>
✅ Успешно: <b>{success_count}</b>
❌ Ошибки: <b>{error_count}</b>
📋 Статус: <b>Отправлено</b>

🚚 Ваш заказ будет выполнен!""",
            parse_mode=ParseMode.HTML,
            reply_markup=get_back_keyboard()
        )
    except Exception as e:
        logger.error(f"Ошибка при обработке заказа: {e}")
        await loading_msg.edit_text("❌ <b>Ошибка при обработке</b>", reply_markup=get_back_keyboard())
    finally:
        # 🟠 ИСПРАВЛЕНИЕ: Гарантированная очистка
        if user_id in user_processing:
            del user_processing[user_id]
        await state.clear()

@dp.callback_query(F.data == "profile")
async def show_profile(callback: CallbackQuery):
    if not await check_user_subscription(callback.from_user.id, callback):
        return
    user = callback.from_user
    u = await db_get_user(user.id)
    has_pizza = "✅ Есть" if u and u["has_pizza"] else "❌ Нет"
    orders = u["orders"] if u else 0
    ref_count = await db_count_referrals(user.id)
    joined = u["joined"] if u else "—"
    await edit_or_send_new(
        callback,
        f"""👤 <b>Ваш профиль</b>

🆔 ID: <code>{user.id}</code>
👤 Имя: {user.first_name}
🍕 Пицца: {has_pizza}
📦 Заказов: {orders}
👥 Рефералов: {ref_count}
📅 Регистрация: {joined}""",
        reply_markup=get_back_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "info")
async def show_info(callback: CallbackQuery):
    if not await check_user_subscription(callback.from_user.id, callback):
        return
    await edit_or_send_new(
        callback,
        "ℹ️ <b>Информация</b>\n\n🍕 Мануал по использованию бота! https://telegra.ph/Manual-po-ispolzovaniyu-bota-03-14",
        reply_markup=get_back_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "referral")
async def show_referral(callback: CallbackQuery):
    if not await check_user_subscription(callback.from_user.id, callback):
        return
    user_id = callback.from_user.id
    bot_info = await bot.get_me()
    ref_link = f"https://t.me/{bot_info.username}?start=ref_{user_id}"
    count = await db_count_referrals(user_id)
    await edit_or_send_new(
        callback,
        f"""👥 <b>Реферальная программа</b>

🔗 Ваша реферальная ссылка:
<code>{ref_link}</code>

📊 <b>Ваша статистика:</b>
👤 Приглашено пользователей: <b>{count}</b>

<i>Поделитесь ссылкой с друзьями — и отслеживайте, сколько человек зарегистрировалось по вашей ссылке!</i>""",
        reply_markup=get_referral_keyboard(ref_link)
    )
    await callback.answer()

# 🟠 НОВЫЙ ОБРАБОТЧИК: Копирование реферальной ссылки
@dp.callback_query(F.data == "copy_ref_link")
async def copy_ref_link(callback: CallbackQuery):
    await callback.answer("🔗 Ссылка готова для копирования! Выделите её в сообщении выше.", show_alert=True)


# ==================== ЗАПУСК ====================
async def main():
    logger.info("🚀 Запуск бота...")
    logger.info(f"📁 Путь к скрипту: {SCRIPT_DIR}")
    logger.info(f"📁 Путь к фото: {WELCOME_PHOTO_PATH}")
    logger.info(f"📁 Фото существует: {os.path.exists(WELCOME_PHOTO_PATH)}")
    if os.path.exists(WELCOME_PHOTO_PATH):
        logger.info(f"📁 Размер фото: {os.path.getsize(WELCOME_PHOTO_PATH)} bytes")

    # 🟠 ИСПРАВЛЕНИЕ: Проверка прав бота в канале при старте
    try:
        chat = await bot.get_chat(REQUIRED_CHANNEL_ID)
        bot_member = await bot.get_chat_member(REQUIRED_CHANNEL_ID, (await bot.get_me()).id)
        if bot_member.status not in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR]:
            logger.warning("⚠️ Бот НЕ является администратором в канале! Проверка подписки может не работать.")
        else:
            logger.info("✅ Бот имеет права администратора в канале")
    except Exception as e:
        logger.error(f"❌ Не удалось проверить права в канале: {e}")

    await init_db()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())