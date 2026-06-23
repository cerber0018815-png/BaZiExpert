import asyncio
import logging
import os
import uuid
from datetime import datetime
from typing import Optional, List, Any

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
    FSInputFile, LabeledPrice, PreCheckoutQuery,
    ReplyKeyboardRemove
)
import asyncpg
from asyncpg import Pool, Record

# ========== КОНФИГУРАЦИЯ ==========
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(id.strip()) for id in os.getenv("ADMIN_IDS", "").split(",") if id.strip()]
DATABASE_URL = os.getenv("DATABASE_URL")
PROVIDER_TOKEN = os.getenv("PROVIDER_TOKEN")
BOT_USERNAME = os.getenv("BOT_USERNAME", "BaziExpert_Bot")

if not all([BOT_TOKEN, DATABASE_URL, PROVIDER_TOKEN]):
    raise ValueError("Не все переменные окружения заданы! Нужны: BOT_TOKEN, DATABASE_URL, PROVIDER_TOKEN")

# ========== ИНИЦИАЛИЗАЦИЯ ==========
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=storage)

db_pool: Optional[Pool] = None

# ========== БАЗА ДАННЫХ ==========

async def create_tables():
    async with db_pool.acquire() as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                tg_id BIGINT UNIQUE NOT NULL,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS orders (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(tg_id),
                status TEXT DEFAULT 'pending',
                birth_date TEXT,
                birth_time TEXT,
                birth_city TEXT,
                gender TEXT,
                email TEXT,
                file_path TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS payments (
                id SERIAL PRIMARY KEY,
                order_id INTEGER REFERENCES orders(id),
                user_id BIGINT,
                amount INTEGER,
                payload TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')
        await conn.execute('''
            INSERT INTO settings (key, value) VALUES ('price', '5000')
            ON CONFLICT (key) DO NOTHING
        ''')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_users_tg_id ON users(tg_id)')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id)')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status)')
    logger.info("Таблицы созданы/проверены")

async def drop_tables():
    async with db_pool.acquire() as conn:
        await conn.execute('DROP TABLE IF EXISTS payments CASCADE')
        await conn.execute('DROP TABLE IF EXISTS orders CASCADE')
        await conn.execute('DROP TABLE IF EXISTS users CASCADE')
        await conn.execute('DROP TABLE IF EXISTS settings CASCADE')
    logger.info("Таблицы удалены")

async def init_db_pool():
    global db_pool
    db_pool = await asyncpg.create_pool(
        dsn=DATABASE_URL,
        min_size=1,
        max_size=10,
        command_timeout=60
    )
    logger.info("Пул соединений с PostgreSQL создан")
    await create_tables()

async def close_db_pool():
    if db_pool:
        await db_pool.close()
        logger.info("Пул соединений закрыт")

# ========== РАБОТА С НАСТРОЙКАМИ ==========
async def get_setting(key: str, default: str = None) -> Optional[str]:
    async with db_pool.acquire() as conn:
        row = await conn.fetchval('SELECT value FROM settings WHERE key = $1', key)
        return row if row else default

async def set_setting(key: str, value: str) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO settings (key, value) VALUES ($1, $2)
            ON CONFLICT (key) DO UPDATE SET value = $2
        ''', key, value)

async def get_price() -> int:
    price_str = await get_setting('price', '5000')
    try:
        price = int(price_str.strip())
        if price <= 0:
            return 5000
        return price
    except (ValueError, AttributeError):
        return 5000

# ========== CRUD ПОЛЬЗОВАТЕЛЕЙ И ЗАКАЗОВ ==========
async def get_user(tg_id: int) -> Optional[Record]:
    async with db_pool.acquire() as conn:
        return await conn.fetchrow('SELECT * FROM users WHERE tg_id = $1', tg_id)

async def create_user(tg_id: int, username: str, first_name: str, last_name: str) -> bool:
    async with db_pool.acquire() as conn:
        try:
            await conn.execute('''
                INSERT INTO users (tg_id, username, first_name, last_name)
                VALUES ($1, $2, $3, $4)
            ''', tg_id, username, first_name, last_name)
            return True
        except asyncpg.UniqueViolationError:
            return False

async def create_order(
    user_id: int,
    birth_date: str,
    birth_time: str,
    birth_city: str,
    gender: str,
    email: Optional[str]
) -> int:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow('''
            INSERT INTO orders (user_id, status, birth_date, birth_time, birth_city, gender, email)
            VALUES ($1, 'pending', $2, $3, $4, $5, $6)
            RETURNING id
        ''', user_id, birth_date, birth_time, birth_city, gender, email)
        return row['id']

async def update_order_status(order_id: int, status: str) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute('''
            UPDATE orders SET status = $1, updated_at = CURRENT_TIMESTAMP
            WHERE id = $2
        ''', status, order_id)

async def get_order(order_id: int) -> Optional[Record]:
    async with db_pool.acquire() as conn:
        return await conn.fetchrow('SELECT * FROM orders WHERE id = $1', order_id)

async def get_orders_by_user(user_id: int) -> List[Record]:
    async with db_pool.acquire() as conn:
        return await conn.fetch('SELECT * FROM orders WHERE user_id = $1 ORDER BY created_at DESC', user_id)

async def get_active_orders_for_user(user_id: int) -> List[Record]:
    async with db_pool.acquire() as conn:
        return await conn.fetch('''
            SELECT * FROM orders WHERE user_id = $1 AND status NOT IN ('done', 'cancelled')
            ORDER BY created_at DESC
        ''', user_id)

async def get_orders_by_status(status: str) -> List[Record]:
    async with db_pool.acquire() as conn:
        return await conn.fetch('SELECT * FROM orders WHERE status = $1 ORDER BY created_at DESC', status)

async def save_order_file(order_id: int, file_path: str) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute('''
            UPDATE orders SET file_path = $1, status = 'done', updated_at = CURRENT_TIMESTAMP
            WHERE id = $2
        ''', file_path, order_id)

async def save_payment_history(order_id: int, user_id: int, amount: int, payload: str) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO payments (order_id, user_id, amount, payload)
            VALUES ($1, $2, $3, $4)
        ''', order_id, user_id, amount, payload)

# ========== FSM СОСТОЯНИЯ ==========
class OrderStates(StatesGroup):
    waiting_birth_date = State()
    waiting_birth_time = State()
    waiting_city = State()
    waiting_gender = State()
    waiting_email = State()
    confirm_order = State()
    waiting_upload_order_id = State()
    waiting_broadcast_content = State()  # универсальное состояние для рассылки
    waiting_new_price = State()

class AdminStates(StatesGroup):
    waiting_reset_confirm1 = State()
    waiting_reset_confirm2 = State()

# ========== КЛАВИАТУРЫ ==========
def get_main_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📝 Заказать разбор")],
            [KeyboardButton(text="ℹ️ О сервисе"), KeyboardButton(text="📞 Контакты")]
        ],
        resize_keyboard=True
    )

def get_admin_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📋 Все заявки")],
            [KeyboardButton(text="⏳ Новые заявки"), KeyboardButton(text="📋 Текущий заказ")],
            [KeyboardButton(text="📊 Статистика"), KeyboardButton(text="📢 Рассылка")],
            [KeyboardButton(text="💰 Изменить цену"), KeyboardButton(text="🔍 Проверить платежи")]
        ],
        resize_keyboard=True
    )

def get_order_confirm_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить и оплатить", callback_data="confirm_order")],
            [InlineKeyboardButton(text="✏️ Исправить данные", callback_data="edit_order")]
        ]
    )

def get_skip_email_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="⏭️ Пропустить")]],
        resize_keyboard=True
    )

def get_status_button_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 Проверить статус", callback_data="check_status")]
        ]
    )

def get_active_order_choice_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Создать ещё один заказ", callback_data="create_new_order")],
            [InlineKeyboardButton(text="📊 Проверить статус", callback_data="check_status")]
        ]
    )

def get_take_order_keyboard(order_id: int):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Взять в работу", callback_data=f"take_{order_id}")]
        ]
    )

# ========== ОБЩИЕ ХЕНДЛЕРЫ ==========
@dp.message(Command("cancel"))
async def cmd_cancel(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        await message.answer("❌ Нет активного действия для отмены.")
        return
    await state.clear()
    await message.answer("✅ Действие отменено.")

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    user = await get_user(user_id)
    if not user:
        await create_user(
            user_id,
            message.from_user.username,
            message.from_user.first_name,
            message.from_user.last_name
        )
    if user_id in ADMIN_IDS:
        await message.answer(
            "👋 Добро пожаловать в панель администратора!",
            reply_markup=get_admin_keyboard()
        )
        return
    price = await get_price()
    text = (
        "🌟 Добро пожаловать в BaziExpertBot!\n\n"
        "Я помогу вам получить персональный разбор вашей карты Ба Цзы.\n\n"
        "📋 Что вы получите:\n"
        "• Полный анализ 4-х столпов судьбы\n"
        "• Определение Хозяина Дня\n"
        "• Прогноз на текущий год\n"
        "• Рекомендации по элементам\n\n"
        f"💰 Стоимость разбора: {price} ₽\n\n"
        "Для заказа нажмите кнопку ниже 👇"
    )
    await message.answer(text, reply_markup=get_main_keyboard())

# ========== ЗАКАЗ: СБОР ДАННЫХ ==========
@dp.message(F.text == "📝 Заказать разбор")
async def cmd_order(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    active_orders = await get_active_orders_for_user(user_id)
    if active_orders:
        await message.answer(
            "📌 У вас есть активный(е) заказ(ы). Вы можете создать новый заказ или проверить статус существующих.",
            reply_markup=get_active_order_choice_keyboard()
        )
        return
    await message.answer(
        "📅 Введите дату рождения в формате ДД.ММ.ГГГГ\nНапример: 15.08.1990",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(OrderStates.waiting_birth_date)

@dp.callback_query(F.data == "create_new_order")
async def create_new_order_callback(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    await state.clear()
    if callback.message:
        await callback.message.edit_text(
            "✅ Начинаем оформление нового заказа. "
            "Ваши предыдущие заказы останутся активными."
        )
    await callback.message.answer(
        "📅 Введите дату рождения в формате ДД.ММ.ГГГГ\nНапример: 15.08.1990",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(OrderStates.waiting_birth_date)
    await callback.answer()

@dp.message(OrderStates.waiting_birth_date)
async def process_birth_date(message: types.Message, state: FSMContext):
    birth_date = message.text.strip()
    try:
        datetime.strptime(birth_date, "%d.%m.%Y")
    except ValueError:
        await message.answer("❌ Неверный формат! Введите ДД.ММ.ГГГГ")
        return
    await state.update_data(birth_date=birth_date)
    await message.answer(
        "🕐 Введите время рождения в формате ЧЧ:ММ (например, 14:30). Если неизвестно – 12:00",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(OrderStates.waiting_birth_time)

@dp.message(OrderStates.waiting_birth_time)
async def process_birth_time(message: types.Message, state: FSMContext):
    birth_time = message.text.strip()
    try:
        datetime.strptime(birth_time, "%H:%M")
    except ValueError:
        await message.answer("❌ Неверный формат! Введите ЧЧ:ММ")
        return
    await state.update_data(birth_time=birth_time)
    await message.answer(
        "🏙️ Введите место рождения:",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(OrderStates.waiting_city)

@dp.message(OrderStates.waiting_city)
async def process_city(message: types.Message, state: FSMContext):
    city = message.text.strip()
    await state.update_data(birth_city=city)
    await message.answer(
        "👤 Укажите ваш пол:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="👨 Мужской"), KeyboardButton(text="👩 Женский")]],
            resize_keyboard=True
        )
    )
    await state.set_state(OrderStates.waiting_gender)

@dp.message(OrderStates.waiting_gender)
async def process_gender(message: types.Message, state: FSMContext):
    if "Мужской" in message.text:
        gender = "male"
    elif "Женский" in message.text:
        gender = "female"
    else:
        await message.answer("Пожалуйста, выберите пол, нажав кнопку.")
        return
    await state.update_data(gender=gender)
    await message.answer(
        "📧 Введите email (необязательно) или нажмите «Пропустить»:",
        reply_markup=get_skip_email_keyboard()
    )
    await state.set_state(OrderStates.waiting_email)

@dp.message(OrderStates.waiting_email)
async def process_email(message: types.Message, state: FSMContext):
    email = message.text.strip()
    if email == "⏭️ Пропустить":
        email = None
    else:
        if '@' not in email or '.' not in email:
            await message.answer("❌ Введите корректный email или нажмите «Пропустить»")
            return
    await state.update_data(email=email)
    await message.answer(
        "✅ Email сохранён." if email else "✅ Email пропущен.",
        reply_markup=ReplyKeyboardRemove()
    )
    data = await state.get_data()
    price = await get_price()
    confirm_text = (
        "📋 Проверьте данные:\n\n"
        f"📅 Дата рождения: {data['birth_date']}\n"
        f"🕐 Время: {data['birth_time']}\n"
        f"🏙️ Место рождения: {data['birth_city']}\n"
        f"👤 Пол: {'Мужской' if data['gender'] == 'male' else 'Женский'}\n"
        f"📧 Email: {email or 'Не указан'}\n\n"
        f"💰 Стоимость разбора: {price} ₽\n\nВсе верно?"
    )
    await message.answer(confirm_text, reply_markup=get_order_confirm_keyboard())
    await state.set_state(OrderStates.confirm_order)

# ========== ПЛАТЁЖНАЯ ЧАСТЬ ==========
@dp.callback_query(F.data == "confirm_order")
async def confirm_order(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    data = await state.get_data()
    price = await get_price()
    
    if price <= 0:
        await callback.message.edit_text("❌ Ошибка: цена не установлена. Обратитесь к администратору.")
        await callback.answer()
        return

    order_id = await create_order(
        user_id=user_id,
        birth_date=data['birth_date'],
        birth_time=data['birth_time'],
        birth_city=data['birth_city'],
        gender=data['gender'],
        email=data.get('email')
    )

    payload = f"bazi_{order_id}_{int(datetime.now().timestamp())}"

    try:
        await bot.send_invoice(
            chat_id=user_id,
            title="Разбор карты Ба Цзы",
            description=(
                f"Заказ №{order_id}\n"
                f"Персональный разбор вашей карты Ба Цзы\n"
                f"Дата рождения: {data['birth_date']}\n"
                f"Время: {data['birth_time']}\n"
                f"Место рождения: {data['birth_city']}"
            ),
            payload=payload,
            provider_token=PROVIDER_TOKEN,
            currency="RUB",
            prices=[LabeledPrice(label="Разбор Ба Цзы", amount=int(price * 100))],
            start_parameter="bazi_order",
            need_email=True,
            need_phone_number=False,
        )
        await state.update_data(payload=payload)
        await update_order_status(order_id, 'pending_payment')
        await callback.message.delete()
    except Exception as e:
        logger.error(f"Ошибка отправки инвойса: {e}")
        await callback.message.edit_text(f"❌ Не удалось создать платёжный счёт. Ошибка: {e}")
    await callback.answer()

@dp.callback_query(F.data == "edit_order")
async def edit_order(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("🔄 Начнем заново. Введите дату рождения:")
    await state.set_state(OrderStates.waiting_birth_date)
    await callback.answer()

@dp.pre_checkout_query()
async def pre_checkout_handler(pre_checkout_query: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(
        pre_checkout_query.id,
        ok=True,
        error_message="Извините, произошла ошибка. Попробуйте позже."
    )

@dp.message(F.successful_payment)
async def successful_payment_handler(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    payment = message.successful_payment

    payload_parts = payment.invoice_payload.split('_')
    if len(payload_parts) >= 2 and payload_parts[0] == 'bazi':
        order_id = int(payload_parts[1])
    else:
        orders = await get_orders_by_user(user_id)
        pending = [o for o in orders if o['status'] == 'pending_payment']
        if pending:
            order_id = pending[0]['id']
        else:
            await message.answer("❌ Не удалось определить заказ. Обратитесь к администратору.")
            return

    await save_payment_history(order_id, user_id, payment.total_amount, payment.invoice_payload)
    await update_order_status(order_id, 'paid')
    user = await get_user(user_id)
    await notify_admin_new_order(order_id, user)

    await state.clear()

    await message.answer(
    "✅ Оплата успешно получена!\n\n"
    f"Ваш заказ №{order_id} поставлен в очередь на обработку. Как только специалист возьмёт его в работу, вы получите уведомление."
    )

    await message.answer(
        "📌 Вы можете заказать новый разбор или проверить статус существующих заказов.",
        reply_markup=get_main_keyboard()
    )

    await message.answer(
        "📊 Вы можете следить за статусом ваших заказов:",
        reply_markup=get_status_button_keyboard()
    )

# ========== ОБРАБОТЧИК СТАТУСА ==========
@dp.callback_query(F.data == "check_status")
async def check_status(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    active_orders = await get_active_orders_for_user(user_id)
    if not active_orders:
        await callback.message.answer("❌ У вас нет активных заказов.")
        await callback.answer()
        return

    status_text = "📋 **Ваши активные заказы:**\n\n"
    for order in active_orders:
        status_emoji = {
            'pending': '🆕', 'pending_payment': '⏳', 'paid': '✅', 'processing': '🔨'
        }.get(order['status'], '❓')
        status_desc = {
            'pending': 'Ожидает оплаты',
            'pending_payment': 'Ожидает оплаты',
            'paid': 'Оплачен, ожидает начала',
            'processing': 'В процессе изготовления'
        }.get(order['status'], 'Неизвестный статус')
        status_text += f"{status_emoji} Заказ №{order['id']}: {status_desc}\n"
    status_text += "\nВыберите действие:"

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 Проверить статусы заказов", callback_data="check_status")],
            [InlineKeyboardButton(text="✅ Создать ещё один заказ", callback_data="create_new_order")]
        ]
    )
    await callback.message.answer(status_text, parse_mode="Markdown", reply_markup=keyboard)
    await callback.answer()

# ========== ИНФОРМАЦИОННЫЕ КНОПКИ ==========
@dp.message(F.text == "ℹ️ О сервисе")
async def about(message: types.Message):
    price = await get_price()
    text = (
        f"🔮 **О сервисе BaziExpert**\n\n"
        "Профессиональный разбор карты Ба Цзы.\n\n"
        "**Входит:**\n"
        "• Анализ 4-х столпов\n"
        "• Хозяин Дня и элементы\n"
        "• Прогноз на текущий год\n"
        "• Рекомендации\n\n"
        f"💰 Стоимость: {price} ₽\nСрок: 24-48 часов."
    )
    await message.answer(text, parse_mode="Markdown")

@dp.message(F.text == "📞 Контакты")
async def contacts(message: types.Message):
    await message.answer(
        "📞 **Контакты:**\n✉️ @admin_username\n📧 support@baziexpert.ru",
        parse_mode="Markdown"
    )

# ========== АДМИН-ФУНКЦИИ ==========

async def notify_admin_new_order(order_id: int, user: Record):
    order = await get_order(order_id)
    text = (
        f"🆕 НОВЫЙ ЗАКАЗ №{order_id}!\n\n"
        f"👤 {user['first_name']} {user['last_name'] or ''} (@{user['username'] or 'нет'})\n"
        f"🆔 ID пользователя: {user['tg_id']}\n"
        f"📅 Дата рождения: {order['birth_date']} в {order['birth_time']}\n"
        f"🏙️ Место рождения: {order['birth_city']}\n"
        f"👤 Пол: {'Мужской' if order['gender'] == 'male' else 'Женский'}\n"
        f"📧 Email: {order['email'] or 'не указан'}\n"
        "Статус: ОПЛАЧЕН"
    )
    keyboard = get_take_order_keyboard(order_id)
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text, reply_markup=keyboard)
        except Exception as e:
            logger.error(f"Ошибка уведомления админа {admin_id}: {e}")

@dp.message(F.text == "📋 Все заявки")
async def admin_all_orders(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    async with db_pool.acquire() as conn:
        orders = await conn.fetch('''
            SELECT o.*, u.tg_id, u.first_name, u.username
            FROM orders o
            JOIN users u ON o.user_id = u.tg_id
            ORDER BY o.created_at DESC
            LIMIT 20
        ''')
    if not orders:
        await message.answer("📭 Заявок нет.")
        return

    text = "📋 **Все заявки (последние 20):**\n\n"
    for order in orders:
        status_emoji = {
            'pending': '🆕', 'pending_payment': '⏳', 'paid': '✅',
            'processing': '🔨', 'done': '📄'
        }.get(order['status'], '❓')
        created_date = order['created_at'].strftime('%Y-%m-%d') if order['created_at'] else '—'
        file_info = f"📁 Файл: {order['file_path'] or 'не загружен'}" if order['status'] == 'done' else ""
        text += (
            f"{status_emoji} Заказ №{order['id']} | {order['first_name'] or 'Без имени'} (ID: {order['tg_id']})\n"
            f"   Дата заказа: {created_date} | Статус: {order['status']}\n"
            f"   {file_info}\n\n"
        )
    await message.answer(text, parse_mode="Markdown")

@dp.message(F.text == "⏳ Новые заявки")
async def admin_new_orders(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    orders = await get_orders_by_status('paid')
    if not orders:
        await message.answer("🆕 Новых оплаченных заявок нет.")
        return

    for order in orders:
        user = await get_user(order['user_id'])
        if not user:
            continue
        order_id = order['id']
        text = (
            f"🆕 **Заказ №{order_id}**\n\n"
            f"👤 {user['first_name']} {user['last_name'] or ''} (@{user['username'] or 'нет'})\n"
            f"📅 Дата рождения: {order['birth_date']} в {order['birth_time']}\n"
            f"🏙️ Место рождения: {order['birth_city']}\n"
            f"👤 Пол: {'Мужской' if order['gender'] == 'male' else 'Женский'}\n"
            f"📧 Email: {order['email'] or 'не указан'}\n"
            f"📅 Заказано: {order['created_at'].strftime('%d.%m.%Y %H:%M')}"
        )
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="✅ Взять в работу", callback_data=f"take_{order_id}")]
            ]
        )
        await message.answer(text, parse_mode="Markdown", reply_markup=keyboard)

@dp.callback_query(F.data.startswith("take_"))
async def take_order_callback(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("⛔ Нет прав", show_alert=True)
        return
    order_id = int(callback.data.split("_")[1])
    order = await get_order(order_id)
    if not order or order['status'] != 'paid':
        await callback.answer("❌ Заказ уже взят или неактивен", show_alert=True)
        return

    await update_order_status(order_id, 'processing')
    user = await get_user(order['user_id'])

    if user:
        try:
            await bot.send_message(
                user['tg_id'],
                f"🔨 Специалист взял ваш заказ №{order_id} в работу. Ожидайте, разбор будет готов в течение 24-48 часов."
            )
        except Exception as e:
            logger.error(f"Ошибка уведомления клиента {user['tg_id']}: {e}")

    await callback.message.edit_text(
        f"✅ Заказ №{order_id} взят в работу! Клиент уведомлён.",
        reply_markup=None
    )
    await callback.answer("✅ Заказ взят в работу!")

@dp.message(F.text == "📋 Текущий заказ")
async def admin_current_order(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    orders = await get_orders_by_status('processing')
    if not orders:
        await message.answer("📭 Нет заказов в работе.")
        return

    for order in orders:
        user = await get_user(order['user_id'])
        if not user:
            continue
        text = (
            f"🔨 **Текущий заказ №{order['id']}**\n\n"
            f"👤 {user['first_name']} {user['last_name'] or ''} (@{user['username'] or 'нет'})\n"
            f"📅 Дата рождения: {order['birth_date']} в {order['birth_time']}\n"
            f"🏙️ Место рождения: {order['birth_city']}\n"
            f"👤 Пол: {'Мужской' if order['gender'] == 'male' else 'Женский'}\n"
            f"📧 Email: {order['email'] or 'не указан'}\n"
            f"📅 Заказано: {order['created_at'].strftime('%d.%m.%Y %H:%M')}"
        )
        await message.answer(text, parse_mode="Markdown")

@dp.message(F.text == "📊 Статистика")
async def admin_stats(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    async with db_pool.acquire() as conn:
        total = await conn.fetchval('SELECT COUNT(*) FROM orders')
        statuses = await conn.fetch('SELECT status, COUNT(*) FROM orders GROUP BY status')
    status_counts = {row['status']: row['count'] for row in statuses}
    text = (
        f"📊 **Статистика:**\n\n"
        f"📦 Всего заказов: {total}\n\n"
        "**По статусам:**\n"
        f"🆕 Ожидают оплаты: {status_counts.get('pending_payment', 0)}\n"
        f"✅ Оплачено: {status_counts.get('paid', 0)}\n"
        f"🔨 В работе: {status_counts.get('processing', 0)}\n"
        f"📄 Готово: {status_counts.get('done', 0)}"
    )
    await message.answer(text, parse_mode="Markdown")

# ========== РАССЫЛКА (УНИВЕРСАЛЬНАЯ) ==========
@dp.message(F.text == "📢 Рассылка")
async def admin_broadcast(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer(
        "📢 Отправьте сообщение для рассылки.\n\n"
        "Поддерживаются:\n"
        "• Текст (с форматированием Markdown)\n"
        "• Фото (с подписью)\n"
        "• Видео (с подписью)\n"
        "• Документы (с подписью)\n"
        "• Голосовые, анимации и любые другие типы\n\n"
        "Для отмены отправьте /cancel"
    )
    await state.set_state(OrderStates.waiting_broadcast_content)

@dp.message(StateFilter(OrderStates.waiting_broadcast_content))
async def process_broadcast(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Рассылка отменена.")
        return

    # Получаем всех пользователей
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('SELECT tg_id FROM users')

    if not rows:
        await message.answer("❌ Нет пользователей для рассылки.")
        await state.clear()
        return

    # Отправляем предупреждение администратору
    await message.answer(f"⏳ Начинаю рассылку {len(rows)} пользователям...")

    sent = 0
    for row in rows:
        try:
            # Копируем сообщение (сохраняет все типы контента)
            await bot.copy_message(
                chat_id=row['tg_id'],
                from_chat_id=message.chat.id,
                message_id=message.message_id
            )
            sent += 1
            await asyncio.sleep(0.05)  # задержка для защиты от блокировки
        except Exception as e:
            logger.error(f"Ошибка отправки пользователю {row['tg_id']}: {e}")

    await message.answer(f"✅ Рассылка завершена. Отправлено: {sent} сообщений.")
    await state.clear()

# ========== ИЗМЕНЕНИЕ ЦЕНЫ ==========
@dp.message(F.text == "💰 Изменить цену")
async def admin_change_price(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    current_price = await get_price()
    await message.answer(
        f"💰 **Текущая цена:** {current_price} ₽\n\n"
        "Введите новую цену в **рублях** (только число):\n"
        "Например: 7000\n\n"
        "Для отмены отправьте /cancel",
        parse_mode="Markdown"
    )
    await state.set_state(OrderStates.waiting_new_price)

@dp.message(StateFilter(OrderStates.waiting_new_price))
async def process_new_price(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    try:
        new_price = int(message.text.strip())
        if new_price <= 0:
            await message.answer("❌ Цена должна быть больше 0.")
            return
    except ValueError:
        await message.answer("❌ Введите число (например, 7000)")
        return
    await set_setting('price', str(new_price))
    await message.answer(f"✅ Цена успешно изменена на {new_price} ₽")
    await state.clear()

@dp.message(F.text == "🔍 Проверить платежи")
async def admin_check_payment_system(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    if not PROVIDER_TOKEN:
        await message.answer("❌ PROVIDER_TOKEN не задан в переменных окружения.")
        return
    await message.answer(
        "ℹ️ Платёжный провайдер подключён. Токен присутствует.\n"
        "Для полноценной проверки создайте тестовый заказ."
    )

# ========== КОМАНДА СБРОСА БД ==========
@dp.message(Command("resetdb"))
async def cmd_resetdb(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔ Нет прав.")
        return
    await message.answer(
        "⚠️ **ВНИМАНИЕ!** Вы собираетесь полностью удалить всю базу данных.\n"
        "Это действие НЕОБРАТИМО! Все заказы, пользователи, платежи и настройки будут удалены.\n\n"
        "Для подтверждения введите **YES** (заглавными буквами).\n"
        "Для отмены отправьте /cancel или любое другое сообщение."
    )
    await state.set_state(AdminStates.waiting_reset_confirm1)

@dp.message(StateFilter(AdminStates.waiting_reset_confirm1))
async def process_reset_confirm1(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    if message.text.strip() != "YES":
        await message.answer("❌ Сброс отменён (неверный код).")
        await state.clear()
        return
    await message.answer(
        "⚠️ **ПОСЛЕДНЕЕ ПРЕДУПРЕЖДЕНИЕ!**\n"
        "Вы уверены, что хотите безвозвратно удалить ВСЕ данные?\n\n"
        "Для окончательного подтверждения введите **CONFIRM** (заглавными буквами).\n"
        "Для отмены отправьте /cancel или любое другое сообщение."
    )
    await state.set_state(AdminStates.waiting_reset_confirm2)

@dp.message(StateFilter(AdminStates.waiting_reset_confirm2))
async def process_reset_confirm2(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    if message.text.strip() != "CONFIRM":
        await message.answer("❌ Сброс отменён (неверный код).")
        await state.clear()
        return
    await message.answer("⏳ Начинаю сброс базы данных...")
    try:
        await drop_tables()
        await create_tables()
        await message.answer("✅ База данных полностью пересоздана. Все данные удалены.")
        logger.warning(f"База данных сброшена администратором {message.from_user.id}")
    except Exception as e:
        logger.error(f"Ошибка сброса БД: {e}")
        await message.answer(f"❌ Ошибка при сбросе: {e}")
    finally:
        await state.clear()

# ========== ЗАГРУЗКА ФАЙЛОВ ==========
@dp.message(F.document)
async def handle_file_upload(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ Нет прав.")
        return
    doc = message.document
    if not doc.file_name.endswith(('.pdf', '.doc', '.docx', '.txt')):
        await message.answer("❌ Поддерживаются только PDF, DOC, DOCX, TXT")
        return
    if doc.file_size > 50 * 1024 * 1024:
        await message.answer("❌ Файл слишком большой (макс 50 МБ)")
        return

    temp_name = f"temp_{uuid.uuid4().hex}_{doc.file_name}"
    temp_path = f"media/{temp_name}"
    os.makedirs('media', exist_ok=True)

    file = await bot.get_file(doc.file_id)
    await bot.download_file(file.file_path, temp_path)

    await state.update_data(temp_file_path=temp_path, original_name=doc.file_name)
    await message.answer("✏️ Введите номер заказа (число), к которому прикрепить этот файл:")
    await state.set_state(OrderStates.waiting_upload_order_id)

@dp.message(StateFilter(OrderStates.waiting_upload_order_id))
async def process_upload_order_id(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return

    try:
        order_id = int(message.text.strip())
    except ValueError:
        await message.answer("❌ Введите число (номер заказа)")
        return

    order = await get_order(order_id)
    if not order:
        await message.answer("❌ Заказ с таким номером не найден.")
        return

    if order['status'] not in ('paid', 'processing'):
        await message.answer(f"❌ Заказ №{order_id} не в статусе оплачен или в работе (текущий статус: {order['status']}).")
        return

    data = await state.get_data()
    temp_path = data.get('temp_file_path')
    original_name = data.get('original_name')

    if not temp_path or not os.path.exists(temp_path):
        await message.answer("❌ Временный файл не найден. Загрузите файл заново.")
        await state.clear()
        return

    base, ext = os.path.splitext(original_name)
    date_str = datetime.now().strftime("%d.%m.%Y")
    new_filename = f"{base}_{order_id}_{date_str}{ext}"
    final_path = f"media/{new_filename}"

    try:
        os.rename(temp_path, final_path)
        logger.info(f"Файл переименован: {final_path}")

        user = await get_user(order['user_id'])
        if user:
            await bot.send_document(
                user['tg_id'],
                FSInputFile(final_path),
                caption=f"📄 Ваш разбор по заказу №{order_id} готов! Благодарим за доверие."
            )

        await save_order_file(order_id, final_path)

        if user:
            await bot.send_message(
                user['tg_id'],
                "📄 Ваш разбор готов! Если хотите заказать ещё один – воспользуйтесь меню ниже.",
                reply_markup=get_main_keyboard()
            )

        await message.answer(
            f"✅ Файл отправлен по заказу №{order_id}.\n📁 Путь: {final_path}"
        )
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка обработки файла: {e}")
        await message.answer(f"❌ Ошибка: {e}")
        if os.path.exists(temp_path):
            os.remove(temp_path)
        await state.clear()

# ========== КОМАНДА /getfile ==========
@dp.message(Command("getfile"))
async def cmd_getfile(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔ Нет прав.")
        return
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Использование: /getfile <номер_заказа>")
        return
    try:
        order_id = int(parts[1])
    except ValueError:
        await message.answer("❌ Номер заказа должен быть числом.")
        return
    order = await get_order(order_id)
    if not order:
        await message.answer(f"❌ Заказ №{order_id} не найден.")
        return
    if not order['file_path'] or not os.path.exists(order['file_path']):
        await message.answer(f"❌ Файл для заказа №{order_id} отсутствует.")
        return
    try:
        await bot.send_document(
            message.chat.id,
            FSInputFile(order['file_path']),
            caption=f"📁 Файл для заказа №{order_id}"
        )
    except Exception as e:
        await message.answer(f"❌ Ошибка отправки файла: {e}")

# ========== ЗАПУСК ==========
async def main():
    await init_db_pool()
    try:
        await dp.start_polling(bot, skip_updates=True)
    finally:
        await close_db_pool()

if __name__ == "__main__":
    asyncio.run(main())
