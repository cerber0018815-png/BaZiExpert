import asyncio
import logging
import os
import json
import uuid
from datetime import datetime
from typing import Optional, List, Dict, Any

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
    FSInputFile
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
import aiohttp
import asyncpg
from asyncpg import Pool, Record

# ========== КОНФИГУРАЦИЯ ==========
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(id.strip()) for id in os.getenv("ADMIN_IDS", "").split(",") if id.strip()]
DATABASE_URL = os.getenv("DATABASE_URL")  # postgresql://user:pass@host:port/db

YOOKASSA_SHOP_ID = os.getenv("YOOKASSA_SHOP_ID")
YOOKASSA_SECRET_KEY = os.getenv("YOOKASSA_SECRET_KEY")

if not all([BOT_TOKEN, DATABASE_URL, YOOKASSA_SHOP_ID, YOOKASSA_SECRET_KEY]):
    raise ValueError("Не все переменные окружения заданы!")

# ========== ИНИЦИАЛИЗАЦИЯ ==========
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=storage)

# Глобальный пул соединений с БД
db_pool: Optional[Pool] = None

# ========== РАБОТА С БАЗОЙ ДАННЫХ ==========

async def init_db_pool():
    """Создаёт пул соединений и создаёт таблицы, если их нет"""
    global db_pool
    db_pool = await asyncpg.create_pool(
        dsn=DATABASE_URL,
        min_size=1,
        max_size=10,
        command_timeout=60
    )
    logger.info("Пул соединений с PostgreSQL создан")

    async with db_pool.acquire() as conn:
        # Таблица пользователей
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                tg_id BIGINT UNIQUE NOT NULL,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                birth_date TEXT,
                birth_time TEXT,
                birth_city TEXT,
                gender TEXT,
                email TEXT,
                order_status TEXT DEFAULT 'new',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                paid_at TIMESTAMP
            )
        ''')
        # Таблица заказов (привязка платежей и файлов)
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS orders (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(tg_id),
                payment_id TEXT UNIQUE,
                amount INTEGER DEFAULT 5000,
                status TEXT DEFAULT 'pending',
                file_path TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        # Таблица платежей (история)
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS payments (
                id SERIAL PRIMARY KEY,
                payment_id TEXT UNIQUE,
                user_id BIGINT,
                amount INTEGER,
                status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        # Индексы для ускорения
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_users_tg_id ON users(tg_id)')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_users_status ON users(order_status)')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id)')
    logger.info("Таблицы созданы/проверены")

async def close_db_pool():
    """Закрывает пул соединений"""
    if db_pool:
        await db_pool.close()
        logger.info("Пул соединений закрыт")

# Функции CRUD (все асинхронные)

async def get_user(tg_id: int) -> Optional[Record]:
    async with db_pool.acquire() as conn:
        return await conn.fetchrow(
            'SELECT * FROM users WHERE tg_id = $1', tg_id
        )

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

async def update_user_data(tg_id: int, field: str, value: Any) -> None:
    allowed = {'birth_date', 'birth_time', 'birth_city', 'gender', 'email', 'order_status', 'paid_at'}
    if field not in allowed:
        raise ValueError(f"Недопустимое поле: {field}")
    query = f'UPDATE users SET {field} = $1 WHERE tg_id = $2'
    async with db_pool.acquire() as conn:
        await conn.execute(query, value, tg_id)

async def update_order_status(tg_id: int, status: str) -> None:
    await update_user_data(tg_id, 'order_status', status)

async def get_all_orders(status: Optional[str] = None) -> List[Record]:
    async with db_pool.acquire() as conn:
        if status:
            return await conn.fetch('''
                SELECT tg_id, username, first_name, birth_date, birth_time,
                       birth_city, gender, email, order_status, created_at
                FROM users
                WHERE order_status = $1
                ORDER BY created_at DESC
            ''', status)
        else:
            return await conn.fetch('''
                SELECT tg_id, username, first_name, birth_date, birth_time,
                       birth_city, gender, email, order_status, created_at
                FROM users
                ORDER BY created_at DESC
            ''')

async def save_payment(payment_id: str, user_id: int, amount: int, status: str) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO payments (payment_id, user_id, amount, status)
            VALUES ($1, $2, $3, $4)
        ''', payment_id, user_id, amount, status)

async def update_payment_status(payment_id: str, status: str) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute('''
            UPDATE payments SET status = $1 WHERE payment_id = $2
        ''', status, payment_id)

async def save_order_file(tg_id: int, file_path: str) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO orders (user_id, file_path, status)
            VALUES ($1, $2, 'done')
            ON CONFLICT (user_id, payment_id) DO UPDATE SET file_path = $2
        ''', tg_id, file_path)

async def get_order_by_user(tg_id: int) -> Optional[Record]:
    async with db_pool.acquire() as conn:
        return await conn.fetchrow(
            'SELECT * FROM orders WHERE user_id = $1 ORDER BY id DESC LIMIT 1', tg_id
        )

# ========== FSM СОСТОЯНИЯ ==========
class OrderStates(StatesGroup):
    waiting_birth_date = State()
    waiting_birth_time = State()
    waiting_city = State()
    waiting_gender = State()
    waiting_email = State()
    confirm_order = State()
    waiting_payment = State()
    waiting_file_upload = State()   # для админа

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
            [KeyboardButton(text="⏳ Новые заявки"), KeyboardButton(text="✅ В работе")],
            [KeyboardButton(text="📊 Статистика"), KeyboardButton(text="📢 Рассылка")]
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

def get_payment_keyboard(payment_url: str):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💳 Оплатить 5000 ₽", url=payment_url)],
            [InlineKeyboardButton(text="✅ Я оплатил", callback_data="check_payment")],
            [InlineKeyboardButton(text="❌ Отменить заказ", callback_data="cancel_order")]
        ]
    )

# ========== ПЛАТЕЖИ ЮKASSA ==========
async def create_payment(amount: int, description: str, user_id: int):
    url = "https://api.yookassa.ru/v3/payments"
    payment_id = str(uuid.uuid4())
    payload = {
        "amount": {"value": str(amount), "currency": "RUB"},
        "payment_method_data": {"type": "bank_card"},
        "confirmation": {
            "type": "redirect",
            "return_url": f"https://t.me/{bot.username}"  # подставьте свой username
        },
        "description": description,
        "metadata": {"user_id": str(user_id), "payment_id": payment_id}
    }
    auth = aiohttp.BasicAuth(YOOKASSA_SHOP_ID, YOOKASSA_SECRET_KEY)
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, json=payload, auth=auth) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    await save_payment(payment_id, user_id, amount, 'pending')
                    return data
                else:
                    logger.error(f"Ошибка ЮKassa: {await resp.text()}")
                    return None
        except Exception as e:
            logger.error(f"Исключение при создании платежа: {e}")
            return None

async def check_payment_status(payment_id: str):
    url = f"https://api.yookassa.ru/v3/payments/{payment_id}"
    auth = aiohttp.BasicAuth(YOOKASSA_SHOP_ID, YOOKASSA_SECRET_KEY)
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, auth=auth) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get('status')
                return None
        except Exception as e:
            logger.error(f"Ошибка проверки платежа: {e}")
            return None

# ========== ХЕНДЛЕРЫ ==========

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
            "👋 Добро пожаловать в панель администратора!\n\nВыберите действие:",
            reply_markup=get_admin_keyboard()
        )
        return
    text = (
        "🌟 Добро пожаловать в BaziExpertBot!\n\n"
        "Я помогу вам получить персональный разбор вашей карты Ба Цзы.\n\n"
        "📋 Что вы получите:\n"
        "• Полный анализ 4-х столпов судьбы\n"
        "• Определение Хозяина Дня\n"
        "• Прогноз на текущий год\n"
        "• Рекомендации по элементам\n\n"
        "💰 Стоимость разбора: 5000 ₽\n\n"
        "Для заказа нажмите кнопку ниже 👇"
    )
    await message.answer(text, reply_markup=get_main_keyboard())

@dp.message(F.text == "📝 Заказать разбор")
async def cmd_order(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    user = await get_user(user_id)
    if user and user['order_status'] in ('paid', 'processing'):
        await message.answer(
            "⚠️ У вас уже есть активный заказ. Мы работаем над ним!\n"
            "Если у вас есть вопросы, напишите администратору."
        )
        return
    await message.answer(
        "📅 Пожалуйста, введите вашу дату рождения в формате:\nДД.ММ.ГГГГ\n\nНапример: 15.08.1990"
    )
    await state.set_state(OrderStates.waiting_birth_date)

@dp.message(OrderStates.waiting_birth_date)
async def process_birth_date(message: types.Message, state: FSMContext):
    birth_date = message.text.strip()
    try:
        datetime.strptime(birth_date, "%d.%m.%Y")
    except ValueError:
        await message.answer("❌ Неверный формат! Введите ДД.ММ.ГГГГ")
        return
    await state.update_data(birth_date=birth_date)
    await update_user_data(message.from_user.id, 'birth_date', birth_date)
    await message.answer(
        "🕐 Теперь укажите точное время рождения в формате ЧЧ:ММ\nНапример: 14:30\nЕсли неизвестно – 12:00"
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
    await update_user_data(message.from_user.id, 'birth_time', birth_time)
    await message.answer("🏙️ Введите город вашего рождения:")
    await state.set_state(OrderStates.waiting_city)

@dp.message(OrderStates.waiting_city)
async def process_city(message: types.Message, state: FSMContext):
    city = message.text.strip()
    await state.update_data(birth_city=city)
    await update_user_data(message.from_user.id, 'birth_city', city)
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
    await update_user_data(message.from_user.id, 'gender', gender)
    await message.answer(
        "📧 Введите ваш email (необязательно) или напишите 'пропустить':"
    )
    await state.set_state(OrderStates.waiting_email)

@dp.message(OrderStates.waiting_email)
async def process_email(message: types.Message, state: FSMContext):
    email = message.text.strip()
    if email.lower() != 'пропустить':
        if '@' not in email or '.' not in email:
            await message.answer("❌ Похоже, это не email. Введите корректный email или 'пропустить'")
            return
        await update_user_data(message.from_user.id, 'email', email)
    else:
        email = None
        await update_user_data(message.from_user.id, 'email', None)
    await state.update_data(email=email)
    data = await state.get_data()
    confirm_text = (
        "📋 Проверьте введенные данные:\n\n"
        f"📅 Дата рождения: {data['birth_date']}\n"
        f"🕐 Время рождения: {data['birth_time']}\n"
        f"🏙️ Город: {data['birth_city']}\n"
        f"👤 Пол: {'Мужской' if data['gender'] == 'male' else 'Женский'}\n"
        f"📧 Email: {email or 'Не указан'}\n\n"
        f"💰 Стоимость разбора: 5000 ₽\n\nВсе верно?"
    )
    await message.answer(confirm_text, reply_markup=get_order_confirm_keyboard())
    await state.set_state(OrderStates.confirm_order)

@dp.callback_query(F.data == "confirm_order")
async def confirm_order(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    data = await state.get_data()
    await update_order_status(user_id, 'pending_payment')
    payment_data = await create_payment(5000, f"Разбор Ба Цзы для {callback.from_user.first_name}", user_id)
    if payment_data:
        payment_url = payment_data['confirmation']['confirmation_url']
        payment_id = payment_data['id']
        await state.update_data(payment_id=payment_id)
        await callback.message.edit_text(
            "💳 Для оплаты перейдите по ссылке:\n\nПосле оплаты нажмите 'Я оплатил'.\n\n⚠️ Ссылка действительна 1 час.",
            reply_markup=get_payment_keyboard(payment_url)
        )
    else:
        await callback.message.edit_text("❌ Ошибка создания платежа. Попробуйте позже.")
    await callback.answer()

@dp.callback_query(F.data == "edit_order")
async def edit_order(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("🔄 Начнем заново. Введите дату рождения:")
    await state.set_state(OrderStates.waiting_birth_date)
    await callback.answer()

@dp.callback_query(F.data == "check_payment")
async def check_payment(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    data = await state.get_data()
    payment_id = data.get('payment_id')
    if not payment_id:
        await callback.answer("❌ Платеж не найден", show_alert=True)
        return
    status = await check_payment_status(payment_id)
    if status == 'succeeded':
        await update_order_status(user_id, 'paid')
        await update_payment_status(payment_id, 'succeeded')
        await update_user_data(user_id, 'paid_at', datetime.now())
        user = await get_user(user_id)
        await notify_admin_new_order(user)
        await callback.message.edit_text(
            "✅ Оплата получена! Ваша заявка принята в работу. Разбор будет готов в течение 24-48 часов."
        )
        await state.clear()
    elif status == 'pending':
        await callback.answer("⏳ Платеж еще не прошел. Подождите или проверьте позже.", show_alert=True)
    else:
        await callback.answer("❌ Платеж не найден или отклонен. Попробуйте снова.", show_alert=True)

@dp.callback_query(F.data == "cancel_order")
async def cancel_order(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("❌ Заказ отменен. Если передумаете, начните заново /start")
    await state.clear()
    await callback.answer()

@dp.message(F.text == "ℹ️ О сервисе")
async def about(message: types.Message):
    text = (
        "🔮 **О сервисе BaziExpert**\n\n"
        "Профессиональный разбор карты Ба Цзы.\n\n"
        "**Входит:**\n"
        "• Анализ 4-х столпов\n"
        "• Хозяин Дня и элементы\n"
        "• Прогноз на текущий год\n"
        "• Рекомендации\n\n"
        "💰 5000 ₽\nСрок: 24-48 часов."
    )
    await message.answer(text, parse_mode="Markdown")

@dp.message(F.text == "📞 Контакты")
async def contacts(message: types.Message):
    await message.answer(
        "📞 **Контакты:**\n✉️ @admin_username\n📧 support@baziexpert.ru",
        parse_mode="Markdown"
    )

# ========== АДМИН-ФУНКЦИИ ==========

async def notify_admin_new_order(user: Record):
    text = (
        "🆕 **НОВЫЙ ЗАКАЗ!**\n\n"
        f"👤 {user['first_name']} {user['last_name'] or ''} (@{user['username'] or 'нет'})\n"
        f"🆔 ID: {user['tg_id']}\n"
        f"📅 Дата: {user['birth_date']} в {user['birth_time']}\n"
        f"🏙️ Город: {user['birth_city']}\n"
        f"👤 Пол: {'Мужской' if user['gender'] == 'male' else 'Женский'}\n"
        f"📧 Email: {user['email'] or 'не указан'}\n\n"
        "Статус: ✅ ОПЛАЧЕН"
    )
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Ошибка уведомления админа {admin_id}: {e}")

@dp.message(F.text == "📋 Все заявки")
async def admin_all_orders(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    orders = await get_all_orders()
    if not orders:
        await message.answer("📭 Заявок нет.")
        return
    text = "📋 **Все заявки (последние 20):**\n\n"
    for order in orders[:20]:
        status_emoji = {
            'new': '🆕', 'pending_payment': '⏳', 'paid': '✅',
            'processing': '🔄', 'done': '📄'
        }.get(order['order_status'], '❓')
        text += (
            f"{status_emoji} ID: {order['tg_id']} | {order['first_name'] or 'Без имени'}\n"
            f"   Дата: {order['birth_date']} | Статус: {order['order_status']}\n"
            f"   Заказано: {order['created_at'][:10]}\n\n"
        )
    await message.answer(text, parse_mode="Markdown")

@dp.message(F.text == "⏳ Новые заявки")
async def admin_new_orders(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    orders = await get_all_orders(status='paid')
    if not orders:
        await message.answer("🆕 Новых оплаченных заявок нет.")
        return
    text = "🆕 **Новые оплаченные заявки:**\n\n"
    for order in orders:
        text += (
            f"👤 {order['first_name']} (ID: {order['tg_id']})\n"
            f"📅 {order['birth_date']} в {order['birth_time']}, {order['birth_city']}\n"
            f"📧 {order['email'] or 'не указан'}\n"
            f"📅 Заказано: {order['created_at'][:10]}\n\n"
        )
    await message.answer(text, parse_mode="Markdown")

@dp.message(F.text == "✅ В работе")
async def admin_work(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer(
        "✏️ Введите ID пользователя (Telegram ID), чей заказ вы берете в работу:\nНапример: 123456789"
    )
    await state.set_state(OrderStates.waiting_file_upload)

@dp.message(OrderStates.waiting_file_upload)
async def process_work_assignment(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    try:
        user_id = int(message.text.strip())
    except ValueError:
        await message.answer("❌ Введите число (ID)")
        return
    user = await get_user(user_id)
    if not user:
        await message.answer("❌ Пользователь не найден.")
        return
    await update_order_status(user_id, 'processing')
    await message.answer(f"✅ Заказ пользователя {user_id} взят в работу!")
    await state.clear()

@dp.message(F.text == "📊 Статистика")
async def admin_stats(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    orders = await get_all_orders()
    total = len(orders)
    status_counts = {}
    for order in orders:
        s = order['order_status']
        status_counts[s] = status_counts.get(s, 0) + 1
    text = (
        f"📊 **Статистика:**\n\n📦 Всего заявок: {total}\n\n"
        "**По статусам:**\n"
        f"🆕 Новые: {status_counts.get('new', 0)}\n"
        f"⏳ Ожидают оплаты: {status_counts.get('pending_payment', 0)}\n"
        f"✅ Оплачено: {status_counts.get('paid', 0)}\n"
        f"🔄 В работе: {status_counts.get('processing', 0)}\n"
        f"📄 Готово: {status_counts.get('done', 0)}"
    )
    await message.answer(text, parse_mode="Markdown")

@dp.message(F.text == "📢 Рассылка")
async def admin_broadcast(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("📢 Введите текст для рассылки (или /cancel для отмены):")
    await state.set_state(OrderStates.waiting_email)  # переиспользуем

@dp.message(StateFilter(OrderStates.waiting_email))
async def process_broadcast(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    if message.text == "/cancel":
        await state.clear()
        await message.answer("❌ Рассылка отменена.")
        return
    text = message.text
    async with db_pool.acquire() as conn:
        rows = await conn.fetch('SELECT tg_id FROM users')
    sent = 0
    for row in rows:
        try:
            await bot.send_message(row['tg_id'], f"📢 **Рассылка**\n\n{text}", parse_mode="Markdown")
            sent += 1
            await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"Ошибка отправки {row['tg_id']}: {e}")
    await message.answer(f"✅ Рассылка завершена. Отправлено: {sent} сообщений.")
    await state.clear()

# ========== ЗАГРУЗКА ФАЙЛОВ (админ) ==========
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
    os.makedirs('media', exist_ok=True)
    file_path = f"media/{doc.file_name}"
    file = await bot.get_file(doc.file_id)
    await bot.download_file(file.file_path, file_path)

    # Спросим, для какого пользователя этот файл
    await message.answer(
        "✏️ Введите ID пользователя (Telegram ID), которому отправить этот файл:"
    )
    # Сохраним путь в состояние
    await state.update_data(upload_file_path=file_path)

@dp.message(StateFilter(OrderStates.waiting_file_upload))  # переиспользуем то же состояние для ввода ID после загрузки
async def process_upload_user_id(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    try:
        user_id = int(message.text.strip())
    except ValueError:
        await message.answer("❌ Введите число (ID)")
        return
    data = await state.get_data()
    file_path = data.get('upload_file_path')
    if not file_path:
        await message.answer("❌ Файл не найден, загрузите заново.")
        await state.clear()
        return
    user = await get_user(user_id)
    if not user:
        await message.answer("❌ Пользователь не найден.")
        return
    # Отправим файл клиенту
    try:
        with open(file_path, 'rb') as f:
            await bot.send_document(
                user_id,
                types.FSInputFile(file_path),
                caption="📄 Ваш разбор Ба Цзы готов! Благодарим за доверие."
            )
        # Обновим статус заказа
        await update_order_status(user_id, 'done')
        await save_order_file(user_id, file_path)
        await message.answer(f"✅ Файл отправлен пользователю {user_id}.")
    except Exception as e:
        logger.error(f"Ошибка отправки файла: {e}")
        await message.answer(f"❌ Ошибка: {e}")
    await state.clear()

# ========== ЗАПУСК ==========
async def main():
    await init_db_pool()
    try:
        await dp.start_polling(bot, skip_updates=True)
    finally:
        await close_db_pool()

if __name__ == "__main__":
    asyncio.run(main())