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
    FSInputFile, LabeledPrice, PreCheckoutQuery
)
import aiohttp
import asyncpg
from asyncpg import Pool, Record

# ========== КОНФИГУРАЦИЯ ==========
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(id.strip()) for id in os.getenv("ADMIN_IDS", "").split(",") if id.strip()]
DATABASE_URL = os.getenv("DATABASE_URL")
PROVIDER_TOKEN = os.getenv("PROVIDER_TOKEN")  # Получить у @BotFather
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

async def init_db_pool():
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
        # Таблица заказов (история файлов)
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS orders (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(tg_id),
                file_path TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        # Таблица платежей – теперь для истории (опционально)
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS payments (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                amount INTEGER,
                payload TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        # Таблица настроек
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')
        # Цена по умолчанию в рублях
        await conn.execute('''
            INSERT INTO settings (key, value) VALUES ('price', '5000')
            ON CONFLICT (key) DO NOTHING
        ''')

        # Индексы
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_users_tg_id ON users(tg_id)')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_users_status ON users(order_status)')
        await conn.execute('CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id)')

    logger.info("Таблицы созданы/проверены")

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
    """Возвращает цену в рублях (целое число)"""
    price_str = await get_setting('price', '5000')
    try:
        return int(price_str)
    except ValueError:
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
                FROM users WHERE order_status = $1 ORDER BY created_at DESC
            ''', status)
        else:
            return await conn.fetch('''
                SELECT tg_id, username, first_name, birth_date, birth_time,
                       birth_city, gender, email, order_status, created_at
                FROM users ORDER BY created_at DESC
            ''')

async def save_payment_history(user_id: int, amount: int, payload: str) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO payments (user_id, amount, payload)
            VALUES ($1, $2, $3)
        ''', user_id, amount, payload)

async def save_order_file(tg_id: int, file_path: str) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO orders (user_id, file_path)
            VALUES ($1, $2)
        ''', tg_id, file_path)

async def get_order_by_user(tg_id: int) -> Optional[Record]:
    async with db_pool.acquire() as conn:
        return await conn.fetchrow(
            'SELECT * FROM orders WHERE user_id = $1 ORDER BY id DESC LIMIT 1', tg_id
        )

async def get_file_path_for_user(tg_id: int) -> Optional[str]:
    order = await get_order_by_user(tg_id)
    return order['file_path'] if order else None

# ========== FSM СОСТОЯНИЯ ==========
class OrderStates(StatesGroup):
    waiting_birth_date = State()
    waiting_birth_time = State()
    waiting_city = State()
    waiting_gender = State()
    waiting_email = State()
    confirm_order = State()
    waiting_file_upload = State()          # для взятия в работу
    waiting_upload_user_id = State()       # для ввода ID после загрузки файла
    waiting_broadcast_text = State()       # для текста рассылки
    waiting_new_price = State()            # для изменения цены

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

# ========== ХЕНДЛЕРЫ ==========

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
    user = await get_user(user_id)
    if user and user['order_status'] in ('paid', 'processing', 'pending_payment'):
        await message.answer(
            "⚠️ У вас уже есть активный заказ. Мы работаем над ним!\n"
            "Если у вас есть вопросы, напишите администратору."
        )
        return
    await message.answer(
        "📅 Введите дату рождения в формате ДД.ММ.ГГГГ\nНапример: 15.08.1990"
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
    await message.answer("🕐 Введите время рождения в формате ЧЧ:ММ (например, 14:30). Если неизвестно – 12:00")
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
    await message.answer("📧 Введите email (необязательно) или напишите 'пропустить':")
    await state.set_state(OrderStates.waiting_email)

@dp.message(OrderStates.waiting_email)
async def process_email(message: types.Message, state: FSMContext):
    email = message.text.strip()
    if email.lower() != 'пропустить':
        if '@' not in email or '.' not in email:
            await message.answer("❌ Введите корректный email или 'пропустить'")
            return
        await update_user_data(message.from_user.id, 'email', email)
    else:
        email = None
        await update_user_data(message.from_user.id, 'email', None)
    await state.update_data(email=email)
    data = await state.get_data()
    price = await get_price()
    confirm_text = (
        "📋 Проверьте данные:\n\n"
        f"📅 Дата рождения: {data['birth_date']}\n"
        f"🕐 Время: {data['birth_time']}\n"
        f"🏙️ Город: {data['birth_city']}\n"
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
    price = await get_price()  # цена в рублях

    # Генерируем уникальный payload (для идентификации заказа)
    payload = f"bazi_{user_id}_{int(datetime.now().timestamp())}"

    try:
        await bot.send_invoice(
            chat_id=user_id,
            title="Разбор карты Ба Цзы",
            description=(
                f"Персональный разбор вашей карты Ба Цзы\n"
                f"Дата рождения: {data['birth_date']}\n"
                f"Время: {data['birth_time']}\n"
                f"Город: {data['birth_city']}"
            ),
            payload=payload,
            provider_token=PROVIDER_TOKEN,
            currency="RUB",
            prices=[LabeledPrice(label="Разбор Ба Цзы", amount=price * 100)],  # переводим в копейки!
            start_parameter="bazi_order",
            need_email=True,
            need_phone_number=False,
        )
        # Сохраняем payload в состоянии на случай проверки
        await state.update_data(payload=payload)
        await update_order_status(user_id, 'pending_payment')
        await callback.message.delete()
    except Exception as e:
        logger.error(f"Ошибка отправки инвойса: {e}")
        await callback.message.edit_text("❌ Не удалось создать платёжный счёт. Попробуйте позже.")
    await callback.answer()

@dp.callback_query(F.data == "edit_order")
async def edit_order(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("🔄 Начнем заново. Введите дату рождения:")
    await state.set_state(OrderStates.waiting_birth_date)
    await callback.answer()

@dp.pre_checkout_query()
async def pre_checkout_handler(pre_checkout_query: PreCheckoutQuery):
    # Здесь можно добавить дополнительную валидацию (например, проверить payload)
    await bot.answer_pre_checkout_query(
        pre_checkout_query.id,
        ok=True,
        error_message="Извините, произошла ошибка. Попробуйте позже."
    )

@dp.message(F.successful_payment)
async def successful_payment_handler(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    payment = message.successful_payment

    # Сохраняем историю платежа (опционально)
    await save_payment_history(user_id, payment.total_amount, payment.payload)

    # Обновляем статус заказа
    await update_order_status(user_id, 'paid')
    await update_user_data(user_id, 'paid_at', datetime.now())

    # Уведомляем администратора
    user = await get_user(user_id)
    await notify_admin_new_order(user)

    # Очищаем состояние
    await state.clear()

    await message.answer(
        "✅ Оплата успешно получена!\n\n"
        "Ваша заявка принята в работу. Разбор будет готов в течение 24-48 часов.\n"
        "Как только разбор будет готов, вы получите уведомление."
    )

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
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])

    for order in orders[:20]:
        status_emoji = {
            'new': '🆕', 'pending_payment': '⏳', 'paid': '✅',
            'processing': '🔄', 'done': '📄'
        }.get(order['order_status'], '❓')
        created_date = order['created_at'].strftime('%Y-%m-%d') if order['created_at'] else '—'

        if order['order_status'] == 'done':
            file_path = await get_file_path_for_user(order['tg_id'])
            file_info = f"📁 Файл: {file_path or 'не найден'}"
            btn = InlineKeyboardButton(
                text="📤 Отправить повторно",
                callback_data=f"resend_{order['tg_id']}"
            )
            keyboard.inline_keyboard.append([btn])
        else:
            file_info = ""

        text += (
            f"{status_emoji} ID: {order['tg_id']} | {order['first_name'] or 'Без имени'}\n"
            f"   Дата: {order['birth_date']} | Статус: {order['order_status']}\n"
            f"   Заказано: {created_date}\n"
            f"   {file_info}\n\n"
        )

    await message.answer(text, parse_mode="Markdown", reply_markup=keyboard)

@dp.callback_query(F.data.startswith("resend_"))
async def resend_file_callback(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("⛔ Нет прав", show_alert=True)
        return
    user_id = int(callback.data.split("_")[1])
    file_path = await get_file_path_for_user(user_id)
    if not file_path:
        await callback.answer("❌ Файл не найден в БД", show_alert=True)
        return
    if not os.path.exists(file_path):
        await callback.answer("❌ Файл отсутствует на сервере", show_alert=True)
        return
    try:
        await bot.send_document(
            user_id,
            FSInputFile(file_path),
            caption="📄 Повторная отправка вашего разбора Ба Цзы."
        )
        await callback.answer("✅ Файл отправлен повторно!", show_alert=True)
        await callback.message.answer(f"✅ Файл повторно отправлен пользователю {user_id}.")
    except Exception as e:
        logger.error(f"Ошибка повторной отправки: {e}")
        await callback.answer(f"❌ Ошибка: {e}", show_alert=True)

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
        created_date = order['created_at'].strftime('%Y-%m-%d') if order['created_at'] else '—'
        text += (
            f"👤 {order['first_name']} (ID: {order['tg_id']})\n"
            f"📅 {order['birth_date']} в {order['birth_time']}, {order['birth_city']}\n"
            f"📧 {order['email'] or 'не указан'}\n"
            f"📅 Заказано: {created_date}\n\n"
        )
    await message.answer(text, parse_mode="Markdown")

@dp.message(F.text == "✅ В работе")
async def admin_work(message: types.Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    await message.answer("✏️ Введите ID пользователя (Telegram ID):")
    await state.set_state(OrderStates.waiting_file_upload)

@dp.message(StateFilter(OrderStates.waiting_file_upload))
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
    await state.set_state(OrderStates.waiting_broadcast_text)

@dp.message(StateFilter(OrderStates.waiting_broadcast_text))
async def process_broadcast_text(message: types.Message, state: FSMContext):
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
    # Сохраняем в рублях
    await set_setting('price', str(new_price))
    await message.answer(f"✅ Цена успешно изменена на {new_price} ₽")
    await state.clear()

# ========== ПРОВЕРКА ПЛАТЕЖНОЙ СИСТЕМЫ (упрощённо) ==========
@dp.message(F.text == "🔍 Проверить платежи")
async def admin_check_payment_system(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    if not PROVIDER_TOKEN:
        await message.answer("❌ PROVIDER_TOKEN не задан в переменных окружения.")
        return
    # Проверить, что токен не пустой
    await message.answer(
        "ℹ️ Платёжный провайдер подключён. Токен присутствует.\n"
        "Для полноценной проверки создайте тестовый заказ."
    )

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
    await message.answer("✏️ Введите ID пользователя (Telegram ID), которому отправить этот файл:")
    await state.set_state(OrderStates.waiting_upload_user_id)

@dp.message(StateFilter(OrderStates.waiting_upload_user_id))
async def process_upload_user_id(message: types.Message, state: FSMContext):
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

    data = await state.get_data()
    temp_path = data.get('temp_file_path')
    original_name = data.get('original_name')

    if not temp_path or not os.path.exists(temp_path):
        await message.answer("❌ Временный файл не найден. Загрузите файл заново.")
        await state.clear()
        return

    base, ext = os.path.splitext(original_name)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_filename = f"bazi_{user_id}_{timestamp}_{base}{ext}"
    final_path = f"media/{new_filename}"

    try:
        os.rename(temp_path, final_path)
        logger.info(f"Файл переименован: {final_path}")

        await bot.send_document(
            user_id,
            FSInputFile(final_path),
            caption="📄 Ваш разбор Ба Цзы готов! Благодарим за доверие."
        )

        await update_order_status(user_id, 'done')
        await save_order_file(user_id, final_path)

        await message.answer(
            f"✅ Файл отправлен пользователю {user_id}.\n📁 Путь: {final_path}"
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
        await message.answer("Использование: /getfile <ID_пользователя>")
        return
    try:
        user_id = int(parts[1])
    except ValueError:
        await message.answer("❌ ID должен быть числом.")
        return
    file_path = await get_file_path_for_user(user_id)
    if not file_path:
        await message.answer(f"❌ Для пользователя {user_id} не найден файл.")
        return
    if not os.path.exists(file_path):
        await message.answer(f"❌ Файл отсутствует на сервере: {file_path}")
        return
    try:
        await bot.send_document(
            message.chat.id,
            FSInputFile(file_path),
            caption=f"📁 Файл для пользователя {user_id}:\n{file_path}"
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
