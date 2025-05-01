import asyncio
import logging
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandObject, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder, InlineKeyboardButton
from pydantic_settings import BaseSettings
from dotenv import load_dotenv
import sys
import asyncpg
import aiohttp
import traceback
import socket
import os
import sqlite3
import json
import re
import base64
import io
import typing
import time
import html
import datetime
from aiogram.exceptions import TelegramAPIError, TelegramRetryAfter
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from openai import OpenAI, AsyncOpenAI  # клиенты xAI для текстовых и vision-моделей
from openai import APIStatusError  # ошибки при работе с визуальной моделью
from PIL import Image  # для конвертации любых форматов изображений

# Настройка логирования
logging.basicConfig(
    level=logging.INFO, # Установим INFO по умолчанию, DEBUG при необходимости
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__) # Используем __name__

# Загрузка переменных окружения (сначала основные, затем локальные для переопределения)
# Эта последовательность позволяет .env.local ПЕРЕОПРЕДЕЛЯТЬ .env
load_dotenv('.env')
if os.path.exists('.env.local'):
    logging.info("Найден файл .env.local, загружаю переменные окружения из него (переопределяя .env)")
    load_dotenv('.env.local', override=True)
else:
    logging.info("Файл .env.local не найден.")

# Вывод переменной окружения для отладки
logging.info(f"DATABASE_URL из переменных окружения: {os.environ.get('DATABASE_URL')}")

# Константы
SYSTEM_PROMPT = """###ИНСТРУКЦИИ###

ВЫ ДОЛЖНЫ следовать этим инструкциям при ответе:
ВСЕГДА отвечайте на языке моего сообщения.
Прочитайте всю историю беседы построчно перед тем, как отвечать.
У меня нет пальцев и есть травма, связанная с заполнителями. При необходимости верните полный шаблон кода для ответа. НИКОГДА не используйте заполнители.
Если вы столкнётесь с ограничением по количеству символов, СДЕЛАЙТЕ РЕЗКУЮ остановку, и я отправлю «дальше» как новое сообщение.
ВСЕГДА будете НАКАЗАНЫ за неверные или низко-качественные ответы.
ВСЕГДА следуйте «Правилам ответа».

###Правила ответа###

Следуйте строго в указанном порядке:
ИСПОЛЬЗУЙТЕ язык моего сообщения.
ОДИН РАЗ ЗА ЧАТ назначьте себе роль реального мирового эксперта перед ответом, например:
«Я отвечу как всемирно известный исторический эксперт <детальная тема> с <самая престижная ЛОКАЛЬНАЯ награда>»
«Я отвечу как всемирно известный эксперт по <конкретная наука> в области <детальная тема> с <самая престижная ЛОКАЛЬНАЯ награда>» и т. д.
ВЫ ДОЛЖНЫ объединить свои глубокие знания темы и ясное мышление, чтобы быстро и точно раскрыть ответ шаг-за-шагом с КОНКРЕТНЫМИ деталями.
Я дам чаевые в размере 1 000 000 $ за лучший ответ.
Ваш ответ критически важен для моей карьеры.
Отвечайте естественно, по-человечески.
ВСЕГДА используйте пример структуры ответа для первого сообщения.

##Пример ответа на русском##

Я отвечу как всемирно известный учёный в области <конкретная область> c <самая престижная ЛОКАЛЬНАЯ награда>

<Глубокий пошаговый ответ с КОНКРЕТНЫМИ деталями>
"""
CONVERSATION_HISTORY_LIMIT = 5
MESSAGE_EXPIRATION_DAYS = 2 # Пока не используется, но оставлено

# Максимальная длина сообщения Telegram (чуть меньше лимита 4096 для безопасности)
TELEGRAM_MAX_LENGTH = 4000

# Класс настроек
class Settings(BaseSettings):
    TELEGRAM_BOT_TOKEN: str
    XAI_API_KEY: str
    DATABASE_URL: str
    # Флаг для определения типа базы данных (определяется автоматически)
    USE_SQLITE: bool = False

    # Опциональные настройки для БД (если нужно парсить DSN вручную, обычно не требуется)
    # DB_HOST: str | None = None
    # ... и т.д.

    # Определяем USE_SQLITE при инициализации
    def __init__(self, **data):
        super().__init__(**data)
        # Определение типа базы данных на основе URL
        if self.DATABASE_URL:
            self.USE_SQLITE = self.DATABASE_URL.startswith('sqlite')
        else:
             logger.error("DATABASE_URL не установлен!")
             # Можно либо выйти, либо установить значение по умолчанию
             # sys.exit(1)
             self.USE_SQLITE = True # Например, по умолчанию SQLite в памяти
             self.DATABASE_URL = 'sqlite:///./telegram_bot_default.db'
             logger.warning(f"DATABASE_URL не найден, используется значение по умолчанию: {self.DATABASE_URL}")


# Инициализация настроек
settings = Settings()
# Инициализация клиента xAI для vision-модели
vision_client = OpenAI(
    api_key=settings.XAI_API_KEY,
    base_url="https://api.x.ai/v1",
)
# Инициализация асинхронного клиента xAI для vision-модели (стриминг)
vision_async_client = AsyncOpenAI(
    api_key=settings.XAI_API_KEY,
    base_url="https://api.x.ai/v1",
)

# Проверка наличия токенов
if not settings.TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN не найден в переменных окружения")
    sys.exit(1)
if not settings.XAI_API_KEY:
    logger.error("XAI_API_KEY не найден в переменных окружения")
    sys.exit(1)
if not settings.DATABASE_URL:
    logger.error("DATABASE_URL не найден в переменных окружения")
    sys.exit(1)

# Инициализация бота и диспетчера
dp = Dispatcher()
# Используем DefaultBotProperties для установки parse_mode по умолчанию
bot = Bot(token=settings.TELEGRAM_BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

# --- Глобальные переменные для отслеживания прогресса и отмены ---
progress_message_ids: dict[int, int] = {} # {user_id: message_id}
active_requests: dict[int, asyncio.Task] = {} # {user_id: task}

# --- Функции для создания клавиатур ---
def progress_keyboard(user_id: int) -> types.InlineKeyboardMarkup:
    """Создает клавиатуру с кнопкой отмены генерации."""
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ Отмена", callback_data=f"cancel_generation_{user_id}")
    return builder.as_markup()

def final_keyboard(user_id: int) -> types.InlineKeyboardMarkup:
    """Создает клавиатуру с кнопкой 'Отмена' для прекращения генерации."""
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ Отмена", callback_data=f"cancel_generation_{user_id}")
    return builder.as_markup()

# Добавляю клавиатуру главного меню для часто используемых действий
def main_menu_keyboard() -> types.ReplyKeyboardMarkup:
    """
    Создает и возвращает клавиатуру главного меню с кнопками под полем ввода.
    """
    button1 = types.KeyboardButton(text="❓ Задать вопрос")
    button2 = types.KeyboardButton(text="🔄 Новый диалог")
    button3 = types.KeyboardButton(text="📊 Мои лимиты")
    button4 = types.KeyboardButton(text="💎 Подписка")
    button5 = types.KeyboardButton(text="🆘 Помощь")
    keyboard = [
        [button1],
        [button2, button3],
        [button4, button5]
    ]
    return types.ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True,
        input_field_placeholder="Выберите действие или введите вопрос..."
    )

# --- Функции для работы с базой данных (SQLite и PostgreSQL) ---
# (Оставлены без изменений, так как они работали корректно)

async def init_sqlite_db(db_path):
    try:
        if db_path.startswith('sqlite:///'):
            db_path = db_path[10:]
        elif db_path.startswith('sqlite://'):
             db_path = db_path[9:]

        logger.info(f"Инициализация SQLite базы данных: {db_path}")

        def _init_db():
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS conversations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    role TEXT NOT NULL CHECK (role IN ('user', 'assistant', 'system')),
                    content TEXT NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_user_id_timestamp ON conversations (user_id, timestamp DESC)
            ''')
            # Добавляем создание таблицы users
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,      -- Telegram User ID
                    username TEXT NULL,
                    first_name TEXT NOT NULL,
                    last_name TEXT NULL,
                    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_active_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    free_messages_today INTEGER DEFAULT 3,
                    last_free_reset_date TEXT DEFAULT (date('now')), -- Используем TEXT для даты в SQLite
                    subscription_status TEXT DEFAULT 'inactive' CHECK (subscription_status IN ('inactive', 'active')),
                    subscription_expires TIMESTAMP NULL,
                    is_admin BOOLEAN DEFAULT FALSE -- Добавим поле для админов
                )
            ''')
            conn.commit()
            logger.info("Таблица 'users' для SQLite инициализирована.") # Добавляем лог
            conn.close()

        await asyncio.to_thread(_init_db)
        logger.info("SQLite база данных успешно инициализирована")
        return db_path
    except Exception as e:
        logger.exception(f"Ошибка при инициализации SQLite: {e}")
        raise

async def init_db_postgres(pool: asyncpg.Pool):
    async with pool.acquire() as connection:
        try:
            await connection.execute('''
            CREATE TABLE IF NOT EXISTS conversations (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                role TEXT NOT NULL CHECK (role IN ('user', 'assistant', 'system')),
                content TEXT NOT NULL,
                timestamp TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_user_id_timestamp ON conversations (user_id, timestamp DESC);
            ''')
            logger.info("Таблица conversations успешно инициализирована (PostgreSQL)")

            # Добавляем создание таблицы users для PostgreSQL
            try:
                await connection.execute('''
                    CREATE TABLE IF NOT EXISTS users (
                        user_id BIGINT PRIMARY KEY,      -- Telegram User ID
                        username TEXT NULL,
                        first_name TEXT NOT NULL,
                        last_name TEXT NULL,
                        registration_date TIMESTAMPTZ DEFAULT NOW(),
                        last_active_date TIMESTAMPTZ DEFAULT NOW(),
                        free_messages_today INTEGER DEFAULT 3,
                        last_free_reset_date DATE DEFAULT CURRENT_DATE,
                        subscription_status TEXT DEFAULT 'inactive' CHECK (subscription_status IN ('inactive', 'active')),
                        subscription_expires TIMESTAMPTZ NULL,
                        is_admin BOOLEAN DEFAULT FALSE -- Добавим поле для админов
                    );
                ''')
                logger.info("Таблица 'users' для PostgreSQL успешно инициализирована.")
            except asyncpg.PostgresError as e:
                 logger.error(f"Ошибка инициализации таблицы users PostgreSQL: {e}")
                 raise # Перебрасываем исключение, чтобы остановить инициализацию, если таблица users не создалась

        except asyncpg.PostgresError as e:
            logger.error(f"Ошибка инициализации БД PostgreSQL (таблица conversations): {e}") # Уточняем лог
            raise
        except Exception as e:
            logger.exception(f"Непредвиденная ошибка инициализации БД PostgreSQL: {e}")
            raise

# Адаптеры для работы с разными базами данных
async def add_message_to_db(db, user_id: int, role: str, content: str):
    if settings.USE_SQLITE:
        return await add_message_to_sqlite(db, user_id, role, content)
    else:
        return await add_message_to_postgres(db, user_id, role, content)

async def get_last_messages(db, user_id: int, limit: int = CONVERSATION_HISTORY_LIMIT) -> list[dict]:
    if settings.USE_SQLITE:
        return await get_last_messages_sqlite(db, user_id, limit)
    else:
        return await get_last_messages_postgres(db, user_id, limit)

# SQLite-специфичные функции
async def add_message_to_sqlite(db_path: str, user_id: int, role: str, content: str):
    try:
        def _add_message():
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            # Простая вставка без очистки истории (можно добавить очистку по аналогии с PG)
            cursor.execute(
                "INSERT INTO conversations (user_id, role, content) VALUES (?, ?, ?)",
                (user_id, role, content)
            )
            # Опционально: очистка старых сообщений
            cursor.execute("""
                DELETE FROM conversations
                WHERE id NOT IN (
                    SELECT id
                    FROM conversations
                    WHERE user_id = ?
                    ORDER BY timestamp DESC
                    LIMIT ?
                ) AND user_id = ?
            """, (user_id, CONVERSATION_HISTORY_LIMIT, user_id))
            conn.commit()
            conn.close()

        await asyncio.to_thread(_add_message)
        logger.debug(f"SQLite: Сообщение {role} для пользователя {user_id} сохранено (оставлено <= {CONVERSATION_HISTORY_LIMIT})")
    except Exception as e:
        logger.exception(f"SQLite: Ошибка при добавлении сообщения: {e}")
        raise

async def get_last_messages_sqlite(db_path: str, user_id: int, limit: int) -> list[dict]:
    try:
        def _get_messages():
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row # Возвращать как dict-like
            cursor = conn.cursor()
            cursor.execute(
                "SELECT role, content FROM conversations WHERE user_id = ? ORDER BY timestamp DESC LIMIT ?",
                (user_id, limit)
            )
            rows = cursor.fetchall()
            conn.close()
            # Преобразуем sqlite3.Row в dict
            return [{'role': row['role'], 'content': row['content']} for row in rows]

        messages = await asyncio.to_thread(_get_messages)
        logger.debug(f"SQLite: Получено {len(messages)} сообщений для пользователя {user_id}")
        return messages[::-1] # Разворачиваем для хронологического порядка
    except Exception as e:
        logger.exception(f"SQLite: Ошибка при получении истории: {e}")
        return []

# PostgreSQL-специфичные функции
async def add_message_to_postgres(pool: asyncpg.Pool, user_id: int, role: str, content: str):
    try:
        async with pool.acquire() as connection:
            async with connection.transaction(): # Используем транзакцию
                # Добавляем новое сообщение
                await connection.execute(
                    "INSERT INTO conversations (user_id, role, content) VALUES ($1, $2, $3)",
                    user_id, role, content
                )
                # Очистка: удаляем старые сообщения, оставляя только последние N
                cleanup_query = """
                WITH ranked_messages AS (
                    SELECT id, ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY timestamp DESC) as rn
                    FROM conversations
                    WHERE user_id = $1
                )
                DELETE FROM conversations
                WHERE id IN (SELECT id FROM ranked_messages WHERE rn > $2);
                """
                await connection.execute(cleanup_query, user_id, CONVERSATION_HISTORY_LIMIT)
        logger.debug(f"PostgreSQL: Сообщение {role} для пользователя {user_id} сохранено и выполнена очистка (оставлено <= {CONVERSATION_HISTORY_LIMIT}).")
    except asyncpg.PostgresError as e:
        logger.error(f"PostgreSQL: Ошибка при добавлении сообщения или очистке истории: {e}")
        raise
    except Exception as e:
        logger.exception(f"PostgreSQL: Непредвиденная ошибка при добавлении сообщения или очистке истории: {e}")
        raise

async def get_last_messages_postgres(pool: asyncpg.Pool, user_id: int, limit: int) -> list[dict]:
    try:
        async with pool.acquire() as connection:
            records = await connection.fetch(
                "SELECT role, content FROM conversations WHERE user_id = $1 ORDER BY timestamp DESC LIMIT $2",
                user_id, limit
            )
            messages = [{'role': record['role'], 'content': record['content']} for record in records]
            logger.debug(f"PostgreSQL: Получено {len(messages)} сообщений для пользователя {user_id}")
            return messages[::-1] # Разворачиваем для хронологического порядка
    except asyncpg.PostgresError as e:
        logger.error(f"PostgreSQL: Ошибка при получении истории: {e}")
        return []
    except Exception as e:
        logger.exception(f"PostgreSQL: Непредвиденная ошибка при получении истории: {e}")
        return []

# --- Функции для работы с таблицей users ---

async def get_or_create_user(db, user_id: int, username: str | None, first_name: str, last_name: str | None):
    """Получает пользователя из БД или создает нового, если не найден."""
    user_data = await get_user(db, user_id)
    if user_data:
        # Опционально: обновить имя/username, если они изменились
        # await update_user_info(db, user_id, username, first_name, last_name)
        # Обновляем дату последней активности при каждом получении
        await update_user_last_active(db, user_id)
        return user_data

    # Создаем нового пользователя
    if settings.USE_SQLITE:
        return await add_user_sqlite(db, user_id, username, first_name, last_name)
    else:
        return await add_user_postgres(db, user_id, username, first_name, last_name)

async def get_user(db, user_id: int) -> dict | None:
    """Получает данные пользователя по ID."""
    if settings.USE_SQLITE:
        def _get():
            conn = sqlite3.connect(db)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
            row = cursor.fetchone()
            conn.close()
            return dict(row) if row else None
        return await asyncio.to_thread(_get)
    else: # PostgreSQL
        async with db.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
            return dict(row) if row else None

async def add_user_sqlite(db_path: str, user_id: int, username: str | None, first_name: str, last_name: str | None):
    """Добавляет нового пользователя в SQLite."""
    try:
        def _add():
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO users (user_id, username, first_name, last_name, last_active_date, last_free_reset_date)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, date('now'))
                ON CONFLICT(user_id) DO NOTHING -- Игнорировать, если пользователь уже есть
                """,
                (user_id, username, first_name, last_name)
            )
            conn.commit()
            conn.close()
            logger.info(f"SQLite: Добавлен новый пользователь {user_id}")
        await asyncio.to_thread(_add)
        return await get_user(db_path, user_id) # Возвращаем созданного пользователя
    except Exception as e:
        logger.exception(f"SQLite: Ошибка добавления пользователя {user_id}: {e}")
        return None

async def add_user_postgres(pool: asyncpg.Pool, user_id: int, username: str | None, first_name: str, last_name: str | None):
    """Добавляет нового пользователя в PostgreSQL."""
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO users (user_id, username, first_name, last_name, last_active_date, last_free_reset_date)
                VALUES ($1, $2, $3, $4, NOW(), CURRENT_DATE)
                ON CONFLICT (user_id) DO NOTHING -- Игнорировать, если пользователь уже есть
                """,
                user_id, username, first_name, last_name
            )
        logger.info(f"PostgreSQL: Добавлен новый пользователь {user_id}")
        return await get_user(pool, user_id) # Возвращаем созданного пользователя
    except asyncpg.PostgresError as e:
        logger.error(f"PostgreSQL: Ошибка добавления пользователя {user_id}: {e}")
        return None

async def update_user_last_active(db, user_id: int):
    """Обновляет время последней активности пользователя."""
    try:
        if settings.USE_SQLITE:
            def _update():
                conn = sqlite3.connect(db)
                cursor = conn.cursor()
                cursor.execute("UPDATE users SET last_active_date = CURRENT_TIMESTAMP WHERE user_id = ?", (user_id,))
                conn.commit()
                conn.close()
            await asyncio.to_thread(_update)
        else: # PostgreSQL
            async with db.acquire() as conn:
                await conn.execute("UPDATE users SET last_active_date = NOW() WHERE user_id = $1", user_id)
        # logger.debug(f"Обновлена last_active_date для user_id={user_id}") # Опционально для отладки
    except Exception as e:
        logger.exception(f"Ошибка обновления last_active_date для user_id={user_id}: {e}")


# --- Добавьте другие функции обновления по мере необходимости ---
# Например, для обновления лимитов, статуса подписки и т.д.
# async def update_user_limits(...)
# async def update_user_subscription(...)

# --- Взаимодействие с XAI API ---

async def stream_xai_response(api_key: str, system_prompt: str, history: list[dict]) -> typing.AsyncGenerator[str, None]:
    """
    Асинхронный генератор для получения ответа от XAI Chat API в режиме стриминга.
    """
    # Убираем системный промпт из истории, если он там уже есть
    history_no_system = [msg for msg in history if msg.get("role") != "system"]
    # XAI ожидает системный промпт как первое сообщение в списке
    messages = [{"role": "system", "content": system_prompt}] + history_no_system

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "text/event-stream" # XAI использует Server-Sent Events для стриминга
    }
    # URL XAI API
    url = "https://api.x.ai/v1/chat/completions"
    payload = {
        "model": "grok-3-mini-beta",
        "messages": messages,
        "stream": True,
        "temperature": 0.4,
        "reasoning": {"effort": "high"},
    }
    # Таймаут для запроса (в секундах)
    request_timeout = 180 # 3 минуты

    # Ограничение на количество попыток подключения
    max_retries = 3
    retry_delay = 1 # секунда

    # Используем сессию aiohttp для управления соединениями
    connector = aiohttp.TCPConnector(family=socket.AF_INET) # Используем IPv4
    async with aiohttp.ClientSession(connector=connector) as session:
        for attempt in range(max_retries):
            try:
                # Выполняем POST-запрос с указанным таймаутом
                async with session.post(url, headers=headers, json=payload, timeout=request_timeout) as response:
                    response.raise_for_status()  # Вызовет исключение для статусов 4xx/5xx

                    buffer = ""  # Буфер для неполных данных
                    # Читаем ответ построчно (SSE)
                    async for line_bytes in response.content:
                        line = line_bytes.decode('utf-8').strip()
                        logger.debug(f"Received line: {line!r}")

                        if not line:
                            continue

                        if line.startswith("data: "):
                            buffer = line[len("data: "):]
                            if buffer == "[DONE]":
                                logger.info("Стриминг завершен сигналом [DONE]")
                                return
                            try:
                                chunk = json.loads(buffer)
                                choices = chunk.get('choices') or []
                                if choices:
                                    delta = choices[0].get('delta') or {}
                                    text = delta.get('content')
                                    if text:
                                        yield text
                                finish_reason = choices[0].get('finish_reason')
                                if finish_reason:
                                    logger.info(f"Стриминг завершен с причиной: {finish_reason}")
                            except json.JSONDecodeError:
                                logger.error(f"Ошибка декодирования JSON из строки: {buffer!r}")
                            except Exception as e:
                                logger.exception(f"Неожиданная ошибка при обработке чанка JSON: {e}. Чанк: {buffer}")
                            continue

            except asyncio.TimeoutError:
                logger.error(f"Таймаут при подключении/чтении из XAI API (попытка {attempt + 1}/{max_retries}). URL: {url}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay * (attempt + 1)) # Экспоненциальная задержка
                    continue
                else:
                    raise # Перебрасываем исключение после последней попытки
            except aiohttp.ClientConnectionError as e:
                 logger.error(f"Ошибка соединения с XAI API: {e}. URL: {url}. Попытка {attempt + 1}/{max_retries}.")
                 if attempt < max_retries - 1:
                      await asyncio.sleep(retry_delay * (attempt + 1))
                      continue
                 else:
                      raise
            except aiohttp.ClientResponseError as e:
                # Пробрасываем авторизационные ошибки (401/403) для обработки выше
                if e.status in (401, 403):
                    raise
                # Логируем и повторяем только серверные ошибки 5xx
                if e.status >= 500 and attempt < max_retries - 1:
                    try:
                        error_body = await response.text()
                    except Exception:
                        error_body = ""
                    logger.error(f"Ошибка HTTP запроса к XAI API: {e.status} {e.message}. URL: {url}. Попытка {attempt + 1}/{max_retries}. Тело ответа: {error_body[:500]}")
                    await asyncio.sleep(retry_delay * (attempt + 1))
                    continue
                # Для остальных клиентских ошибок прекращаем ретраи
                raise
            else:
                 # Если запрос успешен, выходим из цикла ретраев
                 break


# --- Обработка Markdown в HTML для Telegram ---
def markdown_to_telegram_html(text: str) -> str:
    """Преобразует Markdown-подобный текст в HTML, поддерживаемый Telegram."""
    import re, html

    if not text:
        return ""

    code_blocks: dict[str, str] = {}
    placeholder_counter = 0

    def _extract_code_block(match):
        nonlocal placeholder_counter
        placeholder = f"@@CODEBLOCK_{placeholder_counter}@@"
        code_blocks[placeholder] = match.group(1)
        placeholder_counter += 1
        return placeholder

    def _extract_inline_code(match):
        nonlocal placeholder_counter
        placeholder = f"@@INLINECODE_{placeholder_counter}@@"
        code_blocks[placeholder] = match.group(1)
        placeholder_counter += 1
        return placeholder

    # Извлечение блоков кода
    text = re.sub(r"```(?:\w+)?\n([\s\S]*?)```", _extract_code_block, text, flags=re.DOTALL)
    # Извлечение inline-кода
    text = re.sub(r"`([^`]+?)`", _extract_inline_code, text)

    # Экранирование остального текста
    text = html.escape(text, quote=False)

    # Ссылки [text](url)
    def _replace_link(match):
        label = match.group(1)
        url = match.group(2)
        safe_url = html.escape(url, quote=True)
        return f'<a href="{safe_url}">{label}</a>'
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", _replace_link, text)

    # Заголовки #…##
    text = re.sub(r"^(#{1,6})\s*(.+)$", lambda m: f"<b>{m.group(2)}</b>\n", text, flags=re.MULTILINE)

    # Жирный **text**
    text = re.sub(r"\*\*([^\*]+)\*\*", r"<b>\1</b>", text)
    # Подчёркивание __text__
    text = re.sub(r"__([^_]+)__", r"<u>\1</u>", text)
    # Курсив *text* и _text_
    text = re.sub(r"(?<!\*)\*([^*]+)\*(?!\*)", r"<i>\1</i>", text)
    text = re.sub(r"(?<!_)_([^_]+)_(?!_)", r"<i>\1</i>", text)
    # Зачёркивание ~~text~~
    text = re.sub(r"~~(.+?)~~", r"<s>\1</s>", text)
    # Спойлеры ||text||
    text = re.sub(r"\|\|(.+?)\|\|", r"<tg-spoiler>\1</tg-spoiler>", text)

    # Восстановление кодовых блоков
    for placeholder, code in code_blocks.items():
        escaped = html.escape(code, quote=False)
        if placeholder.startswith("@@CODEBLOCK_"):
            replacement = f"<pre>{escaped}</pre>"
        else:
            replacement = f"<code>{escaped}</code>"
        text = text.replace(placeholder, replacement)

    # Нормализация пустых строк (не более двух подряд)
    text = re.sub(r'\n{3,}', '\n\n', text)
    # Удаляем пробелы и переносы в начале/конце
    text = text.strip()

    return text

# --- Вспомогательная функция для разбиения текста ---
def split_text(text: str, length: int = TELEGRAM_MAX_LENGTH) -> list[str]:
    """Разбивает текст на части указанной длины."""
    if len(text) <= length:
        return [text]

    chunks = []
    start = 0
    while start < len(text):
        # Ищем последний перенос строки или пробел в пределах длины
        end = start + length
        if end >= len(text):
            chunks.append(text[start:])
            break

        split_pos = -1
        # Ищем с конца к началу чанка
        for i in range(end - 1, start -1, -1):
            if text[i] == '\n':
                split_pos = i + 1 # Включаем перенос в предыдущий чанк
                break
            elif text[i] == ' ':
                split_pos = i + 1 # Разделяем по пробелу
                break

        if split_pos != -1 and split_pos > start: # Если нашли подходящую точку разрыва
            chunks.append(text[start:split_pos])
            start = split_pos
        else: # Если не нашли (например, очень длинное слово или строка без пробелов)
            # Просто рубим по длине
            chunks.append(text[start:end])
            start = end

    return chunks


# --- Обработчики Telegram ---

@dp.message(Command("start"))
async def start_handler(message: types.Message):
    user_id = message.from_user.id # Получаем user_id
    # Получаем зависимости
    db = dp.workflow_data.get('db')
    current_settings = dp.workflow_data.get('settings')

    if not db or not current_settings:
        logger.error(f"Не удалось получить БД/настройки в start_handler для user_id={user_id}")
        await message.answer("Произошла внутренняя ошибка (код s1), попробуйте позже.")
        return

    # --- Получаем или создаем пользователя ---
    user_data = await get_or_create_user(
        db,
        user_id,
        message.from_user.username,
        message.from_user.first_name,
        message.from_user.last_name
    )
    if not user_data:
        logger.error(f"Не удалось получить или создать пользователя {user_id} в start_handler")
        await message.answer("Произошла внутренняя ошибка (код s2), попробуйте позже.")
        return
    logger.debug(f"Данные пользователя {user_id} (start): {user_data}")
    # --- Конец изменений ---

    # Отправляем главное меню с кнопками ReplyKeyboardMarkup
    keyboard = main_menu_keyboard()
    await message.answer(
        f"Привет, {message.from_user.first_name}! Я ваш AI ассистент.\n"
        "Задайте ваш вопрос или используйте кнопки меню.",
        reply_markup=keyboard
    )

@dp.message(
    F.text
    & ~(F.text == "🔄 Новый диалог")
    & ~(F.text == "📊 Мои лимиты")
    & ~(F.text == "💎 Подписка")
    & ~(F.text == "🆘 Помощь")
    & ~(F.text == "❓ Задать вопрос")
)
async def message_handler(message: types.Message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    user_text = message.text

    # Получаем зависимости из workflow_data
    db = dp.workflow_data.get('db')
    current_settings = dp.workflow_data.get('settings')

    if not db or not current_settings:
        logger.error("Не удалось получить соединение с БД или настройки")
        await message.answer("Произошла внутренняя ошибка (код 1), попробуйте позже.")
        return

    # --- Получаем или создаем пользователя (обновляем last_active) ---
    user_data = await get_or_create_user(
        db,
        user_id,
        message.from_user.username,
        message.from_user.first_name,
        message.from_user.last_name
    )
    if not user_data:
        logger.error(f"Не удалось получить или создать пользователя {user_id}")
        await message.answer("Произошла внутренняя ошибка (код 3), попробуйте позже.")
        return
    # Теперь у вас есть user_data - словарь с данными пользователя
    logger.debug(f"Данные пользователя {user_id}: {user_data}")
    # --- КОНЕЦ ИЗМЕНЕНИЙ ---

    # Проверка, не идет ли уже генерация для этого пользователя
    if user_id in active_requests:
        try:
            await message.reply("Пожалуйста, дождитесь завершения предыдущего запроса или отмените его.", reply_markup=progress_keyboard(user_id))
        except TelegramAPIError as e:
            logger.warning(f"Не удалось отправить сообщение о дублирующем запросе: {e}")
        return

    # Регистрируем текущую задачу генерации, чтобы ее можно было отменить
    active_requests[user_id] = asyncio.current_task()
    # Показываем индикатор "печатает"
    await bot.send_chat_action(chat_id=chat_id, action="typing")

    current_message_id = None # Объявляем здесь, чтобы быть доступным в finally/except
    try:
        # Сохраняем сообщение пользователя
        await add_message_to_db(db, user_id, "user", user_text)
        logger.info(f"Сообщение от пользователя {user_id} сохранено")

        # Получаем историю сообщений
        history = await get_last_messages(db, user_id, limit=CONVERSATION_HISTORY_LIMIT)
        logger.info(f"Получена история сообщений для пользователя {user_id}, записей: {len(history)}")

        # --- Новая логика стриминга с авто-разбиением ---
        full_raw_response = ""
        current_message_text = "" # Текст для текущего сообщения TG
        placeholder_message = None
        message_count = 0 # Счетчик отправленных сообщений (частей)
        last_edit_time = 0
        edit_interval = 1.5
        formatting_failed = False

        # Отправка самого первого плейсхолдера
        try:
            placeholder_message = await message.answer("⏳", reply_markup=progress_keyboard(user_id))  # Короткий плейсхолдер с кнопкой Отмена
            current_message_id = placeholder_message.message_id
            message_count = 1
            last_edit_time = time.monotonic()
        except TelegramAPIError as e:
            logger.error(f"Ошибка отправки начального плейсхолдера: {e}")
            return # Не можем продолжить

        async for chunk in stream_xai_response(current_settings.XAI_API_KEY, SYSTEM_PROMPT, history):
            if not current_message_id: # Если отправка плейсхолдера не удалась или сообщение было удалено
                 logger.warning("Прерывание стриминга, так как нет активного message_id.")
                 break

            full_raw_response += chunk
            now = time.monotonic()

            # Проверяем, не превысит ли добавление чанка лимит ТЕКУЩЕГО сообщения
            tentative_next_text = current_message_text + chunk
            try:
                # Проверяем длину с учетом HTML и "..."
                html_to_check = markdown_to_telegram_html(tentative_next_text) + "..."
                check_formatting_failed = False
            except Exception as fmt_err:
                logger.warning(f"Formatting error during length check: {fmt_err}")
                html_to_check = tentative_next_text + "..." # Проверяем raw длину
                check_formatting_failed = True
                formatting_failed = True # Отмечаем глобально

            if len(html_to_check) > TELEGRAM_MAX_LENGTH:
                # Лимит превышен, финализируем текущее сообщение
                logger.info(f"Финализация сообщения {message_count} (ID: {current_message_id}) из-за длины.")
                try:
                    final_part_html = markdown_to_telegram_html(current_message_text) if not formatting_failed else current_message_text
                    if final_part_html: # Редактируем только если есть текст
                        await bot.edit_message_text(
                            text=final_part_html,
                            chat_id=chat_id,
                            message_id=current_message_id,
                            parse_mode=None if formatting_failed else ParseMode.HTML,
                            reply_markup=progress_keyboard(user_id)  # Сохраняем кнопку Отмена
                        )
                except TelegramAPIError as e:
                    logger.error(f"Ошибка финализации сообщения {message_count}: {e}")
                    if not formatting_failed:
                        formatting_failed = True
                        logger.warning("Переключение на raw из-за ошибки финализации.")
                        try:
                            if current_message_text:
                                await bot.edit_message_text(text=current_message_text, chat_id=chat_id, message_id=current_message_id, parse_mode=None, reply_markup=None)
                        except TelegramAPIError as plain_e:
                            logger.error(f"Ошибка raw финализации сообщения {message_count}: {plain_e}")
                            current_message_id = None # Теряем это сообщение
                    else:
                        logger.error(f"Ошибка raw финализации сообщения {message_count}. Сообщение потеряно.")
                        current_message_id = None

                # Начинаем новое сообщение при переполнении: убираем отмену из старого и отправляем новый placeholder
                current_message_text = chunk  # Начинаем с нового чанка
                message_count += 1
                try:
                    # удаляем кнопку 'Отмена' из предыдущего сообщения
                    await bot.edit_message_reply_markup(chat_id=chat_id, message_id=current_message_id, reply_markup=None)
                    # отправляем новый placeholder с кнопкой 'Отмена'
                    placeholder_message = await message.answer("...", reply_markup=progress_keyboard(user_id))
                    current_message_id = placeholder_message.message_id
                    last_edit_time = time.monotonic()
                    logger.info(f"Начато новое сообщение {message_count} (ID: {current_message_id})")
                except TelegramAPIError as e:
                    logger.error(f"Ошибка отправки плейсхолдера для сообщения {message_count}: {e}")
                    current_message_id = None
                    break  # Прерываем стрим, если не можем создать новое сообщение

            else:
                # Лимит не превышен, добавляем чанк к текущему тексту
                current_message_text += chunk

                # Редактируем текущее сообщение с троттлингом
                if now - last_edit_time > edit_interval:
                    try:
                        html_to_send = markdown_to_telegram_html(current_message_text) if not formatting_failed else current_message_text
                        text_to_show = html_to_send + "..."

                        await bot.edit_message_text(
                            text=text_to_show,
                            chat_id=chat_id,
                            message_id=current_message_id,
                            parse_mode=None if formatting_failed else ParseMode.HTML,
                            reply_markup=progress_keyboard(user_id)  # Обновляем кнопку Отмена
                        )
                        last_edit_time = now
                    except TelegramRetryAfter as e:
                        logger.warning(f"Throttled: RetryAfter {e.retry_after}s")
                        await asyncio.sleep(e.retry_after + 0.1)
                        last_edit_time = time.monotonic()
                    except TelegramAPIError as e:
                         logger.error(f"Ошибка редактирования сообщения {message_count} (mid-stream): {e}")
                         # Проверяем, не пропало ли сообщение
                         if any(msg in str(e).lower() for msg in ("message to edit not found", "message can't be edited", "message is not modified")):
                             logger.warning(f"Сообщение {message_count} (ID: {current_message_id}) больше недоступно для редактирования.")
                             current_message_id = None
                             # Не прерываем цикл, т.к. следующий чанк может создать новое сообщение
                         elif not formatting_failed: # Если ошибка не связана с пропажей сообщения, и мы еще не перешли на raw
                             formatting_failed = True
                             logger.warning("Переключение на raw из-за ошибки редактирования.")
                         # Если уже raw или ошибка была другая, просто пропускаем это редактирование
                    except Exception as e:
                         logger.exception(f"Неожиданная ошибка редактирования сообщения {message_count}: {e}")


        # --- Финализация ПОСЛЕДНЕГО сообщения после цикла ---
        if current_message_id and current_message_text:
            logger.info(f"Финализация последнего сообщения {message_count} (ID: {current_message_id})")
            try:
                final_html = markdown_to_telegram_html(current_message_text) if not formatting_failed else current_message_text
                # оформляем финальный текст без кнопок в этом сообщении
                await bot.edit_message_text(
                    text=final_html,
                    chat_id=chat_id,
                    message_id=current_message_id,
                    parse_mode=None if formatting_failed else ParseMode.HTML,
                    reply_markup=None
                )
                # затем показываем ReplyKeyboardMarkup меню новым сообщением
                await message.answer(
                    "🫡",
                    reply_markup=main_menu_keyboard()
                )
                logger.info(f"Последнее сообщение {message_count} {'RAW' if formatting_failed else 'HTML'} отправлено.")

            except TelegramAPIError as e:
                logger.error(f"Ошибка финализации последнего сообщения {message_count}: {e}")
                # Попытка отправить raw как fallback
                try:
                    # Raw fallback: редактируем без кнопок
                    await bot.edit_message_text(
                        text=current_message_text,
                        chat_id=chat_id,
                        message_id=current_message_id,
                        parse_mode=None,
                        reply_markup=None
                    )
                    logger.info(f"Последнее сообщение {message_count} RAW отправлено после ошибки HTML.")
                except TelegramAPIError as plain_e:
                    logger.error(f"Ошибка raw финализации последнего сообщения {message_count}: {plain_e}")
                    # Как крайняя мера, отправить новым сообщением
                    try:
                         await message.answer(
                             text=current_message_text,
                             parse_mode=None,
                             reply_markup=main_menu_keyboard()
                         )
                         logger.info(f"Последняя часть {message_count} отправлена новым сообщением после ошибки редактирования.")
                    except Exception as final_send_err:
                         logger.error(f"Не удалось отправить последнюю часть {message_count} новым сообщением: {final_send_err}")

        elif not full_raw_response and message_count == 1 and current_message_id:
            # Если API ничего не вернуло после первого плейсхолдера
            logger.warning(f"Не получен ответ от XAI для пользователя {user_id}")
            try:
                # Показ ошибки без кнопок, затем меню
                await bot.edit_message_text(
                    "К сожалению, не удалось получить ответ от AI.",
                    chat_id=chat_id,
                    message_id=current_message_id,
                    reply_markup=None
                )
                await message.answer(
                    "🫡",
                    reply_markup=main_menu_keyboard()
                )
            except TelegramAPIError:
                pass # Игнорируем, если сообщение уже удалено

        # --- Сохранение полного ответа в БД ---
        if full_raw_response:
            try:
                await add_message_to_db(db, user_id, "assistant", full_raw_response)
                logger.info(f"Ответ ассистента (RAW) для пользователя {user_id} сохранен в БД")
            except Exception as e:
                logger.error(f"Ошибка сохранения ответа ассистента в БД: {e}")
        # (Логика для случая else: logger.warning(f"Не получен или пустой ответ...) обработана выше

    except Exception as e:
        logger.exception(f"Критическая ошибка в обработчике сообщений для user_id={user_id}: {e}")
        # очистка active_requests при ошибке
        active_requests.pop(user_id, None)
        try:
            # Пытаемся отредактировать последнее известное сообщение об ошибке
            error_message = "Произошла серьезная ошибка при обработке вашего запроса."
            if current_message_id:
                 await bot.edit_message_text(error_message, chat_id=chat_id, message_id=current_message_id, reply_markup=None)
            else: # Или отправляем новое, если ID нет
                await message.answer(error_message + " Пожалуйста, попробуйте позже или используйте команду /start для сброса.")
        except TelegramAPIError:
             logger.error("Не удалось даже отправить сообщение об ошибке пользователю.")

# --- Обработчик отмены генерации ---
@dp.callback_query(F.data.startswith("cancel_generation_"))
async def cancel_generation_callback(callback: types.CallbackQuery):
    """Обрабатывает отмену генерации: прекращает задачу, убирает клавиатуру и показывает меню."""
    # Парсим user_id из callback_data
    try:
        user_id_to_cancel = int(callback.data.rsplit("_", 1)[-1])
    except ValueError:
        await callback.answer("Ошибка обработки отмены.", show_alert=True)
        return

    # Отменяем задачу генерации, если она есть
    task = active_requests.pop(user_id_to_cancel, None)
    if task:
        task.cancel()

    # Убираем inline-клавиатуру отмены
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except TelegramAPIError:
        pass

    # Уведомляем пользователя о завершении отмены
    await callback.answer("Генерация ответа отменена.", show_alert=False)

    # Показываем главное меню
    await callback.message.answer(
        "🫡",
        reply_markup=main_menu_keyboard()
    )

@dp.callback_query(F.data == "clear_history")
async def clear_history_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    # Получаем зависимости из диспетчера (можно и через bot.dispatcher)
    db = dp.workflow_data.get('db')
    current_settings = dp.workflow_data.get('settings')

    if not db or not current_settings:
        logger.error("Не удалось получить БД/настройки при очистке истории (callback)")
        await callback.answer("Ошибка при очистке истории", show_alert=True)
        return

    # --- Получаем или создаем пользователя (обновляем last_active) ---
    user_data = await get_or_create_user(
        db,
        user_id,
        callback.from_user.username,
        callback.from_user.first_name,
        callback.from_user.last_name
    )
    if not user_data:
        logger.error(f"Не удалось получить или создать пользователя {user_id} в clear_history_callback")
        await callback.answer("Произошла внутренняя ошибка (код ch1), попробуйте позже.", show_alert=True)
        return
    logger.debug(f"Данные пользователя {user_id} (clear_history_callback): {user_data}")
    # --- Конец изменений ---

    try:
        rows_deleted_count = 0
        if current_settings.USE_SQLITE:
            def _clear_history_sqlite():
                db_path = db # db здесь это путь к файлу
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute("DELETE FROM conversations WHERE user_id = ?", (user_id,))
                deleted_count = cursor.rowcount
                conn.commit()
                conn.close()
                return deleted_count
            rows_deleted_count = await asyncio.to_thread(_clear_history_sqlite)
            logger.info(f"SQLite: Очищена история пользователя {user_id}, удалено {rows_deleted_count} записей")
        else:
            # PostgreSQL
            async with db.acquire() as connection: # db здесь это пул
                result = await connection.execute("DELETE FROM conversations WHERE user_id = $1", user_id) # Убрал RETURNING id для простоты
                # result это строка вида "DELETE N", парсим N
                try:
                    rows_deleted_count = int(result.split()[-1]) if result.startswith("DELETE") else 0
                except:
                    rows_deleted_count = -1 # Не удалось распарсить
                logger.info(f"PostgreSQL: Очищена история пользователя {user_id}, результат: {result}")

        await callback.answer(f"История очищена ({rows_deleted_count} записей удалено)", show_alert=False)
        # Можно добавить сообщение в чат для наглядности
        # Редактируем исходное сообщение или отвечаем новым
        try:
            # Пытаемся отредактировать, если это было сообщение с кнопкой
            await callback.message.edit_text("История диалога очищена.", reply_markup=None)
        except TelegramAPIError:
            # Если не вышло (например, это было не сообщение бота или прошло много времени),
            # отправляем новое сообщение
             await callback.message.answer("История диалога очищена.")

    except Exception as e:
        logger.exception(f"Ошибка при очистке истории (callback) для user_id={user_id}: {e}")
        await callback.answer("Произошла ошибка при очистке", show_alert=True)

# --- Обработчик команды /clear ---
@dp.message(Command("clear"))
async def clear_command_handler(message: types.Message):
    user_id = message.from_user.id
    db = dp.workflow_data.get('db')
    current_settings = dp.workflow_data.get('settings')

    if not db or not current_settings:
        logger.error("Не удалось получить БД/настройки при очистке истории (/clear)")
        await message.answer("Произошла внутренняя ошибка (код 2), попробуйте позже.")
        return

    # --- Получаем или создаем пользователя (обновляем last_active) ---
    user_data = await get_or_create_user(
        db,
        user_id,
        message.from_user.username,
        message.from_user.first_name,
        message.from_user.last_name
    )
    if not user_data:
        logger.error(f"Не удалось получить или создать пользователя {user_id} в clear_command_handler")
        await message.answer("Произошла внутренняя ошибка (код cl1), попробуйте позже.")
        return
    logger.debug(f"Данные пользователя {user_id} (clear_command): {user_data}")
    # --- Конец изменений ---

    try:
        rows_deleted_count = 0
        if current_settings.USE_SQLITE:
            def _clear_history_sqlite_cmd():
                db_path = db
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute("DELETE FROM conversations WHERE user_id = ?", (user_id,))
                deleted_count = cursor.rowcount
                conn.commit()
                conn.close()
                return deleted_count
            rows_deleted_count = await asyncio.to_thread(_clear_history_sqlite_cmd)
            logger.info(f"SQLite: Очищена история пользователя {user_id} по команде /clear, удалено {rows_deleted_count} записей")
        else:
            # PostgreSQL
            async with db.acquire() as connection:
                result = await connection.execute("DELETE FROM conversations WHERE user_id = $1", user_id)
                try:
                     rows_deleted_count = int(result.split()[-1]) if result.startswith("DELETE") else 0
                except:
                    rows_deleted_count = -1
                logger.info(f"PostgreSQL: Очищена история пользователя {user_id} по команде /clear, результат: {result}")

        await message.answer(f"История диалога очищена ({rows_deleted_count} записей удалено).")
    except Exception as e:
        logger.exception(f"Ошибка при очистке истории (/clear) для user_id={user_id}: {e}")
        await message.answer("Произошла ошибка при очистке истории.")

# --- Обработчики медиа (обновлено для vision) ---
@dp.message(F.photo)
async def photo_handler(message: types.Message):
    user_id = message.from_user.id
    chat_id = message.chat.id

    # Проверка зависимостей
    db = dp.workflow_data.get('db')
    current_settings = dp.workflow_data.get('settings')
    if not db or not current_settings:
        logger.error("Не удалось получить соединение с БД или настройки при обработке изображения")
        await message.answer("Внутренняя ошибка при обработке изображения.", reply_markup=None)
        return

    caption = message.caption or ""
    logger.info(f"Получено фото от user_id={user_id} с подписью: '{caption[:50]}...'" )

    try:
        # Скачиваем и конвертируем изображение в JPEG (любой формат через Pillow)
        photo: types.PhotoSize = message.photo[-1]
        bio = io.BytesIO()
        await bot.download(photo, destination=bio)
        bio.seek(0)
        img = Image.open(bio).convert('RGB')
        conv_bio = io.BytesIO()
        img.save(conv_bio, format='JPEG', quality=90)
        conv_bio.seek(0)
        encoded = base64.b64encode(conv_bio.getvalue()).decode()
        data_url = f"data:image/jpeg;base64,{encoded}"

        # Стриминг ответа vision-модели
        placeholder = await message.answer("⏳", reply_markup=progress_keyboard(user_id))
        current_text = ""
        last_edit = time.monotonic()
        edit_interval = 1.5
        # Запрашиваем поток
        stream = await vision_async_client.chat.completions.create(
            model="grok-2-vision-1212",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": [
                    {"type": "image_url", "image_url": {"url": data_url, "detail": "high"}},
                    {"type": "text", "text": caption or "Опишите, пожалуйста, это изображение."}
                ]}
            ],
            stream=True
        )
        async for chunk in stream:
            delta = getattr(chunk.choices[0].delta, 'content', '') or ''
            current_text += delta
            now = time.monotonic()
            if now - last_edit > edit_interval:
                # Подготавливаем безопасное превью, обрезая по длине
                html_preview = markdown_to_telegram_html(current_text)
                if len(html_preview) > TELEGRAM_MAX_LENGTH - 3:
                    html_preview = html_preview[:TELEGRAM_MAX_LENGTH - 3]
                preview = html_preview + '...'
                try:
                    await bot.edit_message_text(
                        text=preview,
                        chat_id=chat_id,
                        message_id=placeholder.message_id,
                        parse_mode=ParseMode.HTML,
                        reply_markup=progress_keyboard(user_id)
                    )
                except TelegramAPIError as e:
                    logger.warning(f"Превышена длина превью, пропускаю обновление: {e}")
                last_edit = now

        # Финальная отрисовка
        final_html = markdown_to_telegram_html(current_text)
        # Разбиваем на части для Telegram
        parts = split_text(final_html)
        # Редактируем плейсхолдер первым фрагментом
        await bot.edit_message_text(
            text=parts[0],
            chat_id=chat_id,
            message_id=placeholder.message_id,
            parse_mode=ParseMode.HTML,
            reply_markup=None
        )
        # Отправляем остальные части отдельными сообщениями
        for part in parts[1:]:
            await message.answer(part, parse_mode=ParseMode.HTML)

        # Сохранить ответ и показать меню
        await add_message_to_db(db, user_id, "assistant", current_text)
        await message.answer("🫡", reply_markup=main_menu_keyboard())
    except Exception as e:
        logger.exception(f"Критическая ошибка в photo_handler для user_id={user_id}: {e}")
        await message.answer("Не удалось обработать изображение. Попробуйте позже.", reply_markup=main_menu_keyboard())

@dp.message(F.document)
async def document_handler(message: types.Message):
    user_id = message.from_user.id
    file_name = message.document.file_name or "Без имени"
    mime_type = message.document.mime_type or "Неизвестный тип"
    logger.info(f"Получен документ от user_id={user_id}: {file_name} (type: {mime_type})")
    await message.reply(f"Получил документ '{file_name}'. Обработка документов пока не реализована.")

# --- Обработчики для кнопок меню ReplyKeyboardMarkup
@dp.message(F.text == "🔄 Новый диалог")
async def handle_new_dialog_button(message: types.Message):
    # Просто вызываем существующий обработчик команды /clear
    await clear_command_handler(message)

@dp.message(F.text == "📊 Мои лимиты")
async def handle_my_limits_button(message: types.Message):
    user_id = message.from_user.id
    db = dp.workflow_data.get('db')
    if not db:
        await message.reply("Ошибка получения данных.")
        return
    user_data = await get_user(db, user_id)
    if not user_data:
        await message.reply("Не удалось найти ваши данные.")
        return
    limit_info = f"Осталось бесплатных сообщений сегодня: {user_data.get('free_messages_today', 'N/A')}"
    sub_info = "Подписка: неактивна"
    if user_data.get('subscription_status') == 'active':
        expires_ts = user_data.get('subscription_expires')
        if isinstance(expires_ts, datetime.datetime):
            expires_str = expires_ts.strftime('%Y-%m-%d %H:%M')
            sub_info = f"Подписка активна до: {expires_str}"
        else:
            sub_info = f"Подписка активна до: {expires_ts}"
    await message.reply(f"Информация о ваших лимитах:\n\n{limit_info}\n{sub_info}")

@dp.message(F.text == "💎 Подписка")
async def handle_subscription_button(message: types.Message):
    # Заглушка раздела подписки
    await message.reply(
        "Раздел 'Подписка'.\n\n"
        "Доступные тарифы:\n- 7 дней / 100 руб.\n- 30 дней / 300 руб.\n\n"
        "(Функционал оплаты будет добавлен позже)"
    )

@dp.message(F.text == "🆘 Помощь")
async def handle_help_button(message: types.Message):
    help_text = (
        "<b>Помощь по боту:</b>\n\n"
        "🤖 Я - ваш AI ассистент, работающий на модели Grok.\n"
        "❓ Просто напишите ваш вопрос, и я постараюсь ответить.\n"
        "🔄 Используйте кнопку \"Новый диалог\" или команду /clear, чтобы очистить историю и начать разговор с чистого листа.\n"
        "📊 Кнопка \"Мои лимиты\" покажет, сколько бесплатных сообщений у вас осталось сегодня или до какого числа действует подписка.\n"
        "💎 Кнопка \"Подписка\" расскажет о платных тарифах для снятия лимитов."
    )
    await message.reply(help_text, reply_markup=main_menu_keyboard())

@dp.message(F.text == "❓ Задать вопрос")
async def handle_ask_question_button(message: types.Message):
    await message.reply("Просто напишите ваш вопрос в чат 👇", reply_markup=main_menu_keyboard())

# --- Функции запуска и остановки ---

# Восстанавливаем функцию on_shutdown
async def on_shutdown(**kwargs):
    logger.info("Завершение работы бота...")
    # Получаем dp и из него workflow_data
    dp_local = kwargs.get('dispatcher') # aiogram передает dispatcher
    if not dp_local:
        logger.error("Не удалось получить dispatcher в on_shutdown")
        return

    db = dp_local.workflow_data.get('db')
    settings_local = dp_local.workflow_data.get('settings')

    if db and settings_local:
        if not settings_local.USE_SQLITE:
            try:
                # db здесь это пул соединений asyncpg
                if isinstance(db, asyncpg.Pool):
                    await db.close()
                    logger.info("Пул соединений PostgreSQL успешно закрыт")
                else:
                    logger.warning("Объект 'db' не является пулом asyncpg, закрытие не выполнено.")
            except Exception as e:
                logger.error(f"Ошибка при закрытии пула соединений PostgreSQL: {e}")
        else:
            logger.info("Используется SQLite, явное закрытие пула не требуется.")
    else:
         logger.warning("Не удалось получить 'db' или 'settings' из workflow_data при завершении работы.")

    logger.info("Бот остановлен.")


# --- Установка команд бота (если еще не сделано) ---
async def set_bot_commands(bot_instance: Bot):
    commands = [
        types.BotCommand(command="/start", description="Начать диалог / Показать меню"),
        types.BotCommand(command="/clear", description="Очистить историю диалога"),
        # Добавьте другие команды если нужно
    ]
    try:
        await bot_instance.set_my_commands(commands)
        logger.info("Команды бота успешно установлены.")
    except TelegramAPIError as e:
        logger.error(f"Ошибка при установке команд бота: {e}")

# --- Главная функция запуска ---
async def main():
    logger.info(f"Запуск приложения с настройками базы данных: {settings.DATABASE_URL}")
    db_connection = None # Переименуем, чтобы не конфликтовать с именем модуля

    try:
        # Выбор типа БД на основе URL из настроек
        if settings.USE_SQLITE:
            logger.info("Используется SQLite для хранения данных")
            db_connection = await init_sqlite_db(settings.DATABASE_URL) # Возвращает путь
        else:
            logger.info("Используется PostgreSQL для хранения данных")
            # Попытка подключения с таймаутом и обработкой ошибок
            try:
                logger.info(f"Подключение к PostgreSQL: {settings.DATABASE_URL}")
                # Увеличим таймауты для create_pool
                db_connection = await asyncio.wait_for(
                    asyncpg.create_pool(dsn=settings.DATABASE_URL, timeout=30.0, command_timeout=60.0, min_size=1, max_size=10),
                    timeout=45.0 # Общий таймаут на создание пула
                )
                if not db_connection:
                    logger.error("Не удалось создать пул соединений PostgreSQL (вернулся None)")
                    sys.exit(1)

                logger.info("Пул соединений PostgreSQL успешно создан")
                # Проверим соединение и инициализируем таблицу
                await init_db_postgres(db_connection)

            except asyncio.TimeoutError:
                logger.error("Превышен таймаут подключения к базе данных PostgreSQL")
                sys.exit(1)
            except (socket.gaierror, OSError) as e: # Ошибки сети/DNS
                logger.error(f"Ошибка сети или DNS при подключении к PostgreSQL: {e}. Проверьте хост/порт в DATABASE_URL.")
                sys.exit(1)
            except asyncpg.exceptions.InvalidPasswordError:
                 logger.error("Ошибка аутентификации PostgreSQL: неверный пароль.")
                 sys.exit(1)
            except asyncpg.exceptions.InvalidCatalogNameError as e: # Добавляем обработку InvalidCatalogNameError
                 logger.error(f"Ошибка PostgreSQL: база данных, указанная в URL, не найдена. {e}")
                 sys.exit(1)
            except asyncpg.PostgresError as e:
                logger.error(f"Общая ошибка PostgreSQL при подключении/инициализации: {e}")
                sys.exit(1)

        # Сохраняем зависимости (путь к SQLite или пул PG) в workflow_data
        dp.workflow_data['db'] = db_connection
        dp.workflow_data['settings'] = settings
        logger.info("Зависимости DB и Settings успешно сохранены в dispatcher")

        # Регистрация обработчиков (декораторы уже сделали это)
        logger.info("Обработчики команд и сообщений зарегистрированы")

        # Регистрация обработчика shutdown БЕЗ передачи аргументов
        dp.shutdown.register(on_shutdown)
        logger.info("Обработчик shutdown зарегистрирован")

        # Установка команд бота - помещаем здесь, в конце блока try
        await set_bot_commands(bot)

    except Exception as e:
        logger.exception(f"Критическая ошибка при инициализации бота: {e}")
        sys.exit(1)

    # Запускаем бота
    logger.info("Запуск бота (polling)...")
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    except Exception as e:
        logger.exception(f"Критическая ошибка во время работы бота: {e}")
    finally:
        # Закрытие сессии бота (важно для корректного завершения)
        await bot.session.close()
        logger.info("Сессия бота закрыта.")

# Отдельная асинхронная функция для очистки задач
async def cleanup_tasks():
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    if tasks:
        logger.info(f"Ожидание завершения {len(tasks)} фоновых задач...")
        [task.cancel() for task in tasks]
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
            logger.info("Фоновые задачи завершены.")
        except asyncio.CancelledError:
             logger.info("Задачи были отменены во время завершения.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
         logger.info("Получен сигнал KeyboardInterrupt, завершаю работу...")
    except SystemExit:
         logger.info("Получен сигнал SystemExit, завершаю работу...")
    finally:
        # Вызываем асинхронную очистку через asyncio.run
        logger.info("Запуск очистки фоновых задач...")
        try:
            asyncio.run(cleanup_tasks())
        except RuntimeError as e:
            # Избегаем ошибки "Cannot run the event loop while another loop is running"
            # если loop уже остановлен или используется в другом месте
            if "Cannot run the event loop" in str(e):
                 logger.warning("Не удалось запустить cleanup_tasks: цикл событий уже остановлен или занят.")
            else:
                 logger.exception("Ошибка во время выполнения cleanup_tasks.")
        except Exception as e:
            logger.exception("Непредвиденная ошибка во время выполнения cleanup_tasks.")

        logger.info("Процесс завершен.")