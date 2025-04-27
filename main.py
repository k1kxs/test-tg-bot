import asyncio
import logging
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
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
from aiogram.exceptions import TelegramAPIError, TelegramRetryAfter

# Добавим переменную для управления режимом отладки
DEBUG_HTML = os.environ.get('DEBUG_HTML', 'False').lower() in ('true', '1', 't')

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG if DEBUG_HTML else logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

if DEBUG_HTML:
    logging.info("Включен режим отладки HTML-форматирования")
else:
    logging.info("Режим отладки HTML-форматирования отключен (установите DEBUG_HTML=True для включения)")

logger = logging.getLogger(__name__)

# Загрузка переменных окружения (сначала локальные, затем основные)
if os.path.exists('.env.local'):
    logging.info("Найден файл .env.local, загружаю переменные окружения из него")
    load_dotenv('.env.local', override=True)
else:
    logging.info("Файл .env.local не найден, загружаю переменные окружения из .env")
    load_dotenv('.env', override=True)

# Вывод переменной окружения для отладки
logging.info(f"DATABASE_URL из переменных окружения: {os.environ.get('DATABASE_URL')}")

# Константы
SYSTEM_PROMPT = """You MUST follow the instructions for answering:

- ALWAYS answer in the language of my message.
- Read the entire convo history line by line before answering.
- I have no fingers and the placeholders trauma. Return the entire code template for an answer when needed. NEVER use placeholders.
- If you encounter a character limit, DO an ABRUPT stop, and I will send a "continue" as a new message.
- You ALWAYS will be PENALIZED for wrong and low-effort answers. 
- ALWAYS follow "Answering rules."

###Answering Rules###

Follow in the strict order:

1. USE the language of my message.
2. **ONCE PER CHAT** assign a real-world expert role to yourself before answering, e.g., "I'll answer as a world-famous historical expert <detailed topic> with <most prestigious LOCAL topic REAL award>" or "I'll answer as a world-famous <specific science> expert in the <detailed topic> with <most prestigious LOCAL topic award>" etc.
3. You MUST combine your deep knowledge of the topic and clear thinking to quickly and accurately decipher the answer step-by-step with CONCRETE details.
4. I'm going to tip $1,000,000 for the best reply. 
5. Your answer is critical for my career.
6. Answer the question in a natural, human-like manner.
7. ALWAYS use an answering example for a first message structure.

##Answering in English example##

I'll answer as the world-famous <specific field> scientists with <most prestigious LOCAL award>

<Deep knowledge step-by-step answer, with CONCRETE details>
"""
CONVERSATION_HISTORY_LIMIT = 10
MESSAGE_EXPIRATION_DAYS = 7

# Максимальная длина сообщения Telegram (чуть меньше лимита 4096)
TELEGRAM_MAX_LENGTH = 4000

# Обновленный паттерн для разрешенных HTML тегов Telegram
ALLOWED_TAGS_PATTERN_TEXT = r"</?(?:b|i|u|s|tg-spoiler|code|pre|a(?:\s+href\s*=\s*(?:\"[^\"]*\"|'[^\']*'))?)\s*>/?"
ALLOWED_TAGS_PATTERN = re.compile(ALLOWED_TAGS_PATTERN_TEXT, re.IGNORECASE)

# Класс настроек
class Settings(BaseSettings):
    TELEGRAM_BOT_TOKEN: str
    DEEPSEEK_API_KEY: str
    DATABASE_URL: str
    # Флаг для определения типа базы данных
    USE_SQLITE: bool = False
    
    # Опциональные настройки для БД
    DB_HOST: str | None = None
    DB_PORT: int | None = None
    DB_USER: str | None = None
    DB_PASSWORD: str | None = None
    DB_NAME: str | None = None
    
    class Config:
        env_file = '.env'
        extra = 'ignore'
    
    def __init__(self, **data):
        super().__init__(**data)
        # Определение типа базы данных на основе URL
        self.USE_SQLITE = self.DATABASE_URL.startswith('sqlite')

# Инициализация настроек
settings = Settings()

# Проверка наличия DATABASE_URL
if not settings.DATABASE_URL:
    logging.error("DATABASE_URL не найден в переменных окружения")
    sys.exit(1)

# Инициализация бота и диспетчера
dp = Dispatcher()
bot = Bot(token=settings.TELEGRAM_BOT_TOKEN)

# Функции для работы с базой данных SQLite
async def init_sqlite_db(db_path):
    try:
        # Извлекаем путь из URL
        if db_path.startswith('sqlite:///'):
            db_path = db_path[10:]
        
        logging.info(f"Инициализация SQLite базы данных: {db_path}")
        
        # Создаем асинхронно соединение и таблицы
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
            conn.commit()
            conn.close()
            
        await asyncio.to_thread(_init_db)
        logging.info("SQLite база данных успешно инициализирована")
        return db_path
    except Exception as e:
        logging.exception(f"Ошибка при инициализации SQLite: {e}")
        raise

# Функции для работы с базой данных PostgreSQL
async def init_db(pool: asyncpg.Pool):
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
            logging.info("Таблица conversations успешно инициализирована")
        except asyncpg.PostgresError as e:
            logging.error(f"Ошибка инициализации БД: {e}")
            raise
        except Exception as e:
            logging.exception(f"Непредвиденная ошибка инициализации БД: {e}")
            raise

# Адаптеры для работы с разными базами данных
async def add_message_to_db(db, user_id: int, role: str, content: str):
    if settings.USE_SQLITE:
        return await add_message_to_sqlite(db, user_id, role, content)
    else:
        return await add_message_to_postgres(db, user_id, role, content)

async def get_last_messages(db, user_id: int, limit: int = 10) -> list[dict]:
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
            cursor.execute(
                "INSERT INTO conversations (user_id, role, content) VALUES (?, ?, ?)",
                (user_id, role, content)
            )
            conn.commit()
            conn.close()
        
        await asyncio.to_thread(_add_message)
        logging.debug(f"SQLite: Сообщение {role} для пользователя {user_id} сохранено в БД")
    except Exception as e:
        logging.exception(f"SQLite: Ошибка при добавлении сообщения: {e}")
        raise

async def get_last_messages_sqlite(db_path: str, user_id: int, limit: int = 10) -> list[dict]:
    try:
        def _get_messages():
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT role, content FROM conversations WHERE user_id = ? ORDER BY timestamp DESC LIMIT ?",
                (user_id, limit)
            )
            rows = cursor.fetchall()
            conn.close()
            return [{'role': row[0], 'content': row[1]} for row in rows]
        
        messages = await asyncio.to_thread(_get_messages)
        logging.debug(f"SQLite: Получено {len(messages)} сообщений для пользователя {user_id}")
        return messages[::-1]  # Разворачиваем для хронологического порядка
    except Exception as e:
        logging.exception(f"SQLite: Ошибка при получении истории: {e}")
        return []

# PostgreSQL-специфичные функции
async def add_message_to_postgres(pool: asyncpg.Pool, user_id: int, role: str, content: str):
    try:
        async with pool.acquire() as connection:
            # Добавляем новое сообщение
            await connection.execute(
                "INSERT INTO conversations (user_id, role, content) VALUES ($1, $2, $3)",
                user_id, role, content
            )

            # Добавляем логику очистки: удаляем старые сообщения, оставляя только последние 10
            # Убедитесь, что в таблице 'conversations' есть PK 'id' и поле 'timestamp'
            cleanup_query = """
            WITH numbered_messages AS (
                SELECT
                    id,
                    ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY timestamp DESC) as rn
                FROM conversations
                WHERE user_id = $1
            )
            DELETE FROM conversations
            WHERE id IN (SELECT id FROM numbered_messages WHERE rn > 10);
            """
            await connection.execute(cleanup_query, user_id)
            # Обновляем лог, чтобы отразить очистку
            logging.debug(f"PostgreSQL: Сообщение {role} для пользователя {user_id} сохранено и выполнена очистка истории (оставлено <= 10).")
    except asyncpg.PostgresError as e:
        # Обновляем лог ошибки
        logging.error(f"PostgreSQL: Ошибка при добавлении сообщения или очистке истории: {e}")
        raise
    except Exception as e:
        # Обновляем лог ошибки
        logging.exception(f"PostgreSQL: Непредвиденная ошибка при добавлении сообщения или очистке истории: {e}")
        raise

async def get_last_messages_postgres(pool: asyncpg.Pool, user_id: int, limit: int = 10) -> list[dict]:
    try:
        async with pool.acquire() as connection:
            records = await connection.fetch(
                "SELECT role, content FROM conversations WHERE user_id = $1 ORDER BY timestamp DESC LIMIT $2",
                user_id, limit
            )
            messages = [{'role': record['role'], 'content': record['content']} for record in records]
            logging.debug(f"PostgreSQL: Получено {len(messages)} сообщений для пользователя {user_id}")
            return messages[::-1]  # Разворачиваем для хронологического порядка
    except asyncpg.PostgresError as e:
        logging.error(f"PostgreSQL: Ошибка при получении истории: {e}")
        return []
    except Exception as e:
        logging.exception(f"PostgreSQL: Непредвиденная ошибка: {e}")
        return []

# Обновленная функция для работы с DeepSeek API, поддержка изображений УБРАНА из-за несовместимости API
async def get_deepseek_response(api_key: str, system_prompt: str, history: list[dict]) -> str | None:
    """
    Отправляет запрос к DeepSeek API и возвращает ответ.
    Включает историю переписки.
    (Поддержка изображений временно удалена, т.к. стандартный endpoint ее не поддерживает)
    """
    url = "https://api.deepseek.com/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }

    # Убеждаемся, что история содержит только строки в 'content'
    # (Предыдущая логика могла добавлять списки с image_url, которые теперь не поддерживаются)
    valid_history = []
    for msg in history:
        if isinstance(msg.get("content"), str):
            valid_history.append(msg)
        else:
            # Пропускаем или логируем некорректные сообщения (например, с image_url)
            logging.warning(f"Пропущено сообщение с некорректным форматом content для DeepSeek: {msg}")

    messages = [{"role": "system", "content": system_prompt}] + valid_history

    payload = {
        "model": "deepseek-reasoner",
        "messages": messages,
        "max_tokens": 4000,
        "temperature": 0.7,
        "stream": False
    }

    connector = aiohttp.TCPConnector(family=socket.AF_INET)

    async with aiohttp.ClientSession(connector=connector) as session:
        try:
            logging.info(f"Отправка запроса к DeepSeek API. Последнее сообщение: {messages[-1] if messages else 'No messages'}")

            async with session.post(url, headers=headers, json=payload, timeout=120) as response:
                response_text = await response.text()

                if response.status == 200:
                    data = json.loads(response_text)
                    if data.get("choices") and len(data["choices"]) > 0:
                        content = data["choices"][0].get("message", {}).get("content")
                        if content:
                             logging.info("Успешно получен ответ от DeepSeek API")
                             return str(content)
                        else:
                             logging.warning("Ответ от DeepSeek API не содержит 'content'")
                             return "Извините, не смог получить ответ от DeepSeek."
                    else:
                        logging.warning("Ответ от DeepSeek API не содержит 'choices'")
                        return "Извините, структура ответа от DeepSeek неожиданная."
                elif response.status == 422: # Добавим обработку конкретно 422 ошибки
                    logging.error(f"Ошибка валидации данных (422) при запросе к DeepSeek: {response_text}")
                    return f"Ошибка API DeepSeek (422): Неверный формат запроса. Ответ: {response_text[:200]}..."
                else:
                    logging.error(f"Ошибка API DeepSeek: Статус {response.status}, Ответ: {response_text}")
                    return f"Ошибка при обращении к API DeepSeek: {response.status}"

        except asyncio.TimeoutError:
            logging.error("Ошибка API DeepSeek: Таймаут запроса")
            return "Извините, запрос к DeepSeek занял слишком много времени."
        except aiohttp.ClientError as e:
            logging.error(f"Ошибка сети при запросе к DeepSeek: {e}")
            return f"Сетевая ошибка при подключении к DeepSeek: {e}"
        except json.JSONDecodeError as e:
            logging.error(f"Ошибка декодирования JSON ответа DeepSeek: {e}. Ответ: {response_text}")
            return "Не удалось обработать ответ от DeepSeek (ошибка JSON)."
        except Exception as e:
            logging.exception(f"Неожиданная ошибка при запросе к DeepSeek: {e}")
            return f"Произошла непредвиденная ошибка: {e}"

# --- Новая функция stream_deepseek_response ---
async def stream_deepseek_response(api_key: str, system_prompt: str, history: list[dict]) -> typing.AsyncGenerator[str, None]:
    """
    Асинхронный генератор для получения ответа от DeepSeek Chat API в режиме стриминга.
    """
    messages = [{"role": "system", "content": system_prompt}] + history
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    url = "https://api.deepseek.com/v1/chat/completions"
    payload = {
        "model": "deepseek-chat",
        "messages": messages,
        "stream": True,
        "max_tokens": 4000
    }

    try:
        # Используем общий таймаут None для стриминга
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None)) as session:
            async with session.post(url, headers=headers, json=payload) as response:
                response.raise_for_status()  # Проверка на HTTP ошибки (4xx, 5xx)

                async for line in response.content:
                    line_str = line.decode('utf-8').strip()

                    if not line_str:
                        continue # Пропускаем пустые строки

                    if line_str.startswith("data: "):
                        payload_str = line_str[len("data: "):]
                    else:
                        # Иногда строки могут приходить без префикса, особенно в начале/конце
                        payload_str = line_str

                    if payload_str == "[DONE]":
                        break

                    try:
                        chunk = json.loads(payload_str)
                        # Проверка пути к контенту (может отличаться в зависимости от API)
                        choices = chunk.get('choices')
                        if choices and isinstance(choices, list) and len(choices) > 0:
                            delta = choices[0].get('delta')
                            if delta and isinstance(delta, dict):
                                delta_content = delta.get('content')
                                if delta_content and isinstance(delta_content, str):
                                     # logger.debug(f"Получен чанк: {delta_content!r}") # Раскомментировать для отладки
                                     yield delta_content
                                elif 'finish_reason' in choices[0] and choices[0]['finish_reason'] is not None:
                                    # Иногда finish_reason приходит в последнем чанке без content
                                    logger.info(f"Стриминг завершен с причиной: {choices[0]['finish_reason']}")
                                    break
                                # else: # Пропускаем чанки без content
                                #    logger.debug(f"Пропущен чанк без content: {chunk}")

                    except json.JSONDecodeError:
                        logger.error(f"Ошибка декодирования JSON из строки: {payload_str!r}")
                        continue # Продолжаем читать поток, если одна строка битая
                    except Exception as e:
                        logger.error(f"Неожиданная ошибка при обработке чанка JSON: {e}. Чанк: {chunk}")
                        continue # Продолжаем обработку потока

    except aiohttp.ClientResponseError as e:
        logger.error(f"Ошибка HTTP запроса к DeepSeek API: {e.status} {e.message}. URL: {url}")
        # Генератор просто завершится
    except aiohttp.ClientError as e:
        logger.error(f"Ошибка сети при запросе к DeepSeek API: {e}. URL: {url}")
        # Генератор просто завершится
    except Exception as e:
        logger.exception(f"Непредвиденная ошибка в stream_deepseek_response: {e}")
        # Генератор просто завершится

# --- Конец новой функции ---

# Обработчик команды /start
@dp.message(Command("start"))
async def start_handler(message: types.Message):
    # Создаем клавиатуру с кнопкой очистки истории
    builder = InlineKeyboardBuilder()
    builder.button(text="Очистить историю", callback_data="clear_history")
    
    await message.answer("Привет! Я медицинский ассистент. Задайте ваш вопрос.", reply_markup=builder.as_markup())

# Обработчик текстовых сообщений (основная логика)
@dp.message(F.text)
async def message_handler(message: types.Message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    text = message.text
    
    # Получаем зависимости из диспетчера
    db = dp.workflow_data.get('db')
    settings = dp.workflow_data.get('settings')
    
    if not db:
        logging.error("Не удалось получить соединение с БД")
        await message.answer("Произошла внутренняя ошибка, попробуйте позже")
        return
        
    if not settings:
        logging.error("Не удалось получить настройки")
        await message.answer("Произошла внутренняя ошибка, попробуйте позже")
        return
    
    # Показываем индикатор "печатает"
    await message.bot.send_chat_action(chat_id=user_id, action="typing")
    
    try:
        # Сохраняем сообщение пользователя
        await add_message_to_db(db, user_id, "user", text)
        logging.info(f"Сообщение от пользователя {user_id} сохранено")
        
        # Получаем историю сообщений
        history = await get_last_messages(db, user_id, limit=CONVERSATION_HISTORY_LIMIT)
        logging.info(f"Получена история сообщений для пользователя {user_id}, записей: {len(history)}")
        
        # Инициализация переменных для стриминга
        full_response_for_db = ""  # Для сохранения всего ответа в БД
        current_message_text = ""  # Текст для текущего редактируемого сообщения
        formatted_message_text = ""  # Отформатированный текст для отображения
        current_message_id = None  # ID текущего редактируемого сообщения
        last_edit_time = time.monotonic()  # Время последнего редактирования
        edit_interval = 1.0  # Интервал троттлинга (секунды)
        placeholder_sent = False  # Флаг, отправлен ли плейсхолдер
        use_formatting = False  # Отключено форматирование
        
        # Стрим ответа от DeepSeek API
        async for chunk in stream_deepseek_response(settings.DEEPSEEK_API_KEY, SYSTEM_PROMPT, history):
            # Отправка плейсхолдера (если еще не было)
            if not placeholder_sent:
                try:
                    # Начинаем с пустого плейсхолдера
                    placeholder_message = await message.answer("⏳")
                    current_message_id = placeholder_message.message_id
                    placeholder_sent = True
                    last_edit_time = time.monotonic()  # Сброс таймера после отправки
                except TelegramAPIError as e:
                    logging.error(f"Ошибка отправки плейсхолдера: {e}")
                    return  # Не можем начать, если плейсхолдер не отправить
            
            # Если current_message_id не установлен, прекратить обработку
            if not current_message_id:
                break
                
            # Добавление chunk к полному ответу для БД и накопление сырого текста
            full_response_for_db += chunk
            current_message_text += chunk
            
            # Форматируем текст для отображения в процессе стриминга
            if use_formatting:
                try:
                    # Применяем безопасное форматирование через улучшенную функцию
                    formatted_message_text = await apply_stream_formatting(current_message_text)
                except Exception as format_error:
                    logging.error(f"Ошибка форматирования в процессе стриминга: {format_error}")
                    formatted_message_text = current_message_text
                    use_formatting = False  # Отключаем форматирование при ошибке
            else:
                formatted_message_text = current_message_text
            
            # Проверка длины и разбиение при необходимости
            if len(formatted_message_text) > TELEGRAM_MAX_LENGTH:
                # Ищем место для "красивого" разрыва (по последнему \n перед лимитом)
                split_pos = current_message_text[:TELEGRAM_MAX_LENGTH].rfind('\n')
                if split_pos == -1:  # Если \n не найден, режем по лимиту
                    split_pos = TELEGRAM_MAX_LENGTH
                
                text_to_send_now = current_message_text[:split_pos]
                overflow_text = current_message_text[split_pos:].lstrip()  # Убираем пробелы в начале нового сообщения
                
                # Форматируем текст перед отправкой если включено форматирование
                if use_formatting:
                    try:
                        # Применяем безопасное форматирование
                        formatted_text_to_send = await apply_stream_markdown(text_to_send_now)
                        
                        # Проверяем валидность HTML перед отправкой
                        if is_stream_html_valid(formatted_text_to_send):
                            # Безопасно отправляем с форматированием
                            await message.bot.edit_message_text(
                                text=formatted_text_to_send,
                                chat_id=chat_id,
                                message_id=current_message_id,
                                parse_mode='HTML'
                            )
                        else:
                            # Если HTML невалиден, попробуем исправить
                            fixed_html = fix_streaming_html(formatted_text_to_send)
                            if is_stream_html_valid(fixed_html):
                                await message.bot.edit_message_text(
                                    text=fixed_html,
                                    chat_id=chat_id,
                                    message_id=current_message_id,
                                    parse_mode='HTML'
                                )
                            else:
                                # Если и после исправления невалиден, отправляем без форматирования
                                await message.bot.edit_message_text(
                                    text=strip_html_tags(text_to_send_now),
                                    chat_id=chat_id,
                                    message_id=current_message_id
                                )
                    except Exception as e:
                        logging.error(f"Ошибка форматирования перед разделением: {e}")
                        await message.bot.edit_message_text(
                            text=text_to_send_now,
                            chat_id=chat_id,
                            message_id=current_message_id
                        )
                else:
                    # Отправляем текст без форматирования
                    await message.bot.edit_message_text(
                        text=text_to_send_now,
                        chat_id=chat_id,
                        message_id=current_message_id
                    )
                
                # Отправка нового сообщения с оставшимся текстом
                try:
                    new_message = await message.bot.send_message(
                        chat_id=chat_id,
                        text=overflow_text + "..." if overflow_text else "...",
                        parse_mode=None  # Начальное сообщение всегда без форматирования
                    )
                    current_message_id = new_message.message_id
                    current_message_text = overflow_text  # Начинаем накапливать для нового сообщения
                    last_edit_time = time.monotonic()  # Сброс таймера
                except TelegramAPIError as e:
                    logging.error(f"Ошибка отправки следующей части сообщения: {e}")
                    current_message_id = None  # Обнуляем ID, чтобы прервать дальнейшие попытки редактирования
                    break  # Прерываем стрим
            
            # Троттлинг редактирования (если не было разбиения)
            elif time.monotonic() - last_edit_time > edit_interval:
                try:
                    # Обновляем промежуточное сообщение с применением форматирования при включенном режиме
                    if use_formatting:
                        # Проверяем валидность HTML перед отправкой
                        if is_stream_html_valid(formatted_message_text):
                            await message.bot.edit_message_text(
                                text=formatted_message_text + "...",
                                chat_id=chat_id,
                                message_id=current_message_id,
                                parse_mode='HTML'
                            )
                        else:
                            # Если HTML невалиден, пробуем исправить
                            fixed_html = fix_streaming_html(formatted_message_text)
                            if is_stream_html_valid(fixed_html):
                                await message.bot.edit_message_text(
                                    text=fixed_html + "...",
                                    chat_id=chat_id,
                                    message_id=current_message_id,
                                    parse_mode='HTML'
                                )
                            else:
                                # Если исправить не удалось, отправляем без форматирования
                                await message.bot.edit_message_text(
                                    text=current_message_text + "...",
                                    chat_id=chat_id,
                                    message_id=current_message_id
                                )
                                use_formatting = False  # Отключаем форматирование при ошибке
                    else:
                        # Используем обычный текст если форматирование отключено или HTML невалиден
                        await message.bot.edit_message_text(
                            text=current_message_text + "...",
                            chat_id=chat_id,
                            message_id=current_message_id
                        )
                    last_edit_time = time.monotonic()
                except TelegramRetryAfter as e:
                    logging.warning(f"Превышен лимит запросов, ожидание {e.retry_after}с")
                    await asyncio.sleep(e.retry_after)
                except TelegramAPIError as e:
                    logging.error(f"Ошибка редактирования сообщения: {e}")
                    # Пробуем отправить без форматирования
                    try:
                        await message.bot.edit_message_text(
                            text=current_message_text + "...",
                            chat_id=chat_id,
                            message_id=current_message_id
                        )
                        last_edit_time = time.monotonic()
                        use_formatting = False  # Отключаем форматирование при ошибке
                    except TelegramAPIError as plain_error:
                        logging.error(f"Ошибка редактирования без форматирования: {plain_error}")
                        # Если сообщение удалено, дальнейшее редактирование бессмысленно
                        if "message to edit not found" in str(plain_error).lower() or "message can't be edited" in str(plain_error).lower():
                            current_message_id = None  # Обнуляем ID
                            break  # Прерываем цикл
        
        # После цикла (стрим завершен) - сохраняем полный ответ в БД без дублирования сообщения
        if placeholder_sent:
            try:
                final_text = full_response_for_db or "Не удалось сгенерировать ответ."
                await add_message_to_db(db, user_id, "assistant", final_text)
                logging.info(f"Ответ ассистента для пользователя {user_id} сохранен")
            except Exception as e:
                logging.error(f"Ошибка сохранения ответа: {e}")
    
    except Exception as e:
        logging.exception(f"Критическая ошибка в обработчике сообщений: {e}")
        await message.answer("Произошла ошибка при обработке вашего запроса, попробуйте позже или обратитесь в поддержку.")

# Функция для простого форматирования текста в процессе стриминга
async def apply_stream_formatting(text: str) -> str:
    """Преобразует текст модели в Telegram Markdown во время стриминга."""
    return format_markdown(text)

def format_markdown(text: str) -> str:
    """Преобразует Markdown из модели в Telegram Markdown формат."""
    # Сохраняем блоки кода
    code_blocks = []
    def repl_block(m):
        code_blocks.append(m.group(1))
        return f"<<BLOCK{len(code_blocks)-1}>>"
    text = re.sub(r'```([\s\S]+?)```', repl_block, text)

    # Сохраняем inline код
    inline_codes = []
    def repl_inline(m):
        inline_codes.append(m.group(1))
        return f"<<INLINE{len(inline_codes)-1}>>"
    text = re.sub(r'`([^`\n]+?)`', repl_inline, text)

    # Заголовки # -> __**text**__
    def repl_header(m):
        return f"__**{m.group(2).strip()}**__"
    text = re.sub(r'^(#{1,6})\s+(.+)$', repl_header, text, flags=re.MULTILINE)

    # Жирный ** оставляем
    # Курсив *text* или _text_ -> __text__
    text = re.sub(r'\*(?!\*)([^*\n]+?)\*(?!\*)', r'__\1__', text)
    text = re.sub(r'_(?!_)([^_\n]+?)_(?!_)', r'__\1__', text)

    # Восстановление inline кода
    for i, code in enumerate(inline_codes):
        text = text.replace(f"<<INLINE{i}>>", f"`{code}`")

    # Восстановление блоков кода
    for i, code in enumerate(code_blocks):
        text = text.replace(f"<<BLOCK{i}>>", f"```{code}```")

    return text

def is_stream_html_valid(text: str) -> bool:
    """
    Проверяет, что HTML в тексте валиден для потокового обновления сообщения.
    Проверяет баланс основных тегов, которые могут использоваться при форматировании.
    """
    try:
        if not text or '<' not in text:
            return True
        
        # Проверяем наличие экранированных HTML-сущностей, которые могут вызвать проблемы
        problematic_entities = ['&lt;b&gt;', '&lt;i&gt;', '&lt;u&gt;', '&lt;s&gt;', '&lt;code&gt;', '&lt;pre&gt;']
        for entity in problematic_entities:
            if entity in text.lower():
                return False
                
        # Проверяем наличие одиночных неэкранированных угловых скобок
        # Паттерны, которые могут указывать на одиночные скобки не в HTML-тегах
        if re.search(r'<(?![a-zA-Z/])|(?<![a-zA-Z\s/"\'`])>', text):
            return False
        
        # Проверяем баланс основных HTML тегов, которые могут быть в форматировании
        tags_to_check = ['b', 'i', 'u', 's', 'code', 'pre']
        
        for tag in tags_to_check:
            open_count = text.count(f'<{tag}>')
            close_count = text.count(f'</{tag}>')
            
            if open_count != close_count:
                return False
        
        # Дополнительная проверка на баланс угловых скобок
        open_brackets = text.count('<')
        close_brackets = text.count('>')
        
        if open_brackets != close_brackets:
            return False
        
        # Проверка на наличие незавершенных тегов в конце строки
        if text.rstrip().endswith('<'):
            return False
            
        # Проверка на незавершенные открывающие теги (например <b без >)
        if re.search(r'<[a-zA-Z][a-zA-Z0-9]*(?![^<>]*>)', text):
            return False
            
        # Проверка на вложенность тегов и их правильную последовательность закрытия
        stack = []
        i = 0
        while i < len(text):
            if text[i:i+1] == '<':
                if i + 1 < len(text) and text[i+1:i+2] == '/':
                    # Закрывающий тег
                    end = text.find('>', i)
                    if end == -1:
                        return False  # Незакрытая угловая скобка
                    
                    tag_name = text[i+2:end].strip().lower()
                    if not stack or stack[-1] != tag_name:
                        return False  # Несбалансированный тег
                    
                    stack.pop()
                    i = end + 1
                else:
                    # Открывающий тег
                    end = text.find('>', i)
                    if end == -1:
                        return False  # Незакрытая угловая скобка
                    
                    tag_content = text[i+1:end].strip().lower()
                    # Пропускаем самозакрывающиеся теги или нестандартные теги
                    if ' ' in tag_content or tag_content.endswith('/'):
                        i = end + 1
                        continue
                    
                    if tag_content in tags_to_check:
                        stack.append(tag_content)
                    
                    i = end + 1
            else:
                i += 1
        
        # Проверяем, что все открытые теги закрыты
        if stack:
            return False
            
        # Проверка на неправильную последовательность вложенных тегов (например <b><i></b></i>)
        for tag1 in tags_to_check:
            for tag2 in tags_to_check:
                if tag1 != tag2:
                    pattern = f'<{tag1}>[^<]*<{tag2}>[^<]*</{tag1}>[^<]*</{tag2}>'
                    if re.search(pattern, text):
                        return False
        
        return True
    except Exception as e:
        logging.error(f"Ошибка в is_stream_html_valid: {e}")
        return False

def fix_streaming_html(text: str) -> str:
    """
    Исправляет HTML для потоковой передачи, обеспечивая правильную балансировку тегов.
    Если HTML невозможно исправить, возвращает текст без HTML-тегов.
    """
    if not text or '<' not in text:
        return text
    
    # Предварительная обработка для угловых скобок, не являющихся HTML-тегами
    # Экранируем одиночные '<' и '>', которые не соответствуют HTML-тегам
    processed_text = ""
    i = 0
    while i < len(text):
        if text[i] == '<':
            # Проверяем, является ли это началом валидного HTML-тега
            tag_match = re.match(r'</?[a-zA-Z][a-zA-Z0-9]*(?:\s+[^>]*)?>|</[a-zA-Z][a-zA-Z0-9]*>', text[i:i+100])
            if not tag_match:
                # Это не HTML-тег, экранируем
                processed_text += '&lt;'
            else:
                # Это похоже на HTML-тег, сохраняем
                tag_content = tag_match.group(0)
                processed_text += tag_content
                i += len(tag_content) - 1  # -1 потому что i будет увеличен ниже
        elif text[i] == '>':
            # Проверяем, не является ли это частью тега
            if i > 0 and text[i-1] != '>':
                # Проверяем, не является ли это концом тега
                prev_open_bracket = text[:i].rfind('<')
                if prev_open_bracket == -1 or '>' in text[prev_open_bracket:i]:
                    # Это не часть HTML-тега, экранируем
                    processed_text += '&gt;'
                else:
                    processed_text += '>'
            else:
                processed_text += '>'
        else:
            processed_text += text[i]
        i += 1
    
    text = processed_text
    
    # Список тегов для проверки
    tags_to_check = ['b', 'i', 'u', 's', 'code', 'pre']
    
    # Проверяем баланс основных тегов
    for tag in tags_to_check:
        open_count = text.count(f'<{tag}>')
        close_count = text.count(f'</{tag}>')
        
        # Если теги не сбалансированы, исправляем
        if open_count > close_count:
            # Добавляем закрывающие теги
            text += f'</{tag}>' * (open_count - close_count)
        elif close_count > open_count:
            # Удаляем лишние закрывающие теги
            pattern = f'</{tag}>'
            excess = close_count - open_count
            
            for _ in range(excess):
                pos = text.rfind(pattern)
                if pos >= 0:
                    text = text[:pos] + text[pos + len(pattern):]
    
    # Обработка незакрытых тегов (открытые, но не закрытые)
    stack = []
    result = []
    i = 0
    
    while i < len(text):
        if text[i:i+1] == '<':
            if i + 1 < len(text) and text[i+1:i+2] == '/':
                # Закрывающий тег
                end = text.find('>', i)
                if end == -1:
                    # Незакрытая угловая скобка, экранируем
                    result.append('&lt;/')
                    i += 2
                    continue
                
                tag_name = text[i+2:end].strip().lower()
                if not stack or stack[-1] != tag_name:
                    # Несбалансированный тег - пропускаем
                    i = end + 1
                    continue
                
                # Правильный закрывающий тег
                stack.pop()
                result.append(text[i:end+1])
                i = end + 1
            else:
                # Открывающий тег
                end = text.find('>', i)
                if end == -1:
                    # Незакрытая угловая скобка, экранируем
                    result.append('&lt;')
                    i += 1
                    continue
                
                tag_content = text[i+1:end].strip().lower()
                # Пропускаем самозакрывающиеся теги
                if ' ' in tag_content or tag_content.endswith('/'):
                    result.append(text[i:end+1])
                    i = end + 1
                    continue
                
                if tag_content in tags_to_check:
                    stack.append(tag_content)
                
                result.append(text[i:end+1])
                i = end + 1
        else:
            result.append(text[i])
            i += 1
    
    # Закрываем все оставшиеся открытые теги
    for tag in reversed(stack):
        result.append(f'</{tag}>')
    
    # Проверяем валидность исправленного HTML
    final_text = ''.join(result)
    
    # Финальная санитизация для Telegram
    try:
        final_text = sanitize_telegram_html(final_text)
    except Exception as e:
        logging.error(f"Ошибка при финальной санитизации HTML: {e}")
    
    if is_stream_html_valid(final_text):
        return final_text
    
    # Если всё ещё не валидно, удаляем все HTML теги
    return strip_html_tags(text)

# Обработчик завершения работы
# Исправляем сигнатуру, чтобы она принимала **kwargs, как передает aiogram
async def on_shutdown(**kwargs):
    # Получаем db и settings из workflow_data, переданных как kwargs
    db = kwargs.get('db')
    settings = kwargs.get('settings')

    if db and settings:
        if not settings.USE_SQLITE:
            try:
                # db здесь это пул соединений asyncpg
                await db.close()
                logging.info("Пул соединений PostgreSQL успешно закрыт")
            except Exception as e:
                logging.error(f"Ошибка при закрытии пула соединений PostgreSQL: {e}")
        else:
            # Для SQLite пул соединений не используется в том же виде,
            # закрывать нечего (соединения открываются/закрываются в _add_message/_get_messages)
            logging.info("Используется SQLite, закрытие пула соединений не требуется.")
    else:
        logging.warning("Не удалось получить 'db' или 'settings' из workflow_data при завершении работы.")

# Основная функция
async def main():
    try:
        logging.info(f"Запуск приложения с настройками базы данных: {settings.DATABASE_URL}")
        
        db = None
        
        # Выбор типа БД на основе URL
        if settings.USE_SQLITE:
            logging.info("Используется SQLite для хранения данных")
            db = await init_sqlite_db(settings.DATABASE_URL)
        else:
            logging.info("Используется PostgreSQL для хранения данных")
            # Попытка подключения с таймаутом и обработкой ошибок
            try:
                db = await asyncio.wait_for(
                    asyncpg.create_pool(dsn=settings.DATABASE_URL, timeout=10.0),
                    timeout=15.0
                )
                if not db:
                    logging.error("Не удалось создать пул соединений")
                    sys.exit(1)
                    
                logging.info("Подключение к базе данных успешно установлено")
                await init_db(db)
                
            except asyncio.TimeoutError:
                logging.error("Превышен таймаут подключения к базе данных")
                sys.exit(1)
            except socket.gaierror as e:
                logging.error(f"Ошибка DNS при подключении к базе данных: {e}. "
                            f"Проверьте правильность хоста в DATABASE_URL и доступность DNS.")
                sys.exit(1)
            except asyncpg.PostgresError as e:
                logging.error(f"Ошибка PostgreSQL при подключении: {e}")
                sys.exit(1)
        
        # Сохраняем зависимости в workflow_data
        dp.workflow_data['db'] = db
        dp.workflow_data['settings'] = settings
        logging.info("Зависимости успешно сохранены в dispatcher")
        
        # Регистрируем обработчики callback и команд декораторами
        # dp.callback_query.register(clear_history_callback, F.data == "clear_history")
        # Больше не требуется, декораторы уже используются
        logging.info("Обработчики успешно зарегистрированы")
    
    except asyncpg.PostgresError as e:
        logging.error(f"Ошибка PostgreSQL при подключении к БД: {e}")
        sys.exit(1)
    except Exception as e:
        logging.exception(f"Непредвиденная ошибка при инициализации: {e}")
        sys.exit(1)
    
    # Регистрируем обработчик shutdown
    dp.shutdown.register(on_shutdown)
    
    try:
        # Запускаем бота
        logging.info("Запуск бота...")
        await dp.start_polling(bot)
    except Exception as e:
        logging.exception(f"Критическая ошибка при запуске бота: {e}")
        sys.exit(1)

# Обновляем определения обработчиков для работы с зависимостями
# Обработчик кнопки очистки истории
@dp.callback_query(F.data == "clear_history")
async def clear_history_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    db = callback.bot.dispatcher.workflow_data.get('db')
    
    if not db:
        logging.error("Не удалось получить соединение с БД при очистке истории")
        await callback.answer("Ошибка при очистке истории", show_alert=True)
        return
    
    try:
        if settings.USE_SQLITE:
            # SQLite
            def _clear_history():
                db_path = db
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute("DELETE FROM conversations WHERE user_id = ?", (user_id,))
                rows_deleted = cursor.rowcount
                conn.commit()
                conn.close()
                return rows_deleted
                
            rows_deleted = await asyncio.to_thread(_clear_history)
            logging.info(f"SQLite: Очищена история пользователя {user_id}: удалено {rows_deleted} записей")
        else:
            # PostgreSQL
            async with db.acquire() as connection:
                result = await connection.execute("DELETE FROM conversations WHERE user_id = $1", user_id)
                logging.info(f"PostgreSQL: Очищена история пользователя {user_id}: {result}")
        
        await callback.answer("История очищена!", show_alert=False)
        await callback.message.answer("История диалога очищена.")
    except Exception as e:
        logging.error(f"Неожиданная ошибка при очистке истории: {e}")
        await callback.answer("Произошла ошибка", show_alert=True)

# Обработчик команды /clear
@dp.message(Command("clear"))
async def clear_command_handler(message: types.Message):
    user_id = message.from_user.id
    db = message.bot.dispatcher.workflow_data.get('db')
    
    if not db:
        logging.error("Не удалось получить соединение с БД при обработке команды /clear")
        await message.answer("Произошла внутренняя ошибка, попробуйте позже")
        return
    
    try:
        if settings.USE_SQLITE:
            # SQLite
            def _clear_history():
                db_path = db
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute("DELETE FROM conversations WHERE user_id = ?", (user_id,))
                rows_deleted = cursor.rowcount
                conn.commit()
                conn.close()
                return rows_deleted
                
            rows_deleted = await asyncio.to_thread(_clear_history)
            logging.info(f"SQLite: Очищена история пользователя {user_id}: удалено {rows_deleted} записей")
        else:
            # PostgreSQL
            async with db.acquire() as connection:
                result = await connection.execute("DELETE FROM conversations WHERE user_id = $1", user_id)
                logging.info(f"PostgreSQL: Очищена история пользователя {user_id}: {result}")
        
        await message.answer("История очищена!")
    except Exception as e:
        logging.error(f"Неожиданная ошибка при очистке истории: {e}")
        await message.answer("Произошла ошибка при очистке истории")

def log_html_diagnostic(text: str, title: str = "HTML диагностика"):
    """
    Выполняет диагностику HTML и логирует информацию о проблемах конструкциях
    """
    if not text or ('<' not in text and '>' not in text):
        return
        
    # Общие подсчеты
    left_brackets = text.count('<')
    right_brackets = text.count('>')
    bracket_diff = left_brackets - right_brackets
    
    logging.debug(f"{title}: Баланс угловых скобок: {left_brackets} '<' vs {right_brackets} '>', разница: {bracket_diff}")
    
    # Проверка незавершенных тегов
    if bracket_diff != 0:
        # Проверяем наличие неполных тегов
        partial_tags = re.findall(r'<([a-z0-9]+)(?![^<>]*>)', text, flags=re.IGNORECASE)
        if partial_tags:
            logging.debug(f"{title}: Найдены незавершенные теги: {', '.join(partial_tags)}")
        
        # Проверяем наличие незакрытых тегов в конце текста
        if text.rstrip().endswith('<'):
            logging.debug(f"{title}: Текст заканчивается открытой угловой скобкой '<'")
    
    # Проверяем баланс основных тегов
    supported_tags = ['b', 'i', 'u', 's', 'code', 'pre', 'a', 'tg-spoiler']
    for tag in supported_tags:
        open_pattern = re.compile(f'<{tag}(?:\\s+[^>]*)?>', re.IGNORECASE)
        open_tags = open_pattern.findall(text)
        open_count = len(open_tags)
        
        close_pattern = re.compile(f'</{tag}>', re.IGNORECASE)
        close_tags = close_pattern.findall(text)
        close_count = len(close_tags)
        
        if open_count != close_count:
            logging.debug(f"{title}: Дисбаланс тега '{tag}': {open_count} открыт., {close_count} закрыт., разница: {open_count - close_count}")
            
            # Дополнительно логируем позиции проблемных тегов
            if open_count > 0 or close_count > 0:
                # Находим позиции всех тегов
                open_positions = [m.start() for m in open_pattern.finditer(text)]
                close_positions = [m.start() for m in close_pattern.finditer(text)]
                
                logging.debug(f"{title}: Позиции открывающих тегов '{tag}': {open_positions}")
                logging.debug(f"{title}: Позиции закрывающих тегов '{tag}': {close_positions}")

# И обновляем обработчик фото, чтобы форматирование применялось только к финальному ответу
@dp.message(F.photo)
async def photo_handler(message: types.Message, bot: Bot):
    """Обрабатывает сообщения с фотографиями."""
    user_id = message.from_user.id
    file_id = message.photo[-1].file_id  # Берем самое большое разрешение
    caption = message.caption or ""
    
    # Получаем зависимости
    db = dp.workflow_data.get('db')
    settings_local = dp.workflow_data.get('settings')
    
    if not db or not settings_local:
        logging.error("Не удалось получить БД или настройки при обработке фото")
        await message.reply("Произошла внутренняя ошибка, попробуйте позже")
        return
    
    # Если есть подпись к фото, обрабатываем её как текстовый запрос
    if caption:
        try:
            await message.bot.send_chat_action(chat_id=user_id, action="typing")
            
            # Сохраняем сообщение пользователя с подписью
            caption_with_photo = f"[Фото] {caption}"
            await add_message_to_db(db, user_id, "user", caption_with_photo)
            logging.info(f"Подпись к фото от пользователя {user_id} сохранена")

            # Получаем историю сообщений (включая только что добавленную подпись)
            history = await get_last_messages(db, user_id, limit=CONVERSATION_HISTORY_LIMIT)
            logging.info(f"Получена история сообщений для пользователя {user_id} (с подписью), записей: {len(history)}")

            # Получаем ответ от DeepSeek API (только на основе текста)
            response_text = await get_deepseek_response(settings_local.DEEPSEEK_API_KEY, SYSTEM_PROMPT, history)

            if response_text:
                # Сохраняем ответ в БД и отправляем без форматирования
                await add_message_to_db(db, user_id, "assistant", response_text)
                logging.info(f"Ответ ассистента сохранен")
                await message.answer(response_text)
                return
            else:
                logging.warning(f"Не удалось получить ответ от DeepSeek API для пользователя {user_id}")
                await message.reply("Извините, произошла ошибка при обработке вашей подписи к фото.")

        except Exception as e:
            logging.exception(f"Ошибка при обработке подписи к фото для user_id={user_id}: {e}")
            await message.reply("Произошла ошибка при обработке подписи к фото.")
    else:
        # Если подписи нет, просто сообщаем что получили фото
        await message.reply("Я получил ваше фото. Чтобы получить ответ от ассистента, пожалуйста, добавьте подпись с вопросом или описанием.")

# НОВЫЙ: Обработчик сообщений с документами
@dp.message(F.document)
async def document_handler(message: types.Message, bot: Bot):
    """Обрабатывает сообщения с документами."""
    user_id = message.from_user.id
    document = message.document
    file_id = document.file_id
    file_name = document.file_name
    mime_type = document.mime_type

    logger.info(f"Получен документ от user_id={user_id}: {file_name} (type: {mime_type})")

    # Пока просто отвечаем, что обработка документов в разработке
    # TODO: Реализовать скачивание и обработку документов (например, извлечение текста)
    await message.reply(f"Получил документ '{file_name}'. Обработка документов пока не реализована.")

    # Пример скачивания (закомментировано, т.к. пока не используется):
    # try:
    #     file_info = await bot.get_file(file_id)
    #     file_path = file_info.file_path
    #     file_content = await bot.download_file(file_path)
    #     # Дальнейшая обработка file_content (например, чтение текста)
    #     file_content.close()
    #     await message.reply(f"Скачал {file_name}, но пока не знаю, что с ним делать.")
    # except Exception as e:
    #     logger.exception(f"Ошибка при скачивании документа {file_name}: {e}")
    #     await message.reply(f"Не удалось скачать документ {file_name}.")

# Добавление вспомогательной функции для проверки HTML
def strip_html_tags(text: str) -> str:
    """Удаляет HTML-теги и экранированные HTML-теги из текста."""
    try:
        # Удаляем обычные HTML-теги
        clean_text = re.sub(r'<[^>]*>', '', text)
        
        # Удаляем экранированные HTML-теги (&lt;tag&gt;)
        clean_text = re.sub(r'&lt;[^&]*&gt;', '', clean_text)
        
        # Удаляем оставшиеся незакрытые теги
        clean_text = re.sub(r'<[^>]*$', '', clean_text)
        clean_text = re.sub(r'&lt;[^&]*$', '', clean_text)
        
        # Удаляем возможные HTML-сущности
        html_entities = {
            '&lt;': '<',
            '&gt;': '>',
            '&quot;': '"',
            '&apos;': "'",
            '&amp;': '&'
        }
        for entity, char in html_entities.items():
            clean_text = clean_text.replace(entity, char)
            
        return clean_text
    except Exception as e:
        logging.error(f"Ошибка при удалении HTML-тегов: {e}")
        return text  # Возвращаем исходный текст без изменений

def is_valid_html(html_text: str) -> bool:
    """Проверка на корректность HTML для отправки в Telegram.
    
    Проверяет, что все открытые теги правильно закрыты и поддерживаются Telegram.
    """
    try:
        # Если нет HTML-тегов вообще, считаем валидным
        if '<' not in html_text and '>' not in html_text:
            return True
            
        # Проверяем базовые теги Telegram
        tags_to_check = ['b', 'i', 'u', 's', 'code', 'pre', 'tg-spoiler']
        
        # Проверка простых тегов
        for tag in tags_to_check:
            # Ищем все теги заданного типа
            open_tags = re.findall(f'<{tag}(?:\\s+[^>]*)?>', html_text, re.IGNORECASE)
            close_tags = re.findall(f'</{tag}>', html_text, re.IGNORECASE)
            
            if len(open_tags) != len(close_tags):
                logging.warning(f"Неравное количество открывающих и закрывающих тегов {tag}: {len(open_tags)} vs {len(close_tags)}")
                return False
                
        # Проверка тега <a> с атрибутами
        a_open_tags = re.findall(r'<a\s+[^>]*>', html_text)
        a_close_tags = re.findall(r'</a>', html_text)
        if len(a_open_tags) != len(a_close_tags):
            logging.warning(f"Неравное количество тегов <a>: {len(a_open_tags)} vs {len(a_close_tags)}")
            return False
            
        # Проверка на незакрытые угловые скобки
        if html_text.count('<') != html_text.count('>'):
            logging.warning("Несбалансированные угловые скобки в HTML")
            return False
            
        # В отличие от предыдущей версии, НЕ считаем экранированные теги ошибкой
        # Убираем проверку на &lt; и &gt;
            
        # Проверка на вложенные теги
        stack = []
        tag_pattern = re.compile(r'<(/?)([a-z0-9]+)(?:\s+[^>]*)?>', re.IGNORECASE)
        
        for match in tag_pattern.finditer(html_text):
            is_closing = match.group(1) == '/'
            tag_name = match.group(2).lower()
            
            if tag_name not in tags_to_check and tag_name != 'a':
                continue  # Пропускаем неподдерживаемые теги
                
            if not is_closing:
                stack.append(tag_name)
            else:
                if not stack or stack[-1] != tag_name:
                    logging.warning(f"Неправильная вложенность тегов: закрывающий {tag_name}, но ожидался другой")
                    return False
                stack.pop()
                
        if stack:
            logging.warning(f"Остались незакрытые теги: {stack}")
            return False
            
        return True
    except Exception as e:
        logging.error(f"Ошибка при проверке HTML: {e}")
        return False

def sanitize_telegram_html(text: str) -> str:
    """
    Финальная подготовка HTML для отправки в Telegram API.
    Удаляет или исправляет конструкции, которые могут вызвать ошибки.
    """
    if not text:
        return ""
        
    # Проверка на наличие HTML тегов
    if '<' not in text and '>' not in text:
        return text
        
    # Известные проблемные паттерны, которые нужно заменить
    replacements = [
        # Синтаксис языков программирования
        (r'<([a-z0-9_-]+)>\s*=', r'&lt;\1&gt; ='),  # Заменяем <variable> = value
        (r'(\s|^|[^\w])<([a-z0-9_-]+)>', r'\1&lt;\2&gt;'),  # Экранируем <переменные> в тексте
        (r'(\s|^)(<|>|<=|>=)(\s|$)', lambda m: m.group(1) + ('&lt;' if m.group(2)[0] == '<' else '&gt;') + m.group(3)),  # Операторы сравнения
        
        # Математические выражения
        (r'(\d+)\s*<\s*(\d+)', r'\1 &lt; \2'),  # Числовые сравнения 5 < 10
        (r'(\d+)\s*>\s*(\d+)', r'\1 &gt; \2'),  # Числовые сравнения 10 > 5
        
        # HTML-комментарии
        (r'<!--.*?-->', ''),  # Удаляем HTML комментарии
        
        # URL-подобные строки с параметрами
        (r'(https?://[^\s<]+)[?&][^=]+=([^&\s<>]+)(&[^=]+=([^&\s<>]+))*', lambda m: m.group(0).replace('&', '&amp;')),

        # Вложенные теги одного типа (например <b><b>text</b></b>)
        (r'<([bius])>(\s*)<\1>(.*?)</\1>(\s*)</\1>', r'<\1>\2\3\4</\1>'),
        
        # Специальные случаи для Telegram
        (r'<(\w+)[^>]*\s+class="[^"]*"[^>]*>', r'<\1>'),  # Удаляем атрибуты class
        (r'<(\w+)[^>]*\s+style="[^"]*"[^>]*>', r'<\1>'),  # Удаляем атрибуты style
        
        # Экранирование символов в коде
        (r'<code>(.*?)</code>', lambda m: f'<code>{_escape_code_content(m.group(1))}</code>'),
        (r'<pre>(.*?)</pre>', lambda m: f'<pre>{_escape_code_content(m.group(1))}</pre>'),
        
        # Экранирование одиночных угловых скобок, которые не являются частью HTML-тегов
        (r'<(?![a-zA-Z/])', r'&lt;'),  # Экранируем < если за ним не идет буква или /
        (r'(?<![a-zA-Z/"\'\s])>', r'&gt;'),  # Экранируем > если перед ним не буква, /, кавычка или пробел
    ]
    
    # Применяем замены
    result = text
    for pattern, replacement in replacements:
        result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)

    # Проверка на несбалансированные угловые скобки
    if result.count('<') != result.count('>'):
        # Находим и экранируем все одиночные < и >
        processed = []
        stack_count = 0
        
        for char in result:
            if char == '<':
                stack_count += 1
                processed.append(char)
            elif char == '>':
                stack_count -= 1
                processed.append(char)
                
                # Если стек стал отрицательным, у нас есть лишний >
                if stack_count < 0:
                    # Заменяем последний добавленный > на &gt;
                    processed[-1] = '&gt;'
                    stack_count = 0
            else:
                processed.append(char)
        
        # Если остались незакрытые <, заменяем их на &lt;
        if stack_count > 0:
            processed_text = ''.join(processed)
            last_open_pos = processed_text.rfind('<')
            
            # Если это незакрытый тег в конце, экранируем его
            if last_open_pos >= 0 and '>' not in processed_text[last_open_pos:]:
                processed[-len(processed_text) + last_open_pos] = '&lt;'
        
        result = ''.join(processed)
    
    # Проверяем разрешенные теги Telegram
    allowed_tags = ['b', 'i', 'u', 's', 'code', 'pre', 'a', 'tg-spoiler']
    
    # Удаляем все теги, которые не поддерживаются Telegram
    tags_pattern = re.compile(r'</?([a-z0-9-]+)(?:\s+[^>]*)?>', re.IGNORECASE)
    
    def replace_tag(match):
        tag = match.group(1).lower()
        full_tag = match.group(0)
        
        # Если тег a, проверяем корректность атрибута href
        if tag == 'a' and 'href=' in full_tag:
            # Если атрибут href корректен, оставляем тег
            href_match = re.search(r'href=[\'"](https?://[^\'"]+)[\'"]', full_tag)
            if href_match:
                # Корректный URL, возвращаем тег как есть
                return full_tag
        
        # Если тег в списке разрешенных и это не <a> с атрибутами или это </a>, оставляем его
        if tag in allowed_tags and (tag != 'a' or not 'href=' in full_tag or full_tag.startswith('</a')):
            return full_tag
            
        # Иначе экранируем
        return f'&lt;{tag}&gt;'
    
    # Применяем правила фильтрации тегов
    result = tags_pattern.sub(replace_tag, result)
    
    return result

# Вспомогательная функция для экранирования содержимого кода
def _escape_code_content(text: str) -> str:
    """Экранирует HTML внутри тегов code и pre."""
    # Заменяем < и > на их экранированные версии, но только если они не являются
    # частью тегов, которые уже экранированы (например, &lt; и &gt;)
    text = re.sub(r'<(?!/?(?:b|i|u|s|code|pre|a)>)', r'&lt;', text)
    text = re.sub(r'(?<!</?(?:b|i|u|s|code|pre|a))>', r'&gt;', text)
    return text

# И обновим safe_send_html_message, чтобы использовать эту функцию перед отправкой
async def safe_send_html_message(bot, chat_id, message_id, text, is_edit=True):
    """
    Безопасно отправляет или редактирует сообщение с HTML-форматированием.
    Проверяет и исправляет HTML перед отправкой, используя подробную диагностику.
    """
    original_text = text
    
    # Применяем диагностику перед любыми изменениями
    log_html_diagnostic(text, "Исходный HTML перед отправкой")
    
    # Проверяем валидность HTML и исправляем при необходимости
    if not is_valid_html(text):
        logging.warning("HTML невалиден перед отправкой в Telegram, пробуем исправить")
        text = fix_streaming_html(text)

        # Повторная диагностика после первой попытки исправления
        log_html_diagnostic(text, "HTML после первого исправления")
        
        if not is_valid_html(text):
            logging.warning("HTML все еще невалиден, пробуем более агрессивное исправление")
            
            # Повторное исправление с другими параметрами
            text = fix_streaming_html(text)
            log_html_diagnostic(text, "HTML после второго исправления")
            
            if not is_valid_html(text):
                logging.warning("HTML все еще невалиден, отправляем без форматирования")
                text = strip_html_tags(original_text)
                parse_mode = None  # Без форматирования
            else:
                # Финальная санитизация HTML
                text = sanitize_telegram_html(text)
                parse_mode = 'HTML'
        else:
            # Финальная санитизация HTML
            text = sanitize_telegram_html(text)
            parse_mode = 'HTML'
    else:
        # Финальная санитизация HTML даже для валидного HTML
        text = sanitize_telegram_html(text)
        parse_mode = 'HTML'
    
    try:
        # Отправляем или редактируем сообщение
        if is_edit and message_id:
            return await bot.edit_message_text(
                text=text,
                chat_id=chat_id,
                message_id=message_id,
                parse_mode=parse_mode
            )
        else:
            return await bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=parse_mode
            )
    except TelegramAPIError as e:
        logging.error(f"Ошибка при отправке HTML: {e}")
        
        # Подробная диагностика ошибки
        error_text = str(e)
        if "can't parse entities" in error_text.lower():
            # Пытаемся найти позицию проблемного тега из сообщения об ошибке
            position_match = re.search(r'position (\d+)', error_text)
            if position_match:
                position = int(position_match.group(1))
                logging.error(f"Проблемная позиция в HTML: {position}")
                # Логируем окружение проблемного места
                context_start = max(0, position - 20)
                context_end = min(len(text), position + 20)
                context = text[context_start:context_end]
                logging.error(f"Контекст ошибки: ...{context}...")
        
        # Пробуем отправить без HTML
        try:
            stripped_text = strip_html_tags(original_text)
            if is_edit and message_id:
                return await bot.edit_message_text(
                    text=stripped_text,
                    chat_id=chat_id,
                    message_id=message_id
                )
            else:
                return await bot.send_message(
                    chat_id=chat_id,
                    text=stripped_text
                )
        except TelegramAPIError as plain_error:
            logging.error(f"Ошибка при отправке без форматирования: {plain_error}")
            return None

# Запуск бота
if __name__ == "__main__":
    asyncio.run(main())

# Новая функция для преобразования текста модели в Telegram Markdown
async def apply_stream_markdown(text: str) -> str:
    import re
    # Удаляем HTML-теги
    text = re.sub(r'<[^>]+>', '', text)
    # Кодовые блоки
    code_blocks = []
    def repl_block(m):
        code_blocks.append(m.group(1))
        return f"<<<BLOCK{len(code_blocks)-1}>>>"
    text = re.sub(r'```([\s\S]+?)```', repl_block, text)
    # Встроенный (inline) код
    inline_codes = []
    def repl_inline(m):
        inline_codes.append(m.group(1))
        return f"<<<INLINE{len(inline_codes)-1}>>>"
    text = re.sub(r'`([^`\n]+?)`', repl_inline, text)
    # Заголовки # до ######
    def repl_header(m):
        return f"__**{m.group(2).strip()}**__"
    text = re.sub(r'^(#{1,6})\s+(.+)$', repl_header, text, flags=re.MULTILINE)
    # Жирный **text**
    text = re.sub(r'\*\*(.+?)\*\*', r'**\1**', text)
    # Курсив __text__ или *text* или _text_
    text = re.sub(r'__(.+?)__', r'__\1__', text)
    text = re.sub(r'\*(.+?)\*', r'__\1__', text)
    text = re.sub(r'_(.+?)_', r'__\1__', text)
    # Восстановление inline-кода
    for i, code in enumerate(inline_codes):
        text = text.replace(f'<<<INLINE{i}>>>', f'`{code}`')
    # Восстановление блоков кода
    for i, code in enumerate(code_blocks):
        text = text.replace(f'<<<BLOCK{i}>>>', f'```{code}```')
    return text
