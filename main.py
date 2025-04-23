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
import html
import re
import base64
import io
import typing
import time
from aiogram.exceptions import TelegramAPIError, TelegramRetryAfter
from aiogram.utils.text_decorations import html_decoration

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

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

# Telegram Formatting Instructions (HTML):
# - Your response MUST be formatted exclusively using these Telegram-supported HTML tags: <b>bold</b>, <i>italic</i>, <u>underline</u>, <s>strikethrough</s>, <tg-spoiler>spoiler</tg-spoiler>, <code>inline code</code>, <pre>code block</pre>, <a href='URL'>link</a>.
# - It is absolutely forbidden to use any Markdown syntax. This includes, but is not limited to: **bold**, *italic*, _italic_, `inline code`, ```code blocks```, # Headers (any level, like ## or ####), > blockquotes, - or * list items.
# - The final output must be directly usable in Telegram with parse_mode=HTML without any further processing. Ensure NO Markdown formatting remains.
"""
CONVERSATION_HISTORY_LIMIT = 10
MESSAGE_EXPIRATION_DAYS = 7

# Максимальная длина сообщения Telegram (чуть меньше лимита 4096)
TELEGRAM_MAX_LENGTH = 4000

# Обновленный паттерн для разрешенных HTML тегов Telegram
ALLOWED_TAGS_PATTERN_TEXT = r"</?(?:b|i|u|s|tg-spoiler|code|pre|a(?:\s+href\s*=\s*(?:\"[^\"]*\"|'[^\']*'))?)\s*>/?"
ALLOWED_TAGS_PATTERN = re.compile(ALLOWED_TAGS_PATTERN_TEXT, re.IGNORECASE)

async def clean_html_for_telegram(text: str) -> str:
    """
    Преобразует Markdown в HTML, обрабатывает таблицы и очищает HTML
    для безопасной отправки в Telegram с parse_mode=HTML.
    Также добавляет отступы для иерархической структуры текста.
    """
    if not text:
        return ""

    # Удаляем существующие HTML-теги перед форматированием для избежания конфликтов
    text = strip_html_tags(text)
        
    # 1. Конвертация Markdown в HTML (порядок важен)
    try:
        # Определение уровней заголовков для отступов
        heading_level = {}
        current_level = 0
        indent_char = "    "  # 4 пробела для отступа
        
        # Функция для добавления отступов
        def add_indent(match):
            nonlocal current_level
            header_text = match.group(2)
            # Используем количество # для определения уровня
            level = len(match.group(1))
            if level > 0:
                # Запоминаем уровень заголовка
                heading_level[header_text] = level
                # Заголовок первого уровня - без отступа
                current_level = level - 1
                indentation = indent_char * current_level if current_level > 0 else ""
                return f"{indentation}<b>{header_text}</b>"
            return f"<b>{header_text}</b>"
            
        # Заголовки с отступами
        text = re.sub(r'^(#{1,6})\s+(.+)$', add_indent, text, flags=re.MULTILINE)
        
        # Блоки кода (самые приоритетные из-за символов внутри)
        text = re.sub(r"^\s*```([^\n]*)\n(.*?)\n```", r"<pre>\2</pre>", text, flags=re.DOTALL | re.MULTILINE)
        text = re.sub(r"^\s*```()\n(.*?)\n```", r"<pre>\2</pre>", text, flags=re.DOTALL | re.MULTILINE) # Без языка

        # Встроенный код (моноширный)
        text = re.sub(r"`([^`\n]+)`", r"<code>\1</code>", text)

        # Жирный и подчеркнутый
        text = re.sub(r"\*\*(.*?)\*\*", r"<b>\1</b>", text)
        text = re.sub(r"__(.*?)__", r"<u>\1</u>", text) # Используем <u> для подчеркивания

        # Курсив
        text = re.sub(r"\*(.*?)\*", r"<i>\1</i>", text)
        text = re.sub(r"_(.*?)_", r"<i>\1</i>", text) # Используем <i> для курсива

        # Зачеркнутый
        text = re.sub(r"~~(.*?)~~", r"<s>\1</s>", text)

        # Списки с отступами
        indent_level = 0
        
        def indent_list_item(match):
            nonlocal indent_level
            item_text = match.group(0)
            leading_spaces = len(item_text) - len(item_text.lstrip())
            # Определение уровня вложенности на основе отступа
            indent_level = leading_spaces // 2  # примерно 2 пробела = 1 уровень
            indentation = indent_char * indent_level
            return f"{indentation}• {match.group(1)}"
            
        text = re.sub(r"^\s*[-*+]\s+(.*?)$", indent_list_item, text, flags=re.MULTILINE)

        # --- Обработка таблиц ---
        # Найти блоки, похожие на таблицы Markdown
        table_pattern = re.compile(r"(?:^\|.*\|$\n?)+", re.MULTILINE)

        def format_table(match):
            table_content = match.group(0).strip()
            # Экранируем HTML внутри таблицы и оборачиваем в <pre>
            escaped_table_content = html.escape(table_content)
            return f"<pre>{escaped_table_content}</pre>\n"

        text = table_pattern.sub(format_table, text)
        # --- Конец обработки таблиц ---

    except Exception as e:
        logging.error(f"Ошибка на этапе конвертации Markdown: {e}")
        # В случае ошибки возвращаем текст без форматирования
        return text

    # Финальная проверка для защиты от ошибок форматирования
    if not is_valid_html(text):
        logging.warning("HTML невалиден после форматирования, возвращаем исходный текст")
        return text
        
    return text

def has_partial_html_tags(text: str) -> bool:
    """Проверяет, есть ли незакрытые или частичные HTML-теги в тексте."""
    # Считаем открывающие и закрывающие скобки
    if text.count('<') != text.count('>'):
        return True
    
    # Проверим самые популярные теги
    tags = ['b', 'i', 'u', 's', 'code', 'pre', 'a', 'tg-spoiler']
    for tag in tags:
        if text.count(f'<{tag}') != text.count(f'</{tag}>'):
            return True
    
    return False

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
        current_message_id = None  # ID текущего редактируемого сообщения
        last_edit_time = time.monotonic()  # Время последнего редактирования
        edit_interval = 1.0  # Интервал троттлинга (секунды)
        placeholder_sent = False  # Флаг, отправлен ли плейсхолдер
        
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
                
            # Добавление chunk к полному ответу для БД
            full_response_for_db += chunk
            
            # Очистка чанка от Markdown форматирования и конвертация в HTML
            cleaned_chunk = await clean_html_for_telegram(chunk)
            
            # Добавление очищенного chunk к текущему тексту сообщения
            current_message_text += cleaned_chunk
            
            # Проверка длины и разбиение при необходимости
            if len(current_message_text) > TELEGRAM_MAX_LENGTH:
                # Ищем место для "красивого" разрыва (по последнему \n перед лимитом)
                split_pos = current_message_text[:TELEGRAM_MAX_LENGTH].rfind('\n')
                if split_pos == -1:  # Если \n не найден, режем по лимиту
                    split_pos = TELEGRAM_MAX_LENGTH
                
                text_to_send_now = current_message_text[:split_pos]
                overflow_text = current_message_text[split_pos:].lstrip()  # Убираем пробелы в начале нового сообщения
                
                # Финальное редактирование старого сообщения
                try:
                    # Проверка HTML на валидность
                    use_html = is_valid_html(text_to_send_now)
                    if use_html:
                        await message.bot.edit_message_text(
                            text=text_to_send_now,
                            chat_id=chat_id,
                            message_id=current_message_id,
                            parse_mode='HTML'
                        )
                    else:
                        # Удаляем HTML-теги вместо экранирования
                        clean_text = strip_html_tags(text_to_send_now)
                        await message.bot.edit_message_text(
                            text=clean_text,
                            chat_id=chat_id,
                            message_id=current_message_id
                        )
                except TelegramAPIError as e:
                    logging.error(f"Ошибка финализации части сообщения перед разделением: {e}")
                
                # Отправка нового сообщения с оставшимся текстом
                try:
                    # Удаляем HTML-теги для промежуточного сообщения вместо экранирования
                    clean_overflow = strip_html_tags(overflow_text)
                    new_message = await message.bot.send_message(
                        chat_id=chat_id,
                        text=clean_overflow + "..." if clean_overflow else "...",  # Показываем, что продолжение следует
                        # Отключаем HTML-форматирование для промежуточных сообщений
                        parse_mode=None
                    )
                    current_message_id = new_message.message_id
                    current_message_text = overflow_text  # Начинаем накапливать для нового сообщения
                    last_edit_time = time.monotonic()  # Сброс таймера
                except TelegramAPIError as e:
                    logging.error(f"Ошибка отправки следующей части сообщения после разделения: {e}")
                    current_message_id = None  # Обнуляем ID, чтобы прервать дальнейшие попытки редактирования
                    break  # Прерываем стрим
            
            # Троттлинг редактирования (если не было разбиения)
            elif time.monotonic() - last_edit_time > edit_interval:
                try:
                    # Удаляем HTML-теги для промежуточного отображения вместо экранирования
                    clean_text = strip_html_tags(current_message_text) + "..."  # Индикатор продолжения
                    await message.bot.edit_message_text(
                        text=clean_text,
                        chat_id=chat_id,
                        message_id=current_message_id,
                        # Отключаем HTML-форматирование для промежуточных сообщений
                        parse_mode=None
                    )
                    last_edit_time = time.monotonic()
                except TelegramRetryAfter as e:
                    logging.warning(f"Превышен лимит запросов, ожидание {e.retry_after}с")
                    await asyncio.sleep(e.retry_after)
                except TelegramAPIError as e:
                    logging.error(f"Ошибка редактирования сообщения: {e}")
                    # Если сообщение удалено, дальнейшее редактирование бессмысленно
                    if "message to edit not found" in str(e).lower() or "message can't be edited" in str(e).lower():
                        current_message_id = None  # Обнуляем ID
                        break  # Прерываем цикл
        
        # После цикла (стрим завершен)
        if placeholder_sent and current_message_id:
            try:
                # Проверяем, не осталось ли сообщение пустым
                final_text = current_message_text if current_message_text else "Не удалось сгенерировать ответ."
                
                # Форматируем текст в HTML
                final_html = await clean_html_for_telegram(final_text)
                
                # Проверка HTML на валидность перед отправкой с parse_mode='HTML'
                use_html = is_valid_html(final_html)
                if not use_html:
                    logging.warning("Финальный текст содержит невалидный HTML, отправка без форматирования")
                    # Просто удаляем все теги вместо экранирования
                    final_text = strip_html_tags(final_text)
                    await message.bot.edit_message_text(
                        text=final_text,
                        chat_id=chat_id,
                        message_id=current_message_id,
                        parse_mode=None
                    )
                else:
                    # Отправляем с HTML-форматированием
                    await message.bot.edit_message_text(
                        text=final_html,
                        chat_id=chat_id,
                        message_id=current_message_id,
                        parse_mode='HTML'
                    )
            except TelegramAPIError as e:
                logging.error(f"Ошибка финального редактирования последнего сообщения: {e}")
                # Пробуем отправить новым сообщением, если редактирование не удалось
                try:
                    # Отправляем текст без форматирования в случае ошибки
                    clean_text = strip_html_tags(final_text)
                    await message.answer(clean_text, parse_mode=None)
                except Exception as e2:
                    logging.error(f"Не удалось отправить финальный ответ новым сообщением: {e2}")
        
        elif not placeholder_sent and full_response_for_db:
            # Если плейсхолдер не отправился, но ответ есть, отправить его новым сообщением
            try:
                cleaned_response = await clean_html_for_telegram(full_response_for_db)
                # Проверка HTML на валидность
                use_html = is_valid_html(cleaned_response)
                await message.answer(cleaned_response, parse_mode='HTML' if use_html else None)
            except TelegramAPIError as e:
                logging.error(f"Ошибка отправки финального ответа новым сообщением: {e}")
        
        # Сохраняем ПОЛНЫЙ ответ в БД
        if full_response_for_db:
            await add_message_to_db(db, user_id, 'assistant', full_response_for_db)
            logging.info(f"Полный ответ ассистента для пользователя {user_id} сохранен в БД")
        else:
            logging.warning(f"Пустой ответ от DeepSeek API для пользователя {user_id}")
            await message.answer("Извините, не удалось получить ответ от ассистента.")
        
    except Exception as e:
        logging.exception(f"Непредвиденная ошибка для пользователя {user_id}: {e}")
        # Убедимся, что parse_mode не используется для простого сообщения об ошибке
        await message.answer("Произошла ошибка при обработке сообщения")

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

# НОВЫЙ: Обработчик сообщений с фото (Обновлен: сообщает о неподдержке)
@dp.message(F.photo)
async def photo_handler(message: types.Message, bot: Bot):
    """Обрабатывает сообщения с фотографиями."""
    user_id = message.from_user.id
    photo = message.photo[-1]
    file_id = photo.file_id
    caption = message.caption # Получаем подпись к фото

    logger.info(f"Получено фото от user_id={user_id} (file_id: {file_id}). Подпись: {caption}")

    await message.reply(
        "Получил фото. К сожалению, текущая конфигурация API DeepSeek не поддерживает анализ изображений через этот endpoint. "
        "Если вы добавили подпись к фото, я обработаю ее как обычный текст."
    )

    # Если есть подпись, обрабатываем ее как обычное текстовое сообщение
    if caption:
        logger.info(f"Обработка подписи к фото как текста для user_id={user_id}")
        # Создаем новое "виртуальное" сообщение с текстом из подписи
        # и передаем его в обычный message_handler
        # Важно: message_handler ожидает объект message, создадим аналог
        # с необходимыми полями или вызовем его логику напрямую.
        # Простой вариант: вызвать основную логику обработки текста.

        # Получаем зависимости (аналогично message_handler)
        db = dp.workflow_data.get('db')
        settings_local = dp.workflow_data.get('settings') # Используем другое имя, чтобы не конфликтовать с глобальным settings

        if not db or not settings_local:
            logging.error("Не удалось получить зависимости для обработки подписи к фото")
            await message.reply("Произошла внутренняя ошибка при попытке обработать подпись к фото.")
            return

        await message.bot.send_chat_action(chat_id=user_id, action="typing")

        try:
            # Сохраняем подпись как сообщение пользователя
            await add_message_to_db(db, user_id, "user", caption)
            logging.info(f"Подпись к фото от пользователя {user_id} сохранена")

            # Получаем историю сообщений (включая только что добавленную подпись)
            history = await get_last_messages(db, user_id, limit=CONVERSATION_HISTORY_LIMIT)
            logging.info(f"Получена история сообщений для пользователя {user_id} (с подписью), записей: {len(history)}")

            # Получаем ответ от DeepSeek API (только на основе текста)
            response_text = await get_deepseek_response(settings_local.DEEPSEEK_API_KEY, SYSTEM_PROMPT, history)

            if response_text:
                cleaned_response_text = await clean_html_for_telegram(response_text)
                await add_message_to_db(db, user_id, "assistant", response_text) # Сохраняем оригинал
                logging.info(f"Ответ ассистента (на подпись) для пользователя {user_id} сохранен")

                # Отправляем ответ (как в message_handler)
                if len(cleaned_response_text) > TELEGRAM_MAX_LENGTH:
                    # ... (логика разделения на части, как в message_handler)
                    logging.info(f"Очищенный ответ (на подпись) слишком длинный ({len(cleaned_response_text)} символов), разделяю на части.")
                    parts = []
                    current_part = ""
                    for paragraph in cleaned_response_text.split('\n\n'):
                        if len(current_part) + len(paragraph) + 2 <= TELEGRAM_MAX_LENGTH:
                            current_part += paragraph + "\n\n"
                        else:
                            if len(paragraph) > TELEGRAM_MAX_LENGTH:
                                if current_part.strip():
                                    parts.append(current_part.strip())
                                current_part = ""
                                for i in range(0, len(paragraph), TELEGRAM_MAX_LENGTH):
                                    parts.append(paragraph[i:i + TELEGRAM_MAX_LENGTH])
                            else:
                                if current_part.strip():
                                    parts.append(current_part.strip())
                                current_part = paragraph + "\n\n"
                    if current_part.strip():
                        parts.append(current_part.strip())
                    for i, part in enumerate(parts):
                        logging.info(f"Отправка очищенной части {i+1}/{len(parts)} (на подпись) пользователю {user_id}")
                        try:
                            await message.answer(part, parse_mode='HTML') # Отвечаем на исходное сообщение с фото
                            await asyncio.sleep(0.5)
                        except Exception as send_error:
                            logging.error(f"Ошибка отправки части {i+1} (на подпись): {send_error}")
                            await message.answer(f"Ошибка при отправке части {i+1} ответа на подпись.")
                            break
                else:
                    await message.answer(cleaned_response_text, parse_mode='HTML') # Отвечаем на исходное сообщение с фото
            else:
                logging.warning(f"Не удалось получить ответ от DeepSeek API на подпись к фото для пользователя {user_id}")
                await message.reply("Извините, произошла ошибка при обработке вашей подписи к фото.")

        except Exception as e:
            logging.exception(f"Ошибка при обработке подписи к фото для user_id={user_id}: {e}")
            await message.reply("Произошла ошибка при обработке подписи к фото.")

    # --- Логика скачивания и обработки base64 УДАЛЕНА --- 
    # try:
    #     # Скачиваем фото в память
    #     file_info = await bot.get_file(file_id)
    #     ...
    #     image_base64 = base64.b64encode(file_content.read()).decode('utf-8')
    #     ...
    #     # Отправляем запрос в DeepSeek с изображением
    #     response_text = await get_deepseek_response(
    #         ...
    #         image_data=image_base64 # Передаем base64 изображения
    #     )
    #     ...
    # except Exception as e:
    #     ...
    # ----------------------------------------------------

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
            
        # Проверка на экранированные теги, которые могли остаться
        if '&lt;' in html_text or '&gt;' in html_text:
            logging.warning("Найдены экранированные HTML-теги")
            return False
            
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

# Запуск бота
if __name__ == "__main__":
    asyncio.run(main())
