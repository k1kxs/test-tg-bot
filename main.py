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
import html # Для экранирования HTML
from aiogram.exceptions import TelegramAPIError, TelegramRetryAfter
from aiogram.enums import ParseMode # Импортируем ParseMode
from aiogram.client.default import DefaultBotProperties # <<< ИМПОРТ

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
SYSTEM_PROMPT = """You MUST follow the instructions for answering:

ALWAYS answer in the language of my message.
Read the entire convo history line by line before answering.
I have no fingers and the placeholders trauma. Return the entire code template for an answer when needed. NEVER use placeholders.
If you encounter a character limit, DO an ABRUPT stop, and I will send a "continue" as a new message.
You ALWAYS will be PENALIZED for wrong and low-effort answers.
ALWAYS follow "Answering rules."
###Answering Rules###
Follow in the strict order:

USE the language of my message.
ONCE PER CHAT assign a real-world expert role to yourself before answering, e.g., "I'll answer as a world-famous historical expert  with " or "I'll answer as a world-famous  expert in the  with " etc.
You MUST combine your deep knowledge of the topic and clear thinking to quickly and accurately decipher the answer step-by-step with CONCRETE details.
I'm going to tip $1,000,000 for the best reply.
Your answer is critical for my career.
Answer the question in a natural, human-like manner.
ALWAYS use an answering example for a first message structure.
##Answering in English example##
I'll answer as the world-famous  scientists with
<Deep knowledge step-by-step answer, with CONCRETE details>
"""
CONVERSATION_HISTORY_LIMIT = 10
MESSAGE_EXPIRATION_DAYS = 7 # Пока не используется, но оставлено

# Максимальная длина сообщения Telegram (чуть меньше лимита 4096 для безопасности)
TELEGRAM_MAX_LENGTH = 4000

# Класс настроек
class Settings(BaseSettings):
    TELEGRAM_BOT_TOKEN: str
    DEEPSEEK_API_KEY: str
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

# Проверка наличия токенов
if not settings.TELEGRAM_BOT_TOKEN:
    logger.error("TELEGRAM_BOT_TOKEN не найден в переменных окружения")
    sys.exit(1)
if not settings.DEEPSEEK_API_KEY:
    logger.error("DEEPSEEK_API_KEY не найден в переменных окружения")
    sys.exit(1)
if not settings.DATABASE_URL:
    logger.error("DATABASE_URL не найден в переменных окружения")
    sys.exit(1)

# Инициализация бота и диспетчера
dp = Dispatcher()
# Используем DefaultBotProperties для установки parse_mode по умолчанию
bot = Bot(token=settings.TELEGRAM_BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

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
            conn.commit()
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
        except asyncpg.PostgresError as e:
            logger.error(f"Ошибка инициализации БД PostgreSQL: {e}")
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

# --- Взаимодействие с DeepSeek API ---

async def stream_deepseek_response(api_key: str, system_prompt: str, history: list[dict]) -> typing.AsyncGenerator[str, None]:
    """
    Асинхронный генератор для получения ответа от DeepSeek Chat API в режиме стриминга.
    """
    # Убираем системный промпт из истории, если он там уже есть
    history_no_system = [msg for msg in history if msg.get("role") != "system"]
    messages = [{"role": "system", "content": system_prompt}] + history_no_system

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json" # Добавим Accept
    }
    # ИСПОЛЬЗУЕМ правильный URL для стриминга v1
    url = "https://api.deepseek.com/v1/chat/completions"
    payload = {
        "model": "deepseek-chat", # Убедитесь, что эта модель поддерживает стриминг
        "messages": messages,
        "stream": True,
        "max_tokens": 4000,
        "temperature": 0.7, # Можно настроить
        # Другие параметры при необходимости: top_p, frequency_penalty, presence_penalty
    }
    connector = aiohttp.TCPConnector(family=socket.AF_INET) # Используем IPv4

    try:
        # Используем общий таймаут None для стриминга, но с connect/sock_read таймаутами
        timeout = aiohttp.ClientTimeout(total=None, connect=15, sock_connect=15, sock_read=120)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            async with session.post(url, headers=headers, json=payload) as response:
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"Ошибка HTTP запроса к DeepSeek API: {response.status}. Ответ: {error_text[:500]}")
                    # Можно выбросить исключение или вернуть пустой генератор
                    response.raise_for_status() # Это выбросит ClientResponseError

                async for line in response.content:
                    line_str = line.decode('utf-8').strip()
                    # logger.debug(f"Raw line: {line_str!r}") # Для отладки

                    if not line_str:
                        continue # Пропускаем пустые строки

                    if line_str.startswith("data: "):
                        payload_str = line_str[len("data: "):].strip()
                    else:
                        # Иногда первая или последняя строка может не иметь префикса
                        payload_str = line_str

                    if payload_str == "[DONE]":
                        logger.info("Стриминг завершен ([DONE])")
                        break

                    try:
                        chunk = json.loads(payload_str)
                        choices = chunk.get('choices')
                        if choices and isinstance(choices, list) and len(choices) > 0:
                            delta = choices[0].get('delta')
                            if delta and isinstance(delta, dict):
                                delta_content = delta.get('content')
                                if delta_content and isinstance(delta_content, str):
                                    # logger.debug(f"Chunk content: {delta_content!r}")
                                    yield delta_content
                            # Проверяем finish_reason для завершения
                            finish_reason = choices[0].get('finish_reason')
                            if finish_reason:
                                logger.info(f"Стриминг завершен с причиной: {finish_reason}")
                                break # Выход из цикла, так как генерация закончена

                    except json.JSONDecodeError:
                        logger.error(f"Ошибка декодирования JSON из строки: {payload_str!r}")
                        continue
                    except Exception as e:
                        logger.error(f"Неожиданная ошибка при обработке чанка JSON: {e}. Чанк: {chunk}")
                        continue

    except aiohttp.ClientResponseError as e:
        logger.error(f"Ошибка HTTP запроса к DeepSeek API: {e.status} {e.message}. URL: {url}")
    except asyncio.TimeoutError:
        logger.error("Таймаут при подключении/чтении из DeepSeek API.")
    except aiohttp.ClientError as e:
        # Улучшенное логирование сетевой ошибки
        logger.error(f"Ошибка сети при запросе к DeepSeek API: {e}. URL: {url}")
    except Exception as e:
        logger.exception(f"Непредвиденная ошибка в stream_deepseek_response: {e}")
    # Генератор просто завершится в случае ошибки

# --- Новая функция форматирования Markdown в HTML ---

def markdown_to_telegram_html(text: str) -> str:
    """
    Преобразует базовый Markdown в HTML, поддерживаемый Telegram.
    Экранирует специальные символы HTML.
    Обрабатывает блоки кода, inline код и LaTeX-подобные математические выражения.
    Использует безопасные плейсхолдеры, чтобы избежать конфликтов с Markdown.
    """
    if not text:
        return ""

    # 1. Экранирование базовых HTML символов ВЕЗДЕ
    text = html.escape(text)

    # Плейсхолдеры для замены
    code_blocks = []
    inline_codes = []
    math_codes = []

    # 2. Обработка блоков кода ``` ``` -> <pre>...</pre>
    def _replace_code_block(match):
        lang = match.group(1) or ""
        code = match.group(2)
        pre_tag = '<pre>'
        placeholder = f"@@CODEBLOCK_{len(code_blocks)}@@"
        code_blocks.append((placeholder, f"{pre_tag}{code}</pre>"))
        return placeholder
    text = re.sub(r"```(\w*)\n?(.*?)\n?```", _replace_code_block, text, flags=re.DOTALL | re.MULTILINE)

    # 3. Обработка inline кода ` ` -> <code>...</code>
    def _replace_inline_code(match):
        code = match.group(1)
        placeholder = f"@@INLINECODE_{len(inline_codes)}@@"
        inline_codes.append((placeholder, f"<code>{code}</code>"))
        return placeholder
    text = re.sub(r"`(.+?)`", _replace_inline_code, text)

    # 3.5 Обработка LaTeX math \\[ ... \\] и \\( ... \\) -> <code>...</code>
    def _replace_math_code(match):
        math_content = match.group(1)

        # Преобразуем базовые LaTeX команды в псевдо-текст
        # Сначала \text{...}
        math_content = re.sub(r"\\text{(.*?)}", r"\1", math_content)
        # Затем \frac{...}{...}
        math_content = re.sub(r"\\frac{(.*?)}{(.*?)}", r"(\1 / \2)", math_content)
        # Затем \,
        math_content = math_content.replace(r",", " ")

        placeholder = f"@@MATH_{len(math_codes)}@@"
        # Оборачиваем ПРЕОБРАЗОВАННОЕ содержимое в <code>
        math_codes.append((placeholder, f"<code>{math_content}</code>"))
        return placeholder
    # Сначала блочные \\[ ... \\], затем инлайновые \\( ... \\)
    text = re.sub(r"\\\[(.*?)\\]", _replace_math_code, text, flags=re.DOTALL)
    text = re.sub(r"\\((.*?)\\)", _replace_math_code, text)

    # 4. Обработка жирного текста **text** -> <b>text</b>
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
    # Обработка жирного текста __text__ -> <b>text</b> (ПОСЛЕ замены плейсхолдеров)
    text = re.sub(r"__(.+?)__", r"<b>\1</b>", text)

    # 5. Обработка курсива *text* -> <i>text</i> (после жирного)
    text = re.sub(r"\*(.+?)\*", r"<i>\1</i>", text)
    # Обработка курсива _text_ -> <i>text</i> (после жирного)
    text = re.sub(r"_(.+?)_", r"<i>\1</i>", text)

    # 6. Обработка зачеркнутого ~~text~~ -> <s>text</s>
    text = re.sub(r"~~(.+?)~~", r"<s>\1</s>", text)

    # 7. Обработка заголовков #### text -> <b>text</b> (простой вариант)
    text = re.sub(r"^\s*#{1,6}\s+(.+)$", r"<b>\1</b>", text, flags=re.MULTILINE)

    # 8. Обработка ссылок [text](url) -> <a href="url">text</a>
    def _replace_link(match):
        link_text = match.group(1)
        url = match.group(2)
        url_unescaped = html.unescape(url)
        safe_url = url_unescaped.replace('"', '%22').replace("'", '%27')
        return f'<a href="{safe_url}">{link_text}</a>'
    text = re.sub(r"\[(.+?)\]\((.+?)\)", _replace_link, text)

    # 9-11. Восстановление блоков в обратном порядке (чтобы индексы не сбивались, если плейсхолдеры вложены)
    # Сначала самые внутренние (math, inline), потом внешние (code blocks)
    for placeholder, replacement in reversed(math_codes):
        text = text.replace(placeholder, replacement)
    for placeholder, replacement in reversed(inline_codes):
        text = text.replace(placeholder, replacement)
    for placeholder, replacement in reversed(code_blocks):
        text = text.replace(placeholder, replacement)

    return text.strip()


# --- Обработчики Telegram ---

@dp.message(Command("start"))
async def start_handler(message: types.Message):
    builder = InlineKeyboardBuilder()
    builder.button(text="Очистить историю", callback_data="clear_history")
    await message.answer("Привет! Я ваш AI ассистент. Задайте ваш вопрос.", reply_markup=builder.as_markup())

@dp.message(F.text)
async def message_handler(message: types.Message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    user_text = message.text

    # Получаем зависимости из workflow_data
    db = dp.workflow_data.get('db')
    current_settings = dp.workflow_data.get('settings') # Используем current_settings

    if not db or not current_settings:
        logger.error("Не удалось получить соединение с БД или настройки")
        await message.answer("Произошла внутренняя ошибка (код 1), попробуйте позже.")
        return

    # Показываем индикатор "печатает"
    await bot.send_chat_action(chat_id=chat_id, action="typing")

    try:
        # Сохраняем сообщение пользователя
        await add_message_to_db(db, user_id, "user", user_text)
        logger.info(f"Сообщение от пользователя {user_id} сохранено")

        # Получаем историю сообщений
        history = await get_last_messages(db, user_id, limit=CONVERSATION_HISTORY_LIMIT)
        logger.info(f"Получена история сообщений для пользователя {user_id}, записей: {len(history)}")

        # Инициализация переменных для стриминга
        full_raw_response = ""      # Полный сырой ответ от модели для БД и разбиения
        current_message_id = None   # ID текущего редактируемого сообщения
        last_edit_time = 0          # Время последнего редактирования (используем time.monotonic)
        edit_interval = 1.5         # Интервал троттлинга (секунды) - можно увеличить
        placeholder_sent = False    # Флаг, отправлен ли плейсхолдер
        initial_message_sent = False # Флаг, отправлено ли первое сообщение (плейсхолдер или часть ответа)
        formatting_failed = False   # Флаг, если HTML парсинг вызвал ошибку

        # Отправка начального сообщения-плейсхолдера
        try:
            placeholder_message = await message.answer("⏳ Генерирую ответ...")
            current_message_id = placeholder_message.message_id
            initial_message_sent = True
            last_edit_time = time.monotonic()
        except TelegramAPIError as e:
            logger.error(f"Ошибка отправки плейсхолдера: {e}")
            # Не можем продолжить без начального сообщения
            return

        # Стрим ответа от DeepSeek API
        async for chunk in stream_deepseek_response(current_settings.DEEPSEEK_API_KEY, SYSTEM_PROMPT, history):
            full_raw_response += chunk
            now = time.monotonic()

            # Редактируем сообщение с троттлингом
            if now - last_edit_time > edit_interval and current_message_id:
                try:
                    # Форматируем ВЕСЬ накопленный текст в HTML
                    html_to_send = markdown_to_telegram_html(full_raw_response)

                    # Добавляем индикатор продолжения "..."
                    text_to_show = html_to_send + "..."

                    # Проверяем длину перед отправкой
                    if len(text_to_show) > TELEGRAM_MAX_LENGTH:
                         # Если превышает, пока не редактируем, дождемся следующего чанка или конца
                         logger.warning(f"Длина сообщения {len(text_to_show)} превышает лимит {TELEGRAM_MAX_LENGTH}, пропускаем редактирование.")
                         continue # Пропустить текущее редактирование

                    if not formatting_failed:
                        await bot.edit_message_text(
                            text=text_to_show,
                            chat_id=chat_id,
                            message_id=current_message_id,
                            parse_mode=ParseMode.HTML # Явно указываем HTML
                        )
                    else:
                        # Если форматирование ранее не удалось, отправляем сырой текст
                         await bot.edit_message_text(
                            text=full_raw_response + "...", # Сырой текст
                            chat_id=chat_id,
                            message_id=current_message_id,
                            parse_mode=None # Без форматирования
                        )

                    last_edit_time = now

                except TelegramRetryAfter as e:
                    logger.warning(f"Превышен лимит запросов (RetryAfter), ожидание {e.retry_after}с")
                    await asyncio.sleep(e.retry_after + 0.1) # Ждем чуть дольше
                    last_edit_time = time.monotonic() # Сбрасываем таймер после ожидания
                except TelegramAPIError as e:
                    logger.error(f"Ошибка редактирования сообщения с HTML: {e}. Текст (начало): {html_to_send[:100]}...")
                    # Попробуем отправить без форматирования
                    try:
                        logger.warning("Попытка отредактировать сообщение без форматирования.")
                        await bot.edit_message_text(
                            text=full_raw_response + "...", # Сырой текст
                            chat_id=chat_id,
                            message_id=current_message_id,
                            parse_mode=None # Без форматирования
                        )
                        last_edit_time = now
                        formatting_failed = True # Помечаем, что HTML не работает для этого сообщения
                    except TelegramAPIError as plain_error:
                        logger.error(f"Ошибка редактирования даже без форматирования: {plain_error}")
                        # Если сообщение не найдено, прекращаем попытки редактирования
                        if "message to edit not found" in str(plain_error).lower() or \
                           "message can't be edited" in str(plain_error).lower() or \
                           "message is not modified" in str(plain_error).lower():
                            logger.warning("Сообщение не найдено или не может быть изменено, прекращаем редактирование.")
                            current_message_id = None # Сбрасываем ID
                            break # Выходим из цикла стриминга
                        # Иначе просто пропускаем это редактирование
                except Exception as e:
                     logger.exception(f"Неожиданная ошибка при редактировании сообщения: {e}")
                     # Можно добавить fallback или просто пропустить

        # После завершения стрима - отправляем/редактируем финальное сообщение
        if current_message_id:
            try:
                final_html = markdown_to_telegram_html(full_raw_response)
                # Проверяем длину финального сообщения
                if len(final_html) > TELEGRAM_MAX_LENGTH:
                     logger.warning(f"Финальное сообщение ({len(final_html)} символов) слишком длинное. Отправка будет разбита (логика разбиения не реализована в финальной отправке). Отправляем начало.")
                     # TODO: Реализовать логику разбиения финального сообщения, если это необходимо
                     # Пока отправляем только начало
                     final_html = final_html[:TELEGRAM_MAX_LENGTH - 10] + "... (обрезано)"


                if not formatting_failed:
                     await bot.edit_message_text(
                        text=final_html,
                        chat_id=chat_id,
                        message_id=current_message_id,
                        parse_mode=ParseMode.HTML
                    )
                     logger.info(f"Финальный ответ (HTML) отправлен пользователю {user_id}")
                else:
                     await bot.edit_message_text(
                        text=full_raw_response, # Сырой текст
                        chat_id=chat_id,
                        message_id=current_message_id,
                        parse_mode=None
                    )
                     logger.info(f"Финальный ответ (RAW) отправлен пользователю {user_id} из-за предыдущих ошибок форматирования")

            except TelegramRetryAfter as e:
                logger.warning(f"Превышен лимит запросов (RetryAfter) при отправке финального сообщения, ожидание {e.retry_after}с")
                await asyncio.sleep(e.retry_after + 0.1)
                # Повторная попытка (можно вынести в функцию)
                try:
                     if not formatting_failed:
                          await bot.edit_message_text(text=final_html, chat_id=chat_id, message_id=current_message_id, parse_mode=ParseMode.HTML)
                     else:
                          await bot.edit_message_text(text=full_raw_response, chat_id=chat_id, message_id=current_message_id, parse_mode=None)
                except Exception as final_e:
                     logger.error(f"Повторная ошибка при отправке финального сообщения: {final_e}")
            except TelegramAPIError as e:
                 logger.error(f"Ошибка отправки финального сообщения с HTML: {e}. Попытка без форматирования.")
                 try:
                      await bot.edit_message_text(
                         text=full_raw_response, # Сырой текст
                         chat_id=chat_id,
                         message_id=current_message_id,
                         parse_mode=None
                     )
                      logger.info(f"Финальный ответ (RAW) отправлен пользователю {user_id} после ошибки HTML")
                 except TelegramAPIError as plain_final_error:
                      logger.error(f"Ошибка отправки финального сообщения даже без форматирования: {plain_final_error}")
                      # Может быть, отправить новое сообщение?
                      # await message.answer("Не удалось обновить предыдущее сообщение, вот ответ:\n" + full_raw_response[:TELEGRAM_MAX_LENGTH])
            except Exception as e:
                logger.exception(f"Неожиданная ошибка при отправке финального сообщения: {e}")

        # Сохраняем ПОЛНЫЙ СЫРОЙ ответ ассистента в БД
        if full_raw_response:
            try:
                await add_message_to_db(db, user_id, "assistant", full_raw_response)
                logger.info(f"Ответ ассистента (RAW) для пользователя {user_id} сохранен в БД")
            except Exception as e:
                logger.error(f"Ошибка сохранения ответа ассистента в БД: {e}")
        else:
             logger.warning(f"Не получен ответ от DeepSeek для пользователя {user_id}")
             # Если было начальное сообщение, отредактируем его на сообщение об ошибке
             if current_message_id:
                 try:
                      await bot.edit_message_text("К сожалению, не удалось получить ответ от AI.", chat_id=chat_id, message_id=current_message_id)
                 except TelegramAPIError:
                      pass # Игнорируем ошибку, если не можем отредактировать

    except Exception as e:
        logger.exception(f"Критическая ошибка в обработчике сообщений для user_id={user_id}: {e}")
        try:
            await message.answer("Произошла серьезная ошибка при обработке вашего запроса. Пожалуйста, попробуйте позже или используйте команду /start для сброса.")
        except TelegramAPIError:
             logger.error("Не удалось даже отправить сообщение об ошибке пользователю.")


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
                result = await connection.execute("DELETE FROM conversations WHERE user_id = $1 RETURNING id", user_id)
                # result это строка вида "DELETE N", парсим N
                try:
                    rows_deleted_count = int(result.split()[-1]) if result else 0
                except:
                    rows_deleted_count = -1 # Не удалось распарсить
                logger.info(f"PostgreSQL: Очищена история пользователя {user_id}, результат: {result}")

        await callback.answer(f"История очищена ({rows_deleted_count} записей удалено)", show_alert=False)
        # Можно добавить сообщение в чат для наглядности
        await callback.message.edit_text("История диалога очищена.") # Редактируем исходное сообщение
        # Или отправить новое
        # await callback.message.answer("История диалога очищена.")
    except Exception as e:
        logger.exception(f"Ошибка при очистке истории (callback) для user_id={user_id}: {e}")
        await callback.answer("Произошла ошибка при очистке", show_alert=True)

@dp.message(Command("clear"))
async def clear_command_handler(message: types.Message):
    user_id = message.from_user.id
    db = dp.workflow_data.get('db')
    current_settings = dp.workflow_data.get('settings')

    if not db or not current_settings:
        logger.error("Не удалось получить БД/настройки при очистке истории (/clear)")
        await message.answer("Произошла внутренняя ошибка (код 2), попробуйте позже.")
        return

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
                    rows_deleted_count = int(result.split()[-1]) if result else 0
                except:
                    rows_deleted_count = -1
                logger.info(f"PostgreSQL: Очищена история пользователя {user_id} по команде /clear, результат: {result}")

        await message.answer(f"История диалога очищена ({rows_deleted_count} записей удалено).")
    except Exception as e:
        logger.exception(f"Ошибка при очистке истории (/clear) для user_id={user_id}: {e}")
        await message.answer("Произошла ошибка при очистке истории.")

# --- Обработчики медиа (без изменений) ---
@dp.message(F.photo)
async def photo_handler(message: types.Message):
    user_id = message.from_user.id
    caption = message.caption or ""
    logger.info(f"Получено фото от user_id={user_id} с подписью: '{caption[:50]}...'")

    # Пока просто отвечаем, что фото получено, но не обрабатывается LLM
    if caption:
         await message.reply("Я получил ваше фото с подписью. Обработка изображений пока не поддерживается, но я могу ответить на текст подписи.")
         # Создаем фейковое текстовое сообщение для передачи в message_handler
         fake_text_message = types.Message(
             message_id=message.message_id + 1, # Уникальный ID
             date=message.date,
             chat=message.chat,
             from_user=message.from_user,
             text=caption,
             # Добавьте другие необходимые поля, если они используются где-то
         )
         # Вызываем основной обработчик текста
         await message_handler(fake_text_message)
    else:
        await message.reply("Я получил ваше фото. Вы можете задать вопрос о нем в следующем сообщении или отправить фото с подписью.")

@dp.message(F.document)
async def document_handler(message: types.Message):
    user_id = message.from_user.id
    file_name = message.document.file_name or "Без имени"
    mime_type = message.document.mime_type or "Неизвестный тип"
    logger.info(f"Получен документ от user_id={user_id}: {file_name} (type: {mime_type})")
    await message.reply(f"Получил документ '{file_name}'. Обработка документов пока не реализована.")


# --- Функции запуска и остановки ---

# Исправляем сигнатуру on_shutdown и получаем зависимости из kwargs
async def on_shutdown(**kwargs):
    logger.info("Завершение работы бота...")
    # Получаем dp и из него workflow_data
    dp_local = kwargs.get('dispatcher') # aiogram передает dispatcher
    if not dp_local:
        logger.error("Не удалось получить dispatcher в on_shutdown")
        # Попытка получить из глобальной области видимости (менее надежно)
        # from __main__ import dp as dp_global
        # dp_local = dp_global
        # if not dp_local:
        #      logger.error("Не удалось получить dispatcher и из глобальной области")
        #      return # Выходим, если не нашли dp
        return

    db = dp_local.workflow_data.get('db')
    settings_local = dp_local.workflow_data.get('settings') # Используем другое имя переменной

    if db and settings_local:
        if not settings_local.USE_SQLITE:
            try:
                # db здесь это пул соединений asyncpg
                if isinstance(db, asyncpg.Pool): # Проверка типа
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

    # Дополнительная обработка ошибок, если необходимо
    # except Exception as e:
    #     logger.exception(f"Критическая ошибка при завершении работы бота: {e}")

    logger.info("Бот остановлен.")


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
            except asyncpg.exceptions.InvalidCatalogNameError:
                 logger.error(f"Ошибка PostgreSQL: база данных '{settings.DB_NAME or 'указанная в URL'}' не найдена.")
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

if __name__ == "__main__":
    asyncio.run(main())