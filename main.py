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
from aiogram.exceptions import TelegramAPIError, TelegramRetryAfter
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

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

def final_keyboard() -> types.InlineKeyboardMarkup:
    """Создает финальную клавиатуру (например, с кнопкой очистки истории)."""
    builder = InlineKeyboardBuilder()
    builder.button(text="🔄 Очистить историю", callback_data="clear_history")
    # Можно добавить другие кнопки по желанию
    return builder.as_markup()

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
        "temperature": 0.3,
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
    builder = InlineKeyboardBuilder()
    # Используем final_keyboard для консистентности
    builder.button(text="🔄 Очистить историю", callback_data="clear_history")
    await message.answer("Привет! Я ваш AI ассистент. Задайте ваш вопрос.", reply_markup=builder.as_markup())

@dp.message(F.text)
async def message_handler(message: types.Message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    user_text = message.text

    # Проверка, не идет ли уже генерация для этого пользователя
    if user_id in active_requests:
        try:
            await message.reply("Пожалуйста, дождитесь завершения предыдущего запроса или отмените его.", reply_markup=progress_keyboard(user_id))
        except TelegramAPIError as e:
            logger.warning(f"Не удалось отправить сообщение о дублирующем запросе: {e}")
        return

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

        # Инициализация переменных для стриминга из старой версии
        full_raw_response = ""      # Полный сырой ответ от модели для БД и разбиения
        current_message_id = None   # ID текущего редактируемого сообщения
        last_edit_time = 0          # Время последнего редактирования (используем time.monotonic)
        edit_interval = 1.5         # Интервал троттлинга (секунды) - можно увеличить
        formatting_failed = False   # Флаг, если HTML парсинг вызвал ошибку

        # Отправка начального сообщения-плейсхолдера из старой версии
        try:
            placeholder_message = await message.answer("⏳ Генерирую ответ...")
            current_message_id = placeholder_message.message_id
            last_edit_time = time.monotonic()
        except TelegramAPIError as e:
            logger.error(f"Ошибка отправки плейсхолдера: {e}")
            return # Не можем продолжить без начального сообщения

        # Старый цикл стриминга, адаптированный под stream_xai_response
        async for chunk in stream_xai_response(current_settings.XAI_API_KEY, SYSTEM_PROMPT, history):
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
                final_keyboard_markup = final_keyboard() # Получаем финальную клавиатуру

                # Разбиение на части, если необходимо (логика из split_text)
                message_parts = split_text(final_html, TELEGRAM_MAX_LENGTH)

                if not message_parts: # Если после форматирования текст пустой
                     message_parts = split_text(full_raw_response, TELEGRAM_MAX_LENGTH) # Попробуем разбить сырой текст
                     formatting_failed = True # Считаем, что форматирование не удалось

                if not message_parts: # Если и сырой текст пустой
                    logger.warning("Финальный ответ пуст после форматирования и в сыром виде.")
                    # Удаляем плейсхолдер или заменяем на сообщение об ошибке
                    try:
                        await bot.delete_message(chat_id=chat_id, message_id=current_message_id)
                    except TelegramAPIError:
                        try: # Попытка отредактировать, если удаление не удалось
                            await bot.edit_message_text(
                                text="К сожалению, не удалось сгенерировать ответ.",
                                chat_id=chat_id,
                                message_id=current_message_id
                            )
                        except TelegramAPIError:
                            logger.error("Не удалось ни удалить, ни отредактировать плейсхолдер для пустого ответа.")
                    current_message_id = None # Сбрасываем ID
                else:
                    # Редактируем первое сообщение (плейсхолдер) первой частью
                    try:
                        await bot.edit_message_text(
                            text=message_parts[0],
                            chat_id=chat_id,
                            message_id=current_message_id,
                            parse_mode=None if formatting_failed else ParseMode.HTML,
                            reply_markup=final_keyboard_markup if len(message_parts) == 1 else None # Клавиатура только у последнего сообщения
                        )
                        logger.info(f"Финальный ответ (часть 1/{len(message_parts)}) {'RAW' if formatting_failed else 'HTML'} отправлен пользователю {user_id}")
                    except TelegramAPIError as e:
                        logger.error(f"Ошибка редактирования финального сообщения (часть 1): {e}. Попытка без форматирования.")
                        try:
                             await bot.edit_message_text(
                                text=split_text(full_raw_response, TELEGRAM_MAX_LENGTH)[0], # Первая часть сырого текста
                                chat_id=chat_id,
                                message_id=current_message_id,
                                parse_mode=None,
                                reply_markup=final_keyboard_markup if len(message_parts) == 1 else None
                            )
                             formatting_failed = True # Форматирование точно не сработало
                             logger.info(f"Финальный ответ (часть 1/{len(message_parts)}) RAW отправлен пользователю {user_id} после ошибки HTML")
                        except TelegramAPIError as plain_final_error:
                             logger.error(f"Ошибка редактирования финального сообщения (часть 1) даже без форматирования: {plain_final_error}")
                             # Отправляем как новое сообщение, если редактирование не удалось
                             try:
                                 new_msg = await message.answer(
                                     text=message_parts[0],
                                     parse_mode=None if formatting_failed else ParseMode.HTML,
                                     reply_markup=final_keyboard_markup if len(message_parts) == 1 else None
                                 )
                                 current_message_id = new_msg.message_id # Обновляем ID на новое сообщение
                             except Exception as send_err:
                                 logger.error(f"Не удалось отправить первую часть как новое сообщение: {send_err}")
                                 current_message_id = None # Не можем продолжить


                    # Отправляем остальные части новыми сообщениями
                    if current_message_id: # Продолжаем, только если удалось отправить/отредактировать первую часть
                        for i in range(1, len(message_parts)):
                            try:
                                await asyncio.sleep(0.1) # Небольшая пауза между сообщениями
                                await message.answer(
                                    text=message_parts[i],
                                    parse_mode=None if formatting_failed else ParseMode.HTML,
                                    reply_markup=final_keyboard_markup if i == len(message_parts) - 1 else None
                                )
                                logger.info(f"Финальный ответ (часть {i+1}/{len(message_parts)}) {'RAW' if formatting_failed else 'HTML'} отправлен пользователю {user_id}")
                            except TelegramAPIError as e:
                                logger.error(f"Ошибка отправки части {i+1} финального сообщения: {e}")
                                # Можно попробовать отправить без форматирования или остановить отправку
                                try:
                                    await message.answer(
                                        text=split_text(full_raw_response, TELEGRAM_MAX_LENGTH)[i],
                                        parse_mode=None,
                                        reply_markup=final_keyboard_markup if i == len(message_parts) - 1 else None
                                    )
                                except Exception as send_err_part:
                                     logger.error(f"Не удалось отправить часть {i+1} даже без форматирования: {send_err_part}")
                                     break # Прерываем отправку остальных частей

            except TelegramRetryAfter as e:
                logger.warning(f"Превышен лимит запросов (RetryAfter) при отправке финального сообщения, ожидание {e.retry_after}с")
                await asyncio.sleep(e.retry_after + 0.1)
                # TODO: Добавить повторную попытку отправки финального сообщения
            except Exception as e:
                logger.exception(f"Неожиданная ошибка при отправке финального сообщения: {e}")
                # Попытка отправить простое сообщение об ошибке, если плейсхолдер еще существует
                if current_message_id:
                    try:
                        await bot.edit_message_text("Произошла ошибка при формировании финального ответа.", chat_id=chat_id, message_id=current_message_id)
                    except TelegramAPIError:
                         pass # Игнорировать ошибку, если не можем отредактировать

        # Сохраняем ПОЛНЫЙ СЫРОЙ ответ ассистента в БД
        if full_raw_response:
            try:
                await add_message_to_db(db, user_id, "assistant", full_raw_response)
                logger.info(f"Ответ ассистента (RAW) для пользователя {user_id} сохранен в БД")
            except Exception as e:
                logger.error(f"Ошибка сохранения ответа ассистента в БД: {e}")
        else:
             logger.warning(f"Не получен или пустой ответ от XAI для пользователя {user_id}")
             # Если было начальное сообщение-плейсхолдер, отредактируем его на сообщение об ошибке
             if current_message_id:
                 try:
                      await bot.edit_message_text("К сожалению, не удалось получить ответ от AI.", chat_id=chat_id, message_id=current_message_id)
                 except TelegramAPIError:
                      pass # Игнорируем ошибку, если не можем отредактировать

    except Exception as e:
        logger.exception(f"Критическая ошибка в обработчике сообщений для user_id={user_id}: {e}")
        try:
            # Пытаемся отредактировать плейсхолдер, если он был создан
            if current_message_id:
                 await bot.edit_message_text("Произошла серьезная ошибка при обработке вашего запроса.", chat_id=chat_id, message_id=current_message_id)
            else: # Иначе отправляем новое сообщение
                await message.answer("Произошла серьезная ошибка при обработке вашего запроса. Пожалуйста, попробуйте позже или используйте команду /start для сброса.")
        except TelegramAPIError:
             logger.error("Не удалось даже отправить сообщение об ошибке пользователю.")

# --- Обработчик отмены генерации ---
@dp.callback_query(F.data.startswith("cancel_generation_"))
async def cancel_generation_callback(callback: types.CallbackQuery):
    try:
        user_id_to_cancel = int(callback.data.split("_")[-1])
        requesting_user_id = callback.from_user.id

        # Проверяем, что пользователь отменяет свой собственный запрос
        if user_id_to_cancel != requesting_user_id:
            await callback.answer("Вы не можете отменить чужой запрос.", show_alert=True)
            return

        task_to_cancel = active_requests.get(user_id_to_cancel)

        if task_to_cancel:
            task_to_cancel.cancel()
            logger.info(f"Запрос на генерацию для пользователя {user_id_to_cancel} отменен пользователем.")
            await callback.answer("Генерация ответа отменена.")
            # Сообщение с прогрессом будет удалено в блоке finally/except CancelledError обработчика
            # но можно и здесь попробовать удалить кнопку или отредактировать сообщение
            try:
                await callback.message.edit_text("Генерация ответа отменена.", reply_markup=None)
            except TelegramAPIError as e:
                 logger.warning(f"Не удалось отредактировать сообщение после отмены: {e}")

        else:
            logger.warning(f"Не найден активный запрос для отмены для пользователя {user_id_to_cancel}")
            await callback.answer("Не найден активный запрос для отмены.", show_alert=True)
            # Убираем клавиатуру, если запроса уже нет
            try:
                await callback.message.edit_reply_markup(reply_markup=None)
            except TelegramAPIError:
                pass # Игнорируем ошибку, если не можем изменить

    except ValueError:
        logger.error(f"Ошибка парсинга user_id из callback_data: {callback.data}")
        await callback.answer("Ошибка обработки запроса.", show_alert=True)
    except Exception as e:
        logger.exception(f"Ошибка в cancel_generation_callback: {e}")
        await callback.answer("Произошла ошибка при отмене.", show_alert=True)


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