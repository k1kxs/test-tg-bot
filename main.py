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
    """
    if not text:
        return ""

    # 1. Конвертация Markdown в HTML (порядок важен)
    try:
        # Блоки кода (самые приоритетные из-за символов внутри)
        text = re.sub(r"^\s*```([^\n]*)\n(.*?)\n```", r"<pre>\2</pre>", text, flags=re.DOTALL | re.MULTILINE)
        text = re.sub(r"^\s*```()\n(.*?)\n```", r"<pre>\2</pre>", text, flags=re.DOTALL | re.MULTILINE) # Без языка

        # Встроенный код
        text = re.sub(r"`([^`\n]+)`", r"<code>\1</code>", text)

        # Заголовки (просто жирный текст)
        text = re.sub(r"^\s*#{1,6}\s+(.+)", r"<b>\1</b>", text, flags=re.MULTILINE)

        # Жирный и подчеркнутый
        text = re.sub(r"\*\*(.*?)\*\*", r"<b>\1</b>", text)
        text = re.sub(r"__(.*?)__", r"<u>\1</u>", text) # Используем <u> для подчеркивания

        # Курсив
        text = re.sub(r"\*(.*?)\*", r"<i>\1</i>", text)
        text = re.sub(r"_(.*?)_", r"<i>\1</i>", text) # Используем <i> для курсива

        # Зачеркнутый
        text = re.sub(r"~~(.*?)~~", r"<s>\1</s>", text)

        # Списки (простая замена на маркер)
        text = re.sub(r"^\s*[-*+]\s+", "• ", text, flags=re.MULTILINE)

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
        # В случае ошибки продолжаем с оригинальным текстом, очистка HTML сработает позже

    # 2. Финальная очистка HTML
    try:
        # Сначала экранируем ВСЁ
        escaped_text = html.escape(text, quote=False)

        # Функция для разэкранирования только валидных тегов
        def unescape_valid_tag(match):
            tag = match.group(0)
            unescaped_tag = html.unescape(tag)
            # Используем pre-compiled паттерн
            if ALLOWED_TAGS_PATTERN.fullmatch(unescaped_tag):
                return unescaped_tag
            else:
                # Если невалидный (например, стал &lt;table&gt;), оставляем экранированным
                return tag

        # Паттерн для поиска экранированных ВАЛИДНЫХ тегов Telegram
        # Этот паттерн ищет &lt;b&gt;, &lt;a href...&gt; и т.д.
        potential_escaped_tag_pattern = re.compile(
             r"&lt;/?(?:b|i|u|s|tg-spoiler|code|pre|a(?:\s+href\s*=\s*(?:\"[^\"]*\"|'[^\']*'))?)\s*&gt;/?",
             re.IGNORECASE
        )

        cleaned_text = potential_escaped_tag_pattern.sub(unescape_valid_tag, escaped_text)

    except Exception as e:
        logging.error(f"Ошибка на этапе финальной очистки HTML: {e}")
        # Безопасный fallback - полностью экранированный текст
        cleaned_text = html.escape(text, quote=False)

    return cleaned_text

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

# Функция для получения ответа от DeepSeek API
async def get_deepseek_response(api_key: str, system_prompt: str, history: list[dict]) -> str | None:
    if not history:
        logging.warning("История сообщений пуста")
        
    # Формируем сообщения для API
    messages = [{'role': 'system', 'content': system_prompt}]
    for msg in history:
        if msg.get('role') and msg.get('content'):
            messages.append({'role': msg['role'], 'content': msg['content']})
    
    try:
        # Увеличиваем таймауты для HTTP соединения
        timeout = aiohttp.ClientTimeout(total=180, sock_read=150)
        
        logging.info(f"Отправка запроса к DeepSeek API с {len(messages)} сообщениями")
        
        async with aiohttp.ClientSession(timeout=timeout) as session:
            url = "https://api.deepseek.com/v1/chat/completions"
            headers = {
                "Authorization": f"Bearer {api_key}", 
                "Content-Type": "application/json"
            }
            payload = {
                "model": "deepseek-chat", 
                "messages": messages,
                "temperature": 0.7,  # Возвращаем стандартное значение
                "max_tokens": 4096  # Уменьшаем до ранее работавшего значения
            }
            
            try:
                # Использем метод текстового ответа вместо JSON для избежания проблем с парсингом
                async with session.post(url, headers=headers, json=payload, raise_for_status=True) as response:
                    if response.status == 200:
                        # Читаем текст по частям для избежания таймаутов
                        chunks = []
                        async for chunk in response.content.iter_chunked(1024):
                            chunks.append(chunk)
                        
                        # Объединяем чанки в один текст
                        raw_response = b''.join(chunks).decode('utf-8')
                        
                        try:
                            data = json.loads(raw_response)
                            
                            if not data.get('choices') or not data['choices']:
                                logging.error(f"В ответе API отсутствует поле 'choices': {data}")
                                return None
                            
                            message = data['choices'][0].get('message', {})
                            content = message.get('content')
                            
                            if not content:
                                logging.error(f"В ответе API отсутствует контент: {message}")
                                return None
                            
                            logging.info("Успешно получен ответ от DeepSeek API")
                            return content
                        except json.JSONDecodeError as e:
                            logging.error(f"Ошибка декодирования JSON: {e}, ответ API: {raw_response[:200]}...")
                            return None
                    else:
                        error_text = await response.text()
                        logging.error(f"Ошибка API: статус {response.status}, ответ: {error_text}")
                        return None
            except aiohttp.ClientResponseError as e:
                logging.error(f"Ошибка ответа API: {e}")
                return None
            except aiohttp.ClientPayloadError as e:
                logging.error(f"Ошибка загрузки данных от API: {e}")
                return None
            except asyncio.TimeoutError:
                logging.error("Превышено время ожидания ответа от API")
                return None
    except aiohttp.ClientConnectorError as e:
        logging.error(f"Ошибка подключения к API: {e}")
        return None
    except Exception as e:
        logging.exception(f"Неожиданная ошибка при обращении к DeepSeek API: {e}")
        return None

# Обработчик команды /start
@dp.message(Command("start"))
async def start_handler(message: types.Message):
    # Создаем клавиатуру с кнопкой очистки истории
    builder = InlineKeyboardBuilder()
    builder.button(text="Очистить историю", callback_data="clear_history")
    
    await message.answer("Привет! Я медицинский ассистент. Задайте ваш вопрос.", reply_markup=builder.as_markup())

# Обработчик текстовых сообщений
@dp.message(F.text)
async def message_handler(message: types.Message):
    user_id = message.from_user.id
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
        
        # Получаем ответ от DeepSeek API
        response_text = await get_deepseek_response(settings.DEEPSEEK_API_KEY, SYSTEM_PROMPT, history)
        
        if response_text:
            # Очищаем ответ перед сохранением и отправкой
            cleaned_response_text = await clean_html_for_telegram(response_text)
            
            # Сохраняем ОРИГИНАЛЬНЫЙ ответ ассистента
            await add_message_to_db(db, user_id, "assistant", response_text)
            logging.info(f"Ответ ассистента для пользователя {user_id} сохранен (оригинал)")

            # Работаем дальше с ОЧИЩЕННЫМ текстом
            if len(cleaned_response_text) > TELEGRAM_MAX_LENGTH:
                logging.info(f"Очищенный ответ слишком длинный ({len(cleaned_response_text)} символов), разделяю на части.")
                parts = []
                current_part = ""
                # Делим ОЧИЩЕННЫЙ текст
                for paragraph in cleaned_response_text.split('\n\n'):
                    # Если добавление параграфа не превышает лимит
                    if len(current_part) + len(paragraph) + 2 <= TELEGRAM_MAX_LENGTH:
                        current_part += paragraph + "\n\n"
                    else:
                        # Если сам параграф слишком длинный, делим его грубо
                        if len(paragraph) > TELEGRAM_MAX_LENGTH:
                            if current_part.strip(): # Добавляем накопленное
                                parts.append(current_part.strip())
                            current_part = "" # Сбрасываем текущую часть
                            # Делим длинный параграф
                            for i in range(0, len(paragraph), TELEGRAM_MAX_LENGTH):
                                parts.append(paragraph[i:i + TELEGRAM_MAX_LENGTH])
                        else:
                            # Добавляем накопленную часть и начинаем новую
                            if current_part.strip():
                                parts.append(current_part.strip())
                            current_part = paragraph + "\n\n"

                if current_part.strip(): # Добавляем последнюю часть
                    parts.append(current_part.strip())

                for i, part in enumerate(parts):
                    logging.info(f"Отправка очищенной части {i+1}/{len(parts)} пользователю {user_id}")
                    try:
                        # Отправляем очищенную часть
                        await message.answer(part, parse_mode='HTML')
                        await asyncio.sleep(0.5) # Небольшая задержка
                    except Exception as send_error:
                        logging.error(f"Ошибка отправки части {i+1}: {send_error}")
                        await message.answer(f"Ошибка при отправке части {i+1} ответа.")
                        # Возможно, стоит прервать отправку остальных частей
                        break
            else:
                # Отправляем очищенный ответ пользователю целиком
                await message.answer(cleaned_response_text, parse_mode='HTML')

        else:
            # Отправляем сообщение об ошибке
            logging.warning(f"Не удалось получить ответ от DeepSeek API для пользователя {user_id}")
            await message.answer("Извините, произошла ошибка при обработке вашего запроса. Пожалуйста, попробуйте позже.")
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

# Запуск бота
if __name__ == "__main__":
    asyncio.run(main())
