from __future__ import annotations
import os
import sqlite3
import logging
from datetime import datetime, timezone
from typing import List

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    AIORateLimiter,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ===================== НАЛАШТУВАННЯ =====================
# Список джерел відео: або file_id, або повні HTTPS-URL.
VIDEO_SOURCES: List[str] = [
    "BAACAgIAAxkBAAMDaJ2FmSUaqJHK8QMifzVXlBzVedQAAi59AAKhIelIj65YngFyDuk2BA",
    "BAACAgIAAxkBAAMZaJ2IJY_C-gGkKV5phQnBWEJ2pYoAAkp9AAKhIelI1LisVtq0YbA2BA",
    "BAACAgIAAxkBAAMDaJ2FmSUaqJHK8QMifzVXlBzVedQAAi59AAKhIelIj65YngFyDuk2BA",
    "BAACAgIAAxkBAAMDaJ2FmSUaqJHK8QMifzVXlBzVedQAAi59AAKhIelIj65YngFyDuk2BA",

]

DB_PATH = os.environ.get("DB_PATH", "users.db")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8101668293:AAE9nLdtt7f3C7JZ97Nt6j5NcEgBVstTjKI")
SEND_INTERVAL_SECONDS = 24 * 60 * 60  # 24 години

# Чи надсилати перший ролик одразу після /start
SEND_FIRST_IMMEDIATELY = True

# Логування
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ===================== БАЗА ДАНИХ =====================

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS users (
    chat_id INTEGER PRIMARY KEY,
    started_at TEXT NOT NULL,
    last_index INTEGER NOT NULL DEFAULT -1
);
"""

GET_ALL_USERS_SQL = "SELECT chat_id, started_at, last_index FROM users;"
UPSERT_USER_SQL = (
    "INSERT INTO users(chat_id, started_at, last_index) VALUES(?, ?, ?) "
    "ON CONFLICT(chat_id) DO UPDATE SET started_at=excluded.started_at;"
)
UPDATE_LAST_INDEX_SQL = "UPDATE users SET last_index=? WHERE chat_id=?;"
DELETE_USER_SQL = "DELETE FROM users WHERE chat_id=?;"


def get_db_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


# ===================== ЛОГІКА ВІДПРАВКИ =====================

async def send_next_video(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Надсилає користувачу наступне відео та оновлює індекс у БД.
    Викликається JobQueue-ю періодично для кожного користувача.
    """
    job = context.job
    chat_id = job.chat_id

    if not VIDEO_SOURCES:
        logger.warning("VIDEO_SOURCES порожній. Нічого надсилати.")
        return

    # Витягуємо поточний індекс з БД
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT last_index FROM users WHERE chat_id=?;", (chat_id,))
        row = cur.fetchone()
        if row is None:
            logger.info("Користувача %s немає в БД, скасовуємо job.", chat_id)
            job.schedule_removal()
            return
        last_index = row[0]
        next_index = last_index + 1

        if next_index >= len(VIDEO_SOURCES):
            # Всі відео надіслані — можна або зупинитись, або почати знову.
            # Варіант 1: зупиняємось
            # job.schedule_removal()
            # return
            # Варіант 2: крутимо по колу
            next_index = 0

        source = VIDEO_SOURCES[next_index]

        # Визначаємо, це file_id чи URL
        if source.startswith("http://") or source.startswith("https://"):
            await context.bot.send_video(chat_id=chat_id, video=source)
        else:
            # Вважаємо, що це file_id
            await context.bot.send_video(chat_id=chat_id, video=source)

        # Оновлюємо індекс
        cur.execute(UPDATE_LAST_INDEX_SQL, (next_index, chat_id))
        conn.commit()
    except Exception:
        logger.exception("Помилка при відправці відео користувачу %s", chat_id)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def schedule_user_job(context: ContextTypes.DEFAULT_TYPE, chat_id: int, first_in: float) -> None:
    """Планує періодичне надсилання для конкретного користувача."""
    # Унікальне ім'я job для цього чату
    name = f"daily_video_{chat_id}"
    # Якщо job вже існує — приберемо її, щоб не дублювалось
    for j in context.job_queue.get_jobs_by_name(name):
        j.schedule_removal()

    context.job_queue.run_repeating(
        send_next_video,
        interval=SEND_INTERVAL_SECONDS,
        first=first_in,
        chat_id=chat_id,
        name=name,
    )


# ===================== ХЕНДЛЕРИ КОМАНД =====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    chat_id = update.effective_chat.id

    # Реєструємо користувача в БД (last_index = -1 — ще нічого не надіслано)
    conn = get_db_conn()
    with conn:
        conn.execute(
            UPSERT_USER_SQL,
            (chat_id, datetime.now(timezone.utc).isoformat(), -1),
        )
    conn.close()

    # Плануємо надсилання: перше — одразу (0сек), або через 24год
    first_in = 0 if SEND_FIRST_IMMEDIATELY else SEND_INTERVAL_SECONDS
    schedule_user_job(context, chat_id, first_in)

    await update.message.reply_text(
        (
            "Вітаю, {name}! Я надсилатиму тобі по одному відео щодня.\n"
            "Команди: /status — прогрес, /stop — зупинити, /help — довідка"
        ).format(name=user.first_name or "друже")
    )


async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    name = f"daily_video_{chat_id}"
    jobs = context.job_queue.get_jobs_by_name(name)
    for j in jobs:
        j.schedule_removal()

    # За бажанням — видаляємо користувача з БД
    conn = get_db_conn()
    with conn:
        conn.execute(DELETE_USER_SQL, (chat_id,))
    conn.close()

    await update.message.reply_text("Зупинив розсилку й видалив твій прогрес. Повернутись: /start")


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT started_at, last_index FROM users WHERE chat_id=?;", (chat_id,))
    row = cur.fetchone()
    conn.close()

    if row is None:
        await update.message.reply_text("Поки що ти не підписаний. Натисни /start")
        return

    started_at, last_index = row
    total = len(VIDEO_SOURCES)
    sent = max(0, last_index + 1)
    remaining = max(0, total - sent)

    await update.message.reply_text(
        (
            "Старт: <code>{start}</code>\n"
            "Надіслано відео: <b>{sent}</b> із <b>{total}</b>\n"
            "Залишилось: <b>{remaining}</b>\n"
            "(інтервал: кожні {hours} годин)"
        ).format(
            start=started_at,
            sent=sent,
            total=total,
            remaining=remaining,
            hours=SEND_INTERVAL_SECONDS // 3600,
        ),
        parse_mode=ParseMode.HTML,
    )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        (
            "Я надсилаю по одному відео щодня після /start.\n\n"
            "Як користуватись:\n"
            "1) Додай свої file_id або HTTPS-URL у VIDEO_SOURCES у коді.\n"
            "2) Запусти бота з змінною середовища BOT_TOKEN.\n"
            "3) /start — підписатися, /stop — відписатися, /status — перевірити прогрес.\n\n"
            "Поради:\n"
            "• Якщо хочеш починати щодня о певній годині — легше ставити перший запуск з затримкою до потрібного часу, або використовуй cron-завдання зовні.\n"
            "• Бот витримує рестарти — прогрес у БД, розклад відновлюється."
        )
    )


# Ехо-хендлер (необов'язково): зручно перехопити file_id відео від адміна
async def echo_video(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message and update.message.video:
        file_id = update.message.video.file_id
        await update.message.reply_text(f"Отримав file_id: <code>{file_id}</code>", parse_mode=ParseMode.HTML)


# ===================== ІНІЦІАЛІЗАЦІЯ APP =====================

async def post_init(app: Application) -> None:
    # Створюємо БД та таблицю
    conn = get_db_conn()
    with conn:
        conn.execute(CREATE_TABLE_SQL)
    conn.close()

    # Відновлюємо задачі для всіх існуючих користувачів
    # Почнемо через 10 секунд, щоб не спамити одразу після рестарту
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(GET_ALL_USERS_SQL)
    rows = cur.fetchall()
    conn.close()

    for chat_id, started_at, last_index in rows:
        # Плануємо чергове надсилання через повну добу відтепер
        # (простий варіант; можна обчислити точний офсет від started_at)
        app.job_queue.run_repeating(
            send_next_video,
            interval=SEND_INTERVAL_SECONDS,
            first=10,  # почнемо через 10 секунд після старту бота
            chat_id=chat_id,
            name=f"daily_video_{chat_id}",
        )
        logger.info("Відновив розсилку для chat_id=%s (last_index=%s)", chat_id, last_index)


def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("Не задано BOT_TOKEN у змінній середовища.")

    application = (
        Application.builder()
        .token(BOT_TOKEN)
        # .rate_limiter(AIORateLimiter())  # вимкнено
        .post_init(post_init)
        .build()
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("stop", stop))
    application.add_handler(CommandHandler("status", status_cmd))
    application.add_handler(CommandHandler("help", help_cmd))

    # Ехо для відео: повертає file_id (можна прибрати в продакшені)
    application.add_handler(MessageHandler(filters.VIDEO & filters.ChatType.PRIVATE, echo_video))

    application.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
