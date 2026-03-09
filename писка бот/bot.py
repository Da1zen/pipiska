import asyncio
import logging
import os
import random
import sqlite3
from datetime import datetime, timedelta, time
from pathlib import Path

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ChatType, ChatMemberStatus
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent

# Явно загружаем .env из папки с ботом
load_dotenv(BASE_DIR / ".env")

BOT_TOKEN = os.getenv("BOT_TOKEN")

# Дополнительная подстраховка: если по какой‑то причине python-dotenv не
# подхватил переменную (например, из‑за кодировки файла), читаем .env вручную.
if not BOT_TOKEN:
    env_path = BASE_DIR / ".env"
    if env_path.exists():
        try:
            with env_path.open(encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if line.startswith("BOT_TOKEN="):
                        BOT_TOKEN = line.split("=", 1)[1].strip()
                        break
        except Exception:
            pass

if not BOT_TOKEN:
    raise RuntimeError("Не задан BOT_TOKEN в .env")


DB_PATH = os.path.join("data", "bot.db")

# Тексты рекламных сообщений можно менять по своему желанию.
AD_TEXTS = [
    "",
    "",
]

# Время отправки рекламы по Москве (Europe/Moscow).
AD_TIMES = [
    time(hour=12, minute=0),  # 12:00 по Москве
    time(hour=20, minute=0),  # 20:00 по Москве
]

TIMEZONE = "Europe/Moscow"


CREATE_USERS_TABLE = """
CREATE TABLE IF NOT EXISTS users (
    chat_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    length INTEGER NOT NULL DEFAULT 0,
    best_length INTEGER NOT NULL DEFAULT 0,
    games_played INTEGER NOT NULL DEFAULT 0,
    last_play TIMESTAMP,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (chat_id, user_id)
);
"""

CREATE_CHATS_TABLE = """
CREATE TABLE IF NOT EXISTS chats (
    chat_id INTEGER PRIMARY KEY,
    title TEXT,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
"""


async def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(CREATE_USERS_TABLE)
        conn.execute(CREATE_CHATS_TABLE)
        conn.commit()
    finally:
        conn.close()

    # Чистим возможные старые тестовые данные (вымышленные игроки)
    cleanup_fake_players()


def cleanup_fake_players() -> None:
    """Удаляем вымышленных игроков (chat_id = 0 или user_id < 0), если они есть."""
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("DELETE FROM users WHERE chat_id = 0 OR user_id < 0")
        conn.commit()
    finally:
        conn.close()


async def register_chat(message: Message) -> None:
    if message.chat.type not in {ChatType.GROUP, ChatType.SUPERGROUP}:
        return

    conn = sqlite3.connect(DB_PATH)
    try:
        now = datetime.utcnow()
        conn.execute(
            """
            INSERT INTO chats (chat_id, title, is_active, created_at, updated_at)
            VALUES (?, ?, 1, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                title = excluded.title,
                is_active = 1,
                updated_at = excluded.updated_at
            """,
            (message.chat.id, message.chat.title, now, now),
        )
        conn.commit()
    finally:
        conn.close()


async def can_play(chat_id: int, user_id: int) -> tuple[bool, int | None]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.execute(
            "SELECT last_play FROM users WHERE chat_id = ? AND user_id = ?",
            (chat_id, user_id),
        )
        row = cursor.fetchone()
        cursor.close()
    finally:
        conn.close()

    if row is None or row["last_play"] is None:
        return True, None

    last_play = datetime.fromisoformat(row["last_play"])
    next_time = last_play + timedelta(hours=24)
    if datetime.utcnow() >= next_time:
        return True, None

    remaining = int((next_time - datetime.utcnow()).total_seconds())
    return False, remaining


def format_remaining(seconds: int) -> str:
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    return f"{hours} ч {minutes} мин"


async def update_user_after_dick(message: Message, delta: int) -> int:
    chat_id = message.chat.id
    user_id = message.from_user.id
    username = message.from_user.username
    first_name = message.from_user.first_name
    last_name = message.from_user.last_name

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.execute(
            "SELECT length, best_length, games_played FROM users WHERE chat_id = ? AND user_id = ?",
            (chat_id, user_id),
        )
        row = cursor.fetchone()
        cursor.close()

        now = datetime.utcnow()

        if row is None:
            length = delta
            best_length = max(0, length)
            games_played = 1
            conn.execute(
                """
                INSERT INTO users (
                    chat_id, user_id, username, first_name, last_name,
                    length, best_length, games_played, last_play,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    chat_id,
                    user_id,
                    username,
                    first_name,
                    last_name,
                    length,
                    best_length,
                    games_played,
                    now.isoformat(),
                    now,
                    now,
                ),
            )
        else:
            length = row["length"] + delta
            best_length = max(row["best_length"], length)
            games_played = row["games_played"] + 1
            conn.execute(
                """
                UPDATE users
                SET username = ?,
                    first_name = ?,
                    last_name = ?,
                    length = ?,
                    best_length = ?,
                    games_played = ?,
                    last_play = ?,
                    updated_at = ?
                WHERE chat_id = ? AND user_id = ?
                """,
                (
                    username,
                    first_name,
                    last_name,
                    length,
                    best_length,
                    games_played,
                    now.isoformat(),
                    now,
                    chat_id,
                    user_id,
                ),
            )

        conn.commit()
    finally:
        conn.close()

    return length


async def get_chat_top(chat_id: int, limit: int = 10):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.execute(
            """
            SELECT username, first_name, last_name, length
            FROM users
            WHERE chat_id = ?
            ORDER BY length DESC
            LIMIT ?
            """,
            (chat_id, limit),
        )
        rows = cursor.fetchall()
        cursor.close()
        return rows
    finally:
        conn.close()


async def get_global_top(limit: int = 10):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.execute(
            """
            SELECT username, first_name, last_name, length
            FROM users
            ORDER BY length DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = cursor.fetchall()
        cursor.close()
        return rows
    finally:
        conn.close()


async def get_user_stats(chat_id: int, user_id: int):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.execute(
            """
            SELECT length, best_length, games_played, last_play
            FROM users
            WHERE chat_id = ? AND user_id = ?
            """,
            (chat_id, user_id),
        )
        row = cursor.fetchone()
        cursor.close()
        return row
    finally:
        conn.close()


async def is_chat_admin(message: Message) -> bool:
    """Проверяем, является ли пользователь админом текущего чата."""
    bot = message.bot
    member = await bot.get_chat_member(message.chat.id, message.from_user.id)
    return member.status in {ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR}


async def ensure_admin_in_group(message: Message) -> bool:
    """Проверка, что команда выполняется в группе и от администратора."""
    if message.chat.type not in {ChatType.GROUP, ChatType.SUPERGROUP}:
        await message.answer("Эта команда работает только в групповых чатах.")
        return False

    if not await is_chat_admin(message):
        await message.reply("Эта команда доступна только администраторам чата.")
        return False

    return True


async def set_user_length_admin(
    chat_id: int,
    user_id: int,
    username: str | None,
    first_name: str | None,
    last_name: str | None,
    new_length: int,
) -> int:
    """Установка размера участника админом."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.execute(
            "SELECT best_length, games_played FROM users WHERE chat_id = ? AND user_id = ?",
            (chat_id, user_id),
        )
        row = cursor.fetchone()
        cursor.close()

        now = datetime.utcnow()

        if row is None:
            best_length = new_length
            games_played = 0
            conn.execute(
                """
                INSERT INTO users (
                    chat_id, user_id, username, first_name, last_name,
                    length, best_length, games_played, last_play,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    chat_id,
                    user_id,
                    username,
                    first_name,
                    last_name,
                    new_length,
                    best_length,
                    games_played,
                    None,
                    now,
                    now,
                ),
            )
        else:
            best_length = max(row["best_length"], new_length)
            games_played = row["games_played"]
            conn.execute(
                """
                UPDATE users
                SET username = ?,
                    first_name = ?,
                    last_name = ?,
                    length = ?,
                    best_length = ?,
                    games_played = ?,
                    updated_at = ?
                WHERE chat_id = ? AND user_id = ?
                """,
                (
                    username,
                    first_name,
                    last_name,
                    new_length,
                    best_length,
                    games_played,
                    now,
                    chat_id,
                    user_id,
                ),
            )

        conn.commit()
    finally:
        conn.close()

    return new_length


def display_name(username: str | None, first_name: str | None, last_name: str | None) -> str:
    if username:
        return f"@{username}"
    parts = [p for p in [first_name, last_name] if p]
    return " ".join(parts) if parts else "Безымянный"


async def cmd_start(message: Message) -> None:
    if message.chat.type not in {ChatType.GROUP, ChatType.SUPERGROUP}:
        await message.answer(
            "Привет! я линейка — бот для чатов (групп)\n\n"
            "Смысл бота: бот работает только в чатах. Раз в 24 часа игрок "
            "может прописать команду /dick, где в ответ получит от бота рандомное число.\n\n"
            "Рандом работает от -5 см до +10 см.\n\n"
            "Если у тебя есть вопросы — пиши команду: /help"
        )
        return

    await register_chat(message)

    text = (
        "Привет! я линейка — бот для чатов (групп)\n\n"
        "Смысл бота: бот работает только в чатах. Раз в 24 часа игрок "
        "может прописать команду /dick, где в ответ получит от бота рандомное число.\n\n"
        "Рандом работает от -5 см до +10 см.\n\n"
        "Если у тебя есть вопросы — пиши команду: /help"
    )
    await message.answer(text)


async def cmd_help(message: Message) -> None:
    await register_chat(message)

    text = (
        "Команды бота:\n"
        "/dick — Вырастить/уменьшить пипису\n"
        "/top_dick — Топ 10 пипис чата\n"
        "/stats — Статистика в виде картинки\n"
        "/global_top — Глобальный Топ 10 игроков\n"
        "/buy — Покупка доп. попыток\n\n"
        "Контакты:\n"
        "Наш канал — @pipisa_news\n"
        "Наш чат — https://t.me/+Vc5u7PMtm543YWWi\n"
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Наш канал", url="https://t.me/pipisa_news"
                )
            ],
            [
                InlineKeyboardButton(
                    text="Наш чат", url="https://t.me/+Vc5u7PMtm543YWWi"
                )
            ],
        ]
    )
    await message.answer(text, reply_markup=kb)


async def cmd_dick(message: Message) -> None:
    if message.chat.type not in {ChatType.GROUP, ChatType.SUPERGROUP}:
        await message.answer("Бот работает только в чатах.")
        return

    await register_chat(message)

    can, remaining = await can_play(message.chat.id, message.from_user.id)
    if not can:
        await message.reply(
            f"Ты уже использовал /dick за последние 24 часа.\n"
            f"Следующая попытка через {format_remaining(remaining)}."
        )
        return

    delta = random.randint(-5, 10)
    new_length = await update_user_after_dick(message, delta)

    if delta >= 0:
        change_text = f"твой писюн вырос на {delta} см."
    else:
        change_text = f"твой писюн уменьшился на {abs(delta)} см."

    await message.reply(
        f"{message.from_user.first_name}, {change_text}\n"
        f"Теперь он равен {new_length} см.\n"
        "Следующая попытка завтра!"
    )


async def cmd_top_dick(message: Message) -> None:
    if message.chat.type not in {ChatType.GROUP, ChatType.SUPERGROUP}:
        await message.answer("Команда доступна только в группах.")
        return

    await register_chat(message)

    rows = await get_chat_top(message.chat.id)
    if not rows:
        await message.reply("Здесь пока никто не играл. Используй /dick, чтобы начать!")
        return

    lines = ["Топ 10 игроков 🔝"]
    for idx, row in enumerate(rows, start=1):
        name = display_name(row["username"], row["first_name"], row["last_name"])
        lines.append(f"{idx}) {name} — {row['length']} см.")

    await message.reply("\n".join(lines))


async def cmd_global_top(message: Message) -> None:
    rows = await get_global_top()
    if not rows:
        await message.reply("Ещё никто не играл. Используйте /dick в чатах, чтобы попасть в глобальный топ!")
        return

    lines = ["Глобальный Топ 10 игроков 🌍"]
    for idx, row in enumerate(rows, start=1):
        name = display_name(row["username"], row["first_name"], row["last_name"])
        lines.append(f"{idx}) {name} — {row['length']} см.")

    await message.reply("\n".join(lines))


async def cmd_stats(message: Message) -> None:
    if message.chat.type not in {ChatType.GROUP, ChatType.SUPERGROUP}:
        await message.answer("Команда доступна только в группах.")
        return

    await register_chat(message)

    row = await get_user_stats(message.chat.id, message.from_user.id)
    if row is None:
        await message.reply("У тебя ещё нет статистики. Используй /dick, чтобы начать игру!")
        return

    last_play = (
        datetime.fromisoformat(row["last_play"]).strftime("%d.%m.%Y %H:%M")
        if row["last_play"]
        else "никогда"
    )

    text = (
        f"Статистика для {display_name(message.from_user.username, message.from_user.first_name, message.from_user.last_name)}\n\n"
        f"Текущий размер: {row['length']} см.\n"
        f"Лучший результат: {row['best_length']} см.\n"
        f"Игр сыграно: {row['games_played']}\n"
        f"Последняя попытка: {last_play}\n"
    )
    await message.reply(text)


async def cmd_buy(message: Message) -> None:
    await register_chat(message)
    await message.reply(
        "Здесь могла бы быть покупка дополнительных попыток.\n"
        "Пока что функция не реализована и служит заглушкой."
    )


async def cmd_set_dick(message: Message) -> None:
    """
    Админ-команда: установить размер участника.
    Использование: ответь на сообщение участника командой `/set_dick 150`
    """
    if not await ensure_admin_in_group(message):
        return

    if not message.reply_to_message:
        # Ничего не пишем в чат, чтобы не палиться
        try:
            await message.delete()
        except Exception:
            pass
        # Пытаемся отправить подсказку в личку админу
        try:
            await message.bot.send_message(
                message.from_user.id,
                "Для скрытого изменения результата ответь командой /set_dick N на сообщение участника в группе.\n"
                "Пример: /set_dick 150",
            )
        except Exception:
            pass
        return

    parts = message.text.split()
    if len(parts) < 2:
        try:
            await message.delete()
        except Exception:
            pass
        try:
            await message.bot.send_message(
                message.from_user.id,
                "Нужно указать новый размер.\nПример: /set_dick 150 (в ответ на сообщение участника).",
            )
        except Exception:
            pass
        return

    try:
        new_length = int(parts[1])
    except ValueError:
        try:
            await message.delete()
        except Exception:
            pass
        try:
            await message.bot.send_message(
                message.from_user.id,
                "Размер должен быть целым числом.\nПример: /set_dick 150",
            )
        except Exception:
            pass
        return

    target = message.reply_to_message.from_user
    chat_id = message.chat.id

    new_value = await set_user_length_admin(
        chat_id=chat_id,
        user_id=target.id,
        username=target.username,
        first_name=target.first_name,
        last_name=target.last_name,
        new_length=new_length,
    )

    # Удаляем команду из чата, чтобы никто не видел, что админ менял результат
    try:
        await message.delete()
    except Exception:
        pass

    # Подтверждение отправляем в личку админу
    try:
        await message.bot.send_message(
            message.from_user.id,
            f"Результат изменён скрытно.\n"
            f"Чат: {message.chat.title or message.chat.id}\n"
            f"Игрок: {display_name(target.username, target.first_name, target.last_name)}\n"
            f"Новый размер: {new_value} см.",
        )
    except Exception:
        # Если нет лички с ботом, просто молча ничего не делаем
        pass


async def send_ads(bot: Bot) -> None:
    logger.info("Запуск рассылки рекламы")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.execute(
            "SELECT chat_id FROM chats WHERE is_active = 1"
        )
        rows = cursor.fetchall()
        cursor.close()
    finally:
        conn.close()

    if not rows:
        logger.info("Нет активных чатов для рекламы")
        return

    text = random.choice(AD_TEXTS)
    for row in rows:
        chat_id = row["chat_id"]
        try:
            await bot.send_message(chat_id, text)
        except Exception as e:
            logger.warning("Не удалось отправить рекламу в чат %s: %s", chat_id, e)


def schedule_ads(scheduler: AsyncIOScheduler, bot: Bot) -> None:
    for t in AD_TIMES:
        scheduler.add_job(
            send_ads,
            "cron",
            hour=t.hour,
            minute=t.minute,
            args=[bot],
            timezone=TIMEZONE,
        )


async def main() -> None:
    await init_db()

    bot = Bot(BOT_TOKEN, parse_mode="HTML")
    dp = Dispatcher()

    dp.message.register(cmd_start, CommandStart())
    dp.message.register(cmd_help, Command("help"))
    # Одна и та же команда обрабатывает /dick и /dik
    dp.message.register(cmd_dick, Command(commands=["dick", "dik"]))
    dp.message.register(cmd_top_dick, Command("top_dick"))
    dp.message.register(cmd_global_top, Command("global_top"))
    dp.message.register(cmd_stats, Command("stats"))
    dp.message.register(cmd_buy, Command("buy"))
    dp.message.register(cmd_set_dick, Command("set_dick"))

    scheduler = AsyncIOScheduler()
    schedule_ads(scheduler, bot)
    scheduler.start()

    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())

