import os
import re
import html
import time
import ftplib
import sqlite3
import logging
import asyncio
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import a2s
from dotenv import load_dotenv
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# Налаштування логування
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.FileHandler("all_bot.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

load_dotenv()

# Telegram
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ALLOWED_CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID", "0"))

# Ігровий сервер CS:S
SERVER_IP = os.getenv("SERVER_IP", "195.3.223.128")
SERVER_PORT = int(os.getenv("SERVER_PORT", "27028"))
MAX_PLAYERS = int(os.getenv("MAX_PLAYERS", "16"))
MAP_DURATION_SEC = 40 * 60

# FTP для завантаження баз статистики
FTP_HOST = os.getenv("FTP_HOST", "")
FTP_PORT = int(os.getenv("FTP_PORT", "21"))
FTP_USER = os.getenv("FTP_USER", "")
FTP_PASS = os.getenv("FTP_PASS", "")
FTP_REMOTE_DIR = os.getenv("FTP_REMOTE_DIR", "/cstrike/addons/sourcemod/data/sqlite")
FTP_TLS = os.getenv("FTP_TLS", "0") == "1"

LR_FILE = os.getenv("LR_FILE", "lr_base.sq3")
KNIFE_FILE = os.getenv("KNIFE_FILE", "knife_top.sq3")
DB_TABLE = os.getenv("DB_TABLE", "lvl_base")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_LR_PATH = os.path.join(BASE_DIR, "cache_lr.sq3")
LOCAL_KNIFE_PATH = os.path.join(BASE_DIR, "cache_knife.sq3")
CACHE_TTL_SEC = 180

_last_sync_time: float = 0.0
_sync_lock = asyncio.Lock()


class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, format, *args):
        pass


def start_dummy_server() -> None:
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
    logger.info(f"HTTP-заглушка запущена на порту {port}")
    server.serve_forever()


class ServerTracker:
    last_map: str | None = None
    map_start_time: float | None = None
    max_players_this_map: dict[str, int] = {}

    @staticmethod
    def clean_name(text: str) -> str:
        if not text:
            return ""
        return re.sub(r"\^\d", "", text).strip()

    @classmethod
    def get_time_remaining(cls, current_map: str) -> str:
        now = time.time()
        if cls.last_map != current_map:
            cls.last_map = current_map
            cls.map_start_time = now
            cls.max_players_this_map[current_map] = 0

        elapsed = now - (cls.map_start_time or now)
        remaining_sec = max(0, MAP_DURATION_SEC - elapsed)
        minutes = int(remaining_sec // 60)
        seconds = int(remaining_sec % 60)
        return f"{minutes} хв. {seconds} сек."

    @classmethod
    async def fetch_server_data(cls) -> dict | None:
        address = (SERVER_IP, SERVER_PORT)
        try:
            info, players = await asyncio.gather(
                a2s.ainfo(address, timeout=4.0),
                a2s.aplayers(address, timeout=4.0),
            )
            return {"info": info, "players": players}
        except Exception as e:
            logger.error(f"Помилка опитування сервера {SERVER_IP}:{SERVER_PORT} -> {e}")
            return None


def _download_ftp_files_sync() -> bool:
    if not FTP_HOST or not FTP_USER:
        logger.error("FTP_HOST або FTP_USER не вказані у .env!")
        return False

    ftp = ftplib.FTP_TLS() if FTP_TLS else ftplib.FTP()
    try:
        ftp.connect(FTP_HOST, FTP_PORT, timeout=10)
        ftp.login(FTP_USER, FTP_PASS)
        if FTP_TLS:
            ftp.prot_p()
        ftp.set_pasv(True)
        if FTP_REMOTE_DIR:
            ftp.cwd(FTP_REMOTE_DIR)

        downloaded_any = False
        for remote_name, local_path in [(LR_FILE, LOCAL_LR_PATH), (KNIFE_FILE, LOCAL_KNIFE_PATH)]:
            if not remote_name:
                continue
            tmp_path = local_path + ".tmp"
            try:
                with open(tmp_path, "wb") as f:
                    ftp.retrbinary(f"RETR {remote_name}", f.write)
                os.replace(tmp_path, local_path)
                downloaded_any = True
            except Exception as file_err:
                logger.warning(f"Не вдалося завантажити {remote_name}: {file_err}")
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)

        return downloaded_any
    except Exception as e:
        logger.error(f"Помилка FTP підключення: {e}")
        return False
    finally:
        try:
            ftp.quit()
        except Exception:
            pass


async def sync_databases(force: bool = False) -> bool:
    global _last_sync_time
    now = time.time()
    has_cache = os.path.exists(LOCAL_LR_PATH) or os.path.exists(LOCAL_KNIFE_PATH)

    if not force and has_cache and (now - _last_sync_time < CACHE_TTL_SEC):
        return True

    async with _sync_lock:
        if not force and (os.path.exists(LOCAL_LR_PATH) or os.path.exists(LOCAL_KNIFE_PATH)) and (time.time() - _last_sync_time < CACHE_TTL_SEC):
            return True
        ok = await asyncio.to_thread(_download_ftp_files_sync)
        if ok:
            _last_sync_time = time.time()
        return ok or has_cache


def get_medal(place: int) -> str:
    if place == 1:
        return "🥇"
    if place == 2:
        return "🥈"
    if place == 3:
        return "🥉"
    return "🏅"


def query_lr_top() -> list[tuple[str, int, float]]:
    if not os.path.exists(LOCAL_LR_PATH):
        return []
    conn = sqlite3.connect(LOCAL_LR_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT name, value, kills, deaths FROM {DB_TABLE} ORDER BY value DESC LIMIT 10"
        )
        rows = cur.fetchall()
        result = []
        for name, value, kills, deaths in rows:
            clean = ServerTracker.clean_name(name) or "Гравець"
            kd = round(kills / deaths, 2) if deaths and deaths > 0 else float(kills or 0)
            result.append((clean, int(value or 0), kd))
        return result
    finally:
        conn.close()


def query_knife_top() -> list[tuple[str, int]]:
    if not os.path.exists(LOCAL_KNIFE_PATH):
        return []
    conn = sqlite3.connect(LOCAL_KNIFE_PATH)
    try:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        tables = [r[0] for r in cur.fetchall()]
        if not tables:
            return []

        target_table = tables[0]
        cur.execute(f"PRAGMA table_info({target_table})")
        cols = [r[1] for r in cur.fetchall()]
        cols_lower = {c.lower(): c for c in cols}

        name_col = next((cols_lower[k] for k in ("name", "player_name", "nick", "nickname") if k in cols_lower), None)
        kills_col = next((cols_lower[k] for k in ("kills", "knife_kills", "frags", "score", "value", "points") if k in cols_lower), None)

        if not name_col or not kills_col:
            return []

        cur.execute(f"SELECT {name_col}, {kills_col} FROM {target_table} ORDER BY {kills_col} DESC LIMIT 10")
        rows = cur.fetchall()
        return [(ServerTracker.clean_name(r[0]) or "Гравець", int(r[1] or 0)) for r in rows]
    finally:
        conn.close()


async def send_reply(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str) -> None:
    if not update.effective_chat:
        return
    thread_id = update.effective_message.message_thread_id if update.effective_message else None
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        message_thread_id=thread_id,
        text=text,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "🤖 <b>Бот сервера CS:S</b>\n"
        "👤 Автор: <b>SWAG#</b>\n\n"
        "Доступні команди:\n"
        "• <code>!сервер</code>, або <code>!online</code> — статус та онлайн сервера\n"
        "• <code>!топ</code> — Топ-10 гравців сервера\n"
        "• <code>!нож</code> — Топ-10 гравців на ножах\n"
        "• <code>/start</code> — довідка\n\n"
        "<i>Працює лише за запитом без спаму.</i>"
    )
    await send_reply(update, context, text)


async def server_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    data = await ServerTracker.fetch_server_data()

    if not data:
        await send_reply(
            update,
            context,
            "❌ <b>Сервер не відповідає або змінює карту.</b> Спробуйте через кілька секунд.",
        )
        return

    info = data["info"]
    raw_players = data["players"]

    valid_players = []
    for p in raw_players:
        cleaned = ServerTracker.clean_name(p.name)
        if cleaned and cleaned != ".":
            valid_players.append((cleaned, p.score))

    server_name = html.escape(ServerTracker.clean_name(info.server_name))
    current_map = html.escape(ServerTracker.clean_name(info.map_name))

    current_online = len(valid_players) if valid_players else info.player_count
    max_slots = info.max_players or MAX_PLAYERS

    prev_max = ServerTracker.max_players_this_map.get(current_map, 0)
    ServerTracker.max_players_this_map[current_map] = max(prev_max, current_online)
    max_this_map = ServerTracker.max_players_this_map[current_map]

    time_left = ServerTracker.get_time_remaining(current_map)

    lines = [
        f"<b>{server_name}</b>",
        "",
        f"🎮 Карта: <code>{current_map}</code>",
        f"⏱️ До закінчення карти: <b>{time_left}</b>",
        "",
        "❗️ <b>Для підключення введіть у консолі:</b>",
        f"❗️ <code>connect {SERVER_IP}:{SERVER_PORT}</code>",
        "",
        f"👥 Онлайн гравців: <b>{current_online} з {max_slots}</b>",
        f"🗣 Макс онлайн за карту: <b>{max_this_map} з {max_slots}</b>",
        "",
        "🖥 <b>Список гравців на сервері:</b>",
        "",
    ]

    if valid_players:
        for i, (name, score) in enumerate(valid_players, 1):
            safe_name = html.escape(name)
            lines.append(f"👨‍💻 {i}: {safe_name} [ {score} ]")
    else:
        lines.append("👤 Гравців немає")

    await send_reply(update, context, "\n".join(lines))


async def lr_top_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await sync_databases(force=False)
    try:
        top_list = await asyncio.to_thread(query_lr_top)
    except Exception as e:
        logger.error(f"Помилка читання LR бази: {e}")
        top_list = []

    if not top_list:
        await send_reply(update, context, "❌ <b>База статистики тимчасово недоступна.</b>")
        return

    lines = ["🏆 <b>Топ-10 гравців сервера:</b>", ""]
    for idx, (name, points, kd) in enumerate(top_list, 1):
        medal = get_medal(idx)
        safe_name = html.escape(name)
        lines.append(f"{medal} - {safe_name} [ {points} | K/D: {kd:.2f} ]")

    await send_reply(update, context, "\n".join(lines))


async def knife_top_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await sync_databases(force=False)
    try:
        top_list = await asyncio.to_thread(query_knife_top)
    except Exception as e:
        logger.error(f"Помилка читання Knife бази: {e}")
        top_list = []

    if not top_list:
        await send_reply(update, context, "❌ <b>База ножового топу тимчасово недоступна.</b>")
        return

    lines = ["🔪 <b>Топ-10 гравців на ножах:</b>", ""]
    for idx, (name, kills) in enumerate(top_list, 1):
        medal = get_medal(idx)
        safe_name = html.escape(name)
        lines.append(f"{medal} - {safe_name} [ {kills} ]")

    await send_reply(update, context, "\n".join(lines))


def main() -> None:
    if not TOKEN:
        logger.critical("TELEGRAM_BOT_TOKEN не знайдено у файлі .env!")
        return

    threading.Thread(target=start_dummy_server, daemon=True).start()

    application = ApplicationBuilder().token(TOKEN).build()

    # Працює суворо лише у вказаній групі, особисті повідомлення (ЛС) ігноруються
    chat_filter = filters.Chat(chat_id=ALLOWED_CHAT_ID) if ALLOWED_CHAT_ID != 0 else filters.ALL

    application.add_handler(CommandHandler("start", start_command, filters=chat_filter))
    application.add_handler(
        MessageHandler(
            chat_filter & filters.TEXT & ~filters.COMMAND & filters.Regex(r"(?i)^(!сервер|!server|!online)\b"),
            server_status,
        )
    )
    application.add_handler(
        MessageHandler(
            chat_filter & filters.TEXT & ~filters.COMMAND & filters.Regex(r"(?i)^(!топ|!top)\b"),
            lr_top_command,
        )
    )
    application.add_handler(
        MessageHandler(
            chat_filter & filters.TEXT & ~filters.COMMAND & filters.Regex(r"(?i)^(!нож|!ніж|!knife)\b"),
            knife_top_command,
        )
    )

    logger.info(f"Єдиний бот запущено. Дозволений чат: {ALLOWED_CHAT_ID}")
    application.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
