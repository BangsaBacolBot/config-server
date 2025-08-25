
import logging
import colorlog
import os
from pathlib import Path
import asyncio
import json
import re
from statistics import mean
from collections import defaultdict
from logging.handlers import RotatingFileHandler
from urllib.parse import urlparse
from datetime import datetime, timedelta
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:
    ZoneInfo = None

from dotenv import load_dotenv
from pyrogram.errors import UserNotParticipant
from pyrogram import Client, filters, idle
from pyrogram.types import Message
from pyrogram.enums import ChatMemberStatus, ParseMode
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, ChatPermissions
from pyrogram.errors import MessageNotModified
import aiohttp
import time
import random
import urllib.request

USER_DATA_FILE = Path("data/user_data.json")
VOTES_FILE = "votes.json"

# ---- Badge constants & helpers ----
BADGE_STRANGER = "Stranger 🔰"
BADGE_SHIMMER  = "Shimmer 🥉"   # ejaan dibenarkan
BADGE_STELLAR  = "Stellar 🥈"
BADGE_STARLORD = "Starlord 🥇"

def normalize_badge(name: str) -> str:
    if not name:
        return BADGE_STRANGER
    # toleransi untuk data lama yang tersimpan "Shimmer"
    fixed = name.replace("Shimmer", "Shimmer")
    # jaga-jaga trimming
    return fixed.strip()

def load_user_data():
    if USER_DATA_FILE.exists():
        with open(USER_DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_user_data(data):
    USER_DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(USER_DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def grant_xp_for_command(message, invoked_command: str):
    """Tambahkan XP ke user tiap kali pakai command."""
    if not message.from_user:
        return
    user_id = message.from_user.id
    username = message.from_user.username or "-"
    try:
        update_user_xp(user_id, username, invoked_command, xp_increment=1)
    except Exception as e:
        logger.error(f"Gagal menambahkan XP untuk {user_id}: {e}")

def update_user_xp(user_id: int, username: str, invoked_command: str, xp_increment: int = 1) -> dict:
    data = load_user_data()
    user = data.setdefault(str(user_id), {
        "username": username or "-",
        "xp": 0,
        "badge": BADGE_STRANGER,
        "last_seen": None,
        "last_xp_dates": {}
    })

    user["username"] = username or user.get("username") or "-"
    now = datetime.now(JAKARTA_TZ)
    today = now.date().isoformat()

    last = user.setdefault("last_xp_dates", {})

    # cek apakah sudah dapat XP command ini hari ini
    if last.get(invoked_command) == today:
        user["last_seen"] = now.isoformat()
        save_user_data(data)
        return user

    # tambahkan XP
    user["xp"] = int(user.get("xp", 0)) + xp_increment
    user["last_seen"] = now.isoformat()
    last[invoked_command] = today

    xp = user["xp"]
    if xp >= 150:
        user["badge"] = BADGE_STARLORD
    elif xp >= 80:
        user["badge"] = BADGE_STELLAR
    elif xp >= 20:
        user["badge"] = BADGE_SHIMMER
    else:
        user["badge"] = BADGE_STRANGER

    save_user_data(data)
    return user

def has_stellar_or_higher(user_id):
    data = load_user_data()
    user = data.get(str(user_id))
    if not user:
        return False
    badge = user.get("badge", "")
    return badge in ["Stellar 🥈", "Starlord 🥇"]

def is_admin(message) -> bool:
    return bool(getattr(message, "from_user", None)) and message.from_user.id in ADMIN_IDS

def is_starlord(user_id: int) -> bool:
    data = load_user_data()
    return data.get(str(user_id), {}).get("badge") == "Starlord 🥇"

# Helper load/save
def load_votes():
    try:
        with open(VOTES_FILE, "r") as f:
            return json.load(f)
    except:
        return {}

def save_votes(data):
    with open(VOTES_FILE, "w") as f:
        json.dump(data, f, indent=2)

# ============== JATAH /random ==============
JAKARTA_TZ = ZoneInfo("Asia/Jakarta") if ZoneInfo else None
def _now_jkt():
    if JAKARTA_TZ:
        return datetime.now(JAKARTA_TZ)
    return datetime.utcnow() + timedelta(hours=7)

RANDOM_DAILY_LIMIT = 3
QUOTA_FILE = Path("data/random_quota.json")
_quota_lock = asyncio.Lock()

def _ensure_parent_dir(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)

def _load_quota() -> dict:
    if not QUOTA_FILE.exists():
        return {}
    try:
        with QUOTA_FILE.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_quota(data: dict) -> None:
    _ensure_parent_dir(QUOTA_FILE)
    with QUOTA_FILE.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def _today_key() -> str:
    return _now_jkt().date().isoformat()

def _seconds_until_midnight_jkt() -> int:
    now = _now_jkt()
    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return max(0, int((tomorrow - now).total_seconds()))

def _format_eta(seconds: int) -> str:
    h = seconds // 3600
    m = (seconds % 3600) // 60
    if h > 0:
        return f"{h}j {m}m"
    return f"{m}m"

async def get_random_quota_status(user_id: int):
    async with _quota_lock:
        data = _load_quota()
        today = _today_key()
        if set(data.keys()) - {today}:
            data = {today: data.get(today, {})}
            _save_quota(data)
        used = int(data.get(today, {}).get(str(user_id), 0))
        limit = RANDOM_DAILY_LIMIT
        remaining = max(0, limit - used)
        return used, remaining, limit, _seconds_until_midnight_jkt()

async def consume_random_quota(user_id: int):
    async with _quota_lock:
        data = _load_quota()
        today = _today_key()
        if set(data.keys()) - {today}:
            data = {today: data.get(today, {})}
        daymap = data.setdefault(today, {})
        used = int(daymap.get(str(user_id), 0))
        if used >= RANDOM_DAILY_LIMIT:
            _save_quota(data)
            return False, 0, RANDOM_DAILY_LIMIT, _seconds_until_midnight_jkt()
        daymap[str(user_id)] = used + 1
        _save_quota(data)
        remaining_after = max(0, RANDOM_DAILY_LIMIT - (used + 1))
        return True, remaining_after, RANDOM_DAILY_LIMIT, _seconds_until_midnight_jkt()

# ================================
# Utilitas
# ================================
USER_ACTIVITY_FILE = Path("data/user_activity.json")

def load_user_activity():
    if USER_ACTIVITY_FILE.exists():
        with open(USER_ACTIVITY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_user_activity(data):
    with open(USER_ACTIVITY_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def log_user_activity(user_id, username):
    data = load_user_activity()
    user = str(user_id)
    if user not in data:
        data[user] = {"username": username, "count": 0}
    data[user]["count"] += 1
    data[user]["username"] = username  # update username jika berubah
    save_user_activity(data)


def _safe_parse_ts(ts: str):
    """
    Parse ISO-8601 yang toleran:
    - Mendukung akhiran 'Z'
    - Jika naive (tanpa tz), anggap UTC
    """
    try:
        s = ts.strip()
        if s.endswith('Z'):
            s = s[:-1] + '+00:00'
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=JAKARTA_TZ)
        return dt
    except Exception:
        return None

# ======================
# LOGGER SETUP
# ======================
import logging
import colorlog
from pathlib import Path
from logging.handlers import RotatingFileHandler

# Direktori log & data
LOG_DIR = Path("logs"); LOG_DIR.mkdir(exist_ok=True)
DATA_DIR = Path("data"); DATA_DIR.mkdir(exist_ok=True)

# File log
ACTIVITY_LOG = LOG_DIR / "bot_activity.log"
CLICKS_JSONL = LOG_DIR / "clicks.jsonl"
CLICKS_HUMAN = LOG_DIR / "clicks_human.log"
MOD_LOG = LOG_DIR / "mod_action.log"
HEALTH_LOG = LOG_DIR / "health_check.log"

# Config logging
RETENTION_DAYS = 7
LOG_LEVEL = logging.INFO
MAX_LOG_SIZE = 10 * 1024 * 1024  # 10 MB
BACKUP_COUNT = 5

# Formatter warna utk console
console_formatter = colorlog.ColoredFormatter(
    "%(log_color)s[%(asctime)s] [%(levelname)s]%(reset)s - %(message)s",
    datefmt="%H:%M:%S",
    log_colors={
        "DEBUG": "cyan",
        "INFO": "green",
        "WARNING": "yellow",
        "ERROR": "red",
        "CRITICAL": "bold_red"
    }
)

# Handler console
console_handler = colorlog.StreamHandler()
console_handler.setFormatter(console_formatter)

# Handler file (rotate otomatis)
file_handler = RotatingFileHandler(
    ACTIVITY_LOG, maxBytes=MAX_LOG_SIZE, backupCount=BACKUP_COUNT, encoding="utf-8"
)
file_formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
file_handler.setFormatter(file_formatter)

# Logger utama
logger = logging.getLogger("BangsaBacolBot")
logger.setLevel(LOG_LEVEL)
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# Biar Pyrogram gak spam
logging.getLogger("pyrogram").setLevel(logging.WARNING)

# Contoh log awal
logger.info("🚀 Logger initialized!")

# ================================
# Konfigurasi Lingkungan
# ================================

load_dotenv()
try:
    API_ID = int(os.getenv("API_ID"))
    API_HASH = os.getenv("API_HASH")
    BOT_TOKEN = os.getenv("BOT_TOKEN")
    CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME")
    GROUP_USERNAME = os.getenv("GROUP_USERNAME")
    EXTRA_CHANNEL    = os.getenv("EXTRA_CHANNEL")
    OWNER_ID = int(os.getenv("OWNER_ID", "0"))
    ADMIN_IDS = {7840297414, 7112438057}
except (TypeError, ValueError) as e:
    logger.error(f"Error loading environment variables: {e}")
    raise SystemExit(1)

def is_admin(message) -> bool:
    """Admin bot versi sederhana: OWNER atau ID yang terdaftar di ADMIN_IDS."""
    uid = message.from_user.id if message.from_user else 0
    return (uid == OWNER_ID) or (uid in ADMIN_IDS)

def is_starlord(user_id: int) -> bool:
    data = load_user_data()
    badge = normalize_badge(data.get(str(user_id), {}).get("badge", ""))
    return badge == BADGE_STARLORD

def has_stellar_or_higher(user_id: int) -> bool:
    data = load_user_data()
    badge = normalize_badge(data.get(str(user_id), {}).get("badge", ""))
    return badge in (BADGE_STELLAR, BADGE_STARLORD)

# ================================
# Daftar Bot Mirror
# ================================
BOT_MIRRORS = [
    {"role": "Bot Utama", "name": "🤖 Bangsa Bacol Bot", "username": "BangsaBacolBot"},
    {"role": "Bot Mirror", "name": "🤖 Koleksi Bangsa | Stephander", "username": "Bangsa_BacolBot"},
    {"role": "Bot Lapor", "name": "🤖 Kolpri Bacol | Seraphina", "username": "BangsaBacol_Bot"},
]

# --- Retention Settings ---
try:
    RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "7"))  # default 7 hari
except ValueError:
    RETENTION_DAYS = 7

# ================================
# Config Loader: Badwords & Interaction
# ================================
import re, json, urllib.request
from pathlib import Path
from urllib.parse import urlparse

# --- Variabel global ---
CONFIG_DIR = Path("config")
CONFIG_DIR.mkdir(exist_ok=True)

BADWORDS_CONFIG_URL = os.getenv("BADWORDS_CONFIG_URL")
BADWORDS_FILE = CONFIG_DIR / "badwords.json"
INTERACTION_FILE = CONFIG_DIR / "interaction.json"

BAD_WORDS: set[str] = set()
BAD_WORDS_RE: re.Pattern = re.compile(r"(?!x)x")  # dummy regex
ALLOWED_LINK_DOMAINS: set[str] = {"t.me", "trakteer.id", "telegra.ph"}

INTERACTION_CONFIG: dict = {
    "buttons": [],
    "links": []
}

# --- Helper ---
def _build_badwords_regex(words: set[str]) -> re.Pattern:
    cleaned = [w.strip() for w in words if isinstance(w, str) and w.strip()]
    if not cleaned:
        return re.compile(r"(?!x)x")  # selalu false
    patt = r"\b(?:%s)\b" % "|".join(re.escape(w) for w in cleaned)
    try:
        return re.compile(patt, re.IGNORECASE)
    except re.error:
        return re.compile("|".join(re.escape(w) for w in cleaned), re.IGNORECASE)

def is_allowed_domain(url: str) -> bool:
    """Cek apakah domain (termasuk subdomain) masuk whitelist."""
    try:
        host = (urlparse(url).hostname or "").lower()
        if not host:
            return False
        return any(host == d or host.endswith("." + d) for d in ALLOWED_LINK_DOMAINS)
    except Exception:
        return False

# --- Loader ---
def load_badwords_config():
    global BAD_WORDS, BAD_WORDS_RE, ALLOWED_LINK_DOMAINS

    remote_url = os.getenv("BADWORDS_CONFIG_URL", "").strip()
    data = None

    # 1) Remote
    if remote_url:
        try:
            logger.info(f"🔄 Fetching badwords config dari {remote_url}")
            with urllib.request.urlopen(remote_url, timeout=10) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            logger.warning(f"Gagal fetch remote config: {e}. Coba lokal...")

    # 2) Lokal
    if data is None and BADWORDS_FILE.exists():
        try:
            with open(BADWORDS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            logger.warning(f"Gagal baca {BADWORDS_FILE}: {e}")

    # 3) Default
    if data is None:
        logger.warning("Config badwords tidak ditemukan (remote & lokal). Pakai fallback bawaan.")
        BAD_WORDS = {"tolol", "goblok", "anjing"}
        ALLOWED_LINK_DOMAINS = {"t.me", "trakteer.id", "telegra.ph"}
        BAD_WORDS_RE = _build_badwords_regex(BAD_WORDS)
        return

    # Validasi
    words = data.get("badwords", []) or []
    domains = data.get("allowed_domains", []) or []

    BAD_WORDS = {str(w).strip() for w in words if str(w).strip()}
    ALLOWED_LINK_DOMAINS = {str(d).strip().lower() for d in domains if str(d).strip()}
    if not ALLOWED_LINK_DOMAINS:
        ALLOWED_LINK_DOMAINS = {"t.me", "trakteer.id", "telegra.ph"}

    BAD_WORDS_RE = _build_badwords_regex(BAD_WORDS)
    logger.info(f"✅ Badwords config loaded ({len(BAD_WORDS)} kata, {len(ALLOWED_LINK_DOMAINS)} domain).")
    
# ================================
# Konstanta & State
# ================================

STREAM_MAP_FILE = Path("stream_links.json")
STREAM_MAP: dict[str, dict] = {}
ITEMS_PER_PAGE = 15

# --- Moderasi / Warning DB (per chat) ---
WARN_DB_FILE = DATA_DIR / "warnings.json"
WARN_DB: dict = {}  # { chat_id: { user_id: {"count": int, "history": [...] } } }
WARN_LOCK = asyncio.Lock()
WARN_MUTE_THRESHOLD = 3           # 3 warn -> mute
MUTE_DURATION_HOURS = 24          # durasi mute (jam)

# --- Anti-link / Bad words ---
ANTILINK_ENABLED = True
ALLOWED_LINK_DOMAINS = {"t.me", "trakteer.id", "telegra.ph"}  # <- diperbaiki

def _norm_chat(x: str) -> str:
    x = x.strip()
    return x if x.startswith("@") else f"@{x}"
# (fix regex escaping) gunakan raw string yang benar
URL_REGEX = re.compile(r"(https?://\S+|t\.me/\S+)", re.IGNORECASE)
URL_RE = URL_REGEX
INVITE_REGEX = re.compile(r"(t\.me/joinchat/|t\.me/\+|telegram\.me/joinchat/)", re.IGNORECASE)

BAD_WORDS = {"tolol", "goblok", "anjing"}  # contoh; sesuaikan
BAD_WORDS_RE = re.compile(r"\b(" + "|".join(re.escape(w) for w in BAD_WORDS) + r")\b", re.IGNORECASE)

INTERACTION_CONFIG_URL = os.getenv("INTERACTION_CONFIG_URL")
INTERACTION_FILE = Path("config") / "interaction.json"

INTERACTION_MESSAGES = [...]
INTERACTION_INTERVAL_MINUTES = 180

def load_interaction_config():
    """Load pesan periodik dari URL remote atau file lokal"""
    global INTERACTION_MESSAGES, INTERACTION_INTERVAL_MINUTES
    data = {}
    try:
        if INTERACTION_CONFIG_URL:
            logger.info(f"🔄 Fetching interaction config dari {INTERACTION_CONFIG_URL}")
            with urllib.request.urlopen(INTERACTION_CONFIG_URL, timeout=10) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        elif INTERACTION_FILE.exists():
            with open(INTERACTION_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)

        msgs = data.get("interaction_messages", [])
        if msgs and isinstance(msgs, list):
            INTERACTION_MESSAGES = msgs

        INTERACTION_INTERVAL_MINUTES = int(data.get("interval_minutes", INTERACTION_INTERVAL_MINUTES))

        logger.info(f"✅ Interaction config loaded ({len(INTERACTION_MESSAGES)} pesan, interval {INTERACTION_INTERVAL_MINUTES}m).")

    except Exception as e:
        logger.error(f"Gagal load interaction config: {e}")

# ================================
# Helper
# ================================

async def is_chat_admin(client, chat_id, user_id) -> bool:
    """Cek apakah user admin/owner di suatu chat."""
    try:
        m = await client.get_chat_member(chat_id, user_id)
        return m.status in [ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR]
    except Exception:
        return False

async def _is_operator(client, message) -> bool:
    if not message.from_user:
        return False
    if message.from_user.id == OWNER_ID:
        return True
    return await is_chat_admin(client, message.chat.id, message.from_user.id)

def is_owner(ctx) -> bool:
    """Cek pengirim adalah OWNER (untuk message atau callback)."""
    try:
        uid = ctx.from_user.id
    except Exception:
        return False
    return uid == OWNER_ID

# --- Warning DB helpers ---

def load_warn_db():
    """Muat database warning dari file JSON (aman terhadap error)."""
    global WARN_DB
    if WARN_DB_FILE.exists():
        try:
            with open(WARN_DB_FILE, "r", encoding="utf-8") as f:
                WARN_DB = json.load(f)
        except Exception as e:
            logger.error(f"Gagal load {WARN_DB_FILE}: {e}")
            WARN_DB = {}
    else:
        WARN_DB = {}

def save_warn_db():
    try:
        with open(WARN_DB_FILE, "w", encoding="utf-8") as f:
            json.dump(WARN_DB, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Gagal save {WARN_DB_FILE}: {e}")

async def add_warn(chat_id: int, user_id: int, by_id: int, reason: str = "") -> int:
    async with WARN_LOCK:
        chat = str(chat_id); user = str(user_id)
        WARN_DB.setdefault(chat, {}).setdefault(user, {"count": 0, "history": []})
        rec = WARN_DB[chat][user]
        rec["count"] += 1
        rec["history"].append({"ts": datetime.now(JAKARTA_TZ).isoformat(), "by": by_id, "reason": reason or "-"})
        return rec["count"]

def get_warn_count(chat_id: int, user_id: int) -> int:
    chat = str(chat_id); user = str(user_id)
    return WARN_DB.get(chat, {}).get(user, {}).get("count", 0)

async def clear_warns(chat_id: int, user_id: int):
    async with WARN_LOCK:
        chat = str(chat_id); user = str(user_id)
        if chat in WARN_DB and user in WARN_DB[chat]:
            WARN_DB[chat][user] = {"count": 0, "history": []}
            save_warn_db()

async def apply_auto_action(client: Client, chat_id: int, user_id: int, count: int):
    """Auto mute jika melampaui threshold."""
    if count >= WARN_MUTE_THRESHOLD:
        try:
            until = int(time.time()) + MUTE_DURATION_HOURS * 3600
            perms = ChatPermissions(
                can_send_messages=False,
                can_send_media_messages=False,
                can_send_other_messages=False,
                can_add_web_page_previews=False
            )
            await client.restrict_chat_member(chat_id, user_id, permissions=perms, until_date=until)
            logger.info(f"User {user_id} dimute {MUTE_DURATION_HOURS} jam di chat {chat_id} (warn={count})")
        except Exception as e:
            logger.error(f"Gagal mute user {user_id} di {chat_id}: {e}")

def _modlog_line(action, moderator, target, reason: str | None = None, extra: str | None = None) -> str:
    t = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    mod = f"{moderator.id}(@{moderator.username or 'unknown'})" if moderator else "system"
    tgt = f"{target.id}(@{target.username or 'unknown'})" if target else "-"
    base = f"[{t}] {action} by {mod} → {tgt}"
    if reason:
        base += f" | reason: {reason}"
    if extra:
        base += f" | {extra}"
    return base + "\n"

def mod_log(line: str):
    try:
        with open(MOD_LOG, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception as e:
        logger.error(f"mod_log error: {e}")

# --- Mute/Kick/Ban helpers ---

async def mute_user(client, chat_id: int | str, user_id: int, seconds: int):
    until = int(time.time()) + max(30, seconds)
    perms = ChatPermissions(
        can_send_messages=False,
        can_send_media_messages=False,
        can_send_other_messages=False,
        can_send_polls=False,
        can_add_web_page_previews=False,
    )
    await client.restrict_chat_member(chat_id, user_id, permissions=perms, until_date=until)

async def unmute_user(client, chat_id, user_id):
    perms = ChatPermissions(
        can_send_messages=True,
        can_send_media_messages=True,
        can_send_other_messages=True,
        can_send_polls=True,
        can_add_web_page_previews=True,
    )
    await client.restrict_chat_member(chat_id, user_id, permissions=perms)

async def ban_user(client, chat_id, user_id):
    await client.ban_chat_member(chat_id, user_id)

async def kick_user(client, chat_id, user_id):
    await client.ban_chat_member(chat_id, user_id)
    try:
        await client.unban_chat_member(chat_id, user_id)
    except Exception:
        pass

def _parse_duration_to_seconds(s: str | None, default: int = 30 * 60) -> int:
    """Parse '10m', '2h', '1d' → detik. Fallback ke default (30 menit)."""
    if not s:
        return default
    s = s.strip().lower()
    if s.isdigit():
        return int(s)
    m = re.match(r"^(\d+)([smhd])$", s)
    if not m:
        return default
    n, unit = int(m.group(1)), m.group(2)
    return n * {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]

# --- Stream map helpers ---

def load_stream_map():
    global STREAM_MAP
    if not STREAM_MAP_FILE.exists():
        logger.warning(f"Berkas '{STREAM_MAP_FILE}' tidak ditemukan. Memulai dengan map kosong.")
        STREAM_MAP = {}
        return STREAM_MAP
    try:
        with open(STREAM_MAP_FILE, "r", encoding="utf-8") as f:
            STREAM_MAP = json.load(f)
    except Exception as e:
        logger.error(f"Gagal membaca {STREAM_MAP_FILE}: {e}. Memulai map kosong.")
        STREAM_MAP = {}
    return STREAM_MAP

def save_stream_map():
    with open(STREAM_MAP_FILE, "w", encoding="utf-8") as f:
        json.dump(STREAM_MAP, f, indent=4, ensure_ascii=False)
    logger.info("Stream map disimpan.")

def get_stream_data(code: str):
    data = STREAM_MAP.get(code)
    if isinstance(data, dict):
        return data.get("link"), data.get("thumbnail")
    elif isinstance(data, str):
        return data, None
    return None, None

def search_codes(query: str):
    q = query.lower()
    return [c for c in STREAM_MAP.keys() if q in c.lower()]

# --- Click Logging ---

def append_click_log(user_id, username, code, link):
    """
    Tulis event klik ke dua format:
    - JSONL (analitik/dashboard) → logs/clicks.jsonl
    - Human-readable (monitoring cepat) → logs/clicks_human.log
    """
    ts_human = datetime.now(JAKARTA_TZ).strftime("%Y-%m-%d %H:%M:%S")
    uname = f"@{username}" if username else "(unknown)"
    line = f"[{ts_human}] User {user_id} ({uname}) klik: {code} → {link}\n"

    event = {
        "ts": datetime.now(JAKARTA_TZ).isoformat(),
        "user_id": user_id,
        "username": username or None,
        "code": code,
        "link": link,
    }
    try:
        with open(CLICKS_JSONL, "a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.error(f"Gagal menulis clicks.jsonl: {e}")
    try:
        with open(CLICKS_HUMAN, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception as e:
        logger.error(f"Gagal menulis clicks_human.log: {e}")

def prune_clicks_log(retention_days: int = RETENTION_DAYS):
    """Simpan hanya event dalam N hari terakhir (atomic replace)."""
    if not CLICKS_JSONL.exists():
        return
    cutoff = datetime.now(JAKARTA_TZ) - timedelta(days=RETENTION_DAYS)
    tmp_path = CLICKS_JSONL.with_suffix(".jsonl.tmp")
    with open(CLICKS_JSONL, "r", encoding="utf-8") as src, open(tmp_path, "w", encoding="utf-8") as dst:
        for line in src:
            try:
                ev = json.loads(line)
                ts = _safe_parse_ts(ev.get("ts", ""))
                if ts and ts >= cutoff:
                    dst.write(json.dumps(ev, ensure_ascii=False) + "\n")
            except Exception:
                continue
    os.replace(tmp_path, CLICKS_JSONL)

def prune_clicks_human(retention_days: int = RETENTION_DAYS):
    if not CLICKS_HUMAN.exists():
        return
    cutoff = datetime.now(JAKARTA_TZ) - timedelta(days=RETENTION_DAYS)
    out = []
    with open(CLICKS_HUMAN, "r", encoding="utf-8") as f:
        for ln in f:
            # format: "[YYYY-mm-dd HH:MM:SS] ...\n"
            try:
                ts = ln[1:20]
                dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=JAKARTA_TZ)
                if dt >= cutoff:
                    out.append(ln)
            except Exception:
                out.append(ln)
    with open(CLICKS_HUMAN, "w", encoding="utf-8") as f:
        f.writelines(out)

def parse_clicks_log_json(days_back: int = 7):
    """Ringkas logs/clicks.jsonl untuk N hari terakhir."""
    base = {
        "total_clicks": 0, "unique_users": 0, "by_day": {}, "by_code": {},
        "status": "success", "message": "", "debug": {}
    }
    if not CLICKS_JSONL.exists():
        r = base.copy(); r.update({"status": "no_log_file", "message": "File log belum ada."})
        return r

    cutoff = datetime.now(JAKARTA_TZ) - timedelta(days=RETENTION_DAYS)
    total, users, by_day, by_code = 0, set(), defaultdict(int), defaultdict(int)
    processed, errors = 0, 0

    try:
        with open(CLICKS_JSONL, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s:
                    continue
                try:
                    row = json.loads(s)
                except json.JSONDecodeError:
                    errors += 1; continue
                dt = _safe_parse_ts(row.get("ts", ""))
                if not dt:
                    errors += 1; continue
                if dt >= cutoff:
                    total += 1
                    uid = row.get("user_id"); 
                    if uid is not None: users.add(uid)
                    code = row.get("code") or row.get("link_key") or row.get("video_key") or "unknown"
                    by_code[code] += 1
                    by_day[dt.strftime("%Y-%m-%d")] += 1
                processed += 1

        status = "success" if total > 0 else "no_recent_clicks"
        out = base.copy()
        out.update({
            "status": status,
            "total_clicks": total,
            "unique_users": len(users),
            "by_day": dict(by_day),
            "by_code": dict(by_code),
            "message": "" if total > 0 else f"Tidak ada klik dalam {days_back} hari.",
            "debug": {"processed_lines": processed, "error_lines": errors, "cutoff_iso": cutoff.isoformat()}
        })
        return out
    except Exception as e:
        logger.error(f"Error membaca clicks.jsonl: {e}")
        r = base.copy(); r.update({"status": "read_error", "message": f"Error: {e}"})
        return r

def paginate_codes(codes, page, per_page=ITEMS_PER_PAGE):
    total = len(codes)
    pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(page, pages))
    start = (page - 1) * per_page
    end = min(start + per_page, total)
    return codes[start:end], page, pages, total

def build_list_keyboard(page_codes, page, pages):
    buttons = [[InlineKeyboardButton(code, callback_data=f"list_show|{code}|{page}")] for code in page_codes]
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"list|{page-1}"))
    if page < pages:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"list|{page+1}"))
    if nav:
        buttons.append(nav)
    buttons.append([InlineKeyboardButton("❌ Tutup", callback_data="list_close")])
    return InlineKeyboardMarkup(buttons)

async def is_member(client: Client, user_id: int, chat_username: str) -> bool:
    try:
        m = await client.get_chat_member(_norm_chat(chat_username), user_id)
        return m.status in [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]
    except UserNotParticipant:
        # Bukan error—user memang belum join
        return False
    except Exception as e:
        logger.warning(f"Gagal cek membership {user_id} di {chat_username}: {e}")
        return False

def _check_log_file_status():
    info = {"exists": CLICKS_JSONL.exists(), "size": 0, "lines": 0, "tail": []}
    if not info["exists"]:
        return info
    try:
        info["size"] = CLICKS_JSONL.stat().st_size
        with open(CLICKS_JSONL, "r", encoding="utf-8") as f:
            lines = f.readlines()
        info["lines"] = len(lines)
        info["tail"] = [ln.strip() for ln in lines[-3:]]
    except Exception as e:
        info["error"] = str(e)
    return info

def build_dashboard_text(period_days: int = 7, top_n: int = 5):
    stats = parse_clicks_log_json(days_back=period_days)
    if stats["status"] in ("no_log_file", "read_error", "no_recent_clicks"):
        head = f"📊 Dashboard — {period_days} hari terakhir\n"
        body = f"• Total klik: {stats.get('total_clicks', 0)}\n• Pengguna unik: {stats.get('unique_users', 0)}\n"
        note = stats.get("message", "Belum ada data.")
        return head + body + f"\nℹ️ {note}"
    items = sorted(stats.get("by_code", {}).items(), key=lambda x: x[1], reverse=True)[:top_n]
    lines = [
        f"📊 Dashboard — {period_days} hari terakhir",
        f"• Total klik: {stats['total_clicks']}",
        f"• Pengguna unik: {stats['unique_users']}",
        "",
    ]
    if items:
        lines.append(f"🏆 Top {len(items)} Kode:")
        for i, (code, count) in enumerate(items, 1):
            lines.append(f"{i}. {code} — {count} klik")
    else:
        lines.append("Tidak ada data kode untuk periode ini.")
    if stats.get("by_day"):
        lines.append("")
        lines.append("🗓️ Ringkasan Harian:")
        for d, c in sorted(stats["by_day"].items())[-7:]:
            lines.append(f"• {d}: {c}")
    return "\n".join(lines)

def build_dashboard_keyboard(current_period: int = 7):
    periods = [1, 7, 30]
    row = []
    for p in periods:
        label = f"{p}d" if p != current_period else f"• {p}d"
        row.append(InlineKeyboardButton(label, callback_data=f"dashboard:{p}"))
    return InlineKeyboardMarkup([row, [InlineKeyboardButton("🔄 Refresh", callback_data=f"dashboard:{current_period}")]])

# ================================
# Bot Initialization
# ================================

app = Client("bangsabacolbot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# ================================
# Commands & Handlers
# ================================

# --- Moderator Commands (group) ---

@Client.on_message(filters.command("reload_badwords") & filters.user([OWNER_ID]))
async def reload_badwords_cmd(client, message):
    try:
        load_badwords_config()
        await message.reply(
            f"✅ Reload OK.\n• Badwords: {len(BAD_WORDS)}\n• Allowed domains: {len(ALLOWED_LINK_DOMAINS)}"
        )
    except Exception:
        logger.exception("reload_config failed")
        await message.reply("❌ Gagal reload config. Cek log.")

@app.on_message(filters.command("warn") & filters.group)
async def warn_cmd(client, message):
    if not message.from_user: return
    if not await _is_operator(client, message): return
    if not message.reply_to_message or not message.reply_to_message.from_user:
        await message.reply_text("Balas pesan user yang ingin di-warn. Format: /warn [alasan]"); return
    target = message.reply_to_message.from_user
    reason = message.text.split(" ", 1)[1].strip() if len(message.command) > 1 else ""
    count = await add_warn(message.chat.id, target.id, by_id=message.from_user.id, reason=reason)
    extra = None
    if count >= WARN_MUTE_THRESHOLD:
        try:
            await mute_user(client, message.chat.id, target.id, MUTE_DURATION_HOURS * 3600)
            extra = f"auto-mute {MUTE_DURATION_HOURS}h"
            await clear_warns(message.chat.id, target.id)
            await message.reply_text(f"⚠️ Warn {count}/{WARN_MUTE_THRESHOLD} → 🔇 {target.mention} di-mute {MUTE_DURATION_HOURS} jam.", quote=True)
        except Exception as e:
            logger.error(f"Mute gagal: {e}")
    else:
        await message.reply_text(f"⚠️ {target.mention} mendapatkan peringatan {count}/{WARN_MUTE_THRESHOLD}.", quote=True)
    mod_log(_modlog_line("WARN", message.from_user, target, reason, extra))

@app.on_message(filters.command("warns") & filters.group)
async def warns_cmd(client, message):
    if not message.from_user: return
    if not await _is_operator(client, message): return
    target = message.reply_to_message.from_user if message.reply_to_message and message.reply_to_message.from_user else message.from_user
    cnt = get_warn_count(message.chat.id, target.id)
    await message.reply_text(f"ℹ️ Warn {target.mention}: {cnt}/{WARN_MUTE_THRESHOLD}", quote=True)

@app.on_message(filters.command("resetwarn") & filters.group)
async def resetwarn_cmd(client, message):
    if not message.from_user: return
    if not await _is_operator(client, message): return
    target = message.reply_to_message.from_user if message.reply_to_message and message.reply_to_message.from_user else None
    if not target:
        await message.reply_text("Balas pesan user yang ingin direset peringatannya."); return
    await clear_warns(message.chat.id, target.id)
    await message.reply_text(f"✅ Warn {target.mention} direset.")
    mod_log(_modlog_line("RESETWARN", message.from_user, target))

@app.on_message(filters.command("mute") & filters.group)
async def mute_cmd(client, message):
    if not message.from_user: return
    if not await _is_operator(client, message): return
    if not message.reply_to_message or not message.reply_to_message.from_user:
        await message.reply_text("Balas pesan user. Format: /mute [durasi] (cth: 10m, 2h, 1d)."); return
    dur_arg = message.command[1] if len(message.command) > 1 else None
    seconds = _parse_duration_to_seconds(dur_arg)
    target = message.reply_to_message.from_user
    try:
        await mute_user(client, message.chat.id, target.id, seconds)
        await message.reply_text(f"🔇 {target.mention} di-mute {seconds//60} menit.")
        mod_log(_modlog_line("MUTE", message.from_user, target, extra=f"{seconds}s"))
    except Exception as e:
        await message.reply_text("❌ Gagal mute. Pastikan bot admin."); logger.error(f"/mute error: {e}")

@app.on_message(filters.command("unmute") & filters.group)
async def unmute_cmd(client, message):
    if not message.from_user: return
    if not await _is_operator(client, message): return
    if not message.reply_to_message or not message.reply_to_message.from_user:
        await message.reply_text("Balas pesan user yang ingin di-unmute."); return
    target = message.reply_to_message.from_user
    try:
        await unmute_user(client, message.chat.id, target.id)
        await message.reply_text(f"✅ {target.mention} sudah boleh bicara lagi.")
        mod_log(_modlog_line("UNMUTE", message.from_user, target))
    except Exception as e:
        await message.reply_text("❌ Gagal unmute."); logger.error(f"/unmute error: {e}")

@app.on_message(filters.command("ban") & filters.group)
async def ban_cmd(client, message):
    if not message.from_user: return
    if not await _is_operator(client, message): return
    if not message.reply_to_message or not message.reply_to_message.from_user:
        await message.reply_text("Balas pesan user yang ingin di-ban."); return
    target = message.reply_to_message.from_user
    try:
        await ban_user(client, message.chat.id, target.id)
        await message.reply_text(f"🚫 {target.mention} di-ban.")
        mod_log(_modlog_line("BAN", message.from_user, target))
    except Exception as e:
        await message.reply_text("❌ Gagal ban."); logger.error(f"/ban error: {e}")

@app.on_message(filters.command("kick") & filters.group)
async def kick_cmd(client, message):
    if not message.from_user: return
    if not await _is_operator(client, message): return
    if not message.reply_to_message or not message.reply_to_message.from_user:
        await message.reply_text("Balas pesan user yang ingin di-kick."); return
    target = message.reply_to_message.from_user
    try:
        await kick_user(client, message.chat.id, target.id)
        await message.reply_text(f"👢 {target.mention} di-kick.")
        mod_log(_modlog_line("KICK", message.from_user, target))
    except Exception as e:
        await message.reply_text("❌ Gagal kick."); logger.error(f"/kick error: {e}")

@app.on_message(filters.command("del") & filters.group)
async def del_cmd(client, message):
    if not message.from_user: return
    if not await _is_operator(client, message): return
    if message.reply_to_message:
        try: await message.reply_to_message.delete()
        except Exception: pass
    try: await message.delete()
    except Exception: pass

# --- Anti-link & Badwords (group) ---

def _extract_domains(text: str):
    domains = set()
    for match in URL_REGEX.findall(text or ""):
        url = match
        if "://" not in url:
            url = "https://" + url
        try:
            netloc = urlparse(url).netloc.lower()
            if netloc:
                domains.add(netloc.split(":")[0])
        except Exception:
            continue
    return domains

@app.on_message(filters.text & filters.group, group=5)  # group bebas, asal tidak tabrakan
async def moderation_guard(client, message):
    text = (message.text or message.caption or "").strip()
    if not text:
        return

    # 1) Filter badwords
    if BAD_WORDS and BAD_WORDS_RE.search(text):
        try:
            await message.delete()
        except Exception:
            pass
        try:
            await message.reply("⚠️ Bahasa jaga ya, hindari kata-kata kasar.")
        except Exception:
            pass
        return

    # 2) Anti-link (sederhana)
    #    Deteksi URL http/https. (Optional: kamu bisa tambah deteksi skema-less)
    urls = [m.group(0) for m in URL_REGEX.finditer(text)]


    for u in urls:
        if not is_allowed_domain(u):
            try:
                await message.delete()
            except Exception:
                pass
            try:
                await message.reply("🔗 Link luar tidak diizinkan di sini.")
            except Exception:
                pass
            return

# --- Perintah Umum ---

from pyrogram.enums import ParseMode

@app.on_message(filters.command("start") & filters.private)
async def start_command(client, message):
    if len(message.command) > 1:
        param = message.command[1].lower()

        # === START LAPOR ===
        if param == "lapor":
            grant_xp_for_command(message, "lapor")
            user_id = message.from_user.id
            if user_id not in waiting_lapor_users:
                waiting_lapor_users.add(user_id)

            await message.reply(
                "👋 Hai, silahkan melapor!\n"
                "✍️ Kirim **teks atau media** (Foto, Video, Voice, Dokumen).\n"
                "❌ Kalau berubah pikiran, ketik **/batal**.\n\n"
                "⚠ **Tips:**\n"
                "Tuliskan semua laporanmu dalam satu kali kirim supaya Admin Pusat bisa langsung membacanya dengan jelas.",
                parse_mode=ParseMode.MARKDOWN
            )
            return

        # === START PANDUAN ===
        elif param == "panduan":
            await cmd_panduan(client, message)
            return

        # === START KOLEKSI ===
        else:
            start_param = param
            stream_link, _ = get_stream_data(start_param)

            if not stream_link:
                await message.reply(
                    f"❌ KODE <code>{start_param}</code> tidak ditemukan.\n\n"
                    f"Silakan periksa kembali kodenya di channel @{CHANNEL_USERNAME}.\n\n"
                    "👉 Jika masih ada kendala:\n"
                    f"📩 <a href='https://t.me/BangsaBacol_Bot?start=lapor'>Lapor ke Admin</a>\n"
                    f"📜 <a href='https://t.me/BangsaBacolBot?start=panduan'>Baca Panduan Bot</a>",
                    parse_mode=ParseMode.HTML
                )
                return

            buttons = [
                [InlineKeyboardButton("📢 CHANNEL UTAMA", url=f"https://t.me/{CHANNEL_USERNAME}")],
                [InlineKeyboardButton("🔁 CHANNEL BACKUP", url=f"https://t.me/{EXTRA_CHANNEL}")],
                [InlineKeyboardButton("👥 JOIN GROUP", url=f"https://t.me/{GROUP_USERNAME}")],
                [InlineKeyboardButton("🔒 BUKA KOLEKSI", callback_data=f"verify_{start_param}")],
            ]

            await message.reply_photo(
                photo="Img/terkunci.jpg",
                caption=(
                    "✨ <b>Akses Koleksi Tersedia!</b> ✨\n\n"
                    "Pastikan kamu sudah join channel & group untuk membuka koleksi.\n\n"
                    "👉 Jika masih ada kendala:\n"
                    f"📩 <a href='https://t.me/BangsaBacol_Bot?start=lapor'>Lapor ke Admin</a>\n"
                    f"📜 <a href='https://t.me/BangsaBacolBot?start=panduan'>Baca Panduan Bot</a>"
                ),
                reply_markup=InlineKeyboardMarkup(buttons),
                parse_mode=ParseMode.HTML
            )

            logger.info(
                f"User {message.from_user.id} (@{message.from_user.username or 'unknown'}) "
                f"requested code '{start_param}'."
            )
            return

    # === DEFAULT START TANPA PARAMETER ===
    teks = (
        "👋 <b>Selamat Datang di Bangsa Bacol Bot</b> 🤖\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Aku adalah bot utama dari komunitas <b>Bangsa Bacol</b>.\n"
        "Tugasku adalah membuka akses koleksi, bantu cari kode, "
        "serta memberi info seputar dunia Bacol 🚀\n\n"
    
        "📌 <b>Cara Menggunakan Bot:</b>\n"
        "• Ketik: <code>/start nama_koleksi</code>\n"
        "  Contoh: <code>/start fifisharma</code>\n"
        "• Lihat daftar kode koleksi di channel [@BANGSABACOL]\n\n"
    
        "🔑 <b>Perintah Utama:</b>\n"
        "• <code>/lapor</code> → Laporan ke admin-pusat\n"
        "• <code>/ping</code> → Cek status bot\n"
        "• <code>/profile</code> → Lihat profil kamu\n"
        "• <code>/list</code> → Daftar koleksi\n"
        "• <code>/search</code> → Cari koleksi\n"
        "• <code>/random</code> → Pilih koleksi random\n"
        "• <code>/joinvip</code> → Unlock full koleksi\n"
        "• <code>/request</code> → Request koleksi\n"
        "• <code>/about</code> → Tentang bot ini\n"
        "• <code>/bot</code> → Daftar bot resmi\n"
        "• <code>/panduan</code> → Panduan penggunaan\n\n"
    
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🆘 <b>Bantuan & Dukungan</b>\n"
        "🔔 <a href='https://t.me/BangsaBacol/8'>Daftar Bantuan</a>\n"
        "📩 <a href='https://t.me/BangsaBacol_Bot?start=lapor'>Lapor ke Admin-Pusat</a>\n\n"
        "🔥 <i>Jangan lupa, join VIP untuk pengalaman tanpa batas!</i>"
    )

    await message.reply(teks, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

@app.on_message(filters.command("stats"))
async def stats_command(client, message):
    if not is_owner(message):  # <-
        await message.reply("❌ Perintah ini hanya untuk OWNER, gak penting kok buat kamu.")
        return
    try:
        period_days = 7
        stats = parse_clicks_log_json(days_back=period_days)
        if stats["status"] in ("no_log_file", "read_error", "no_recent_clicks"):
            text = (
                f"📈 Statistik ({period_days} hari)\n\n"
                f"🔢 Total klik: {stats.get('total_clicks', 0)}\n"
                f"👥 Pengguna unik: {stats.get('unique_users', 0)}\n\n"
                f"ℹ️ {stats.get('message', 'Belum ada data.')}"
            )
            if message.from_user and message.from_user.id == OWNER_ID:
                log = _check_log_file_status()
                text += (
                    f"\n\n🔧 Debug (Admin)\n"
                    f"• File log: {'✅' if log['exists'] else '❌'}\n"
                    f"• Baris: {log.get('lines', 0)}\n"
                    f"• Ukuran: {log.get('size', 0)} B\n"
                )
            await message.reply(text, parse_mode=ParseMode.MARKDOWN); return

        items = sorted(stats.get("by_code", {}).items(), key=lambda x: x[1], reverse=True)[:5]
        lines = [
            f"📈 Statistik ({period_days} hari)",
            f"🔢 Total klik: {stats['total_clicks']}",
            f"👥 Pengguna unik: {stats['unique_users']}",
            "",
            "🏆 Top 5 Kode:" if items else "Tidak ada data kode untuk periode ini."
        ]
        for i, (code, count) in enumerate(items, 1):
            lines.append(f"{i}. {code} — {count}")
        await message.reply("\n".join(lines), parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Error di /stats: {e}")
        await message.reply("❌ Terjadi kesalahan saat menghasilkan statistik.")

@app.on_message(filters.command("log"))
async def log_command(client, message):
    """OWNER: tampilkan 20 baris terakhir klik human log."""
    if not is_owner(message):
        await message.reply("❌ Apa sih?! Perintah ini hanya untuk OWNER."); return
    if not CLICKS_HUMAN.exists():
        await message.reply("📭 Belum ada log akses tercatat."); return
    try:
        with open(CLICKS_HUMAN, "r", encoding="utf-8") as f:
            lines = f.readlines()
        last = lines[-20:] if len(lines) > 20 else lines
        text = "".join(last)
        if len(text) > 3500:
            text = "... (dipotong)\n" + text[-3500:]
        await message.reply(f"<b>📜 20 Log Akses Terakhir</b>\n\n<pre>{text}</pre>", parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Gagal membaca clicks_human.log: {e}")
        await message.reply("❌ Gagal membaca file log.")

@app.on_message(filters.command("dashboard"))
async def dashboard_command(client, message):
    if not is_owner(message):  # <-
        await message.reply("❌ Gak usah kepo! Perintah ini hanya untuk OWNER.")
        return
    try:
        period_days = 7
        text = build_dashboard_text(period_days)
        kb = build_dashboard_keyboard(period_days)
        await message.reply(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        logger.error(f"Error di /dashboard: {e}")
        await message.reply("❌ Error memuat dashboard.")

@app.on_callback_query(filters.regex(r"^dashboard:\d+$"))
async def dashboard_cb_period(client, cq: CallbackQuery):
    try:
        period_days = int(cq.data.split(":")[1])
        text = build_dashboard_text(period_days)
        kb = build_dashboard_keyboard(period_days)
        try:
            await cq.message.edit_text(text, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
        except MessageNotModified:
            pass
        await cq.answer()
    except Exception as e:
        logger.error(f"Error dashboard callback: {e}")
        await cq.answer("❌ Gagal memperbarui dashboard.", show_alert=False)

@app.on_message(filters.command("reload_interaction") & filters.user(OWNER_ID))
async def reload_interaction_cmd(client, message):
    try:
        load_interaction_config()
        await message.reply(f"✅ Reload Interaction OK. ({len(INTERACTION_MESSAGES)} pesan, interval {INTERACTION_INTERVAL_MINUTES}m)")
    except Exception:
        await message.reply("❌ Gagal reload interaction config. Cek log.")

@app.on_message(filters.command("list"))
async def list_command(client, message):
    grant_xp_for_command(message, "list")
    user_id = message.from_user.id
    username = message.from_user.username or ""
    log_user_activity(user_id, username)

    # --- Gabungan cek akses ---
    if not (is_owner(message) or is_admin(message) or has_stellar_or_higher(user_id)):
        teks = (
            "❌ <b>Akses Ditolak!</b>\n\n"
            "Fitur ini hanya tersedia untuk pengguna dengan badge tingkat lanjut:\n"
            "• <b>Stellar 🥈</b>\n"
            "• <b>Starlord 🥇</b>\n\n"
            "👉 Cara mendapatkannya:\n"
            "1. Gunakan perintah /profile untuk cek XP & badge kamu.\n"
            "2. Kumpulkan XP setiap hari dengan memakai perintah bot.\n"
            "3. Naikkan level badge-mu sampai minimal <b>Stellar 🥈</b>.\n\n"
            "✨ Setelah badge cukup, kamu otomatis bisa membuka fitur ini."
        )
        await message.reply(teks, parse_mode=ParseMode.HTML)
        return
    # --------------------------

    codes = sorted(list(STREAM_MAP.keys()))
    if not codes:
        await message.reply("📭 Daftar koleksi kosong.")
        return
    
    page_codes, page, pages, total = paginate_codes(codes, 1)

    txt = (f"📜 DAFTAR KOLEKSI BANGSA BACOL\n"
           "Pilih kode di bawah untuk melihat detail:")
           
    await message.reply(
        txt,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=build_list_keyboard(page_codes, page, pages)
    )

@app.on_message(filters.command("healthcheck") & filters.private)
async def healthcheck_cmd(client, message):
    checked_at = datetime.now(JAKARTA_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
    """OWNER: Health check semua URL di STREAM_MAP."""
    user = message.from_user
    if user.id != OWNER_ID:
        await message.reply_text("❌ Kepo amat! Hanya owner yang dapat menggunakan command ini."); return
    try:
        await message.reply_text("🔄 Sedang melakukan health check semua URLs...")
        results = await health_check_all_urls()
        if not results:
            await message.reply_text("❌ Tidak ada URL untuk di-check."); return
        healthy_count = sum(1 for r in results if r['is_healthy'])
        total_count = len(results)
        success_rate = (healthy_count / total_count * 100) if total_count > 0 else 0
        summary = f"""
📊 HEALTH CHECK REPORT
━━━━━━━━━━━━━━━━━━
✅ Healthy: {healthy_count}
❌ Unhealthy: {total_count - healthy_count}
📈 Success Rate: {success_rate:.1f}%
f"🕐 checked_at: {checked_at}"

Detail (maks 20):
"""
        for r in results[:20]:
            icon = "✅" if r['is_healthy'] else "❌"
            summary += f"\n{icon} `{r['key']}` - {r['status_code']} ({r['response_time_ms']:.0f}ms)" + (f" - {r['error']}" if r['error'] else "")
        if len(results) > 20:
            summary += f"\n\n... dan {len(results) - 20} URLs lainnya"
        await message.reply_text(summary, parse_mode=ParseMode.MARKDOWN)
        with open(HEALTH_LOG, "a", encoding="utf-8") as f:
            f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] User {user.id} (@{user.username or 'unknown'}) health check: {healthy_count}/{total_count} healthy\n")
    except Exception as e:
        await message.reply_text(f"❌ Error during health check: {str(e)}")

# Extra: prune logs command (owner)
@app.on_message(filters.command("prune_logs"))
async def prune_logs_cmd(client, message):
    if not is_owner(message):
        await message.reply("❌ Hadeh! Perintah ini hanya untuk OWNER."); return
    days = RETENTION_DAYS
    try:
        if len(message.command) > 1 and message.command[1].isdigit():
            days = max(1, int(message.command[1]))
    except Exception:
        pass
    prune_clicks_log(days)
    await message.reply(f"🧹 Log dikompak untuk {days} hari terakhir.")

# --- Admin-Only: manage links ---
@app.on_message(filters.command("add") & filters.private)
async def add_link_command(client, message):
    if not is_owner(message):
        await message.reply("❌ Ngapain?! Perintah ini hanya untuk owner.")
        logger.warning(f"Unauthorized access attempt to /add by user {message.from_user.id}")
        return
    try:
        # /add <kode> <link> [thumbnail]
        parts = message.text.split(maxsplit=3)
        if len(parts) < 3:
            raise ValueError("❌ Format tidak valid. Gunakan:\n`/add <kode> <link> [thumbnail_dengan_ekstensi]`")
        _, code, link, *rest = parts
        thumbnail = rest[0].strip() if rest else None
        if thumbnail and "." not in thumbnail:
            thumbnail += ".jpg"

        if code in STREAM_MAP:
            await message.reply(f"⚠️ Kode `{code}` sudah ada. Link akan diupdate.", parse_mode=ParseMode.MARKDOWN)

        STREAM_MAP[code] = {"link": link}
        if thumbnail:
            STREAM_MAP[code]["thumbnail"] = thumbnail
        save_stream_map()

        await message.reply(
            f"✅ Berhasil menambahkan/mengupdate kode `{code}`.\nLink: `{link}`\nThumbnail: `{thumbnail or 'Tidak ada'}`",
            parse_mode=ParseMode.MARKDOWN,
        )
        logger.info(f"Owner {message.from_user.id} menambahkan/mengupdate kode '{code}'")
    except Exception as e:
        logger.error(f"Invalid format for /add: {e}")
        await notify_owner(f"/add error: {e}")
        await message.reply(
            "❌ Format tidak valid. Gunakan:\n`/add <kode> <link> [nama_thumbnail_tanpa_ekstensi]`",
            parse_mode=ParseMode.MARKDOWN,
        )

@app.on_message(filters.command("delete") & filters.private)
async def delete_link_command(client, message):
    if not is_owner(message):
        await message.reply("❌ Kamu siapa? Perintah ini hanya untuk owner.")
        logger.warning(f"Unauthorized access attempt to /delete by user {message.from_user.id}")
        return
    try:
        parts = message.text.split()
        if len(parts) < 2:
            await message.reply("❌ Gunakan:\n`/delete <kode>`", parse_mode=ParseMode.MARKDOWN)
            return
        code = parts[1]
        if code not in STREAM_MAP:
            await message.reply(f"⚠️ Kode `{code}` tidak ditemukan.", parse_mode=ParseMode.MARKDOWN)
            return
        del STREAM_MAP[code]
        save_stream_map()
        await message.reply(f"🗑️ Berhasil menghapus kode `{code}`.", parse_mode=ParseMode.MARKDOWN)
        logger.info(f"Owner {message.from_user.id} menghapus kode '{code}'")
    except Exception as e:
        logger.error(f"Error /delete: {e}")
        await notify_owner(f"/delete error: {e}")
        await message.reply("❌ Terjadi kesalahan saat memproses perintah.")

# ============================================================
# 5) HANDLER UMUM (bisa diakses semua orang)
# ============================================================
BADGE_TIERS = [
    ("Starlord 🥇", 100),
    ("Stellar 🥈", 50),
    ("Shimmerr 🥉", 20),
    ("Stranger 🔰", 0),
]

def has_shimmer_or_higher(user_id: int) -> bool:
    """Cek apakah user minimal punya badge Shimmer 🥉 atau lebih tinggi."""
    data = load_user_data()
    info = data.get(str(user_id), {})
    xp = int(info.get("xp", 0))
    badge = _badge_for_xp(xp)
    return badge in ["Shimmer 🥉", "Stellar 🥈", "Starlord 🥇"]

def _badge_for_xp(xp: int) -> str:
    for name, threshold in BADGE_TIERS:
        if xp >= threshold:
            return name
    return "Stranger 🔰"

def _next_tier_info(xp: int):
    tiers = sorted(BADGE_TIERS, key=lambda t: t[1])
    for name, threshold in tiers:
        if xp < threshold:
            return name, threshold - xp
    return None, 0  # sudah max

def _progress_bar(xp: int) -> str:
    # progress menuju tier berikutnya
    next_name, remain = _next_tier_info(xp)
    if not next_name:
        return "▰▰▰▰▰ MAX"
    tiers = sorted([t[1] for t in BADGE_TIERS])
    # cari batas bawah & atas segment saat ini
    lower = max([t for t in tiers if t <= xp], default=0)
    upper_candidates = [t for t in tiers if t > xp]
    upper = min(upper_candidates) if upper_candidates else lower
    span = max(upper - lower, 1)
    filled = int(round(5 * (xp - lower) / span))
    filled = max(0, min(5, filled))
    return "▰" * filled + "▱" * (5 - filled)

@app.on_message(filters.command("profile"))
async def profile_cmd(client, message):
    user = message.from_user
    if not user:
        return
    user_id = user.id
    username = user.username or "-"

    # Tambah XP lewat helper (maks 1x per hari per command)
    grant_xp_for_command(message, "profile")

    # Ambil data user
    data = load_user_data()
    info = data.get(str(user_id), {
        "username": username,
        "xp": 0,
        "badge": "Stranger 🔰",
        "last_xp_dates": {}
    })
    xp = int(info.get("xp", 0))
    badge = _badge_for_xp(xp)

    today = _now_jkt().strftime("%Y-%m-%d")
    sudah_dapat = info.get("last_xp_dates", {}).get("profile") == today

    # Hitung progress & target tier berikut
    next_name, remain = _next_tier_info(xp)
    progress = _progress_bar(xp)

    # Ambil riwayat XP hari ini (command apa saja yang sudah kasih XP)
    last_xp_dates = info.get("last_xp_dates", {})
    claimed_today = [cmd for cmd, d in last_xp_dates.items() if d == today]
    claimed_today.sort()

    # Hitung total XP hari ini
    claimed_count = len(claimed_today)

    # Daftar semua command yang bisa kasih XP
    xp_commands = [
        "profile", "ping", "random", "list", "lapor", "about",
        "bot", "joinvip", "panduan", "search", "request"
    ]
    max_daily = len(xp_commands)

    # Teks riwayat
    if claimed_today:
        riwayat = "📌 XP hari ini dari: " + ", ".join(f"<code>/{c}</code>" for c in claimed_today)
    else:
        riwayat = "📌 Belum ada XP hari ini."

    # Bangun teks profil
    teks = (
        "👤 <b>PROFIL PENGGUNA</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<pre>"
        f"User   : @{username or 'unknown'}\n"
        f"ID     : {user_id}\n"
        f"Badge  : {badge}\n"
        f"XP     : {xp}  {progress}\n"
        "</pre>"
    )

    if next_name:
        teks += f"⬆️ Menuju : <b>{next_name}</b> ({remain} XP lagi)\n"
    else:
        teks += "🚀 Kamu sudah di <b>tier tertinggi!</b>\n"

    # Klaim harian
    teks += "\n🎁 <b>Klaim Harian</b>\n"
    if sudah_dapat:
        teks += "✅ Kamu sudah klaim XP hari ini lewat /profile.\n"
    else:
        teks += "🕓 Belum klaim hari ini. (XP otomatis ditambahkan saat /profile)\n"

    # Statistik harian
    teks += (
        f"\n📊 <b>XP Harian</b>\n"
        f"• Total klaim hari ini : <b>{claimed_count}</b> / {max_daily} kemungkinan\n"
    )

    # Riwayat
    if claimed_today:
        teks += "• Sumber XP hari ini   : " + ", ".join(f"<code>/{c}</code>" for c in claimed_today) + "\n"
    else:
        teks += "• Sumber XP hari ini   : belum ada\n"

    teks += "━━━━━━━━━━━━━━━━━━━━━━━"

    await message.reply_text(teks, parse_mode=ParseMode.HTML)

@app.on_message(filters.command("panduan"))
async def cmd_panduan(client, message):
    grant_xp_for_command(message, "panduan")
    user = message.from_user.first_name if message.from_user else "Pengguna"
    username = f"@{message.from_user.username}" if (message.from_user and message.from_user.username) else user

    teks = f"""
◢ 📖 <b>PANDUAN</b> ◣

👋 Hallo {username}  
Aku <b>Bangsa Bacol Bot</b> 🤖 — Bot Utama channel <a href="https://t.me/BangsaBacol">@BangsaBacol</a>!  
Aku jelasin cara pakai Bot ini, biar kamu gak puyeng! 🚀  
————————————————————————
📌 <b>BEGINI CARA PENGGUNAANNYA:</b>  

1️⃣ <b>Syarat Wajib!</b>  
► Pastikan kamu sudah join <b>channel & group Bangsa Bacol</b>  

2️⃣ <b>Coba ketik:</b> <code>/start fifisharma</code>  
► <code>fifisharma</code> adalah nama kode, untuk membuka akses koleksinya

3️⃣ <b>Penjelasan Respon Bot!</b>  
► Jika kode benar → akses koleksi terbuka  
► Jika kode salah → cek ulang kodemu  
► Atau kode koleksi tidak ada  

4️⃣ <b>Daftar Kode Koleksi!</b>  
► Cek di <a href="https://t.me/BangsaBacol">@BangsaBacol</a>  

5️⃣ <b>Beres!</b>  
► Gampang banget kan 🥰
————————————————————————
🅿 <b>STATUS BOT:</b> Cek dengan <code>/ping</code>  
✅ Bot merespon → <b>Bot aktif</b>  
⛔ Diam → <b>Bot nonaktif</b>  
🆘 Laporan bisa ke <a href="https://t.me/BangsaBacol_Bot?start=lapor">ADMIN-PUSAT!</a>  

✨ Coba juga perintah <code>/random</code> untuk seru seru-an! 😜  

📝 <b>Catatan:</b>  
Mau akses lebih cepat? Gabung <b>VIP BangsaBacol</b>. Karena para Admin & Menteri Bangsa Bacol jauh lebih aktif di sana. Aku sendiri hanya ditugaskan untuk mengelola Channel Publik Bangsa Bacol.  

🔔 Daftar Bantuan → <a href="https://t.me/BangsaBacol/8">Klik di sini</a>  
🔑 Join VIP → <a href="https://trakteer.id/BangsaBacol/showcase">Klik di sini</a>  
💰 Support Seikhlasnya → <a href="https://trakteer.id/BangsaBacol/tip">Klik di sini</a>  
🔥 Okey terimakasih, silahkan <b>ritual kenikmatan</b>! 😏
"""
    await message.reply_text(teks, disable_web_page_preview=True, parse_mode=ParseMode.HTML)

# ============================================================
# 6) HANDLER OWNER-ONLY
# ============================================================
@app.on_message(filters.command("helper") & filters.private)
async def cmd_helper(_: Client, m) -> None:
    user_id = m.from_user.id

    if user_id != OWNER_ID:
        await m.reply_text("❌ Hadeh! Perintah ini hanya untuk OWNER!")
        return

    teks = """
◢ 🛠 <b>HELPER</b> ◣

🔗 <b>Link Web</b>  
➡ https://namakoleksi.netlify.app/

🤖 <b>Start Bot</b>  
➡ https://t.me/BangsaBacolBot?start=

⚠️ <b>PENGGUNAAN</b> ⚠️  

🌐 <b>Link Web → Bot Publik</b>  
✦ <code>/add</code> kode linkweb thumbnail.jpg  
Menambahkan update ke <b>stream.json</b>  
(Thumbnail wajib ada di folder <b>Img Bot</b>)  

📷 <b>Link Start → Bot Post</b>  
✦ <code>/post1</code> → kirim/reply gambar  
[Mengirim Spoiler Post di Publik (Post 1)]  

✦ <code>/editpost</code> kode linkstart  
[Memperbarui & Drop Post Publik (Post 1)]

✦ <code>/post4</code> Custom Text → kirim/reply gambar  
[Mengirim Spoiler Post di Publik (Post 4)]  

✦ <code>/editpost2</code> kode linkstart  
[Memperbarui & Drop Post Publik (Post 4)]  

↜ <b>KEEP IT UP</b> ↝
"""
    await m.reply_text(teks, disable_web_page_preview=True, parse_mode=ParseMode.HTML)

# --- General ---

@app.on_message(filters.command("bot"))
async def bot_command(client, message):
    grant_xp_for_command(message, "bot")
    # Tombol → baris 1 (utama), baris 2 (mirror + lapor)
    buttons = [
        [InlineKeyboardButton("✅ BOT UTAMA", url=f"https://t.me/{BOT_MIRRORS[0]['username']}")],
        [
            InlineKeyboardButton("🤖 Stephander", url=f"https://t.me/{BOT_MIRRORS[1]['username']}"),
            InlineKeyboardButton("🤖 Seraphina", url=f"https://t.me/{BOT_MIRRORS[2]['username']}")
        ]
    ]

    kb = InlineKeyboardMarkup(buttons)

    teks = (
        "🤖 <b>DAFTAR BOT RESMI BANGSA BACOL</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🟢 <b>Bot Utama</b> | Bangsa Bacol\n"
        f"➥ @{BOT_MIRRORS[0]['username']}\n\n"
        f"🟡 <b>Bot Mirror</b> | Stephander\n"
        f"➥ @{BOT_MIRRORS[1]['username']}\n\n"
        f"🔵 <b>Bot Lapor</b> | Seraphina\n"
        f"➥ @{BOT_MIRRORS[2]['username']}\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "📌 <b>Panduan Pemakaian:</b>\n"
        "• Gunakan 🟢 <b>Bot Utama</b> untuk semua aktivitas normal.\n"
        "• Jika Bot Utama <b>sibuk/error</b>, gunakan 🟡 <b>Bot Mirror</b> sebagai cadangan.\n"
        "• Untuk <b>laporan & pesan admin</b>, gunakan 🔵 <b>Bot Lapor</b>.\n\n"
        "⚠️ <i>Gunakan hanya bot resmi di atas. Jangan percaya pada akun lain yang mengatasnamakan Bangsa Bacol!</i>"
    )

    await message.reply(teks, reply_markup=kb, parse_mode=ParseMode.HTML)

@app.on_message(filters.command("ping"))
async def ping_cmd(client, message):
    grant_xp_for_command(message, "ping")
    await message.reply("✅ Pong! Bot aktif dan responsif.")

@app.on_message(filters.private & filters.command("profile"))
async def profile_command(client: Client, message: Message):
    user = update_user_xp(message.from_user.id, message.from_user.username, "profile")

    # progress badge
    xp = int(user.get("xp", 0))
    badge = normalize_badge(user.get("badge", BADGE_STRANGER))

    # threshold & next target
    tiers = [
        (BADGE_STRANGER, 0),
        (BADGE_SHIMMER, 20),
        (BADGE_STELLAR, 80),
        (BADGE_STARLORD, 150),
    ]
    # cari next tier
    next_label, next_need = None, None
    for i, (label, need) in enumerate(tiers):
        if xp < need:
            next_label, next_need = label, need
            break
    if next_label is None:
        next_label, next_need = BADGE_STARLORD, 150

    # progress bar teks
    def bar(x):
        total = 10
        # estimasi relatif ke target saat ini
        # cari current floor
        floor_need = 0
        for lbl, need in tiers:
            if x >= need:
                floor_need = need
        span = max(1, next_need - floor_need)
        filled = int(round(min(1.0, (x - floor_need)/span) * total))
        return "[" + "█"*filled + "·"*(total - filled) + "]"

    # status kuota random (pakai helper yang sudah ada jika ada)
    quota_text = ""
    try:
        status = get_random_quota_status(message.from_user.id)   # jika fungsi ini sudah ada di kode
        # status dict: {"remaining":int, "resets_at": datetime/iso str} — sesuaikan dengan punyamu
        remaining = status.get("remaining")
        resets_at = status.get("resets_at")
        quota_text = f"\n🎲 Random: {remaining} tersisa, reset: {resets_at}"
    except Exception:
        pass

    text = (
        f"👤 <b>PROFILE</b>\n"
        f"• User: @{message.from_user.username or message.from_user.id}\n"
        f"• XP: <b>{xp}</b>\n"
        f"• Badge: <b>{badge}</b>\n"
        f"• Progress {bar(xp)} menuju <b>{next_label}</b> ({xp}/{next_need})"
        f"{quota_text}"
    )
    await message.reply_text(text, disable_web_page_preview=True)

@app.on_message(filters.command("random"))
async def random_command(client, message):
    grant_xp_for_command(message, "random")
    user_id = message.from_user.id

    log_user_activity(user_id, message.from_user.username or "")

    in_channel = await is_member(client, user_id, CHANNEL_USERNAME)
    in_group   = await is_member(client, user_id, GROUP_USERNAME)
    is_extra_member   = await is_member(client, user_id, EXTRA_CHANNEL)
    if not in_channel or not in_group:
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📢 Channel Utama", url=f"https://t.me/{CHANNEL_USERNAME}")],
            [InlineKeyboardButton("🔁 Channel Backup", url=f"https://t.me/{EXTRA_CHANNEL}")],
            [InlineKeyboardButton("💬 Join Group",   url=f"https://t.me/{GROUP_USERNAME}")]
        ])
        await message.reply_text("⚠️ **TERCYDUK BELUM JOIN! ⚠️**\nKAMU HARUS JOIN GROUP & CHANNEL DULU WAHAI ORANG ASING!", reply_markup=keyboard)
        return

    allowed, remaining_after, limit, reset_sec = await consume_random_quota(user_id)
    if not allowed:
        await message.reply_text(f"⛔ Jatah harian /random habis.\nLimit {limit}x/hari • Reset { _format_eta(reset_sec) } lagi.")
        return

    if not STREAM_MAP:
        await message.reply_text("⚠️ Belum ada koleksi tersedia.")
        return

    valid = []
    for k, v in STREAM_MAP.items():
        if isinstance(v, str) and v.strip():
            valid.append((k, v.strip(), None))
        elif isinstance(v, dict) and v.get("link"):
            valid.append((k, v["link"], v.get("thumbnail")))

    if not valid:
        await message.reply_text("⚠️ Tidak ada koleksi valid.")
        return

    kode, link, thumb = random.choice(valid)
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔗 TONTON SEKARANG", url=link)]])
    caption = f"🎲 Koleksi Random\n<b>Kode:</b> <code>{kode}</code>\n<i>Sisa jatah hari ini: {remaining_after}/{limit}</i>"

    if thumb and Path(f"Img/{thumb}").exists():
        await message.reply_photo(photo=f"Img/{thumb}", caption=caption, reply_markup=kb, parse_mode=ParseMode.HTML)
    else:
        await message.reply_text(caption, reply_markup=kb, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

@app.on_message(filters.command("quota"))
async def quota_command(client, message):
    used, remaining, limit, reset_sec = await get_random_quota_status(message.from_user.id)
    await message.reply_text(
        f"📊 Jatah /random kamu hari ini:\nDipakai: {used}\nSisa: {remaining}\nLimit: {limit}\nReset: { _format_eta(reset_sec) } lagi"
    )

@app.on_message(filters.command("help") & filters.private)
async def help_command(_: Client, m):
    # cek owner
    if m.from_user.id != OWNER_ID:
        await m.reply_text("❌ Sorry! Perintah ini hanya untuk OWNER! Baca Panduan untuk lebih jelasnya.")
        return

    teks = """
🤖 <b>Daftar Perintah Bangsa Bacol Bot</b>

👥 <b>Untuk Semua Pengguna:</b>
• <code>/start</code> Kode → Buka koleksi
• <code>/random</code> → Pilih koleksi acak
• <code>/top</code> → Top user paling aktif (leaderboard)
• <code>/panduan</code> → Cara penggunaan bot
• <code>/ping</code> → Cek status bot
• <code>/joinvip</code> → Unlock Full Koleksi
• <code>/about</code> → Info tentang bot
• <code>/request</code> → Request Koleksi
• <code>/lapor</code> → Lapor ke admin
• <code>/bot</code> → Daftar bot

🛡️ <b>Untuk Moderator:</b>
• <code>/mute @username</code> → bisukan user di group
• <code>/unmute @username</code> → lepas bisu
• <code>/ban @username</code> → blokir user dari group
• <code>/unban @username</code> → unblokir user
• <code>/kick @username</code> → keluarkan user dari group
• <code>/warn @username</code> → beri peringatan (misalnya spam atau badwords)
• <code>/clean</code> → hapus pesan terakhir (spam / iklan)
• <code>/badwords</code> → tampilkan daftar kata terlarang

👑 <b>Khusus Owner/Admin:</b>
• <code>/search</code> Kata Kunci → Cari koleksi
• <code>/list</code> → Tampilkan semua koleksi
• <code>/stats</code> → Akses 7 hari terakhir
• <code>/log</code> → 20 log terakhir
• <code>/dashboard</code> → Dashboard interaktif
• <code>/healthcheck</code> → Cek URL koleksi
• <code>/add</code> Kode Link Thumb → Update koleksi
• <code>/delete</code> Kode → Hapus koleksi
• <code>/helper</code> → Reminder
• <code>/prune_logs</code> Hari → Pangkas log klik sesuai hari
• <code>/reload_badwords</code> → Update Badwords
• <code>/reload_interaction</code> → Update pesan interaksi periodik
• <code>/reset_top</code> → Reset data leaderboard (top user)
"""
    await m.reply_text(teks, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

# -------------------- ABOUT --------------------
@app.on_message(filters.command("about"))
async def about_command(client, message):
    grant_xp_for_command(message, "about")
    teks = """
◢ ℹ️ <b>ABOUT</b> ◣

Hallo Bacolers!
Aku <a href="https://t.me/BangsaBacolBot">@BangsaBacolBot</a>,  
pelayan setia kebangsaan kita! 

Aku diciptakan untuk mengelola <b>Channel Publik Bangsa Bacol</b>,  
serta memberikan akses ke semua koleksi.  
Sedangkan <b>Admin & Menteri</b> aktif di Channel VIP.  

📌 <b>Info Cepat:</b>  
- 📩 Lapor → <a href='https://t.me/BangsaBacol_Bot?start=lapor'>Admin-Pusat</a>
- 📜 Bantuan → <a href='https://t.me/BangsaBacolBot?start=panduan'>Baca Panduan Bot</a>
- 🔑 Join VIP → <a href="https://trakteer.id/BangsaBacol/showcase">Klik di sini</a>  

📢 Channel: <a href="https://t.me/BangsaBacol">@BangsaBacol</a>  
💬 Group: <a href="https://t.me/BangsaBacolGroup">@BangsaBacolGroup</a>
"""
    await message.reply_text(teks, disable_web_page_preview=True, parse_mode=ParseMode.HTML)

@app.on_message(filters.command("joinvip") & filters.private)
async def join_vip(client, message):
    grant_xp_for_command(message, "joinvip")
    url_vip = "https://trakteer.id/BangsaBacol/showcase"
    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔑 Join VIP Sekarang", url=url_vip)]
        ]
    )

    teks = (
        "🌟 <b>BANGSA BACOL VIP</b> 🌟\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "✨ Terima kasih sudah tertarik untuk bergabung menjadi <b>VIP Member</b>!\n"
        "Dengan bergabung, kamu akan mendapatkan keuntungan spesial:\n\n"
        "🔑 <b>Keuntungan VIP:</b>\n"
        "• ✅ <b>Akses Premium</b> ke semua koleksi eksklusif\n"
        "• ⚡ <b>Update lebih cepat</b> & selalu terdepan\n"
        "• 🔒 <b>Channel Privat VIP</b> khusus member\n"
        "• ♾️ <b>Lifetime Access</b> (sekali join, berlaku selamanya)\n\n"
        "————————————————————————\n"
        "💡 <i>VIP adalah jalan tercepat untuk nikmati seluruh koleksi tanpa batas, "
        "plus dapat prioritas dari admin & tim Bangsa Bacol.</i>\n\n"
        "👉 Klik tombol di bawah untuk langsung bergabung ⬇️"
    )

    # Path file video lokal
    video_path = "Img/joinvip.mp4"

    # Kirim video dengan caption dan keyboard
    await message.reply_video(video=video_path, caption=teks, reply_markup=keyboard)

# --- Leaderboard Komunitas ---
USER_ACTIVITY_FILE = Path("data/user_activity.json")

def load_user_activity():
    if USER_ACTIVITY_FILE.exists():
        with open(USER_ACTIVITY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_user_activity(data):
    USER_ACTIVITY_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(USER_ACTIVITY_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def log_user_activity(user_id, username):
    data = load_user_activity()
    user = str(user_id)
    if user not in data:
        data[user] = {"username": username, "count": 0}
    data[user]["count"] += 1
    data[user]["username"] = username  # update username jika berubah
    save_user_activity(data)

@app.on_message(filters.command("top"))
async def top_users_command(client, message):
    data = load_user_activity()
    if not data:
        await message.reply("📊 Belum ada data aktivitas user.")
        return
    # Urutkan berdasarkan count
    top = sorted(data.items(), key=lambda x: x[1]["count"], reverse=True)[:10]
    lines = ["🏆 <b>Top Bacolers</b> (paling aktif):\n"]
    for i, (uid, info) in enumerate(top, 1):
        uname = f"@{info['username']}" if info['username'] else f"ID:{uid}"
        lines.append(f"{i}. {uname} — {info['count']} akses")
    await message.reply("\n".join(lines), parse_mode=ParseMode.HTML)

@app.on_message(filters.command("reset_top") & filters.user(OWNER_ID))
async def reset_top_command(client, message):
    save_user_activity({})
    await message.reply("✅ Data leaderboard direset.")

# ================================
# Lapor System (/lapor)
# ================================
waiting_lapor_users = set()
waiting_feedback_users = set()
last_feedback_time = {}
last_lapor_time = {}
LAPOR_COOLDOWN = timedelta(minutes=1)

@app.on_message(filters.command("lapor") & filters.private, group=10)
async def lapor_start(client, message):
    user_id = message.from_user.id
    now = datetime.now(JAKARTA_TZ)

    last_time = last_lapor_time.get(user_id)
    if last_time and now - last_time < LAPOR_COOLDOWN:
        remain = int((LAPOR_COOLDOWN - (now - last_time)).total_seconds())
        await message.reply(f"⏳ Tunggu {remain} detik sebelum mengirim laporan lagi.")
        return

    # Mode langsung: /lapor <teks>
    args = message.text.split(maxsplit=1)
    if len(args) > 1 and args[1].strip():
        laporan_text = args[1].strip()
        last_lapor_time[user_id] = now
        try:
            user = message.from_user
            mention = f'<a href="tg://user?id={user.id}">{user.first_name}</a>'
            username = f"@{user.username}" if user.username else "(no username)"
            waktu = now.strftime("%Y-%m-%d %H:%M:%S")
            header = (
                "📩 <b>LAPORAN BARU</b>\n"
                f"• Dari: {mention} {username}\n"
                f"• User ID: {user_id}\n"
                f"• Waktu: {waktu}\n"
            )
            await client.send_message(OWNER_ID, header, parse_mode=ParseMode.HTML)
            await client.send_message(OWNER_ID, f"Pesan:\n{laporan_text}")
            await message.reply("✅ Terima kasih! Laporanmu sudah terkirim ke owner.")
        except Exception as e:
            logger.error(f"Gagal kirim laporan langsung: {e}")
            await message.reply("❌ Gagal mengirim laporan.")
        return

    if user_id in waiting_lapor_users:
        await message.reply("⚠️ Kamu masih dalam mode laporan. Kirim pesan/mediamu sekarang atau /batal untuk batal.")
        return

    waiting_lapor_users.add(user_id)
    await message.reply(
        "👋 Hai, silahkan melapor!\n"
        "✍️ Kirim **teks atau media** (Foto, Video, Voice, Dokumen).\n"
        "❌ Kalau berubah pikiran, ketik **/batal**.\n\n"
        "⚠ **Tips:**\n"
        "Tuliskan semua laporanmu dalam satu kali kirim supaya Admin Pusat bisa langsung membacanya dengan jelas.",
        parse_mode=ParseMode.MARKDOWN
    )

@app.on_message(filters.command("batal") & filters.private, group=10)
async def lapor_cancel(client, message):
    user_id = message.from_user.id
    if user_id in waiting_lapor_users:
        waiting_lapor_users.discard(user_id)
        await message.reply("✅ Mode laporan dibatalkan.")
    else:
        await message.reply("ℹ️ Kamu tidak sedang dalam mode laporan.")

@app.on_message(filters.private & ~filters.regex(r"^/"), group=11)
async def lapor_receive(client, message):
    user_id = message.from_user.id
    if user_id not in waiting_lapor_users:
        return  

    try:
        user = message.from_user
        mention = f'<a href="tg://user?id={user.id}">{user.first_name}</a>'
        username = f"@{user.username}" if user.username else "(no username)"
        waktu = datetime.now(JAKARTA_TZ).strftime("%Y-%m-%d %H:%M:%S")

        header = (
            "📩 <b>LAPORAN BARU</b>\n"
            f"• Dari: {mention} {username}\n"
            f"• User ID: {user_id}\n"
            f"• Waktu: {waktu}\n"
        )
        await client.send_message(OWNER_ID, header, parse_mode=ParseMode.HTML)

        if message.media:
            await client.copy_message(OWNER_ID, message.chat.id, message.id)
        elif (message.text or "").strip():
            await client.send_message(OWNER_ID, f"Pesan:\n{message.text}")
        else:
            await client.send_message(OWNER_ID, "⚠️ (Pesan kosong/tidak didukung)")

        await message.reply("✅ Laporanmu sudah diteruskan ke owner.")

    except Exception as e:
        logger.error(f"Gagal terima laporan: {e}")
        await message.reply("❌ Gagal mengirim laporan.")
    finally:
        waiting_lapor_users.discard(user_id)
        last_lapor_time[user_id] = datetime.now(JAKARTA_TZ)
        message.stop_propagation()  # 🔑 hentikan fallback

@app.on_message(filters.command("search"))
async def search_command(client, message):
    user_id = message.from_user.id

    # Cek akses: owner, admin, atau starlord
    if not (is_owner(message) or is_admin(message) or is_starlord(user_id)):
        teks = (
            "❌ <b>Akses Ditolak!</b>\n\n"
            "Perintah ini eksklusif hanya untuk pengguna dengan badge tertinggi:\n"
            "• <b>Starlord 🥇</b>\n\n"
            "👉 Cara mencapainya:\n"
            "1. Gunakan /profile untuk cek XP & badge kamu sekarang.\n"
            "2. Aktif gunakan perintah bot setiap hari untuk kumpulkan XP.\n"
            "3. Tingkatkan level badge-mu step by step:\n"
            "   🔰 Stranger → 🥉 Shimmer → 🥈 Stellar → 🥇 Starlord\n\n"
            "🚀 Setelah mencapai <b>Starlord 🥇</b>, kamu otomatis bisa membuka fitur ini."
        )
        await message.reply(teks, parse_mode=ParseMode.HTML)
        return

    # Validasi keyword
    if len(message.command) < 2:
        await message.reply(
            "ℹ️ Masukkan kata kunci pencarian. Contoh:\n`/search anime`",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    query = " ".join(message.command[1:]).strip()
    if len(query) < 3:
        await message.reply(
            "❌ Kata kunci pencarian minimal 3 huruf.\n\nContoh:\n`/search fif`",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # Cari koleksi
    found = search_codes(query)
    if not found:
        await message.reply(
            f"❌ Tidak ada koleksi yang cocok dengan kata kunci `{query}`.",
            parse_mode=ParseMode.MARKDOWN
        )
        return

    # Tampilkan hasil
    results = "✨ **Hasil Pencarian:**\n\n" + "\n".join([f"• `/start {c}`" for c in found])
    await message.reply(results, parse_mode=ParseMode.MARKDOWN)

# Command request
@app.on_message(filters.private & filters.command("request"))
async def request_cmd(client, message):
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🇮🇩 Lokal", callback_data="vote_lokal")],
        [InlineKeyboardButton("🇨🇳 Chindo", callback_data="vote_chindo")],
        [InlineKeyboardButton("🌍 Bule", callback_data="vote_bule")]
    ])
    await message.reply("📊 Silakan pilih untuk hari ini 👇", reply_markup=keyboard)

# Callback vote
@app.on_callback_query(filters.regex(r"^vote_"))
async def handle_vote(client, callback_query: CallbackQuery):
    user_id = str(callback_query.from_user.id)
    today = datetime.now().date().isoformat()
    votes = load_votes()

    # Cek user sudah vote belum
    if user_id in votes and votes[user_id]["date"] == today:
        await callback_query.answer("⚠️ Kamu sudah vote hari ini!", show_alert=True)
        return

    # Mapping pilihan
    mapping = {
        "vote_lokal": "🇮🇩 Lokal",
        "vote_chindo": "🇨🇳 Chindo",
        "vote_bule": "🌍 Bule"
    }

    choice = mapping.get(callback_query.data, "❓ Tidak diketahui")

    # Simpan vote
    votes[user_id] = {
        "date": today,
        "choice": choice
    }
    save_votes(votes)

    await callback_query.answer(f"✅ Pilihanmu: {choice} tersimpan!", show_alert=True)

# Hasil rekap (khusus admin)
@app.on_message(filters.command("hasil_request") & filters.user([123456789]))  # ganti ID admin
async def hasil_request(client, message):
    today = datetime.now().date().isoformat()
    votes = load_votes()

    lokal = sum(1 for v in votes.values() if v["date"] == today and v["choice"] == "🇮🇩 Lokal")
    chindo = sum(1 for v in votes.values() if v["date"] == today and v["choice"] == "🇨🇳 Chindo")
    bule = sum(1 for v in votes.values() if v["date"] == today and v["choice"] == "🌍 Bule")

    await message.reply(
        f"📊 Rekap hari ini ({today}):\n\n"
        f"🇮🇩 Lokal: {lokal}\n"
        f"🇨🇳 Chindo: {chindo}\n"
        f"🌍 Bule: {bule}"
    )

# ================================
# Unknown / Fallback (paling akhir)
# ================================
@app.on_message(filters.private & ~filters.regex(r"^/"), group=99)
async def unknown_message(client, message):
    user_id = message.from_user.id if message.from_user else None

    # Jika user sedang di mode laporan atau feedback, jangan balas fallback
    if user_id in waiting_lapor_users or user_id in waiting_feedback_users:
        return

    teks = f"""
🤖 <b>Hmmm...</b> aku nggak paham maksudmu.

💡 Coba ketik <code>/start</code> <i>kode_koleksi</i> untuk akses koleksi.  
📌 Daftar kode: <a href="https://t.me/{CHANNEL_USERNAME}">@{CHANNEL_USERNAME}</a>  

Jika kamu masih mengalami kendala:
📜 Daftar Bantuan → <a href="https://t.me/BangsaBacol/8">Klik di sini</a>  
📩 Lapor ke Admin-Pusat → <a href="https://t.me/BangsaBacol_Bot?start=lapor">Klik di sini</a>  
📺 Cara Nonton → <a href="https://t.me/BangsaBacol/26">Klik di sini</a>  
🔑 Join VIP → <a href="https://trakteer.id/BangsaBacol/showcase">Klik di sini</a>  
"""
    await message.reply_text(
        teks,
        disable_web_page_preview=True,
        parse_mode=ParseMode.HTML
    )

# --- Group Welcome ---

@app.on_message(filters.group & filters.new_chat_members)
async def greet_new_member(client, message):
    for user in message.new_chat_members:
        if user.is_bot: continue
        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("📢 JOIN CHANNEL", url=f"https://t.me/{CHANNEL_USERNAME}")],
            [InlineKeyboardButton("👥 JOIN GROUP", url=f"https://t.me/{GROUP_USERNAME}")],
        ])
        welcome_text = (
            f"👋 Selamat datang {user.mention} di **{message.chat.title}**!\n\n"
            "📢 Pastikan join channel & group untuk akses koleksi.\n"
            "📺 Cara nonton: https://t.me/BangsaBacol/26\n\n"
            "Ketik: `/start kode_koleksi` untuk mulai."
        )
        sent = await message.reply_text(welcome_text, parse_mode=ParseMode.MARKDOWN, reply_markup=buttons)
        await asyncio.sleep(120)
        try: await sent.delete()
        except Exception: pass

# --- Callback Query Handlers ---

@app.on_callback_query(filters.regex(r"^(verify|list|list_show|list_close).*"))
async def handle_callback(client: Client, cq: CallbackQuery):
    data = cq.data
    user_id = cq.from_user.id

    # Hanya owner yang boleh akses list
    if data.startswith("list") and not is_owner(cq):
        await cq.answer("Perintah ini khusus untuk owner.", show_alert=True)
        logger.warning(f"Unauthorized list callback attempt by user {user_id}")
        return

    # Daftar kode koleksi (pagination)
    if data.startswith("list|"):
        try:
            page = int(data.split("|")[1])
        except (ValueError, IndexError):
            page = 1
        codes = sorted(list(STREAM_MAP.keys()))
        page_codes, page, pages, total = paginate_codes(codes, page)
        txt = (
            f"📜 Daftar Kode (hal {page}/{pages})\n"
            f"Total: {total} item\n\n"
            "Pilih kode di bawah untuk melihat detail."
        )
        await cq.message.edit_text(
            txt,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=build_list_keyboard(page_codes, page, pages)
        )
        await cq.answer()
        return

    # Tampilkan detail koleksi
    if data.startswith("list_show|"):
        try:
            _, code, return_page = data.split("|", 2)
        except (ValueError, IndexError):
            await cq.answer("Data tidak valid.", show_alert=True)
            return
        link, thumbnail = get_stream_data(code)
        if not link:
            await cq.answer("Kode tidak ditemukan.", show_alert=True)
            return
        txt = f"💿 Koleksi: `{code}`\n🔗 Link: [Tonton Sekarang]({link})"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ Kembali", callback_data=f"list|{return_page}")],
            [InlineKeyboardButton("❌ Tutup", callback_data="list_close")],
        ])
        await cq.message.edit_text(
            txt,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb
        )
        await cq.answer()
        return

    # Tutup daftar koleksi
    if data == "list_close":
        try:
            await cq.message.delete()
        except Exception:
            await cq.message.edit_text("✅ Ditutup.")
        await cq.answer()
        return

    # Verifikasi join group & channel sebelum akses koleksi
    if data.startswith("verify_"):
        code = data.replace("verify_", "")
        is_channel_member = await is_member(client, user_id, CHANNEL_USERNAME)
        is_group_member = await is_member(client, user_id, GROUP_USERNAME)
        is_extra_member   = await is_member(client, user_id, EXTRA_CHANNEL)
        if not (is_channel_member and is_group_member and is_extra_member):
            await cq.answer(
                "❌ TERCYDUK BELUM JOIN! ❌\nKamu harus join channel dan group dulu ya! 😜",
                show_alert=True
            )
            return

        stream_link, thumbnail = get_stream_data(code)
        if not stream_link:
            await cq.message.reply("❌ Oopps... Link streaming tidak ditemukan.")
            logger.error(f"Link for code '{code}' not found.")
            return

        logger.info(f"User {user_id} (@{cq.from_user.username or 'unknown'}) klik: {code}")
        try:
            append_click_log(user_id, cq.from_user.username, code, stream_link)
        except Exception as e:
            logger.error(f"Gagal mencatat klik untuk user {user_id}: {e}")

        button = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔗 TONTON SEKARANG", url=stream_link)]
        ])

        # Kirim thumbnail jika ada
        if thumbnail and Path(f"Img/{thumbnail}").exists():
            await cq.message.reply_photo(
                photo=f"Img/{thumbnail}",
                caption="✅ Klik tombol di bawah untuk menonton!",
                reply_markup=button
            )
        else:
            await cq.message.reply(
                "✅ Koleksi terbuka!\n\nKlik tombol di bawah untuk menonton:",
                reply_markup=button
            )

        await cq.answer()

# --- Health check URLs ---

async def check_url_health_async(session, url, timeout=10):
    start_time = time.time()
    try:
        async with session.get(url, timeout=timeout, allow_redirects=True) as response:
            response_time = round((time.time() - start_time) * 1000, 2)  # ms
            is_healthy = 200 <= response.status < 400
            return (url, response.status, response_time, is_healthy, None)
    except asyncio.TimeoutError:
        response_time = round((time.time() - start_time) * 1000, 2)
        return (url, 0, response_time, False, "Timeout")
    except Exception as e:
        response_time = round((time.time() - start_time) * 1000, 2)
        return (url, 0, response_time, False, str(e))

async def health_check_all_urls():
    if not STREAM_MAP: return []
    urls = []; url_keys = {}
    for key, value in STREAM_MAP.items():
        if isinstance(value, dict) and 'link' in value:
            url = value['link']; urls.append(url); url_keys[url] = key
    results = []
    async with aiohttp.ClientSession() as session:
        tasks = [check_url_health_async(session, url, 15) for url in urls]
        health_results = await asyncio.gather(*tasks)
        for url, status, response_time, is_healthy, error in health_results:
            key = url_keys.get(url, "unknown")
            results.append({
                'key': key, 'url': url, 'status_code': status,
                'response_time_ms': response_time, 'is_healthy': is_healthy,
                'error': error, 'checked_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
    return results

async def notify_owner(msg):
    try:
        await app.send_message(OWNER_ID, f"[NOTIF] {msg}")
    except Exception as e:
        logger.error(f"Gagal kirim notif ke owner: {e}")

# ================================
# Background Tasks
# ================================

# --- Periodic Message Task ---
async def send_periodic_message():
    logger.info("Periodic message task started!")
    while True:
        try:
            logger.info("Periodic message loop tick!")  # <--- Tambahkan ini
            if INTERACTION_MESSAGES:
                msg = random.choice(INTERACTION_MESSAGES)
                logger.info(f"Periodic message: {msg}")
                await app.send_message(chat_id=f"@{GROUP_USERNAME}", text=msg)
        except Exception as e:
            logger.error(f"Gagal kirim pesan periodik: {e}")
        await asyncio.sleep(INTERACTION_INTERVAL_MINUTES * 60)


async def periodic_log_prune():
    await asyncio.sleep(30)
    while True:
        try:
            prune_clicks_log()
            logger.info(f"Pruned clicks.jsonl (retention {RETENTION_DAYS} hari)")
        except Exception as e:
            logger.error(f"Gagal prune clicks.jsonl: {e}")
        await asyncio.sleep(24 * 3600)

# ================================
# Main
# ================================

if __name__ == "__main__":
    load_stream_map()
    load_badwords_config()
    load_interaction_config()
    load_warn_db()
    try:
        app.start()
        logger.info("🚀 BOT AKTIF ✅ @BangsaBacolBot")
        
        # Tambahkan periodic tasks ke event loop milik app
        app.loop.create_task(send_periodic_message())
        app.loop.create_task(periodic_log_prune())
        
        app.loop.run_forever()
    except KeyboardInterrupt:
        logger.info("👋 Bot dimatikan. Sampai jumpa!")
    except Exception as e:
        logger.error(f"Terjadi kesalahan fatal saat menjalankan bot: {e}")
    finally:
        app.stop()

