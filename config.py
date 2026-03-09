import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
USERBOT_SESSION = os.environ.get("USERBOT_SESSION", "userbot")
ADMIN_IDS = [int(x.strip()) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip()]
DB_PATH = os.environ.get("DB_PATH", "telegram_reload.db")
TRANSFER_DELAY = float(os.environ.get("TRANSFER_DELAY", "1.5"))
IS_PREMIUM = bool(os.environ.get("IS_PREMIUM", ""))
CONCURRENT_TRANSFERS = int(os.environ.get("CONCURRENT_TRANSFERS", "3"))
CACHE_DIR = os.environ.get("CACHE_DIR", "cache")
MAX_CACHE_SIZE = int(os.environ.get("MAX_CACHE_SIZE", str(1024 * 1024 * 1024)))  # bytes, default 1 GB
# Real disk path for temp files during transfer — must NOT be a tmpfs mount
TEMP_DIR = Path(os.environ.get("TEMP_DIR", "tmp"))
