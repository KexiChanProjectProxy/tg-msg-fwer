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
# Seconds with no transfer progress before a bulk job is force-killed (default 10 min)
STUCK_TIMEOUT = int(os.environ.get("STUCK_TIMEOUT", "600"))
# Seconds before a single download or upload is considered hung (default 10 min)
OPERATION_TIMEOUT = int(os.environ.get("OPERATION_TIMEOUT", "600"))
# Seconds before a single iter_messages batch call is considered hung (default 2 min)
ITER_TIMEOUT = int(os.environ.get("ITER_TIMEOUT", "120"))
# Parallel chunk workers for document downloads (higher = faster for large files)
DOWNLOAD_WORKERS = int(os.environ.get("DOWNLOAD_WORKERS", "4"))
# Parallel part workers for large file uploads (>10 MB)
UPLOAD_WORKERS = int(os.environ.get("UPLOAD_WORKERS", "4"))
# Number of independent MTProto TCP connections for parallel uploads
SENDER_POOL_SIZE = int(os.environ.get("SENDER_POOL_SIZE", "4"))
