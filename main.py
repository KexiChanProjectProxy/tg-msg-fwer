import asyncio
import logging

from telethon import TelegramClient

import config
import database as db_module
import bot as bot_module
import userbot as userbot_module
from cache import FileCache
from utils import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


async def main():
    db = await db_module.init_db(config.DB_PATH)

    recovered = await db_module.recover_interrupted_jobs(db)
    if recovered:
        logger.info(f"Marked {recovered} interrupted job(s) as 'interrupted'")

    userbot = TelegramClient(
        config.USERBOT_SESSION,
        config.API_ID,
        config.API_HASH,
        flood_sleep_threshold=60,
        use_ipv6=True,
    )

    bot = TelegramClient(
        "bot_session",
        config.API_ID,
        config.API_HASH,
    )

    await userbot.start()
    await bot.start(bot_token=config.BOT_TOKEN)

    file_cache = FileCache(config.CACHE_DIR, config.MAX_CACHE_SIZE)
    bot_module.register_handlers(bot, userbot, db, file_cache)
    userbot_module.register_handlers(userbot)

    logger.info("Both clients connected. Running...")

    await asyncio.gather(
        userbot.run_until_disconnected(),
        bot.run_until_disconnected(),
    )


if __name__ == "__main__":
    asyncio.run(main())
