# tg-msg-fwer

A Telegram userbot + bot system for transferring messages between channels.
Runs as a single process with two [Telethon](https://github.com/LonamiWebs/Telethon) clients sharing one asyncio event loop.

## Features

- **Single message transfer** — copy any message (photo, video, document, voice, GIF, sticker, …) from a channel to another, with the original URL appended to the caption
- **Bulk channel transfer** — copy an entire channel in order, with resume support (already-transferred messages are skipped)
- **Album support** — grouped media albums are detected and re-uploaded as albums
- **Forwarded message handler** — forward any media to the bot in DM, then reply with a target channel to send it there
- **LRU disk cache** — downloaded files are cached on disk; the same file is never downloaded twice across any transfer
- **Video conversion** — non-H.264 videos are converted to a Telegram-compatible H.264/AAC MP4 via ffmpeg; MP4s are re-muxed for faststart
- **Strict admin whitelist** — only users listed in `ADMIN_IDS` can interact with the bot
- **Cancellable bulk jobs** — stop a running bulk transfer at any time with `/cancel`

## Requirements

- Python 3.10+
- ffmpeg (for video conversion — `apt install ffmpeg` or `brew install ffmpeg`)
- A [Telegram API app](https://my.telegram.org/apps) (API ID + API hash)
- A bot token from [@BotFather](https://t.me/BotFather)
- A Telegram account for the userbot

## Installation

```bash
git clone git@github.com:KexiChanProjectProxy/tg-msg-fwer.git
cd tg-msg-fwer
pip install -r requirements.txt
cp .env.example .env
# edit .env with your credentials
```

## Configuration

All configuration is via environment variables (or a `.env` file).

| Variable | Default | Description |
|---|---|---|
| `API_ID` | — | Telegram API ID from my.telegram.org |
| `API_HASH` | — | Telegram API hash from my.telegram.org |
| `BOT_TOKEN` | — | Bot token from @BotFather |
| `USERBOT_SESSION` | `userbot` | Session file name for the userbot (no `.session` extension) |
| `ADMIN_IDS` | — | Comma-separated Telegram user IDs allowed to use the bot. **Leave empty to deny everyone.** |
| `DB_PATH` | `telegram_reload.db` | SQLite database file path |
| `TRANSFER_DELAY` | `1.5` | Seconds between transfers (±0.5 s jitter applied automatically) |
| `IS_PREMIUM` | _(empty)_ | Set to any non-empty value if the userbot has Telegram Premium (enables >2 GB uploads) |
| `CONCURRENT_TRANSFERS` | `3` | Max prepared transfers that can be staged in the bounded `transfer_bulk()` queue between download/prepare work and upload work. This is a queue limit, not a direct worker count. |
| `CACHE_DIR` | `cache` | Directory for the LRU disk cache |
| `MAX_CACHE_SIZE` | `1073741824` | Maximum cache size in bytes (default 1 GB) |
| `MAX_INFLIGHT_BYTES` | `1073741824` | Dedicated bulk-transfer staging budget in bytes for prepared temp media. It is configured separately from cache eviction, even though the default still falls back to `MAX_CACHE_SIZE` when unset. |
| `UPLOAD_WORKERS` | `4` | Per-file parallel upload part workers for large uploads. Effective upload lanes are still capped by sender-pool capacity. |
| `SENDER_POOL_SIZE` | `4` | Number of MTProto sender connections available for upload fanout. Effective upload concurrency is clamped to this ceiling when a sender pool is active; otherwise uploads fall back to the main sender. |

## First run

```bash
python main.py
```

On first run the userbot will prompt for your phone number and the one-time code sent by Telegram. The session is saved to `{USERBOT_SESSION}.session` and reused on subsequent starts.

## Bot commands

| Command | Description |
|---|---|
| `/start` | Show the welcome menu |
| `/transfer <url> <target>` | Transfer a single message |
| `/bulk <source> <target>` | Transfer an entire channel |
| `/bulkfiles <source> <target>` | Transfer media-only messages (oldest first) |
| `/status [job_id]` | Show job progress (last 5 jobs if no ID given) |
| `/cancel <job_id>` | Cancel a running bulk job |
| `/resume <job_id>` | Resume an interrupted or failed job |
| `/help` | Show command reference |

### Chat references

`<source>`, `<target>`, and URL arguments accept:

- `@username` — public channel or group username
- `https://t.me/username` — public channel link
- `https://t.me/c/1234567890` — private channel link
- A numeric chat ID (e.g. `-1001234567890`)

### Examples

```
/transfer https://t.me/somechannel/42 @mychannel
/bulk @sourcechannel @targetchannel
/bulkfiles @sourcechannel @targetchannel
/status 3
/cancel 3
/resume 3
```

## Forwarding a message

1. Forward any message with media to the bot in a private chat.
2. The bot replies: *"Where should I send this?"*
3. Reply with a channel username or URL (e.g. `@mychannel`).
4. The bot transfers the media and replies *"Done!"*

Forwarded messages are handled from the bot-owned forwarded copy first. Only if that BOT download path fails, and the forward metadata points to an accessible source channel and source post, does the app fall back to fetching the original through the userbot. Exact message-URL flows such as `/transfer <url> <target>` still stay on the userbot-driven fetch path directly.

## LRU file cache

Downloaded files are stored in `CACHE_DIR` as flat files keyed by Telegram's stable media ID. When the total size of the cache exceeds `MAX_CACHE_SIZE`, the least-recently-used files are deleted until the cache is back at 90% of the limit.

The cache is shared across all transfer modes — bulk jobs, single transfers, and forwarded messages.

To disable caching, set `MAX_CACHE_SIZE=0` (or point `CACHE_DIR` to a tmpfs).

## Architecture

```
main.py          entry point: init DB, start both clients, register handlers
├── bot.py       bot command & message handlers; admin whitelist; forward flow
├── userbot.py   userbot event handlers (extend as needed)
├── transfer.py  resolve_chat / resolve_message / transfer_one_message /
│                transfer_album / transfer_bulk / _download_to_file (with cache)
├── cache.py     FileCache — LRU disk cache with asyncio.Lock
├── media.py     get_media_type / needs_video_conversion / convert_video /
│                ensure_faststart (ffmpeg wrappers)
├── database.py  aiosqlite: jobs + transferred_messages tables
├── models.py    Job dataclass, TransferStatus enum
├── config.py    env-based configuration
└── utils.py     setup_logging / build_message_url / retry_on_flood /
                 rate_limit_sleep
```

Two Telethon clients run concurrently on the same event loop:

- **userbot** — performs all actual Telegram operations (downloads, uploads, entity resolution). Needs to be a member of source/target channels.
- **bot** — receives commands and messages from admins, drives the transfer logic by calling transfer functions directly (no IPC).

### Bulk transfer pipeline

`transfer_bulk` uses a bounded producer-consumer queue (`asyncio.Queue(maxsize=CONCURRENT_TRANSFERS)`) so that downloading message N+1 overlaps with uploading message N without allowing unbounded queued work. Prepared media also reserves from the dedicated `MAX_INFLIGHT_BYTES` staging budget before enqueue, which limits how much disk-backed temp media can pile up even if the cache itself is larger. Upload fanout uses `UPLOAD_WORKERS`, but the effective lane count is still capped by `SENDER_POOL_SIZE` when a sender pool is active. Progress and transferred message IDs are flushed to SQLite every 20 messages, enabling resume after a crash or cancellation.

### Synthetic benchmark protocol

The benchmark harness is local and synthetic on purpose: it copies files through a temporary disk-backed producer/consumer pipeline that mirrors the queue, inflight-budget, and upload-lane controls without claiming real Telegram WAN throughput.

```bash
python -m benchmarks.transfer_perf --scenario matrix --output .sisyphus/evidence/task-9-benchmark.json
python -m benchmarks.transfer_perf --scenario cleanup --output .sisyphus/evidence/task-9-cleanup.json
```

- `matrix` emits machine-readable case results for `upload_large_single`, `upload_album`, `upload_many_small`, `budget_saturation`, and `cleanup`.
- Each JSON result records scenario metadata, bytes transferred, elapsed seconds, MB/s, peak RSS, cleanup status, and residue counts.
- These numbers are useful for repeatable local regression checks of queue/budget/cleanup behavior; they are not measurements of real Telegram throughput.

### Resume behaviour

Before a bulk job starts, all already-transferred source message IDs for that job are loaded into a `set` in one query. Each message is checked against this set before downloading, so restarting a job skips completed work at essentially zero cost.

## Database schema

```sql
CREATE TABLE jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    source_chat TEXT NOT NULL,
    target_chat TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',   -- pending/running/done/cancelled/failed
    total INTEGER NOT NULL DEFAULT 0,
    transferred INTEGER NOT NULL DEFAULT 0,
    failed_ids TEXT NOT NULL DEFAULT '[]',    -- JSON array of message IDs
    error TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE transferred_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,
    source_msg_id INTEGER NOT NULL,
    target_msg_id INTEGER,
    UNIQUE(job_id, source_msg_id)
);
```

## Extending

**Add userbot handlers** — edit `userbot.py` and add handlers inside `register_handlers(userbot)`.

**Support more media types** — `media.py:get_media_type` returns the type string; `transfer.py:transfer_one_message` and `_upload_prepared` dispatch on it. Add a new branch in both.

**Custom caption format** — edit `transfer.py:_build_caption`.

## License

MIT
