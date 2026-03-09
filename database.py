import aiosqlite
import json
from typing import Optional, List, Set, Tuple
from models import Job, TransferStatus


async def init_db(db_path: str) -> aiosqlite.Connection:
    db = await aiosqlite.connect(db_path)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            source_chat TEXT NOT NULL,
            target_chat TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            total INTEGER NOT NULL DEFAULT 0,
            transferred INTEGER NOT NULL DEFAULT 0,
            failed_ids TEXT NOT NULL DEFAULT '[]',
            error TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS transferred_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            source_msg_id INTEGER NOT NULL,
            target_msg_id INTEGER,
            UNIQUE(job_id, source_msg_id)
        )
    """)
    await db.commit()
    return db


async def create_job(db, user_id: int, source_chat: str, target_chat: str) -> int:
    cursor = await db.execute(
        "INSERT INTO jobs (user_id, source_chat, target_chat, status) VALUES (?, ?, ?, 'pending')",
        (user_id, source_chat, target_chat),
    )
    await db.commit()
    return cursor.lastrowid


async def get_job(db, job_id: int) -> Optional[Job]:
    async with db.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)) as cursor:
        row = await cursor.fetchone()
    if not row:
        return None
    return _row_to_job(row)


async def get_user_jobs(db, user_id: int) -> List[Job]:
    async with db.execute(
        "SELECT * FROM jobs WHERE user_id = ? ORDER BY id DESC LIMIT 10", (user_id,)
    ) as cursor:
        rows = await cursor.fetchall()
    return [_row_to_job(r) for r in rows]


async def update_job(db, job_id: int, **kwargs):
    fields = dict(kwargs)
    if "failed_ids" in fields:
        fields["failed_ids"] = json.dumps(fields["failed_ids"])
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [job_id]
    await db.execute(
        f"UPDATE jobs SET {set_clause}, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        values,
    )
    await db.commit()


async def is_transferred(db, job_id: int, source_msg_id: int) -> bool:
    async with db.execute(
        "SELECT 1 FROM transferred_messages WHERE job_id = ? AND source_msg_id = ?",
        (job_id, source_msg_id),
    ) as cursor:
        return await cursor.fetchone() is not None


async def record_transfer(db, job_id: int, source_msg_id: int, target_msg_id: Optional[int] = None):
    await db.execute(
        "INSERT OR IGNORE INTO transferred_messages (job_id, source_msg_id, target_msg_id) VALUES (?, ?, ?)",
        (job_id, source_msg_id, target_msg_id),
    )
    await db.commit()


async def get_transferred_ids(db, job_id: int) -> Set[int]:
    """Load all transferred source message IDs for a job in one query."""
    async with db.execute(
        "SELECT source_msg_id FROM transferred_messages WHERE job_id = ?", (job_id,)
    ) as cursor:
        rows = await cursor.fetchall()
    return {row[0] for row in rows}


async def record_transfers_batch(db, records: List[Tuple[int, int, Optional[int]]]):
    """Batch insert transferred message records. Each record is (job_id, source_msg_id, target_msg_id)."""
    await db.executemany(
        "INSERT OR IGNORE INTO transferred_messages (job_id, source_msg_id, target_msg_id) VALUES (?, ?, ?)",
        records,
    )
    await db.commit()


def _row_to_job(row) -> Job:
    return Job(
        id=row[0],
        user_id=row[1],
        source_chat=row[2],
        target_chat=row[3],
        status=TransferStatus(row[4]),
        total=row[5],
        transferred=row[6],
        failed_ids=json.loads(row[7]),
        error=row[8],
        created_at=row[9],
        updated_at=row[10],
    )
