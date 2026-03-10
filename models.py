from dataclasses import dataclass, field
from typing import Optional, List
from enum import Enum


class TransferStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    INTERRUPTED = "interrupted"
    DONE = "done"
    CANCELLED = "cancelled"
    FAILED = "failed"


@dataclass
class Job:
    id: int
    user_id: int
    source_chat: str
    target_chat: str
    status: TransferStatus
    total: int
    transferred: int
    failed_ids: List[int]
    error: Optional[str]
    created_at: str
    updated_at: str
