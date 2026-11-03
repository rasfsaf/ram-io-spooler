"""State machine, task definitions, and metrics for ram-io-spooler."""

import hashlib
import os
import time
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Deque, Dict, List, Optional


class OpType(str, Enum):
    ATOMIC_WRITE = "ATOMIC_WRITE"
    APPEND = "APPEND"
    SQLITE_SYNC = "SQLITE_SYNC"


class TaskStatus(str, Enum):
    QUEUED = "QUEUED"
    COALESCED = "COALESCED"
    FLUSHING = "FLUSHING"
    SYNCED = "SYNCED"
    EVICTED = "EVICTED"
    FAILED = "FAILED"


@dataclass
class SpoolTask:
    task_id: str
    target_path: str
    op_type: OpType
    byte_size: int
    checksum: str
    created_at: float = field(default_factory=time.time)
    status: TaskStatus = TaskStatus.QUEUED
    staging_file: Optional[str] = None
    in_memory_data: Optional[bytes] = None
    flushed_at: Optional[float] = None
    evicted_at: Optional[float] = None
    error_message: Optional[str] = None
    coalesced_into: Optional[str] = None

    @property
    def state(self) -> TaskStatus:
        return self.status

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "target_path": self.target_path,
            "op_type": self.op_type.value,
            "byte_size": self.byte_size,
            "checksum": self.checksum,
            "created_at": self.created_at,
            "status": self.status.value,
            "state": self.status.value,
            "staging_file": self.staging_file,
            "flushed_at": self.flushed_at,
            "evicted_at": self.evicted_at,
            "error_message": self.error_message,
            "coalesced_into": self.coalesced_into,
        }


@dataclass
class SpoolerMetrics:
    total_queued: int = 0
    total_coalesced: int = 0
    total_flushed: int = 0
    total_synced: int = 0
    total_evicted: int = 0
    total_failed: int = 0
    total_bytes_received: int = 0
    total_bytes_written: int = 0
    total_bytes_written_to_disk: int = 0
    staging_bytes_current: int = 0


class TaskManager:
    """Manages active queues, state transitions, and completed task history."""

    def __init__(self, config_or_limit: Any = 500) -> None:
        if hasattr(config_or_limit, "history_limit"):
            self.history_limit = config_or_limit.history_limit
            self.config = config_or_limit
        elif isinstance(config_or_limit, int):
            self.history_limit = config_or_limit
            self.config = None
        else:
            self.history_limit = 500
            self.config = None

        self.queue: List[SpoolTask] = []
        self.history: Deque[SpoolTask] = deque(maxlen=self.history_limit)
        self.metrics = SpoolerMetrics()
        self.tasks_by_id: Dict[str, SpoolTask] = {}

    def enqueue(
        self,
        target_path: str,
        data: bytes,
        op_type: OpType = OpType.ATOMIC_WRITE,
        staging_dir: Optional[str] = None,
        in_memory_threshold: int = 65536,
    ) -> SpoolTask:
        """Create and queue a task, storing payload in memory or tmpfs."""
        task_id = f"task_{uuid.uuid4().hex[:12]}"
        checksum = hashlib.sha256(data).hexdigest()
        byte_size = len(data)

        staging_path = staging_dir or (self.config.staging_dir if self.config else None)
        threshold = self.config.in_memory_threshold_bytes if self.config else in_memory_threshold

        staging_file: Optional[str] = None
        in_memory_data: Optional[bytes] = None

        if byte_size > threshold and staging_path:
            os.makedirs(staging_path, exist_ok=True)
            staging_file = os.path.join(staging_path, f"{task_id}.bin")
            with open(staging_file, "wb") as f:
                f.write(data)
        else:
            in_memory_data = data

        task = SpoolTask(
            task_id=task_id,
            target_path=target_path,
            op_type=op_type,
            byte_size=byte_size,
            checksum=checksum,
            staging_file=staging_file,
            in_memory_data=in_memory_data,
        )
        self.add_task(task)
        return task

    def add_task(self, task: SpoolTask) -> None:
        self.queue.append(task)
        self.tasks_by_id[task.task_id] = task
        self.metrics.total_queued += 1
        self.metrics.total_bytes_received += task.byte_size
        self.metrics.staging_bytes_current += task.byte_size

    def get_task(self, task_id: str) -> Optional[SpoolTask]:
        return self.tasks_by_id.get(task_id)

    # Alias for server compatibility
    create_task = enqueue

    @property
    def pending_count(self) -> int:
        return len(self.queue)

    def extract_queue(self) -> List[SpoolTask]:
        """Extract all queued tasks for a flush cycle."""
        batch = self.queue
        self.queue = []
        return batch

    def drain_queue(self) -> List[SpoolTask]:
        """Alias for extract_queue."""
        return self.extract_queue()

    def requeue_tasks(self, tasks: List[SpoolTask]) -> None:
        """Put back tasks that failed or could not be processed immediately."""
        self.queue = tasks + self.queue

    def record_completed(self, task: SpoolTask) -> None:
        self.history.append(task)
        if len(self.tasks_by_id) > self.history_limit * 2:
            current_active = {t.task_id for t in self.queue}
            history_ids = {t.task_id for t in self.history}
            valid_ids = current_active | history_ids
            self.tasks_by_id = {k: v for k, v in self.tasks_by_id.items() if k in valid_ids}

    def get_metrics_dict(self) -> Dict[str, Any]:
        """Return metrics as a dictionary for status inspection."""
        return asdict(self.metrics)
