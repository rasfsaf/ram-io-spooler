"""State machine, task definitions, and metrics for ram-io-spooler."""

import hashlib
import os
import threading
import time
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Deque, Dict, List, Optional
from spooler.config import SpoolerConfig

default_config = SpoolerConfig()


class OpType(str, Enum):
    ATOMIC_WRITE = "ATOMIC_WRITE"
    APPEND = "APPEND"
    SQLITE_SYNC = "SQLITE_SYNC"


class TaskStatus(str, Enum):
    QUEUED = "QUEUED"
    COALESCED = "COALESCED"
    FLUSHING = "FLUSHING"
    SYNCED = "SYNCED"
    COMPLETED = "SYNCED"
    DONE = "SYNCED"
    EVICTED = "EVICTED"
    FAILED = "FAILED"

TaskState = TaskStatus
SpoolState = TaskStatus
SpoolOpType = OpType


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

    @state.setter
    def state(self, value: TaskStatus) -> None:
        self.status = value

    @property
    def error(self) -> Optional[str]:
        return self.error_message

    @error.setter
    def error(self, value: Optional[str]) -> None:
        self.error_message = value

    @property
    def retries(self) -> int:
        return getattr(self, "_retries", 0)

    @retries.setter
    def retries(self, value: int) -> None:
        self._retries = value

    @property
    def max_retries(self) -> int:
        return getattr(self, "_max_retries", 3)

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

    @property
    def total_bytes_flushed(self) -> int:
        return self.total_bytes_written_to_disk

    @total_bytes_flushed.setter
    def total_bytes_flushed(self, value: int) -> None:
        self.total_bytes_written_to_disk = value


class TaskManager:
    """Manages active queues, state transitions, and completed task history."""

    def __init__(self, config_or_limit: Any = 500, *args: Any, **kwargs: Any) -> None:
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
        self.in_flight_tasks: Dict[str, SpoolTask] = {}
        self.history: Deque[SpoolTask] = deque(maxlen=self.history_limit)
        self.metrics = SpoolerMetrics()
        self.tasks_by_id: Dict[str, SpoolTask] = {}
        self._lock = threading.Lock()
        self.lock = self._lock

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

        staging_path = (
            staging_dir
            or getattr(self, "staging_dir", None)
            or (getattr(self.config, "staging_dir", None) if self.config else None)
            or (getattr(self.config, "shm_dir", None) if self.config else None)
            or getattr(default_config, "staging_dir", None)
            or getattr(default_config, "shm_dir", None)
            or "/dev/shm/io-spool"
        )
        if in_memory_threshold is not None:
            threshold = in_memory_threshold
        elif self.config and hasattr(self.config, "in_memory_threshold_bytes"):
            threshold = self.config.in_memory_threshold_bytes
        elif hasattr(default_config, "in_memory_threshold_bytes"):
            threshold = default_config.in_memory_threshold_bytes
        else:
            threshold = 65536

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
        with self.lock:
            self.queue.append(task)
            self.tasks_by_id[task.task_id] = task
            self.metrics.total_queued += 1
            self.metrics.total_bytes_received += task.byte_size
            self.metrics.staging_bytes_current += task.byte_size

    def get_task(self, task_id: str) -> Optional[SpoolTask]:
        with self.lock:
            return self.tasks_by_id.get(task_id)

    def fail_task(self, task_id: str, error: str = "", retryable: bool = True) -> None:
        """Mark task as failed or return it to queue if retryable."""
        with self.lock:
            task = self.in_flight_tasks.pop(task_id, None) or self.tasks_by_id.get(task_id)
            if task:
                task.last_error = error
                if retryable and task.retry_count < 3:
                    task.retry_count += 1
                    task.state = SpoolState.QUEUED
                    self.queue.append(task)
                else:
                    task.state = SpoolState.FAILED
                    task.completed_at = time.time()

    def get_pending_data(self, target_path: str) -> Optional[bytes]:
        """
        Inspect the active queue for unwritten modifications to target_path.
        Implements Read-Your-Own-Writes consistency.
        Returns the resolved bytes if pending data exists, or None.
        """
        norm_target = os.path.abspath(target_path)
        with self.lock:
            # Match tasks in active in-flight flush AND queued for this exact target path
            in_flight = [
                t for t in self.in_flight_tasks.values()
                if os.path.abspath(t.target_path) == norm_target and t.state == SpoolState.FLUSHING
            ]
            in_flight.sort(key=lambda t: t.created_at)

            queued = [
                t for t in self.queue
                if os.path.abspath(t.target_path) == norm_target and t.state in (SpoolState.QUEUED, SpoolState.COALESCED)
            ]
            queued.sort(key=lambda t: t.created_at)

            matching_tasks = in_flight + queued

        if not matching_tasks:
            return None

        # Helper to read payload
        def read_payload(t: SpoolTask) -> bytes:
            if t.in_memory_data is not None:
                return t.in_memory_data
            if t.staging_file and os.path.exists(t.staging_file):
                try:
                    with open(t.staging_file, "rb") as sf:
                        return sf.read()
                except OSError:
                    return b""
            return b""

        # Check if the latest task is ATOMIC_WRITE or SQLITE_SYNC
        # If so, it supersedes previous writes
        last_idx = len(matching_tasks) - 1
        last_atomic_idx = -1
        for i in range(last_idx, -1, -1):
            if matching_tasks[i].op_type in (SpoolOpType.ATOMIC_WRITE, SpoolOpType.SQLITE_SYNC):
                last_atomic_idx = i
                break

        if last_atomic_idx != -1:
            base_data = read_payload(matching_tasks[last_atomic_idx])
            remaining = matching_tasks[last_atomic_idx + 1:]
            for t in remaining:
                if t.op_type == SpoolOpType.APPEND:
                    base_data += read_payload(t)
            return base_data

        # If only APPEND tasks exist in queue, read base from disk (if exists) and append
        base_data = b""
        if os.path.exists(norm_target):
            try:
                with open(norm_target, "rb") as f:
                    base_data = f.read()
            except OSError:
                base_data = b""
        for t in matching_tasks:
            if t.op_type == SpoolOpType.APPEND:
                base_data += read_payload(t)
        return base_data

    # Alias for server compatibility
    create_task = enqueue

    @property
    def pending_count(self) -> int:
        return len(self.queue)

    def get_active_paths(self) -> List[str]:
        with self.lock:
            return list({t.target_path for t in self.queue})

    def drain_queue(self, max_items: Optional[int] = None) -> List[SpoolTask]:
        """Extract queued tasks for a flush cycle and register them as in-flight."""
        with self.lock:
            if max_items is None or max_items >= len(self.queue):
                batch = self.queue
                self.queue = []
            else:
                batch = self.queue[:max_items]
                self.queue = self.queue[max_items:]

            for task in batch:
                task.state = SpoolState.FLUSHING
                self.in_flight_tasks[task.task_id] = task
            return batch

    def extract_queue(self) -> List[SpoolTask]:
        """Extract all queued tasks for a flush cycle."""
        return self.drain_queue()

    def requeue_tasks(self, tasks: List[SpoolTask]) -> None:
        """Put back tasks that failed or could not be processed immediately."""
        with self.lock:
            for task in tasks:
                self.in_flight_tasks.pop(task.task_id, None)
                task.state = SpoolState.QUEUED
            self.queue = tasks + self.queue

    def record_completed(self, task: SpoolTask) -> None:
        with self.lock:
            self.in_flight_tasks.pop(task.task_id, None)
            if task.status != TaskStatus.EVICTED:
                task.status = TaskStatus.SYNCED
            task.completed_at = time.time()
            self.metrics.total_flushed += 1
            self.metrics.total_bytes_flushed += task.byte_size
            self.metrics.staging_bytes_current = max(0, self.metrics.staging_bytes_current - task.byte_size)
            self.history.append(task)
            if len(self.tasks_by_id) > self.history_limit * 2:
                current_active = {t.task_id for t in self.queue} | set(self.in_flight_tasks.keys())
                history_ids = {t.task_id for t in self.history}
                valid_ids = current_active | history_ids
                self.tasks_by_id = {k: v for k, v in self.tasks_by_id.items() if k in valid_ids}

    def get_metrics_dict(self) -> Dict[str, Any]:
        """Return metrics as a dictionary for status inspection."""
        return asdict(self.metrics)

    def get_stats(self) -> Dict[str, Any]:
        """Return metrics and current queue size for status inspection."""
        stats = self.get_metrics_dict()
        with self.lock:
            stats["pending_tasks"] = len(self.queue)
            stats["in_flight_tasks"] = len(self.in_flight_tasks)
        return stats
