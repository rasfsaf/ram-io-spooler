"""Write Coalescing engine for deduping and consolidating pending disk I/O."""

import hashlib
import os
import uuid
from collections import defaultdict
from typing import List

from .state import OpType, SpoolTask, TaskManager, TaskStatus


class WriteCoalescer:
    """Coalesces multiple pending write operations targeting the same files."""

    def __init__(self, task_manager: TaskManager, staging_dir: str = "") -> None:
        self.task_manager = task_manager
        self.staging_dir = staging_dir

    def coalesce(self, tasks: List[SpoolTask]) -> List[SpoolTask]:
        """Process a list of queued tasks, deduplicating overwrites and concatenating appends."""
        if not tasks:
            return []

        # Group tasks by target path preserving arrival order
        grouped: defaultdict[str, List[SpoolTask]] = defaultdict(list)
        for task in tasks:
            grouped[task.target_path].append(task)

        coalesced_output: List[SpoolTask] = []

        for target_path, path_tasks in grouped.items():
            if len(path_tasks) == 1:
                coalesced_output.append(path_tasks[0])
                continue

            processed = self._coalesce_path_tasks(path_tasks)
            coalesced_output.extend(processed)

        return coalesced_output

    def _coalesce_path_tasks(self, tasks: List[SpoolTask]) -> List[SpoolTask]:
        """Coalesce multiple tasks for a single target path."""
        runs: List[List[SpoolTask]] = []
        current_run: List[SpoolTask] = []

        for task in tasks:
            if not current_run:
                current_run.append(task)
            elif current_run[-1].op_type == task.op_type:
                current_run.append(task)
            else:
                runs.append(current_run)
                current_run = [task]
        if current_run:
            runs.append(current_run)

        result: List[SpoolTask] = []
        for run in runs:
            op_type = run[0].op_type
            if op_type in (OpType.ATOMIC_WRITE, OpType.SQLITE_SYNC):
                result.append(self._coalesce_atomic_overwrites(run))
            elif op_type == OpType.APPEND:
                result.append(self._coalesce_appends(run))
            else:
                result.extend(run)

        return result

    def _coalesce_atomic_overwrites(self, run: List[SpoolTask]) -> SpoolTask:
        """Keep only the latest atomic write and drop superseded ones."""
        if len(run) == 1:
            return run[0]

        survivor = run[-1]
        superseded = run[:-1]

        for task in superseded:
            task.status = TaskStatus.COALESCED
            task.coalesced_into = survivor.task_id
            self._cleanup_task_payload(task)

            self.task_manager.metrics.total_coalesced += 1
            self.task_manager.record_completed(task)

        return survivor

    def _coalesce_appends(self, run: List[SpoolTask]) -> SpoolTask:
        """Merge multiple append payloads into one single consolidated sequential write."""
        if len(run) == 1:
            return run[0]

        merged_task_id = f"append_{uuid.uuid4().hex[:12]}"
        merged_staging = os.path.join(self.staging_dir, f"{merged_task_id}.bin") if self.staging_dir else None

        hasher = hashlib.sha256()
        total_size = 0

        collected_bytes: List[bytes] = []

        for task in run:
            payload = self._read_task_payload(task)
            if payload:
                collected_bytes.append(payload)
                hasher.update(payload)
                total_size += len(payload)

            task.status = TaskStatus.COALESCED
            task.coalesced_into = merged_task_id
            self._cleanup_task_payload(task)
            self.task_manager.record_completed(task)

        self.task_manager.metrics.total_coalesced += len(run) - 1

        all_data = b"".join(collected_bytes)

        if merged_staging:
            os.makedirs(self.staging_dir, exist_ok=True)
            with open(merged_staging, "wb") as out_f:
                out_f.write(all_data)
            in_memory_data = None
        else:
            in_memory_data = all_data
            merged_staging = None

        merged_task = SpoolTask(
            task_id=merged_task_id,
            target_path=run[0].target_path,
            op_type=OpType.APPEND,
            byte_size=total_size,
            checksum=hasher.hexdigest(),
            staging_file=merged_staging,
            in_memory_data=in_memory_data,
        )
        self.task_manager.tasks_by_id[merged_task_id] = merged_task
        return merged_task

    def _read_task_payload(self, task: SpoolTask) -> bytes:
        if task.in_memory_data is not None:
            return task.in_memory_data
        if task.staging_file and os.path.exists(task.staging_file):
            with open(task.staging_file, "rb") as f:
                return f.read()
        return b""

    def _cleanup_task_payload(self, task: SpoolTask) -> None:
        """Free memory or unlink staging file from tmpfs."""
        if task.staging_file and os.path.exists(task.staging_file):
            try:
                os.unlink(task.staging_file)
            except OSError:
                pass
            task.staging_file = None

        if task.in_memory_data is not None:
            task.in_memory_data = None

        self.task_manager.metrics.staging_bytes_current = max(
            0, self.task_manager.metrics.staging_bytes_current - task.byte_size
        )


def coalesce_tasks(tasks: List[SpoolTask], task_manager: TaskManager, staging_dir: str = "") -> List[SpoolTask]:
    """Helper function to coalesce tasks using WriteCoalescer."""
    coalescer = WriteCoalescer(task_manager=task_manager, staging_dir=staging_dir)
    return coalescer.coalesce(tasks)
