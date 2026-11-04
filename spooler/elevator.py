"""Path / Inode Elevator Sorter for optimizing mechanical HDD head trajectory."""

import os
from typing import List, Tuple

from .state import SpoolTask


def _get_sort_key(task: SpoolTask) -> Tuple:
    """Generate a sorting tuple that groups writes by directory hierarchy and inode."""
    norm_path = os.path.abspath(task.target_path)
    dirname = os.path.dirname(norm_path)
    basename = os.path.basename(norm_path)

    # Inode number if file exists (on ext4, inode order roughly correlates with block groups)
    inode_num = 0
    if os.path.exists(norm_path):
        try:
            inode_num = os.stat(norm_path).st_ino
        except OSError:
            inode_num = 0

    drive, path_without_drive = os.path.splitdrive(norm_path)
    path_components = [comp for comp in path_without_drive.split(os.sep) if comp]

    # Elevator ordering:
    # 1. Drive/Device
    # 2. Directory tree hierarchy components
    # 3. Existing inode (or 0)
    # 4. Filename
    return (drive, path_components[:-1], inode_num, basename)


def elevator_sort(tasks: List[SpoolTask]) -> List[SpoolTask]:
    """Sort a list of tasks into an optimal sequential order for physical disk writing."""
    if len(tasks) <= 1:
        return tasks

    return sorted(tasks, key=_get_sort_key)


class PathElevatorSorter:
    @staticmethod
    def sort(tasks: List[SpoolTask]) -> List[SpoolTask]:
        return elevator_sort(tasks)

    @staticmethod
    def sort_paths(paths: List[str]) -> List[str]:
        def _path_sort_key(p: str):
            norm_path = os.path.abspath(p)
            dirname = os.path.dirname(norm_path)
            basename = os.path.basename(norm_path)
            inode_num = 0
            if os.path.exists(norm_path):
                try:
                    inode_num = os.stat(norm_path).st_ino
                except OSError:
                    inode_num = 0
            drive, path_without_drive = os.path.splitdrive(norm_path)
            path_components = [comp for comp in path_without_drive.split(os.sep) if comp]
            return (drive, path_components[:-1], inode_num, basename)
        return sorted(paths, key=_path_sort_key)


LBAPathElevator = PathElevatorSorter
