"""RAM Write-Behind Scribe Engine (ram-io-spooler) for mechanical HDDs."""

from .client import SpoolerClient
from .config import Config
from .state import OpType, SpoolTask, TaskStatus

__all__ = ["SpoolerClient", "Config", "OpType", "SpoolTask", "TaskStatus"]
__version__ = "1.0.0"
