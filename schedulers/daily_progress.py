"""Resumable progress for the daily job.

The daily job covers many symbols and many days of history, so one invocation may not finish inside the
function timeout. Progress is recorded per task so the next invocation continues where the last one stopped,
and a finished day is not re-run.
"""
import json
import logging
from pathlib import Path
from typing import Optional

from azure.core.exceptions import ResourceNotFoundError

from utils.blob_utils import BlobSync, BlobUtils

logger = logging.getLogger(__name__)

STATUS_DONE = "done"


class DailyProgress:
    """Per-task status for one daily run: "done", or the number of failed attempts so far."""

    MAX_ATTEMPTS = 3
    BLOB_PREFIX = "daily_progress"

    def __init__(self, run_key: str, container: Optional[str] = None, local_dir: Optional[str] = None):
        self.run_key = run_key
        self._container = container
        self._is_local = BlobUtils.is_running_locally()
        self._local_path = Path(local_dir or "local_data") / self.BLOB_PREFIX / f"{run_key}.json"
        self._blob_name = f"{self.BLOB_PREFIX}/{run_key}.json"
        self._blob_sync: Optional[BlobSync] = None
        self.tasks: dict[str, object] = {}
        self._load()

    def _blob(self):
        if self._blob_sync is None:
            self._blob_sync = BlobSync()
        container = self._blob_sync.get_container(self._container or BlobUtils.market_data_cache_blob())
        return container.get_blob_client(self._blob_name) if container else None

    def _load(self) -> None:
        try:
            if self._is_local:
                raw = self._local_path.read_text() if self._local_path.exists() else None
            else:
                blob = self._blob()
                raw = blob.download_blob().readall().decode() if blob else None
        except ResourceNotFoundError:
            raw = None
        except Exception:
            logger.exception("[DailyProgress] Could not read progress %s; starting from scratch", self.run_key)
            raw = None
        if raw:
            try:
                self.tasks = dict(json.loads(raw).get("tasks", {}))
            except Exception:
                logger.exception("[DailyProgress] Progress %s is unreadable; starting from scratch", self.run_key)

    def save(self) -> None:
        payload = json.dumps({"run": self.run_key, "tasks": self.tasks})
        try:
            if self._is_local:
                self._local_path.parent.mkdir(parents=True, exist_ok=True)
                self._local_path.write_text(payload)
                return
            blob = self._blob()
            if blob:
                blob.upload_blob(payload.encode(), overwrite=True)
        except Exception:
            logger.exception("[DailyProgress] Could not save progress %s", self.run_key)

    def is_finished(self, task: str) -> bool:
        """True when the task is done, or has used up its attempts and should not be retried today."""
        status = self.tasks.get(task)
        return status == STATUS_DONE or (isinstance(status, int) and status >= self.MAX_ATTEMPTS)

    def record(self, task: str, success: bool) -> None:
        if success:
            self.tasks[task] = STATUS_DONE
        else:
            previous = self.tasks.get(task)
            self.tasks[task] = (previous if isinstance(previous, int) else 0) + 1
        self.save()

    def pending(self, tasks: list[str]) -> list[str]:
        return [task for task in tasks if not self.is_finished(task)]
