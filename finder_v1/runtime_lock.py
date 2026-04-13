from __future__ import annotations

import json
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator

import fcntl


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class PipelineBusyError(RuntimeError):
    def __init__(self, message: str, metadata: Dict[str, Any] | None = None):
        super().__init__(message)
        self.metadata = metadata or {}


def read_lock_metadata(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        raw = path.read_text().strip()
    except OSError:
        return {}
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"raw": raw}


@contextmanager
def pipeline_lock(path: Path, command: str, extra: Dict[str, Any] | None = None) -> Iterator[Dict[str, Any]]:
    path.parent.mkdir(parents=True, exist_ok=True)
    fh = path.open("a+", encoding="utf-8")
    try:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            metadata = read_lock_metadata(path)
            raise PipelineBusyError("Another finder_v1 process already holds the pipeline lock.", metadata) from exc

        metadata = {
            "command": command,
            "pid": os.getpid(),
            "locked_at": utc_now_iso(),
        }
        if extra:
            metadata.update(extra)
        fh.seek(0)
        fh.truncate()
        fh.write(json.dumps(metadata, indent=2, sort_keys=True))
        fh.flush()
        os.fsync(fh.fileno())
        yield metadata
    finally:
        try:
            fh.seek(0)
            fh.truncate()
            fh.flush()
            os.fsync(fh.fileno())
        except OSError:
            pass
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        fh.close()
