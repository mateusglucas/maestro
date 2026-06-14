import enum
from typing import Any

from pydantic import BaseModel


class JobStatus(enum.StrEnum):
    PENDING = enum.auto()
    ASSIGNED = enum.auto()
    COMPLETED = enum.auto()
    CANCELLED = enum.auto()
    FAILED = enum.auto()


class JobResults(BaseModel):
    status: JobStatus
    error: str | None = None
    results: dict[str, Any] | None = None
