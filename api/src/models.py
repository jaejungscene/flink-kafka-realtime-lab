from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from realtime_lab.dlq_tools import REPLAY_RUN_ID_PATTERN


class DlqRecordRef(BaseModel):
    model_config = ConfigDict(extra="forbid")

    partition: int = Field(ge=0)
    offset: int = Field(ge=0)


class DlqReplayRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_messages: int = Field(default=20, ge=1, le=200)
    scan_limit: int = Field(default=200, ge=1, le=1000)
    timeout_seconds: float = Field(default=8.0, ge=1.0, le=30.0)
    from_beginning: bool = True
    dry_run: bool = True
    confirm: bool = False
    replay_run_id: str | None = Field(
        default=None,
        pattern=REPLAY_RUN_ID_PATTERN.pattern,
    )
    records: list[DlqRecordRef] = Field(default_factory=list, max_length=200)
