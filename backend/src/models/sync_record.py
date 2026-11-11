from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.config.settings import settings
from src.models.base import Base, new_uuid, utcnow


class SyncRecord(Base):
    """按管道 + 记录指纹持久化的处置账本。

    status 只有三种最终口径：
    - written：已经写入目标表；
    - skipped：按冲突策略明确跳过（目标表已有等价记录）；
    - pending：仍待处理（转换失败、冲突拒绝、写库异常等），水位线不得越过它。

    同一条源记录重复出现时指纹不变，因此重跑可以跳过已完成记录、只重试 pending。
    """

    __tablename__ = "sync_record_ledger"
    __table_args__ = (
        UniqueConstraint("pipeline_id", "record_key", name="uq_sync_record_pipeline_key"),
        {"schema": settings.meta_schema},
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid)
    pipeline_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey(f"{settings.meta_schema}.sync_pipelines.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    record_key: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    conflict_key_value: Mapped[str | None] = mapped_column(String(255), nullable=True)
    watermark_value: Mapped[str | None] = mapped_column(String(120), nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending", index=True)
    source_row_index: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    raw_payload_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    reason_code: Mapped[str | None] = mapped_column(String(40), nullable=True)
    reason_detail: Mapped[str] = mapped_column(Text, nullable=False, default="")
    first_run_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    last_run_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False
    )


class SyncRecordOutcome(Base):
    """一次运行内的逐条处置轨迹（written / skipped / pending）。

    与账本不同，这张表保留“每一批”的现场：即使后续重跑或重放改变了账本状态，
    仍可以追溯某次运行里每条源记录最终去了哪里。
    """

    __tablename__ = "sync_record_outcomes"
    __table_args__ = {"schema": settings.meta_schema}

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid)
    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey(f"{settings.meta_schema}.sync_runs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    pipeline_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    record_key: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    watermark_value: Mapped[str | None] = mapped_column(String(120), nullable=True)
    source_row_index: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    raw_payload_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    reason_code: Mapped[str | None] = mapped_column(String(40), nullable=True)
    reason_detail: Mapped[str] = mapped_column(Text, nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
