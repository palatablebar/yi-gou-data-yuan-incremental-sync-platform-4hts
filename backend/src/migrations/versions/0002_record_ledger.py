"""逐条记录账本与批次处置轨迹。

Revision ID: 0002_record_ledger
Revises: 0001_initial

注意：0001 使用 Base.metadata.create_all 建表；新模型加入后，全新数据库会在
0001 阶段就创建这两张表。本迁移因此对“是否已存在”做幂等判断，同时兼容从
旧库升级（表尚不存在的场景）。
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from src.config.settings import settings

revision = "0002_record_ledger"
down_revision = "0001_initial"
branch_labels = None
depends_on = None

META = settings.meta_schema


def _has_table(name: str) -> bool:
    inspector = sa.inspect(op.get_bind())
    return inspector.has_table(name, schema=META)


def _has_index(table: str, index: str) -> bool:
    inspector = sa.inspect(op.get_bind())
    return any(item["name"] == index for item in inspector.get_indexes(table, schema=META))


def _index(table: str, name: str, columns: list[str]) -> None:
    if _has_table(table) and not _has_index(table, name):
        op.create_index(name, table, columns, schema=META)


def upgrade() -> None:
    if not _has_table("sync_record_ledger"):
        op.create_table(
            "sync_record_ledger",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
            sa.Column(
                "pipeline_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey(f"{META}.sync_pipelines.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("record_key", sa.String(80), nullable=False),
            sa.Column("conflict_key_value", sa.String(255), nullable=True),
            sa.Column("watermark_value", sa.String(120), nullable=True),
            sa.Column("status", sa.String(20), nullable=False, server_default="pending"),
            sa.Column("source_row_index", sa.Integer(), nullable=False, server_default="0"),
            sa.Column(
                "raw_payload_json",
                postgresql.JSONB(astext_type=sa.Text()),
                nullable=False,
                server_default=sa.text("'{}'::jsonb"),
            ),
            sa.Column("reason_code", sa.String(40), nullable=True),
            sa.Column("reason_detail", sa.Text(), nullable=False, server_default=""),
            sa.Column("first_run_id", postgresql.UUID(as_uuid=True), nullable=True),
            sa.Column("last_run_id", postgresql.UUID(as_uuid=True), nullable=True),
            sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="1"),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
            sa.UniqueConstraint(
                "pipeline_id", "record_key", name="uq_sync_record_pipeline_key"
            ),
            schema=META,
        )
    _index("sync_record_ledger", "ix_sync_record_ledger_pipeline_id", ["pipeline_id"])
    _index("sync_record_ledger", "ix_sync_record_ledger_record_key", ["record_key"])
    _index("sync_record_ledger", "ix_sync_record_ledger_status", ["status"])

    if not _has_table("sync_record_outcomes"):
        op.create_table(
            "sync_record_outcomes",
            sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
            sa.Column(
                "run_id",
                postgresql.UUID(as_uuid=True),
                sa.ForeignKey(f"{META}.sync_runs.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column("pipeline_id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("record_key", sa.String(80), nullable=False),
            sa.Column("status", sa.String(20), nullable=False),
            sa.Column("watermark_value", sa.String(120), nullable=True),
            sa.Column("source_row_index", sa.Integer(), nullable=False, server_default="0"),
            sa.Column(
                "raw_payload_json",
                postgresql.JSONB(astext_type=sa.Text()),
                nullable=False,
                server_default=sa.text("'{}'::jsonb"),
            ),
            sa.Column("reason_code", sa.String(40), nullable=True),
            sa.Column("reason_detail", sa.Text(), nullable=False, server_default=""),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            schema=META,
        )
    _index("sync_record_outcomes", "ix_sync_record_outcomes_run_id", ["run_id"])
    _index("sync_record_outcomes", "ix_sync_record_outcomes_pipeline_id", ["pipeline_id"])
    _index("sync_record_outcomes", "ix_sync_record_outcomes_status", ["status"])
    _index("sync_record_outcomes", "ix_sync_record_outcomes_record_key", ["record_key"])


def downgrade() -> None:
    if _has_table("sync_record_outcomes"):
        op.drop_table("sync_record_outcomes", schema=META)
    if _has_table("sync_record_ledger"):
        op.drop_table("sync_record_ledger", schema=META)
