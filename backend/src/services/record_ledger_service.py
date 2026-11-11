"""逐条记录账本服务：状态查询、处置落账、待处理水位线与批次轨迹。"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Iterable

from sqlalchemy import func
from sqlalchemy.orm import Session

from src.constants.enums import RecordStatus
from src.models.rejected_record import RejectedRecord
from src.models.sync_record import SyncRecord, SyncRecordOutcome
from src.utils.json_safe import json_safe_payload
from src.utils.logger import get_logger
from src.utils.record_identity import conflict_key_text, has_conflict_key, record_key
from src.utils.watermark import compare, max_watermark

logger = get_logger("syncmesh.record_ledger")

FINAL_STATUSES = RecordStatus.final()
_STATUS_PRIORITY = {
    RecordStatus.PENDING.value: 0,
    RecordStatus.SKIPPED.value: 1,
    RecordStatus.WRITTEN.value: 2,
}
_CHUNK_SIZE = 500


def build_disposition(
    *,
    raw_payload: dict[str, Any],
    status: str,
    watermark_value: Any = None,
    conflict_key: str | None = None,
    source_row_index: int = 0,
    reason_code: str | None = None,
    reason_detail: str = "",
) -> dict[str, Any]:
    """把一条源记录整理成账本/轨迹可消费的统一结构。"""
    return {
        "record_key": record_key(raw_payload, conflict_key),
        "conflict_key_value": conflict_key_text(raw_payload, conflict_key),
        "conflict_key_complete": has_conflict_key(raw_payload, conflict_key),
        "status": status,
        "watermark_value": None if watermark_value is None else str(watermark_value),
        "raw_payload": json_safe_payload(raw_payload),
        "source_row_index": int(source_row_index or 0),
        "reason_code": reason_code,
        "reason_detail": str(reason_detail or "")[:2000],
    }


def load_states(
    db: Session, pipeline_id: uuid.UUID, keys: Iterable[str]
) -> dict[str, SyncRecord]:
    unique_keys = list({key for key in keys if key})
    states: dict[str, SyncRecord] = {}
    for start in range(0, len(unique_keys), _CHUNK_SIZE):
        chunk = unique_keys[start : start + _CHUNK_SIZE]
        rows = (
            db.query(SyncRecord)
            .filter(SyncRecord.pipeline_id == pipeline_id, SyncRecord.record_key.in_(chunk))
            .all()
        )
        states.update({row.record_key: row for row in rows})
    return states


def get_state(
    db: Session, pipeline_id: uuid.UUID, raw_payload: dict[str, Any], conflict_key: str | None
) -> SyncRecord | None:
    return (
        db.query(SyncRecord)
        .filter(
            SyncRecord.pipeline_id == pipeline_id,
            SyncRecord.record_key == record_key(raw_payload, conflict_key),
        )
        .one_or_none()
    )


def pending_watermarks(db: Session, pipeline_id: uuid.UUID) -> list[Any]:
    rows = (
        db.query(SyncRecord.watermark_value)
        .filter(
            SyncRecord.pipeline_id == pipeline_id,
            SyncRecord.status == RecordStatus.PENDING.value,
        )
        .all()
    )
    return [row[0] for row in rows]


def ledger_watermarks(db: Session, pipeline_id: uuid.UUID) -> tuple[list[Any], list[Any]]:
    """返回 (全部已登记水位线, pending 水位线)，供重放后重新计算断点。"""
    rows = (
        db.query(SyncRecord.status, SyncRecord.watermark_value)
        .filter(SyncRecord.pipeline_id == pipeline_id)
        .all()
    )
    values = [value for _status, value in rows]
    pending = [value for status, value in rows if status == RecordStatus.PENDING.value]
    return values, pending


def known_identities(
    db: Session, pipeline_id: uuid.UUID, identities: Iterable[str | None]
) -> set[str]:
    """返回已经登记过的业务键（冲突键文本）。

    用于识别水位线以下、但属于已同步业务记录的迟到更新/回滚。
    """
    unique = list({value for value in identities if value})
    found: set[str] = set()
    for start in range(0, len(unique), _CHUNK_SIZE):
        chunk = unique[start : start + _CHUNK_SIZE]
        rows = (
            db.query(SyncRecord.conflict_key_value)
            .filter(
                SyncRecord.pipeline_id == pipeline_id,
                SyncRecord.conflict_key_value.in_(chunk),
            )
            .all()
        )
        found.update(row[0] for row in rows if row[0])
    return found


def apply_dispositions(
    db: Session,
    *,
    pipeline_id: uuid.UUID,
    run_id: uuid.UUID,
    dispositions: list[dict[str, Any]],
    write_outcomes: bool = True,
) -> dict[str, dict[str, Any]]:
    """更新管道账本，并为本次运行写入逐条轨迹。

    同一条源记录在一批中可能出现多次（例如重复推送）：
    - 账本按 record_key 合并，优先级 written > skipped > pending；
    - 轨迹按出现次数保留，方便追溯每条记录的实际去向。
    """
    aggregates: dict[str, dict[str, Any]] = {}
    for disposition in dispositions:
        key = disposition["record_key"]
        current = aggregates.get(key)
        if current is None:
            aggregates[key] = dict(disposition)
            continue
        if _STATUS_PRIORITY[disposition["status"]] > _STATUS_PRIORITY[current["status"]]:
            current.update(
                {
                    "status": disposition["status"],
                    "reason_code": disposition.get("reason_code"),
                    "reason_detail": disposition.get("reason_detail") or "",
                }
            )
        current["conflict_key_complete"] = current.get("conflict_key_complete") or disposition.get(
            "conflict_key_complete", False
        )
        # 保留最近一次出现的水位线与行号，便于定位
        if disposition.get("watermark_value") is not None:
            current["watermark_value"] = disposition["watermark_value"]
        current["source_row_index"] = disposition.get("source_row_index", 0)

    states = load_states(db, pipeline_id, aggregates.keys())
    now = datetime.now(timezone.utc)
    for key, aggregate in aggregates.items():
        state = states.get(key)
        if state is None:
            state = SyncRecord(
                pipeline_id=pipeline_id,
                record_key=key,
                conflict_key_value=aggregate.get("conflict_key_value"),
                watermark_value=aggregate.get("watermark_value"),
                status=aggregate["status"],
                source_row_index=aggregate.get("source_row_index", 0),
                raw_payload_json=aggregate.get("raw_payload") or {},
                reason_code=aggregate.get("reason_code"),
                reason_detail=aggregate.get("reason_detail") or "",
                first_run_id=run_id,
                last_run_id=run_id,
                attempt_count=1,
                created_at=now,
                updated_at=now,
            )
            db.add(state)
            continue

        new_status = aggregate["status"]
        # 已完成记录不会被后来的一次失败结果降级，避免重跑时反复计数/反复写。
        if state.status in FINAL_STATUSES and new_status == RecordStatus.PENDING.value:
            new_status = state.status
        state.status = new_status
        state.last_run_id = run_id
        state.source_row_index = aggregate.get("source_row_index", state.source_row_index)
        state.raw_payload_json = aggregate.get("raw_payload") or state.raw_payload_json
        state.conflict_key_value = (
            aggregate.get("conflict_key_value") or state.conflict_key_value
        )
        if aggregate.get("watermark_value") is not None:
            state.watermark_value = aggregate["watermark_value"]
        if new_status == RecordStatus.PENDING.value:
            state.attempt_count = (state.attempt_count or 0) + 1
            state.reason_code = aggregate.get("reason_code")
            state.reason_detail = aggregate.get("reason_detail") or ""
        else:
            state.reason_code = None
            state.reason_detail = ""
        state.updated_at = now

    # 同一业务键出现了更新的已写入/已跳过版本时，之前 pending 的旧版本可视为已被取代。
    # 仅在同一运行之外的历史 pending 条目上生效，本批 pending（如 reject 冲突重复）仍然保留。
    superseding: dict[str, list[Any]] = {}
    for key, aggregate in aggregates.items():
        if (
            aggregate.get("conflict_key_complete")
            and aggregate.get("conflict_key_value")
            and aggregate["status"] in FINAL_STATUSES
        ):
            superseding.setdefault(aggregate["conflict_key_value"], []).append(
                aggregate.get("watermark_value")
            )
    superseded_values = list(superseding)
    for start in range(0, len(superseded_values), _CHUNK_SIZE):
        chunk = superseded_values[start : start + _CHUNK_SIZE]
        old_pending = (
            db.query(SyncRecord)
            .filter(
                SyncRecord.pipeline_id == pipeline_id,
                SyncRecord.status == RecordStatus.PENDING.value,
                SyncRecord.conflict_key_value.in_(chunk),
            )
            .all()
        )
        for state in old_pending:
            if state.record_key in aggregates:
                continue
            final_watermarks = [
                value
                for value in superseding.get(state.conflict_key_value, [])
                if value is not None
            ]
            max_final = max_watermark(final_watermarks, None)
            if (
                max_final is not None
                and state.watermark_value is not None
                and compare(state.watermark_value, max_final) > 0
            ):
                # 旧 pending 的水位线更新，说明它不是被本次新版本取代的旧数据。
                continue
            state.status = RecordStatus.SKIPPED.value
            state.reason_code = None
            state.reason_detail = "superseded_by_newer_record"
            state.updated_at = now

    if write_outcomes:
        for disposition in dispositions:
            db.add(
                SyncRecordOutcome(
                    run_id=run_id,
                    pipeline_id=pipeline_id,
                    record_key=disposition["record_key"],
                    status=disposition["status"],
                    watermark_value=disposition.get("watermark_value"),
                    source_row_index=disposition.get("source_row_index", 0),
                    raw_payload_json=disposition.get("raw_payload") or {},
                    reason_code=disposition.get("reason_code"),
                    reason_detail=disposition.get("reason_detail") or "",
                    created_at=now,
                )
            )
    db.commit()
    logger.info(
        "record ledger applied pipeline=%s run=%s records=%s keys=%s",
        pipeline_id, run_id, len(dispositions), len(aggregates),
    )
    return aggregates


def reset_run_attempt(db: Session, run_id: uuid.UUID) -> None:
    """重试同一个 run 时清掉上一轮的批次轨迹与拒绝行，避免重复累计。"""
    db.query(SyncRecordOutcome).filter(SyncRecordOutcome.run_id == run_id).delete(
        synchronize_session=False
    )
    db.query(RejectedRecord).filter(RejectedRecord.run_id == run_id).delete(
        synchronize_session=False
    )


def mark_replayed(
    db: Session,
    *,
    pipeline_id: uuid.UUID,
    run_id: uuid.UUID,
    raw_payload: dict[str, Any],
    conflict_key: str | None,
    watermark_value: Any = None,
    source_row_index: int = 0,
    status: str = RecordStatus.WRITTEN.value,
) -> None:
    """重放成功后把账本和本批轨迹同步为最终状态（written/skipped），并清除 pending 原因。"""
    disposition = build_disposition(
        raw_payload=raw_payload,
        status=status,
        watermark_value=watermark_value,
        conflict_key=conflict_key,
        source_row_index=source_row_index,
    )
    apply_dispositions(
        db,
        pipeline_id=pipeline_id,
        run_id=run_id,
        dispositions=[disposition],
        write_outcomes=False,
    )
    updated = (
        db.query(SyncRecordOutcome)
        .filter(
            SyncRecordOutcome.run_id == run_id,
            SyncRecordOutcome.record_key == disposition["record_key"],
        )
        .update(
            {
                SyncRecordOutcome.status: status,
                SyncRecordOutcome.reason_code: None,
                SyncRecordOutcome.reason_detail: "",
            },
            synchronize_session=False,
        )
    )
    if not updated:
        db.add(
            SyncRecordOutcome(
                run_id=run_id,
                pipeline_id=pipeline_id,
                record_key=disposition["record_key"],
                status=status,
                watermark_value=disposition.get("watermark_value"),
                source_row_index=source_row_index,
                raw_payload_json=disposition.get("raw_payload") or {},
            )
        )
    db.commit()


def outcome_summary(db: Session, run_id: uuid.UUID) -> dict[str, int]:
    rows = (
        db.query(SyncRecordOutcome.status, func.count(SyncRecordOutcome.id))
        .filter(SyncRecordOutcome.run_id == run_id)
        .group_by(SyncRecordOutcome.status)
        .all()
    )
    summary = {
        RecordStatus.WRITTEN.value: 0,
        RecordStatus.SKIPPED.value: 0,
        RecordStatus.PENDING.value: 0,
    }
    for status, count in rows:
        summary[status] = int(count)
    summary["total"] = sum(
        summary[status] for status in (RecordStatus.WRITTEN.value, RecordStatus.SKIPPED.value, RecordStatus.PENDING.value)
    )
    return summary
