"""单次同步编排：probe → extract → transform → load → verify 全在 worker 里跑。"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from src.config.database import session_scope
from src.constants import error_codes as codes
from src.constants.enums import RunStatus, Stage, SyncMode, TriggerType
from src.models.data_source import DataSource
from src.models.rejected_record import RejectedRecord
from src.models.sync_pipeline import SyncPipeline
from src.models.sync_run import SyncRun
from src.services import mapping_service, record_ledger_service
from src.utils import record_identity
from src.utils.errors import AppError
from src.utils.logger import get_logger
from worker_src.config.celery_app import celery_app
from worker_src.core import lock, progress, storage
from worker_src.core.retry import next_delay, should_retry
from worker_src.pipelines import extract, incremental, load, transform, verify

logger = get_logger("syncmesh.worker.sync")


@celery_app.task(bind=True, name="syncmesh.sync_pipeline", max_retries=0)
def sync_pipeline(self, run_id: str) -> dict:
    with session_scope() as db:
        run = db.get(SyncRun, uuid.UUID(str(run_id)))
        if run is None:
            logger.warning("run not found run=%s", run_id)
            return {"ok": False, "reason": "run_not_found"}
        if run.status == RunStatus.CANCELED.value:
            return {"ok": False, "reason": "canceled"}
        if run.status in RunStatus.terminal():
            # 同一 run 的重复投递不再执行，避免重复写/重复累计。
            return {"ok": False, "reason": "already_finished", "status": run.status}

        pipeline = db.get(SyncPipeline, run.pipeline_id)
        source = db.get(DataSource, pipeline.source_id) if pipeline else None
        if pipeline is None or source is None:
            progress.finish(
                db, run, status=RunStatus.FAILED.value,
                error_message="管道或数据源已被删除", error_code=codes.NOT_FOUND,
            )
            return {"ok": False, "reason": "pipeline_missing"}

        if not lock.acquire(db, pipeline.id, run.id):
            progress.finish(
                db, run, status=RunStatus.FAILED.value,
                error_message="管道锁冲突：同一条管道已有运行在跑", error_code=codes.PIPELINE_LOCKED,
            )
            return {"ok": False, "reason": "locked"}

        progress.mark_running(db, run)

        try:
            return _run(db, run, pipeline, source)
        except AppError as exc:
            return _handle_failure(db, run, pipeline, exc.code, exc.message, exc.detail)
        except Exception as exc:  # noqa: BLE001
            logger.error("run crashed run=%s error=%s", run.id, exc, exc_info=True)
            return _handle_failure(db, run, pipeline, codes.INTERNAL_ERROR, str(exc), "")
        finally:
            lock.release(db, pipeline.id, run.id)


def _run(db, run: SyncRun, pipeline: SyncPipeline, source: DataSource) -> dict:
    mappings = mapping_service.mapping_dicts(db, pipeline.id)
    if not mappings:
        raise AppError(
            codes.PIPELINE_INVALID_MAPPING, "管道没有字段映射",
            f"SyncPipeline[id={pipeline.id}] run failed: mapping empty",
        )

    incremental_mode = pipeline.sync_mode == SyncMode.INCREMENTAL.value
    watermark_column = pipeline.watermark_column

    # 重试同一个 run 时先清掉上一轮的批次轨迹/拒绝行，计数器归零，避免重复累计。
    record_ledger_service.reset_run_attempt(db, run.id)
    run.rows_read = 0
    run.rows_written = 0
    run.rows_rejected = 0
    run.rows_skipped = 0
    run.watermark_value = None

    # 水位线起点：优先用上次安全推进的值，否则取目标表当前最大值
    target_max = load.target_max_watermark(
        pipeline.target_schema, pipeline.target_table, watermark_column
    )
    wm_from = incremental.watermark_from(pipeline.last_watermark, target_max)
    run.watermark_from = None if wm_from is None else str(wm_from)
    db.commit()

    progress.set_stage(db, run, Stage.EXTRACT.value, watermark_from=run.watermark_from)
    logger.info("extract start run=%s source=%s", run.id, source.source_type)
    source_columns = [mapping["source_field"] for mapping in mappings]
    rows = extract.read_rows(source.source_type, source.config_json or {}, columns=source_columns)

    source_keys = [
        record_identity.record_key(row, pipeline.conflict_key) for row in rows
    ]
    source_identities = [
        record_identity.conflict_key_text(row, pipeline.conflict_key)
        if record_identity.has_conflict_key(row, pipeline.conflict_key)
        else None
        for row in rows
    ]
    states = (
        record_ledger_service.load_states(db, pipeline.id, source_keys)
        if incremental_mode
        else {}
    )
    known_identities = (
        record_ledger_service.known_identities(db, pipeline.id, source_identities)
        if incremental_mode
        else set()
    )
    kept, filtered, _ = incremental.filter_rows(
        rows,
        sync_mode=pipeline.sync_mode,
        watermark_column=watermark_column,
        watermark=wm_from,
        record_keys=source_keys,
        states=states,
        identities=source_identities,
        known_identities=known_identities,
    )
    progress.bump_counters(db, run, rows_read=len(kept))
    logger.info(
        "extract done run=%s rows_read=%s filtered=%s", run.id, len(kept), filtered
    )

    source_watermarks = _watermark_values(rows, watermark_column)

    if not kept:
        next_watermark = incremental.safe_watermark(
            wm_from,
            source_watermarks,
            record_ledger_service.pending_watermarks(db, pipeline.id),
        )
        pipeline.last_watermark = None if next_watermark is None else str(next_watermark)
        progress.bump_counters(
            db, run,
            watermark_value=None if next_watermark is None else str(next_watermark),
        )
        db.commit()
        progress.set_stage(db, run, Stage.VERIFY.value, note="no_new_data")
        progress.finish(db, run, status=RunStatus.SUCCEEDED.value)
        logger.info("run succeeded (no new data) run=%s", run.id)
        return {"ok": True, "rows_read": 0, "no_new_data": True}

    kept_keys = [record_identity.record_key(row, pipeline.conflict_key) for row in kept]

    progress.set_stage(db, run, Stage.TRANSFORM.value)
    mapped_records, transform_failures = transform.transform_batch(kept, mappings)
    progress.bump_counters(db, run, rows_rejected=len(transform_failures))

    # 把记录指纹/水位线随映射结果带到写入层，供逐条账本与追踪使用。
    for record in mapped_records:
        source_index = _source_index(record)
        if source_index is not None and 0 <= source_index < len(kept_keys):
            record["__record_key__"] = kept_keys[source_index]
            record["__watermark_value__"] = _row_watermark(kept[source_index], watermark_column)
    for failure in transform_failures:
        source_index = _source_index(failure)
        if source_index is not None and 0 <= source_index < len(kept_keys):
            failure["record_key"] = kept_keys[source_index]
            failure["watermark_value"] = _row_watermark(kept[source_index], watermark_column)

    progress.set_stage(db, run, Stage.LOAD.value)
    staging_table = storage.prepare_staging(pipeline.id)
    load.write_staging(pipeline.id, staging_table, mapped_records)
    outcome = load.load_to_target(
        schema=pipeline.target_schema,
        table=pipeline.target_table,
        records=mapped_records,
        conflict_strategy=pipeline.conflict_strategy,
        conflict_key=pipeline.conflict_key,
        run_id=run.id,
    )

    rejected_items = list(transform_failures) + list(outcome.rejected)
    for item in rejected_items:
        db.add(
            RejectedRecord(
                run_id=run.id,
                pipeline_id=pipeline.id,
                raw_payload_json=_json_safe(item.get("raw_payload") or {}),
                reason_code=item.get("reason_code") or "transform_failed",
                reason_detail=str(item.get("reason_detail") or "")[:2000],
                source_row_index=int(item.get("source_row_index") or 0),
            )
        )
    db.commit()

    # 每一行源记录都要有明确去向：transform 失败 → pending；写入层给出 written/skipped/pending。
    dispositions = _build_dispositions(
        kept=kept,
        kept_keys=kept_keys,
        watermark_column=watermark_column,
        transform_failures=transform_failures,
        load_dispositions=outcome.dispositions,
        conflict_key=pipeline.conflict_key,
    )
    record_ledger_service.apply_dispositions(
        db,
        pipeline_id=pipeline.id,
        run_id=run.id,
        dispositions=dispositions,
    )

    progress.set_stage(db, run, Stage.VERIFY.value)
    staging_rows = storage.count_staging(pipeline.id, staging_table)
    verification = verify.verify_run(
        rows_read=len(kept),
        staging_rows=staging_rows,
        written=outcome.written,
        skipped=outcome.skipped,
        rejected=rejected_items,
        transform_failed_rows=len({_source_index(item) for item in transform_failures}),
    )
    status = verify.final_status(len(rejected_items), None)

    # 断点只推进到所有 pending 记录之前；迟到但已完成的行不重复计数。
    pending_values = record_ledger_service.pending_watermarks(db, pipeline.id)
    next_watermark = incremental.safe_watermark(
        wm_from, source_watermarks, pending_values
    )
    progress.bump_counters(
        db, run,
        rows_written=outcome.written,
        rows_rejected=len(rejected_items),
        rows_skipped=outcome.skipped,
        watermark_value=None if next_watermark is None else str(next_watermark),
    )
    pipeline.last_watermark = None if next_watermark is None else str(next_watermark)
    db.commit()

    progress.finish(db, run, status=status)
    if status == RunStatus.PARTIAL.value:
        logger.info(
            "run partial run=%s read=%s written=%s rejected=%s pending_watermarks=%s",
            run.id, len(kept), outcome.written, len(rejected_items), len(pending_values),
        )
    else:
        logger.info(
            "run succeeded run=%s read=%s written=%s", run.id, len(kept), outcome.written
        )
    return {
        "ok": True,
        "status": status,
        "rows_read": len(kept),
        "rows_written": outcome.written,
        "rows_rejected": len(rejected_items),
        "verification": verification,
    }


def _watermark_values(rows: list[dict], watermark_column: str | None) -> list:
    if not watermark_column:
        return []
    return [row.get(watermark_column) for row in rows]


def _row_watermark(row: dict, watermark_column: str | None):
    if not watermark_column:
        return None
    value = row.get(watermark_column)
    return None if value is None else str(value)


def _source_index(item: dict) -> int | None:
    value = item.get("source_row_index")
    if value is None:
        value = item.get("__source_row_index__")
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _build_dispositions(
    *,
    kept: list[dict],
    kept_keys: list[str],
    watermark_column: str | None,
    transform_failures: list[dict],
    load_dispositions: list[dict],
    conflict_key: str | None,
) -> list[dict]:
    """汇总本批每条源记录的最终去向；缺失处置的 rows 兜底记为 pending。"""
    by_index: dict[int, dict] = {}

    for failure in transform_failures:
        index = _source_index(failure)
        if index is None or not (0 <= index < len(kept)):
            continue
        if index in by_index:
            continue
        raw_payload = failure.get("raw_payload") or kept[index]
        by_index[index] = record_ledger_service.build_disposition(
            raw_payload=raw_payload,
            status="pending",
            watermark_value=_row_watermark(kept[index], watermark_column),
            conflict_key=conflict_key,
            source_row_index=index,
            reason_code=failure.get("reason_code") or "transform_failed",
            reason_detail=failure.get("reason_detail") or "",
        )

    for item in load_dispositions:
        index = _source_index(item)
        if index is None or not (0 <= index < len(kept)):
            continue
        raw_payload = item.get("raw_payload") or kept[index]
        by_index[index] = record_ledger_service.build_disposition(
            raw_payload=raw_payload,
            status=item.get("status") or "pending",
            watermark_value=item.get("watermark_value") or _row_watermark(
                kept[index], watermark_column
            ),
            conflict_key=conflict_key,
            source_row_index=index,
            reason_code=item.get("reason_code"),
            reason_detail=item.get("reason_detail") or "",
        )

    dispositions: list[dict] = []
    for index, row in enumerate(kept):
        item = by_index.get(index)
        if item is None:
            item = record_ledger_service.build_disposition(
                raw_payload=row,
                status="pending",
                watermark_value=_row_watermark(row, watermark_column),
                conflict_key=conflict_key,
                source_row_index=index,
                reason_code="unaccounted",
                reason_detail="row completed without a write/skip disposition",
            )
        # 兜底：确保指纹与当前源行一致（transform 可能改变 mapped 的 raw_payload 引用）。
        item["record_key"] = kept_keys[index] if index < len(kept_keys) else item["record_key"]
        dispositions.append(item)
    return dispositions


def _handle_failure(db, run: SyncRun, pipeline: SyncPipeline, code: str, message: str, detail: str) -> dict:
    logger.error("run failed run=%s code=%s error=%s detail=%s", run.id, code, message, detail)
    retryable = should_retry(run.retry_count, code)
    if retryable:
        run.retry_count = run.retry_count + 1
        run.status = RunStatus.QUEUED.value
        run.error_code = code
        run.error_message = f"{message}（第 {run.retry_count} 次重试已排期）"
        db.commit()
        delay = next_delay(run.retry_count)
        logger.warning("run retry scheduled run=%s retry=%s delay=%s", run.id, run.retry_count, delay)
        sync_pipeline.apply_async(args=[str(run.id)], countdown=delay)
        return {"ok": False, "retry": run.retry_count, "delay": delay}
    progress.finish(db, run, status=RunStatus.FAILED.value, error_message=f"{message} {detail}".strip(), error_code=code)
    return {"ok": False, "code": code, "error": message}


def _json_safe(payload: dict) -> dict:
    from datetime import date

    safe = {}
    for key, value in payload.items():
        if isinstance(value, (str, int, float, bool)) or value is None:
            safe[key] = value
        elif isinstance(value, (datetime, date)):
            safe[key] = value.isoformat()
        else:
            safe[key] = str(value)
    return safe
