"""校验：行数账目 + 必填已由 transform 保证 + staging 与写入数对账。"""
from __future__ import annotations

from typing import Any

from src.services import verify_service
from worker_src.constants.reason_codes import (
    REASON_CONFLICT_REJECTED,
    REASON_TRANSFORM_FAILED,
)


def verify_run(
    *,
    rows_read: int,
    staging_rows: int,
    written: int,
    skipped: int,
    rejected: list[dict[str, Any]],
    transform_failed_rows: int = 0,
) -> dict[str, Any]:
    totals = verify_service.verify_totals(rows_read, written, skipped, len(rejected))
    codes: dict[str, int] = {}
    for item in rejected:
        key = item.get("reason_code") or REASON_TRANSFORM_FAILED
        codes[key] = codes.get(key, 0) + 1
    conflict_rejects = codes.get(REASON_CONFLICT_REJECTED, 0)
    # 拒绝项既包含 transform 失败，也包含写入层拒绝；staging 只承载成功转换的行。
    load_rejected = max(len(rejected) - transform_failed_rows, 0)
    staging_ok = staging_rows == written + skipped + load_rejected
    return {
        "ok": totals["ok"] and staging_ok,
        "totals": totals,
        "rejected_codes": codes,
        "conflict_rejects": conflict_rejects,
        "staging_rows": staging_rows,
        "written": written,
        "skipped": skipped,
        "rejected": len(rejected),
        "staging_ok": staging_ok,
    }


def final_status(rejected_count: int, error: str | None) -> str:
    from src.constants.enums import RunStatus

    if error:
        return RunStatus.FAILED.value
    if rejected_count > 0:
        return RunStatus.PARTIAL.value
    return RunStatus.SUCCEEDED.value
