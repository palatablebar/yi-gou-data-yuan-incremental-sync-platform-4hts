"""增量过滤与水位线推进：真实比较 watermark_column，并尊重逐条记录账本。"""
from __future__ import annotations

from typing import Any

from src.utils.watermark import compare, max_watermark, safe_checkpoint


def watermark_from(configured: Any, target_max: Any) -> Any:
    if configured not in (None, ""):
        return configured
    return target_max


def _status_of(states: dict[str, Any] | None, key: str | None) -> str | None:
    if not states or not key:
        return None
    state = states.get(key)
    if state is None:
        return None
    return getattr(state, "status", state)


def filter_rows(
    rows: list[dict[str, Any]],
    *,
    sync_mode: str,
    watermark_column: str | None,
    watermark: Any,
    record_keys: list[str] | None = None,
    states: dict[str, Any] | None = None,
    identities: list[str | None] | None = None,
    known_identities: set[str] | None = None,
) -> tuple[list[dict[str, Any]], int, Any]:
    """返回 (本批需要处理的行, 被过滤掉的行数, 本批最大水位线)。

    增量语义：
    - 水位线未变（value == watermark）的记录也要重新检查，才能接住迟到补传；
      账本里已经 written/skipped 的记录由 states 过滤掉，不会重复计数/重复写；
    - 账本里 pending 的记录无论水位线高低都要重试；
    - 已登记业务键（冲突键）的迟到更新/回滚，即使水位线低于断点也要重新处理；
    - 其余严格早于水位线且从未登记过的记录按“历史数据”跳过，保持增量起点语义。
    """
    if sync_mode != "incremental" or not watermark_column:
        values = [row.get(watermark_column) for row in rows] if watermark_column else []
        return rows, 0, max_watermark(values) if watermark_column else None

    known = known_identities or set()
    kept: list[dict[str, Any]] = []
    filtered = 0
    for index, row in enumerate(rows):
        value = row.get(watermark_column)
        key = record_keys[index] if record_keys and index < len(record_keys) else None
        status = _status_of(states, key)
        if status in ("written", "skipped"):
            filtered += 1
            continue
        if status == "pending":
            kept.append(row)
            continue
        identity = identities[index] if identities and index < len(identities) else None
        cmp = compare(value, watermark)
        if cmp > 0 or cmp == 0:
            kept.append(row)
        elif identity and identity in known:
            # 同一个业务键的迟到更新/回滚：即使水位线已越过也要补入。
            kept.append(row)
        else:
            filtered += 1
    next_watermark = max_watermark([row.get(watermark_column) for row in kept], watermark)
    return kept, filtered, next_watermark


def safe_watermark(current: Any, values: list[Any], pending_values: list[Any]) -> Any:
    """不会越过 pending 记录的安全断点，供同步与重放共用。"""
    return safe_checkpoint(current, values, pending_values)


def describe(watermark: Any, next_watermark: Any) -> str:
    from src.utils.formatters import watermark_text

    return watermark_text(watermark, next_watermark)


def is_later(left: Any, right: Any) -> bool:
    return compare(left, right) > 0
