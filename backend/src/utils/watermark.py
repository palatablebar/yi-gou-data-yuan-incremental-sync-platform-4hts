"""水位线比较：支持时间戳 / 自增 ID / 字符串三种形态。"""
from __future__ import annotations

from datetime import datetime
from typing import Any


def coerce(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return value
    text = str(value).strip()
    if text == "":
        return None
    if text.isdigit():
        return int(text)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(text[: len(fmt) + 4], fmt)
        except ValueError:
            continue
    return text


def compare(value: Any, watermark: Any) -> int:
    """返回 -1/0/1；None 视为最小。"""
    left, right = coerce(value), coerce(watermark)
    if left is None and right is None:
        return 0
    if left is None:
        return -1
    if right is None:
        return 1
    try:
        if left == right:
            return 0
        return 1 if left > right else -1
    except TypeError:
        return 1 if str(left) > str(right) else -1


def should_advance(value: Any, watermark: Any) -> bool:
    return compare(value, watermark) > 0


def max_watermark(values: list[Any], current: Any = None) -> Any:
    best = current
    for value in values:
        if should_advance(value, best):
            best = value
    return best


def safe_checkpoint(current: Any, values: list[Any], pending_values: list[Any]) -> Any:
    """计算可安全推进到的水位线：绝不能越过任何待处理记录。

    - 有 pending：只推进到 pending 最小水位线之前最大的已完成水位线；
      若找不到（pending 就是最早一条），保持当前水位线。
    - 无 pending：推进到本批源记录的最大水位线。
    - pending 缺少水位线值：无法排序，保持当前水位线不动。
    """
    pending = list(pending_values or [])
    if any(coerce(value) is None for value in pending):
        return current

    if pending:
        pending_min = pending[0]
        for value in pending[1:]:
            if compare(value, pending_min) < 0:
                pending_min = value
        completed_below = [
            value
            for value in values
            if coerce(value) is not None and compare(value, pending_min) < 0
        ]
        best = max_watermark(completed_below, None)
        if best is not None:
            return best
        if current is not None and compare(current, pending_min) < 0:
            return current
        # 逻辑上前进到了 pending 之后（例如历史脏数据），回退到起点重新扫描更安全。
        return None

    best = max_watermark([value for value in values if coerce(value) is not None], None)
    if best is None:
        return current
    if current is not None and compare(best, current) < 0:
        return current
    return best
