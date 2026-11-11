"""源记录指纹：让「同一条记录」在多次运行、重试与重放之间保持稳定。

指纹同时包含冲突键（便于排查）和完整原始载荷（便于发现同键但内容变化的补传/回滚）。
只要内容不变，指纹就不变：重复触发时可以直接跳过已完成记录；
一旦内容发生变化（即使水位线时间没变），指纹也会变化，从而被重新纳入本批。
"""
from __future__ import annotations

import hashlib
import json
from typing import Any


def parse_conflict_key(conflict_key: str | None) -> list[str]:
    if not conflict_key:
        return []
    return [part.strip() for part in conflict_key.split(",") if part.strip()]


def _canonical(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def conflict_key_text(raw_payload: dict[str, Any] | None, conflict_key: str | None) -> str | None:
    keys = parse_conflict_key(conflict_key)
    if not keys:
        return None
    payload = raw_payload or {}
    return "+".join(f"{column}={_canonical(payload.get(column))}" for column in keys)


def has_conflict_key(raw_payload: dict[str, Any] | None, conflict_key: str | None) -> bool:
    """冲突键是否完整存在；缺失主键的行不能按“同一业务记录”互相取代。"""
    keys = parse_conflict_key(conflict_key)
    if not keys:
        return False
    payload = raw_payload or {}
    return all(payload.get(column) not in (None, "") for column in keys)


def record_key(raw_payload: dict[str, Any] | None, conflict_key: str | None = None) -> str:
    """返回带前缀的 sha256 指纹（67 个字符，可安全放入 varchar(80)）。"""
    payload = raw_payload or {}
    material = {
        "conflict_key": conflict_key_text(payload, conflict_key),
        "payload": {
            str(column): _canonical(payload[column])
            for column in sorted(payload.keys(), key=str)
        },
    }
    encoded = json.dumps(
        material, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str
    )
    return "rk_" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()
