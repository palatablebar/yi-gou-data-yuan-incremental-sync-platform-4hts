"""JSONB 载荷清洗：日期/时间等非 JSON 原生类型统一转成字符串。"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any


def json_safe_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    safe: dict[str, Any] = {}
    for key, value in (payload or {}).items():
        if isinstance(value, (str, int, float, bool)) or value is None:
            safe[key] = value
        elif isinstance(value, (datetime, date)):
            safe[key] = value.isoformat()
        else:
            safe[key] = str(value)
    return safe
