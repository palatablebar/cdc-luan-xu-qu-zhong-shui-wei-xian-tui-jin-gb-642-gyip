"""档案行契约：信封、字段类型、统一标识与电话的格式校验（纯规则）。

任何一项不通过 → 整行失败（P_ROW_INVALID），一个字段也不应用，档案保留旧版。
空值哨兵与空串一律归一为 None（语义：该字段本次未提供，不是「清空」）。
"""
from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass

from ... import config
from ..m2_schema.coerce import is_sentinel, parse_time

# 统一社会信用代码：18 位，字符集 0-9 与 A-Z 去掉 I/O/S/Z/V（GB 32100-2015）
UNIFIED_ID_RE = re.compile(r"^[0-9A-HJ-NP-RT-UWXY]{18}$")
# 电话：数字与常见分隔符，5-25 位
PHONE_RE = re.compile(r"^[0-9+\-() ]{5,25}$")

MAX_VALUE_LEN = 200


@dataclass
class NormRow:
    """通过契约的档案行：值已归一（哨兵 → None），时间已归一 UTC。"""

    customer_id: str
    source: str
    event_time: dt.datetime
    fields: dict[str, str | None]
    verified: frozenset[str]
    row_ref: str
    seq: int
    row_sig: str = ""


def _clean_value(raw) -> tuple[str | None, str | None]:
    """返回 (归一值, 错误码)；哨兵与空串 → None，非字符串 → 类型错误。"""
    if raw is None:
        return None, None
    if isinstance(raw, bool) or not isinstance(raw, str):
        return None, "E_TYPE_MISMATCH"
    text = raw.strip()
    if not text or is_sentinel(text):
        return None, None
    if len(text) > MAX_VALUE_LEN:
        return None, "E_VALUE_TOO_LONG"
    return text, None


def validate(raw: dict, *, file_source: str, row_ref: str, seq: int,
             event_time_parser=parse_time) -> tuple[NormRow | None, list[dict]]:
    """校验一条原始档案行；返回 (归一行, 错误列表)。有错误时归一行为 None。

    错误列表元素：{"code", "field", "message"}，供裁决记录与页面展示。
    """
    errors: list[dict] = []

    def err(code: str, field_name: str, message: str) -> None:
        errors.append({"code": code, "field": field_name, "message": message})

    customer_id = raw.get("customer_id")
    if not isinstance(customer_id, str) or not customer_id.strip():
        err("P_ROW_INVALID", "customer_id", "customer_id 缺失或不是非空字符串")
        customer_id = ""
    else:
        customer_id = customer_id.strip()

    source = raw.get("source")
    if source not in config.PROFILE_SOURCE_KIND_LABELS:
        err("P_ROW_INVALID", "source",
            f"source 必须是 {' / '.join(config.PROFILE_SOURCE_KIND_LABELS)}")
    elif source != file_source:
        err("P_ROW_INVALID", "source",
            f"行内来源 {source} 与文件来源 {file_source} 不一致")

    event_time, time_codes, _raw_time = event_time_parser(raw.get("event_time"))
    if event_time is None:
        err("P_ROW_INVALID", "event_time", "event_time 无法解析为时刻")

    fields_raw = raw.get("fields")
    if not isinstance(fields_raw, dict):
        err("P_ROW_INVALID", "fields", "fields 必须是对象")
        fields_raw = {}

    unknown = sorted(set(fields_raw) - set(config.PROFILE_FIELD_ORDER))
    for name in unknown:
        err("P_ROW_INVALID", name, f"契约外字段 {name}")

    fields: dict[str, str | None] = {}
    for name in config.PROFILE_FIELD_ORDER:
        value, code = _clean_value(fields_raw.get(name))
        if code:
            err("P_ROW_INVALID", name, f"字段 {name} 类型不符或超长")
            continue
        if value is not None:
            if name == "unified_id" and not UNIFIED_ID_RE.match(value):
                err("P_ROW_INVALID", name,
                    "统一标识须为 18 位（数字与大写字母，不含 I/O/S/Z/V）")
            if name == "phone" and not PHONE_RE.match(value):
                err("P_ROW_INVALID", name, "电话格式不符（数字与 +-() 空格，5-25 位）")
        fields[name] = value

    verified_raw = raw.get("verified", [])
    if not isinstance(verified_raw, list) or not all(
            isinstance(item, str) for item in verified_raw):
        err("P_ROW_INVALID", "verified", "verified 必须是字段名数组")
        verified_raw = []
    verified = frozenset(verified_raw)
    bad_verified = sorted(verified - set(config.PROFILE_FIELD_ORDER))
    for name in bad_verified:
        err("P_ROW_INVALID", "verified", f"verified 含契约外字段 {name}")

    if errors:
        return None, errors
    return NormRow(customer_id=customer_id, source=source,
                   event_time=event_time, fields=fields, verified=verified,
                   row_ref=row_ref, seq=seq), []
