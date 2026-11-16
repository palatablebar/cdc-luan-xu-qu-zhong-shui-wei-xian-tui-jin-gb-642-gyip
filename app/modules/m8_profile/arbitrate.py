"""整行裁决引擎：对每一行决定「采用新来源」还是「沿用旧来源」（纯规则）。

字段级应用规则（确定性，与到达顺序无关的部分只依赖 event_time 与核验标记）：

1. 空不覆盖：新值为空一律不覆盖已有值——已确认字段绝不改回空值，普通字段同样适用；
2. 空则采纳：旧值为空、新值非空 → 采用，核验标记随行带上；
3. 同值不动：值相同不改写；若新行带核验标记而旧值未核验 → 升级为已核验；
4. 标识字段（企业名称 / 统一标识）异值：未核验值不能替换已有值；核验值可替换
   未核验值；两边都核验且来自不同来源 → 保留现值。凡异值都判待核对冲突；
5. 普通字段（电话 / 联系人 / 地址）异值：event_time 较新者胜，平手保留现值。

整行结论：任一字段被改写 → adopt（采用新来源）；全部字段保留 → keep（沿用旧来源）。
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field

from ... import config
from ..m2_schema.coerce import parse_iso
from .contract import NormRow

# 字段级动作
ACTIONS = (
    "keep_empty",        # 两边都空，不动
    "keep_blank_guard",  # 新值留空，保留旧值（空不覆盖）
    "adopt_fill",        # 旧值空，采用新值补齐
    "keep_same",         # 同值，不动
    "confirm_upgrade",   # 同值但新行带核验标记，升级为已核验
    "adopt_verified",    # 标识字段：核验值替换未核验值 / 同源核验更新
    "keep_identity",     # 标识字段：未核验值不能替换已有值，或两核验来源不一致
    "adopt_fresh",       # 普通字段：新值 event_time 更新，采用
    "keep_fresh",        # 普通字段：新值不更新，保留
)
ADOPT_ACTIONS = frozenset(
    {"adopt_fill", "confirm_upgrade", "adopt_verified", "adopt_fresh"})

ACTION_LABELS = {
    "keep_empty": "两边都空",
    "keep_blank_guard": "新值留空，保留原值",
    "adopt_fill": "补齐空字段",
    "keep_same": "同值不动",
    "confirm_upgrade": "同值核验升级",
    "adopt_verified": "采用核验值",
    "keep_identity": "保留现标识值",
    "adopt_fresh": "采用较新值",
    "keep_fresh": "保留较新值",
}


@dataclass
class FieldState:
    """档案中某字段的当前状态（空档案时 value/source 为 None）。"""

    value: str | None = None
    source: str | None = None
    confirmed: bool = False
    updated_event_time: dt.datetime | None = None

    @classmethod
    def from_row(cls, row: dict | None) -> "FieldState":
        if not row:
            return cls()
        return cls(value=row.get("value"), source=row.get("source"),
                   confirmed=bool(row.get("confirmed")),
                   updated_event_time=parse_iso(row.get("updated_event_time")))


@dataclass
class FieldOutcome:
    field: str
    action: str
    old_value: str | None
    new_value: str | None
    old_source: str | None
    confirmed_after: bool
    note: str = ""
    conflict: bool = False

    def as_dict(self) -> dict:
        return {"field": self.field, "action": self.action,
                "label": ACTION_LABELS[self.action],
                "old_value": self.old_value, "new_value": self.new_value,
                "old_source": self.old_source,
                "confirmed_after": self.confirmed_after, "note": self.note,
                "conflict": self.conflict}


@dataclass
class Decision:
    """一行的整行裁决结果。"""

    verdict: str  # adopt / keep
    outcomes: list[FieldOutcome] = field(default_factory=list)

    @property
    def changed(self) -> bool:
        return any(o.action in ADOPT_ACTIONS for o in self.outcomes)

    @property
    def conflicts(self) -> list[FieldOutcome]:
        return [o for o in self.outcomes if o.conflict]

    def detail(self) -> dict:
        return {"fields": [o.as_dict() for o in self.outcomes]}


def arbitrate(profile: dict[str, FieldState], row: NormRow) -> Decision:
    """对一条已通过契约的档案行做整行裁决；profile 是 {字段: FieldState}。"""
    outcomes = [_judge_field(profile.get(name) or FieldState(), name, row)
                for name in config.PROFILE_FIELD_ORDER]
    verdict = "adopt" if any(o.action in ADOPT_ACTIONS for o in outcomes) else "keep"
    return Decision(verdict=verdict, outcomes=outcomes)


def _judge_field(old: FieldState, name: str, row: NormRow) -> FieldOutcome:
    new = row.fields.get(name)
    verified = name in row.verified
    base = {"field": name, "old_value": old.value, "new_value": new,
            "old_source": old.source}

    def outcome(action: str, *, confirmed: bool, note: str = "",
                conflict: bool = False) -> FieldOutcome:
        return FieldOutcome(action=action, confirmed_after=confirmed,
                            note=note, conflict=conflict, **base)

    if new is None:
        if old.value is None:
            return outcome("keep_empty", confirmed=old.confirmed)
        note = "已确认字段不改回空值" if old.confirmed else "空值不覆盖已有值"
        return outcome("keep_blank_guard", confirmed=old.confirmed, note=note)
    if old.value is None:
        return outcome("adopt_fill", confirmed=verified,
                       note="新值补齐空字段")
    if new == old.value:
        if verified and not old.confirmed:
            return outcome("confirm_upgrade", confirmed=True,
                           note="同值经核验来源确认")
        return outcome("keep_same", confirmed=old.confirmed)

    # 两边非空且不一致
    if name in config.PROFILE_IDENTITY_FIELDS:
        conflict = True
        if verified and (not old.confirmed or old.source == row.source):
            note = ("同源核验更新" if old.confirmed else "核验值优先于未核验值")
            return outcome("adopt_verified", confirmed=True, note=note,
                           conflict=conflict)
        if old.confirmed and verified:
            note = "两来源均已核验但不一致，保留现值待人工核对"
        elif old.confirmed:
            note = "未核验值不能替换已核验标识"
        else:
            note = "未核验值不能替换已有标识"
        return outcome("keep_identity", confirmed=old.confirmed, note=note,
                       conflict=conflict)

    # 普通字段：按 event_time 新旧裁决，平手保留现值（确定性）
    old_time = old.updated_event_time
    if old_time is None or row.event_time > old_time:
        return outcome("adopt_fresh", confirmed=verified,
                       note="新值时间更新")
    return outcome("keep_fresh", confirmed=old.confirmed,
                   note="新值时间不更新，保留现值")


def apply_decision(profile: dict[str, FieldState], row: NormRow,
                   decision: Decision) -> dict[str, FieldState]:
    """把裁决结果应用到内存中的档案状态，返回新状态（不改入参）。"""
    next_state = dict(profile)
    for outcome in decision.outcomes:
        if outcome.action not in ADOPT_ACTIONS:
            continue
        old = profile.get(outcome.field) or FieldState()
        if outcome.action == "confirm_upgrade":
            next_state[outcome.field] = FieldState(
                value=old.value, source=old.source, confirmed=True,
                updated_event_time=row.event_time)
        else:
            next_state[outcome.field] = FieldState(
                value=row.fields[outcome.field], source=row.source,
                confirmed=outcome.confirmed_after,
                updated_event_time=row.event_time)
    return next_state
