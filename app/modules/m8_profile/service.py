"""档案裁决编排：读文件 → 契约 → 整行裁决 → 单事务落库 → 页面/接口读模型。

状态跨批次持久（profile_fields 是唯一对外档案），批次键幂等：
同一输入重跑命中已完成批次，直接复用结果，一行也不新增。
"""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

from ... import config, errors, manifest
from ..m1_events import reader
from ..m2_schema.coerce import iso_utc
from ..m6_batches import state as batch_state
from . import arbitrate, contract


def resolve_files(settings, source: str) -> list[str]:
    base = Path(settings.sample_dir)
    if source in ("", "all"):
        names = [n for n in config.PROFILE_SOURCE_ORDER if (base / n).exists()]
    elif source in config.PROFILE_SOURCES:
        names = [n for n in config.PROFILE_SOURCES[source] if (base / n).exists()]
    else:
        raise errors.BadRequest(
            f"未知档案来源 {source!r}；可选 all / " + " / ".join(config.PROFILE_SOURCES),
            code="E_BAD_SOURCE")
    if not names:
        raise errors.BadRequest(f"档案来源 {source!r} 在样例目录内没有文件",
                                code="E_NO_INPUT")
    return names


def row_signature(row: contract.NormRow) -> str:
    """行签名：来源 + 客户 + UTC 时刻 + 归一字段 + 核验标记。

    同一来源重跑、同一行再到达都命中同一签名，不重复应用。
    """
    body = {
        "source": row.source,
        "customer_id": row.customer_id,
        "event_time": iso_utc(row.event_time),
        "fields": {name: row.fields.get(name) for name in config.PROFILE_FIELD_ORDER},
        "verified": sorted(row.verified),
    }
    return manifest.digest_parts(
        [json.dumps(body, ensure_ascii=False, sort_keys=True)])


def _now_iso() -> str:
    return dt.datetime.now(config.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def run(*, settings, store, source: str = "all", params: dict | None = None) -> dict:
    params = batch_state.normalize_params(params)
    base = Path(settings.sample_dir)
    names = resolve_files(settings, source)

    files = [reader.read_file(base / name, name) for name in names]
    input_digest = manifest.digest_parts([f"{f.name}:{f.sha256}" for f in files])
    key = batch_state.batch_key(f"profile|{input_digest}", settings.rule_version,
                                params)
    batch_id = f"PB-{key}"

    replay = store.find_profile_batch_by_key(key)
    if replay is not None:
        return _replay_payload(store, replay, key)

    # ---- 契约：失败行整行不应用 ----
    valid: list[contract.NormRow] = []
    failed: list[dict] = []
    seq = 0
    for item in files:
        file_source = config.PROFILE_FILE_SOURCE.get(item.name, source)
        for raw_row in item.rows:
            seq += 1
            raw_row.seq = seq
            if raw_row.error or not isinstance(raw_row.raw, dict):
                failed.append({"row_ref": raw_row.row_ref, "seq": seq,
                               "customer_id": "", "source": file_source,
                               "event_time": None,
                               "errors": [{"code": "P_ROW_INVALID", "field": "",
                                           "message": f"行解析失败：{raw_row.error}"}]})
                continue
            norm, row_errors = contract.validate(
                raw_row.raw, file_source=file_source, row_ref=raw_row.row_ref, seq=seq)
            if row_errors:
                raw_customer = raw_row.raw.get("customer_id")
                failed.append({
                    "row_ref": raw_row.row_ref, "seq": seq,
                    "customer_id": raw_customer if isinstance(raw_customer, str)
                    else "",
                    "source": file_source,
                    "event_time": raw_row.raw.get("event_time"),
                    "errors": row_errors})
                continue
            norm.row_sig = row_signature(norm)
            valid.append(norm)

    # ---- 整行裁决：确定性顺序（事件时间 → 来源优先级 → 到达序号） ----
    priority = config.PROFILE_SOURCE_PRIORITY
    valid.sort(key=lambda r: (r.event_time, priority.get(r.source, 9), r.seq))

    profiles = {cid: {name: arbitrate.FieldState.from_row(f)
                      for name, f in fields.items()}
                for cid, fields in store.load_profile_fields().items()}
    seen = store.load_profile_seen()

    decisions: list[dict] = []
    seen_new: list[dict] = []
    conflicts_new: list[dict] = []
    counters = {"rows_total": 0, "adopted": 0, "kept": 0, "failed": 0,
                "duplicates": 0, "customers_touched": 0, "fields_written": 0,
                "conflicts_flagged": 0}
    touched: set[str] = set()

    for row in valid:
        counters["rows_total"] += 1
        if row.row_sig in seen:
            decisions.append(_decision_row(row, "duplicate", "P_DUPLICATE_ROW",
                                           {"note": "同签名行已应用过"}))
            counters["duplicates"] += 1
            continue
        profile = profiles.get(row.customer_id, {})
        decision = arbitrate.arbitrate(profile, row)
        detail = decision.detail()
        if decision.verdict == "adopt":
            profiles[row.customer_id] = arbitrate.apply_decision(
                profile, row, decision)
            touched.add(row.customer_id)
            counters["adopted"] += 1
            counters["fields_written"] += sum(
                1 for o in decision.outcomes
                if o.action in arbitrate.ADOPT_ACTIONS)
            reason = "P_ADOPT_ROW"
        else:
            counters["kept"] += 1
            reason = "P_KEEP_ROW"
        for outcome in decision.conflicts:
            conflicts_new.append({
                "customer_id": row.customer_id, "field": outcome.field,
                "old_value": outcome.old_value, "old_source": outcome.old_source,
                "new_value": outcome.new_value, "new_source": row.source,
                "resolution": "adopted" if outcome.action in arbitrate.ADOPT_ACTIONS
                else "kept"})
        counters["conflicts_flagged"] += len(decision.conflicts)
        decisions.append(_decision_row(row, decision.verdict, reason, detail))
        seen.add(row.row_sig)
        seen_new.append({"row_sig": row.row_sig, "customer_id": row.customer_id,
                         "source": row.source})

    for item in failed:
        counters["rows_total"] += 1
        counters["failed"] += 1
        decisions.append({
            "customer_id": item["customer_id"], "source": item["source"],
            "event_time": item["event_time"], "row_ref": item["row_ref"],
            "row_sig": manifest.digest_parts(
                [f"failed|{item['row_ref']}|{item['source']}"]),
            "verdict": "failed", "reason_code": "P_ROW_INVALID",
            "detail": {"errors": item["errors"],
                       "note": "整行不应用，档案保留旧版"}})

    counters["customers_touched"] = len(touched)

    # ---- 落库：单事务，失败整体回滚，档案保持旧版 ----
    field_rows = [
        {"customer_id": cid, "field": name,
         "value": state.value, "source": state.source,
         "confirmed": state.confirmed,
         "updated_event_time": iso_utc(state.updated_event_time)}
        for cid in touched
        for name, state in profiles[cid].items()
    ]
    batch = {
        "batch_id": batch_id, "batch_key": key,
        "run_id": manifest.new_run_id("PR"), "source": source,
        "status": "done", "created_at": _now_iso(), "finished_at": _now_iso(),
        "input_digest": input_digest, "counters": counters,
        "note": _run_note(counters),
    }
    store.save_profile_run(batch=batch, decisions=decisions, fields=field_rows,
                           conflicts=conflicts_new, seen=seen_new)
    # 待核对总数在落库后按库内实际状态补齐（再判的冲突只累次不新增）
    counters["conflicts_pending"] = len(store.load_profile_conflicts("pending"))
    batch["counters"] = counters
    store.update_profile_batch_counters(batch_id, counters)
    return {
        "status": "done", "batch": batch, "batch_key": key,
        "run_id": batch["run_id"], "idempotent_replay": False,
        "counters": counters,
        "inputs": [f.as_input_row() for f in files],
        "note": batch["note"],
    }


def _decision_row(row: contract.NormRow, verdict: str, reason: str,
                  detail: dict) -> dict:
    return {"customer_id": row.customer_id, "source": row.source,
            "event_time": iso_utc(row.event_time), "row_ref": row.row_ref,
            "row_sig": row.row_sig, "verdict": verdict, "reason_code": reason,
            "detail": detail}


def _run_note(counters: dict) -> str:
    parts = [f"采用 {counters['adopted']}", f"沿用 {counters['kept']}"]
    if counters["failed"]:
        parts.append(f"失败 {counters['failed']}")
    if counters["duplicates"]:
        parts.append(f"重复 {counters['duplicates']}")
    if counters["conflicts_flagged"]:
        parts.append(f"新冲突 {counters['conflicts_flagged']}")
    return " · ".join(parts)


def _replay_payload(store, replay: dict, key: str) -> dict:
    counters = json.loads(replay.get("counters_json") or "{}")
    return {
        "status": "done",
        "batch": replay,
        "batch_key": key,
        "run_id": replay.get("run_id"),
        "idempotent_replay": True,
        "replay_of": replay["batch_id"],
        "counters": counters,
        "inputs": [],
        "note": "同一输入重跑命中已完成批次，不新增行",
    }


# ---------- 读模型（页面与接口共用） ----------

def profile_payload(store) -> dict:
    fields_by_customer = store.load_profile_fields()
    decisions = store.load_profile_decisions(limit=1000)
    conflicts = store.load_profile_conflicts()
    last_decision: dict[str, dict] = {}
    for row in sorted(decisions, key=lambda r: r["id"]):
        last_decision[row["customer_id"]] = row
    pending_by_customer: dict[str, int] = {}
    for row in conflicts:
        if row["status"] == "pending":
            pending_by_customer[row["customer_id"]] = \
                pending_by_customer.get(row["customer_id"], 0) + 1

    customers = []
    for cid in sorted(fields_by_customer):
        stored = fields_by_customer[cid]
        fields = {}
        for name in config.PROFILE_FIELD_ORDER:
            state = stored.get(name) or {}
            fields[name] = {
                "value": state.get("value"),
                "source": state.get("source"),
                "source_label": config.PROFILE_SOURCE_KIND_LABELS.get(
                    state.get("source") or "", "—"),
                "confirmed": bool(state.get("confirmed")),
                "updated_event_time": state.get("updated_event_time"),
            }
        last = last_decision.get(cid) or {}
        customers.append({
            "customer_id": cid,
            "fields": fields,
            "pending_conflicts": pending_by_customer.get(cid, 0),
            "last_verdict": last.get("verdict"),
            "last_source": last.get("source"),
            "last_source_label": config.PROFILE_SOURCE_KIND_LABELS.get(
                last.get("source") or "", "—"),
            "last_event_time": last.get("event_time"),
        })
    confirmed_fields = sum(
        1 for cid in fields_by_customer for name in config.PROFILE_FIELD_ORDER
        if (fields_by_customer[cid].get(name) or {}).get("confirmed"))
    summary = {
        "customers": len(customers),
        "confirmed_fields": confirmed_fields,
        "pending_conflicts": sum(pending_by_customer.values()),
        "decisions": len(decisions),
        "last_batch_id": (store.latest_profile_batch() or {}).get("batch_id"),
    }
    return {"customers": customers, "summary": summary}


def decisions_payload(store, limit: int = 100,
                      batch_id: str | None = None) -> dict:
    rows = store.load_profile_decisions(limit=limit, batch_id=batch_id)
    return {"count": len(rows), "decisions": rows}


def conflicts_payload(store, status: str | None = None) -> dict:
    rows = store.load_profile_conflicts(status)
    return {"count": len(rows), "conflicts": rows,
            "pending": sum(1 for r in rows if r["status"] == "pending")}


def batches_payload(store, limit: int = 20) -> dict:
    rows = store.list_profile_batches(limit=limit)
    for row in rows:
        row["counters"] = json.loads(row.get("counters_json") or "{}")
    return {"count": len(rows), "batches": rows}
