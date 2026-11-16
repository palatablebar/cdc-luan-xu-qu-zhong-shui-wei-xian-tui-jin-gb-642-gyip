"""M8 客户档案裁决：整行裁决、逐字段溯源、待核对冲突、失败保留旧版、幂等重跑。

对应需求：
- 裁决整行采用新来源还是沿用旧来源（adopt / keep / failed / duplicate 四态）；
- 对外档案保留每个采用值的来源（profile_fields 逐字段 source）；
- 标识字段不一致判待核对冲突；
- 失败后保留旧版（行级原子 + 批次级事务）；
- 同一来源重跑不能新增（批次键幂等 + 行签名去重）；
- 已确认字段不改回空值（空不覆盖）。
"""
from __future__ import annotations

import json
from dataclasses import replace

import pytest

from app import errors
from app.modules.m8_profile import arbitrate, contract, service

ORDER_ROW = {"customer_id": "C2001", "source": "order",
             "event_time": "2026-11-15T08:00:00Z",
             "fields": {"name": "上海云图科技有限公司",
                        "unified_id": "91310115MA1K4A2B8C",
                        "phone": "021-58880001", "contact": "张伟",
                        "address": "上海市浦东新区张江路 100 号"},
             "verified": ["name", "unified_id"]}


def _write(directory, name, rows):
    lines = [f"# profile-export rows={len(rows)}"]
    lines += [json.dumps(row, ensure_ascii=False) for row in rows]
    (directory / name).write_text("\n".join(lines) + "\n", encoding="utf-8")


def _fields(store, customer_id):
    customers = service.profile_payload(store)["customers"]
    return {c["customer_id"]: c["fields"] for c in customers}[customer_id]


# ---------- 样例数据：双来源裁决主流程 ----------

def test_import_all_counters_and_provenance(settings, store):
    result = service.run(settings=settings, store=store, source="all")
    counters = result["counters"]
    assert counters["rows_total"] == 12
    assert counters["adopted"] == 10
    assert counters["failed"] == 1
    assert counters["duplicates"] == 1
    assert counters["conflicts_flagged"] == 2
    assert counters["conflicts_pending"] == 2

    fields = _fields(store, "C1001")
    # 电话采用结算库（更新更快），企业名称沿用订单库（已核验）
    assert fields["phone"]["value"] == "139-1111-0001"
    assert fields["phone"]["source"] == "settlement"
    assert fields["name"]["value"] == "上海云图科技有限公司"
    assert fields["name"]["source"] == "order"
    assert fields["name"]["confirmed"] is True
    assert fields["unified_id"]["source"] == "order"


def test_confirmed_identity_not_blanked(settings, store):
    """已确认字段不改回空值：结算库留空名称/标识，档案保留订单库核验值。"""
    service.run(settings=settings, store=store, source="all")
    fields = _fields(store, "C1001")
    assert fields["name"]["value"] == "上海云图科技有限公司"
    assert fields["unified_id"]["value"] == "91310115MA1K4A2B8C"
    decisions = store.load_profile_decisions(limit=50)
    c1001 = [d for d in decisions
             if d["customer_id"] == "C1001" and d["source"] == "settlement"][0]
    guards = {f["field"]: f for f in c1001["detail"]["fields"]
              if f["action"] == "keep_blank_guard"}
    assert set(guards) == {"name", "unified_id", "address"}
    assert "已确认字段不改回空值" in guards["name"]["note"]


def test_identity_conflict_marked_pending(settings, store):
    """两来源标识字段不一致 → 待核对冲突；对外档案只保留一套名称。"""
    service.run(settings=settings, store=store, source="all")
    fields = _fields(store, "C1002")
    assert fields["name"]["value"] == "北京岚山贸易有限公司"
    assert fields["name"]["source"] == "order"
    conflicts = store.load_profile_conflicts("pending")
    assert len(conflicts) == 2
    name_conflict = [c for c in conflicts if c["field"] == "name"][0]
    assert name_conflict["customer_id"] == "C1002"
    assert name_conflict["old_value"] == "北京岚山贸易有限公司"
    assert name_conflict["old_source"] == "order"
    assert name_conflict["new_value"] == "北京岚山贸易公司"
    assert name_conflict["new_source"] == "settlement"
    assert name_conflict["status"] == "pending"
    uid_conflict = [c for c in conflicts if c["field"] == "unified_id"][0]
    assert uid_conflict["customer_id"] == "C1003"
    assert uid_conflict["resolution"] == "kept"


def test_invalid_row_keeps_old_version_atomically(settings, store):
    """失败后保留旧版：统一标识非法 → 整行失败，同行的电话也不应用。"""
    service.run(settings=settings, store=store, source="all")
    fields = _fields(store, "C1005")
    assert fields["phone"]["value"] == "028-86660005"
    assert fields["phone"]["source"] == "order"
    decisions = store.load_profile_decisions(limit=50)
    failed = [d for d in decisions if d["verdict"] == "failed"]
    assert len(failed) == 1
    assert failed[0]["customer_id"] == "C1005"
    assert failed[0]["reason_code"] == "P_ROW_INVALID"
    assert failed[0]["detail"]["errors"][0]["field"] == "unified_id"


def test_exact_duplicate_not_reapplied(settings, store):
    service.run(settings=settings, store=store, source="all")
    decisions = store.load_profile_decisions(limit=50)
    dups = [d for d in decisions if d["verdict"] == "duplicate"]
    assert len(dups) == 1
    assert dups[0]["customer_id"] == "C1002"
    assert dups[0]["reason_code"] == "P_DUPLICATE_ROW"
    # 12 行 = 10 应用签名 + 1 失败（不入签名）+ 1 重复（不新增签名）
    assert store.count_profile_rows()["profile_seen"] == 10


def test_rerun_same_source_adds_nothing(settings, store):
    """同一来源重跑：命中批次键直接复用，一行也不新增。"""
    first = service.run(settings=settings, store=store, source="all")
    before = store.count_profile_rows()
    second = service.run(settings=settings, store=store, source="all")
    after = store.count_profile_rows()
    assert second["idempotent_replay"] is True
    assert second["batch"]["batch_id"] == first["batch"]["batch_id"]
    assert before == after


def test_cross_batch_duplicate_rows_not_reapplied(settings, store):
    """换个批次键重投同一文件：行级签名去重，档案不动、冲突不增。"""
    service.run(settings=settings, store=store, source="all")
    before = _fields(store, "C1001")
    result = service.run(settings=settings, store=store, source="settlement")
    assert result["idempotent_replay"] is False  # 输入摘要不同，是新批次
    counters = result["counters"]
    assert counters["adopted"] == 0
    assert counters["duplicates"] == 6
    assert counters["fields_written"] == 0
    assert _fields(store, "C1001") == before
    assert len(store.load_profile_conflicts()) == 2


# ---------- 构造场景：核验优先、新鲜度、沿用旧来源 ----------

def test_verified_replaces_unverified(settings, store, tmp_samples):
    """核验值可替换未核验值（即使 event_time 更旧），并判待核对冲突。"""
    _write(tmp_samples, "customers_settlement.jsonl", [
        {"customer_id": "C2001", "source": "settlement",
         "event_time": "2026-11-15T09:00:00Z",
         "fields": {"name": "云图贸易", "unified_id": "",
                    "phone": "139-0000-0001", "contact": "", "address": ""},
         "verified": []}])
    _write(tmp_samples, "customers_order.jsonl", [ORDER_ROW])
    crafted = replace(settings, sample_dir=str(tmp_samples))
    service.run(settings=crafted, store=store, source="settlement")
    assert _fields(store, "C2001")["name"]["value"] == "云图贸易"
    assert _fields(store, "C2001")["name"]["confirmed"] is False

    service.run(settings=crafted, store=store, source="order")
    fields = _fields(store, "C2001")
    assert fields["name"]["value"] == "上海云图科技有限公司"
    assert fields["name"]["confirmed"] is True
    assert fields["name"]["source"] == "order"
    # 电话按新鲜度仍属结算库（订单库的更旧）
    assert fields["phone"]["value"] == "139-0000-0001"
    assert fields["phone"]["source"] == "settlement"
    conflicts = store.load_profile_conflicts("pending")
    assert len(conflicts) == 1
    assert conflicts[0]["resolution"] == "adopted"


def test_unverified_cannot_replace_confirmed(settings, store, tmp_samples):
    _write(tmp_samples, "customers_order.jsonl", [ORDER_ROW])
    _write(tmp_samples, "customers_settlement.jsonl", [
        {"customer_id": "C2001", "source": "settlement",
         "event_time": "2026-11-15T09:00:00Z",
         "fields": {"name": "云图贸易（结算）", "unified_id": "",
                    "phone": "139-0000-0001", "contact": "", "address": ""},
         "verified": []}])
    crafted = replace(settings, sample_dir=str(tmp_samples))
    service.run(settings=crafted, store=store, source="order")
    service.run(settings=crafted, store=store, source="settlement")
    fields = _fields(store, "C2001")
    assert fields["name"]["value"] == "上海云图科技有限公司"
    assert fields["phone"]["value"] == "139-0000-0001"
    conflicts = store.load_profile_conflicts("pending")
    assert len(conflicts) == 1
    assert conflicts[0]["resolution"] == "kept"


def test_ordinary_field_freshness_not_arrival_order(settings, store, tmp_samples):
    """普通字段按 event_time 裁决：晚到的旧值不覆盖已有的新值。"""
    _write(tmp_samples, "customers_settlement.jsonl", [
        {"customer_id": "C2001", "source": "settlement",
         "event_time": "2026-11-15T09:00:00Z",
         "fields": {"name": "", "unified_id": "", "phone": "139-0000-0001",
                    "contact": "", "address": ""},
         "verified": []}])
    _write(tmp_samples, "customers_order.jsonl", [ORDER_ROW])  # 08:00，电话更旧
    crafted = replace(settings, sample_dir=str(tmp_samples))
    service.run(settings=crafted, store=store, source="settlement")
    service.run(settings=crafted, store=store, source="order")  # 后到但电话更旧
    fields = _fields(store, "C2001")
    assert fields["phone"]["value"] == "139-0000-0001"
    assert fields["phone"]["source"] == "settlement"


def test_keep_verdict_when_nothing_new(settings, store, tmp_samples):
    """没有任何字段更优 → 整行沿用旧来源。"""
    _write(tmp_samples, "customers_order.jsonl", [ORDER_ROW])
    stale = dict(ORDER_ROW, source="settlement",
                 event_time="2026-11-15T09:00:00Z", verified=[],
                 fields={**ORDER_ROW["fields"], "name": "", "unified_id": "",
                         "address": ""})
    _write(tmp_samples, "customers_settlement.jsonl", [stale])
    crafted = replace(settings, sample_dir=str(tmp_samples))
    service.run(settings=crafted, store=store, source="order")
    result = service.run(settings=crafted, store=store, source="settlement")
    assert result["counters"]["kept"] == 1
    assert result["counters"]["adopted"] == 0
    decisions = store.load_profile_decisions(limit=10)
    assert decisions[0]["verdict"] == "keep"
    assert decisions[0]["reason_code"] == "P_KEEP_ROW"


def test_unknown_source_rejected_and_profile_intact(settings, store):
    with pytest.raises(errors.BadRequest):
        service.run(settings=settings, store=store, source="nope")
    assert store.count_profile_rows()["profile_fields"] == 0


# ---------- 冲突核对、接口与页面 ----------

def test_conflict_acknowledgement(settings, store):
    service.run(settings=settings, store=store, source="all")
    conflict = store.load_profile_conflicts("pending")[0]
    row = store.ack_profile_conflict(conflict["id"], "已电话核实")
    assert row["status"] == "acknowledged"
    assert row["note"] == "已电话核实"
    assert store.ack_profile_conflict(9999) is None
    # 核对只改状态，不改档案值
    assert _fields(store, "C1002")["name"]["value"] == "北京岚山贸易有限公司"


def test_api_and_page(client):
    resp = client.post("/api/profile/import", json={"source": "all"})
    assert resp.status_code == 200
    assert resp.json()["counters"]["adopted"] == 10

    profile = client.get("/api/profile").json()
    assert profile["summary"]["customers"] == 6
    c1001 = [c for c in profile["customers"] if c["customer_id"] == "C1001"][0]
    assert c1001["fields"]["phone"]["source_label"] == "结算库"
    assert c1001["fields"]["name"]["source_label"] == "订单库"

    conflicts = client.get("/api/profile/conflicts?status=pending").json()
    assert conflicts["count"] == 2
    ack = client.post(
        f"/api/profile/conflicts/{conflicts['conflicts'][0]['id']}/ack",
        json={"note": "核对无误"})
    assert ack.status_code == 200
    assert client.get("/api/profile/conflicts?status=pending").json()["count"] == 1

    decisions = client.get("/api/profile/decisions?limit=5").json()
    assert decisions["count"] == 5

    page = client.get("/profile")
    assert page.status_code == 200
    assert "上海云图科技有限公司" in page.text
    assert "待核对" in page.text

    replay = client.post("/api/profile/import", json={"source": "all"})
    assert replay.json()["idempotent_replay"] is True


# ---------- 契约与裁决单元规则 ----------

def test_contract_rejects_bad_unified_id_and_phone():
    raw = {"customer_id": "C9", "source": "order",
           "event_time": "2026-11-15T08:00:00Z",
           "fields": {"name": "某企业", "unified_id": "91310115MA1K4A2B8!",
                      "phone": "abc", "contact": "", "address": ""},
           "verified": []}
    norm, errs = contract.validate(raw, file_source="order", row_ref="t:1", seq=1)
    assert norm is None
    assert {e["field"] for e in errs} == {"unified_id", "phone"}


def test_contract_sentinel_and_blank_become_none():
    raw = {"customer_id": "C9", "source": "settlement",
           "event_time": "2026-11-15T08:00:00Z",
           "fields": {"name": "未知", "unified_id": "-", "phone": "  ",
                      "contact": "李四", "address": ""},
           "verified": []}
    norm, errs = contract.validate(raw, file_source="settlement",
                                   row_ref="t:1", seq=1)
    assert not errs
    assert norm.fields["name"] is None
    assert norm.fields["unified_id"] is None
    assert norm.fields["phone"] is None
    assert norm.fields["contact"] == "李四"


def test_contract_source_must_match_file():
    norm, errs = contract.validate(ORDER_ROW, file_source="settlement",
                                   row_ref="t:1", seq=1)
    assert norm is None
    assert errs[0]["field"] == "source"


def test_confirm_upgrade_on_same_value():
    old = {"name": arbitrate.FieldState(value="上海云图科技有限公司",
                                        source="settlement", confirmed=False)}
    row = contract.NormRow(
        customer_id="C1", source="order",
        event_time=contract.parse_time("2026-11-15T08:00:00Z")[0],
        fields={"name": "上海云图科技有限公司", "unified_id": None,
                "phone": None, "contact": None, "address": None},
        verified=frozenset({"name"}), row_ref="t:1", seq=1)
    decision = arbitrate.arbitrate(old, row)
    name_outcome = [o for o in decision.outcomes if o.field == "name"][0]
    assert name_outcome.action == "confirm_upgrade"
    assert decision.verdict == "adopt"
    new_state = arbitrate.apply_decision(old, row, decision)
    assert new_state["name"].confirmed is True
    assert new_state["name"].source == "settlement"  # 值的来源不变，核验状态升级
