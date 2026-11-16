"""SQLite 存储：批次、输入文件、事件、快照、水位序列、阶段、合计、运行清单。

约定：

- 金额列一律 TEXT（Decimal 字符串），不落 float；主键 pk 一律 TEXT，前导零与
  超过 2^53 的整数都按原样保存；
- 一次运行的所有行在同一个事务里「先删后插」，失败整体回滚，不留半批数据；
- 页面永远只展示一个批次，所有查询都带 batch_id。
"""
from __future__ import annotations

import datetime as dt
import json
import sqlite3
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path


def dt_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    batch_id TEXT PRIMARY KEY,
    batch_key TEXT NOT NULL,
    run_id TEXT NOT NULL,
    source TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    finished_at TEXT,
    input_digest TEXT NOT NULL,
    watermark_utc TEXT,
    max_event_time TEXT,
    params_json TEXT NOT NULL DEFAULT '{}',
    note TEXT NOT NULL DEFAULT '',
    replay_of TEXT
);
CREATE INDEX IF NOT EXISTS idx_batches_key ON batches(batch_key, status);
CREATE TABLE IF NOT EXISTS inputs (
    batch_id TEXT NOT NULL, seq INTEGER NOT NULL, name TEXT NOT NULL,
    bytes INTEGER NOT NULL, sha256 TEXT NOT NULL, declared_rows INTEGER,
    encoding TEXT, declared TEXT, kind TEXT, status TEXT NOT NULL, code TEXT,
    note TEXT, lines INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS events (
    batch_id TEXT NOT NULL, row_ref TEXT NOT NULL, seq INTEGER NOT NULL,
    table_name TEXT NOT NULL, pk TEXT NOT NULL, version INTEGER,
    op TEXT, event_time_utc TEXT, event_time_raw TEXT, schema_version TEXT,
    payload_json TEXT NOT NULL DEFAULT '{}', amount TEXT, qty INTEGER,
    row_class TEXT NOT NULL, reason_code TEXT, field TEXT, raw_value TEXT,
    detail TEXT, late INTEGER NOT NULL DEFAULT 0, flags_json TEXT NOT NULL DEFAULT '[]',
    raw_text TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_events_class ON events(batch_id, row_class);
CREATE TABLE IF NOT EXISTS snapshots (
    batch_id TEXT NOT NULL, table_name TEXT NOT NULL, pk TEXT NOT NULL,
    version INTEGER NOT NULL, op TEXT NOT NULL, amount TEXT, qty INTEGER,
    event_time_utc TEXT NOT NULL, row_ref TEXT NOT NULL,
    late INTEGER NOT NULL DEFAULT 0, pending INTEGER NOT NULL DEFAULT 0,
    payload_json TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS watermarks (
    batch_id TEXT NOT NULL, seq INTEGER NOT NULL, row_ref TEXT NOT NULL,
    event_time_utc TEXT NOT NULL, watermark_utc TEXT NOT NULL,
    late INTEGER NOT NULL DEFAULT 0, behind_seconds INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS stages (
    batch_id TEXT NOT NULL, stage TEXT NOT NULL, in_count INTEGER NOT NULL,
    out_count INTEGER NOT NULL, quarantine_count INTEGER NOT NULL,
    status TEXT NOT NULL, elapsed_ms INTEGER NOT NULL, note TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS totals (
    batch_id TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS manifests (
    batch_id TEXT PRIMARY KEY, json TEXT NOT NULL
);
-- M8 客户档案裁决：跨批次持久状态（档案是唯一对外版本，不按批次隔离）
CREATE TABLE IF NOT EXISTS profile_batches (
    batch_id TEXT PRIMARY KEY,
    batch_key TEXT NOT NULL,
    run_id TEXT NOT NULL,
    source TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    finished_at TEXT,
    input_digest TEXT NOT NULL,
    counters_json TEXT NOT NULL DEFAULT '{}',
    note TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_profile_batches_key
    ON profile_batches(batch_key, status);
CREATE TABLE IF NOT EXISTS profile_fields (
    customer_id TEXT NOT NULL,
    field TEXT NOT NULL,
    value TEXT,
    source TEXT,
    confirmed INTEGER NOT NULL DEFAULT 0,
    updated_event_time TEXT,
    updated_batch_id TEXT,
    PRIMARY KEY (customer_id, field)
);
CREATE TABLE IF NOT EXISTS profile_decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    source TEXT NOT NULL,
    event_time TEXT,
    row_ref TEXT NOT NULL DEFAULT '',
    row_sig TEXT NOT NULL,
    verdict TEXT NOT NULL,
    reason_code TEXT,
    detail_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_profile_decisions_batch
    ON profile_decisions(batch_id);
CREATE TABLE IF NOT EXISTS profile_conflicts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id TEXT NOT NULL,
    field TEXT NOT NULL,
    old_value TEXT,
    old_source TEXT,
    new_value TEXT,
    new_source TEXT,
    resolution TEXT NOT NULL DEFAULT 'kept',
    status TEXT NOT NULL DEFAULT 'pending',
    note TEXT NOT NULL DEFAULT '',
    first_batch_id TEXT NOT NULL,
    last_batch_id TEXT NOT NULL,
    times INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(customer_id, field, old_value, new_value)
);
CREATE TABLE IF NOT EXISTS profile_seen (
    row_sig TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    source TEXT NOT NULL,
    first_batch_id TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""


class Store:
    def __init__(self, path: str):
        self.path = str(path)
        Path(self.path).resolve().parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            conn.executescript(SCHEMA)
            self._ensure_columns(conn)

    @staticmethod
    def _ensure_columns(conn) -> None:
        """轻量迁移：老库缺列时补上，避免升级后 500。"""
        wanted = {"inputs": {"declared": "TEXT"}}
        for table, columns in wanted.items():
            existing = {row["name"] for row in conn.execute(
                f"PRAGMA table_info({table})").fetchall()}
            for column, ddl in columns.items():
                if column not in existing:
                    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(self.path, timeout=15.0)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ---------- 批次 ----------
    def get_batch(self, batch_id: str) -> dict | None:
        rows = self._all("SELECT * FROM batches WHERE batch_id=?", (batch_id,))
        return rows[0] if rows else None

    def find_done_by_key(self, batch_key: str) -> dict | None:
        rows = self._all(
            "SELECT * FROM batches WHERE batch_key=? AND status='done' "
            "ORDER BY created_at LIMIT 1", (batch_key,))
        return rows[0] if rows else None

    def latest_batch(self, status: str = "done") -> dict | None:
        rows = self._all(
            "SELECT * FROM batches WHERE status=? ORDER BY created_at DESC, rowid DESC LIMIT 1",
            (status,))
        if not rows:
            rows = self._all("SELECT * FROM batches ORDER BY created_at DESC, rowid DESC LIMIT 1",
                             ())
        return rows[0] if rows else None

    def list_batches(self, limit: int = 30) -> list[dict]:
        return self._all(
            "SELECT * FROM batches ORDER BY created_at DESC, rowid DESC LIMIT ?", (limit,))

    def watermark_high(self) -> str | None:
        rows = self._all(
            "SELECT watermark_utc FROM batches WHERE status='done' "
            "AND watermark_utc IS NOT NULL ORDER BY watermark_utc DESC LIMIT 1", ())
        return rows[0]["watermark_utc"] if rows else None

    # ---------- 一次运行落库（单事务替换） ----------
    def save_run(self, *, batch: dict, inputs: list[dict], events: list[dict],
                 snapshots: list[dict], watermarks: list[dict], stages: list[dict],
                 totals: dict, manifest: dict) -> None:
        with self.connect() as conn:
            for table in ("inputs", "events", "snapshots", "watermarks", "stages", "totals"):
                conn.execute(f"DELETE FROM {table} WHERE batch_id=?", (batch["batch_id"],))
            for i, item in enumerate(inputs, 1):
                conn.execute(
                    """INSERT INTO inputs (batch_id,seq,name,bytes,sha256,declared_rows,
                        encoding,declared,kind,status,code,note,lines)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (batch["batch_id"], i, item["name"], item["bytes"], item["sha256"],
                     item.get("declared_rows"), item.get("encoding"),
                     item.get("declared"), item.get("kind"), item["status"],
                     item.get("code"), item.get("note", ""), item.get("lines", 0)))
            for row in events:
                conn.execute(
                    """INSERT INTO events (batch_id,row_ref,seq,table_name,pk,version,op,
                        event_time_utc,event_time_raw,schema_version,payload_json,amount,qty,
                        row_class,reason_code,field,raw_value,detail,late,flags_json,raw_text)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (batch["batch_id"], row["row_ref"], row["seq"], row["table"], row["pk"],
                     row.get("version"), row.get("op"), row.get("event_time"),
                     row.get("event_time_raw"), row.get("schema"),
                     json.dumps(row.get("payload", {}), ensure_ascii=False,
                                default=str),
                     None if row.get("amount") is None else str(row["amount"]),
                     row.get("qty"), row["row_class"], row.get("reason_code"),
                     row.get("field"), row.get("raw_value"), row.get("detail"),
                     1 if row.get("late") else 0,
                     json.dumps(row.get("flags", []), ensure_ascii=False),
                     row.get("raw_text", "")))
            for s in snapshots:
                conn.execute(
                    """INSERT INTO snapshots (batch_id,table_name,pk,version,op,amount,qty,
                        event_time_utc,row_ref,late,pending,payload_json)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (batch["batch_id"], s["table"], s["pk"], s["version"], s["op"],
                     None if s.get("amount") is None else str(s["amount"]), s.get("qty"),
                     s["event_time"], s["row_ref"], 1 if s.get("late") else 0,
                     1 if s.get("pending") else 0,
                     json.dumps(s.get("payload", {}), ensure_ascii=False, default=str)))
            for w in watermarks:
                conn.execute(
                    """INSERT INTO watermarks (batch_id,seq,row_ref,event_time_utc,
                        watermark_utc,late,behind_seconds) VALUES (?,?,?,?,?,?,?)""",
                    (batch["batch_id"], w["seq"], w["row_ref"], w["event_time"],
                     w["watermark"], 1 if w.get("late") else 0, w.get("behind_seconds", 0)))
            for s in stages:
                conn.execute(
                    """INSERT INTO stages (batch_id,stage,in_count,out_count,
                        quarantine_count,status,elapsed_ms,note) VALUES (?,?,?,?,?,?,?,?)""",
                    (batch["batch_id"], s["stage"], s["in_count"], s["out_count"],
                     s["quarantine_count"], s["status"], s["elapsed_ms"], s.get("note", "")))
            for key, value in totals.items():
                conn.execute("INSERT INTO totals (batch_id,key,value) VALUES (?,?,?)",
                             (batch["batch_id"], key, str(value)))
            conn.execute(
                "INSERT INTO manifests (batch_id,json) VALUES (?,?) "
                "ON CONFLICT(batch_id) DO UPDATE SET json=excluded.json",
                (batch["batch_id"], json.dumps(manifest, ensure_ascii=False, default=str)))
            conn.execute(
                """INSERT INTO batches (batch_id,batch_key,run_id,source,status,created_at,
                        finished_at,input_digest,watermark_utc,max_event_time,params_json,
                        note,replay_of)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(batch_id) DO UPDATE SET
                     status=excluded.status, finished_at=excluded.finished_at,
                     note=excluded.note, input_digest=excluded.input_digest,
                     watermark_utc=excluded.watermark_utc,
                     max_event_time=excluded.max_event_time""",
                (batch["batch_id"], batch["batch_key"], batch["run_id"], batch["source"],
                 batch["status"], batch["created_at"], batch.get("finished_at"),
                 batch["input_digest"], batch.get("watermark_utc"),
                 batch.get("max_event_time"),
                 json.dumps(batch.get("params", {}), ensure_ascii=False),
                 batch.get("note", ""), batch.get("replay_of")))


    # ---------- 快照（可重复物化） ----------
    def replace_snapshot(self, batch_id: str, rows: list[dict], totals: dict) -> None:
        """按同一 as_of 重算快照：先删后插，不新增事件行。"""
        with self.connect() as conn:
            conn.execute("DELETE FROM snapshots WHERE batch_id=?", (batch_id,))
            for s in rows:
                conn.execute(
                    """INSERT INTO snapshots (batch_id,table_name,pk,version,op,amount,qty,
                        event_time_utc,row_ref,late,pending,payload_json)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (batch_id, s["table"], s["pk"], s["version"], s["op"],
                     None if s.get("amount") is None else str(s["amount"]), s.get("qty"),
                     s["event_time"], s["row_ref"], 1 if s.get("late") else 0,
                     1 if s.get("pending") else 0,
                     json.dumps(s.get("payload", {}), ensure_ascii=False, default=str)))
            for key, value in totals.items():
                conn.execute("DELETE FROM totals WHERE batch_id=? AND key=?",
                             (batch_id, key))
                conn.execute("INSERT INTO totals (batch_id,key,value) VALUES (?,?,?)",
                             (batch_id, key, str(value)))

    def save_attempt(self, *, batch: dict, inputs: list[dict], note: str = "") -> None:
        """失败尝试：只写批次与输入，不动任何已存在批次的结果行。"""
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO batches (batch_id,batch_key,run_id,source,status,created_at,
                        finished_at,input_digest,watermark_utc,max_event_time,params_json,
                        note,replay_of)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(batch_id) DO UPDATE SET
                     status=excluded.status, finished_at=excluded.finished_at,
                     note=excluded.note""",
                (batch["batch_id"], batch["batch_key"], batch["run_id"], batch["source"],
                 batch["status"], batch["created_at"], batch.get("finished_at"),
                 batch["input_digest"], batch.get("watermark_utc"),
                 batch.get("max_event_time"),
                 json.dumps(batch.get("params", {}), ensure_ascii=False),
                 note or batch.get("note", ""), batch.get("replay_of")))
            conn.execute("DELETE FROM inputs WHERE batch_id=?", (batch["batch_id"],))
            for i, item in enumerate(inputs, 1):
                conn.execute(
                    """INSERT INTO inputs (batch_id,seq,name,bytes,sha256,declared_rows,
                        encoding,declared,kind,status,code,note,lines)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (batch["batch_id"], i, item["name"], item["bytes"], item["sha256"],
                     item.get("declared_rows"), item.get("encoding"),
                     item.get("declared"), item.get("kind"), item["status"],
                     item.get("code"), item.get("note", ""), item.get("lines", 0)))

    # ---------- 读取 ----------
    def load_inputs(self, batch_id: str) -> list[dict]:
        return self._all("SELECT * FROM inputs WHERE batch_id=? ORDER BY seq", (batch_id,))

    def load_events(self, batch_id: str, row_class: str | None = None) -> list[dict]:
        if row_class:
            rows = self._all(
                "SELECT * FROM events WHERE batch_id=? AND row_class=? ORDER BY seq",
                (batch_id, row_class))
        else:
            rows = self._all("SELECT * FROM events WHERE batch_id=? ORDER BY seq", (batch_id,))
        for r in rows:
            r["payload"] = json.loads(r.pop("payload_json") or "{}")
            r["flags"] = json.loads(r.pop("flags_json") or "[]")
            r["amount"] = None if r["amount"] is None else Decimal(r["amount"])
            r["late"] = bool(r["late"])
        return rows

    def load_snapshot(self, batch_id: str) -> list[dict]:
        rows = self._all(
            "SELECT * FROM snapshots WHERE batch_id=? ORDER BY table_name, pk", (batch_id,))
        for r in rows:
            r["payload"] = json.loads(r.pop("payload_json") or "{}")
            r["amount"] = None if r["amount"] is None else Decimal(r["amount"])
            r["late"] = bool(r["late"])
            r["pending"] = bool(r["pending"])
        return rows

    def load_watermarks(self, batch_id: str) -> list[dict]:
        return self._all(
            "SELECT * FROM watermarks WHERE batch_id=? ORDER BY seq", (batch_id,))

    def load_stages(self, batch_id: str) -> list[dict]:
        from . import config
        rows = self._all("SELECT * FROM stages WHERE batch_id=? ORDER BY rowid", (batch_id,))
        for row in rows:
            row["label"] = config.STAGE_LABELS.get(row["stage"], row["stage"])
        return rows

    def load_totals(self, batch_id: str) -> dict:
        rows = self._all("SELECT key, value FROM totals WHERE batch_id=?", (batch_id,))
        return {r["key"]: r["value"] for r in rows}

    def load_manifest(self, batch_id: str) -> dict | None:
        rows = self._all("SELECT json FROM manifests WHERE batch_id=?", (batch_id,))
        return json.loads(rows[0]["json"]) if rows else None

    def count_rows(self, batch_id: str) -> dict:
        out = {}
        for table in ("inputs", "events", "snapshots", "watermarks", "stages"):
            out[table] = self._all(
                f"SELECT COUNT(*) AS n FROM {table} WHERE batch_id=?", (batch_id,))[0]["n"]
        return out

    # ---------- M8 客户档案裁决 ----------
    def find_profile_batch_by_key(self, batch_key: str) -> dict | None:
        rows = self._all(
            "SELECT * FROM profile_batches WHERE batch_key=? AND status='done' "
            "ORDER BY created_at LIMIT 1", (batch_key,))
        return rows[0] if rows else None

    def latest_profile_batch(self) -> dict | None:
        rows = self._all(
            "SELECT * FROM profile_batches WHERE status='done' "
            "ORDER BY created_at DESC, rowid DESC LIMIT 1", ())
        return rows[0] if rows else None

    def list_profile_batches(self, limit: int = 20) -> list[dict]:
        return self._all(
            "SELECT * FROM profile_batches ORDER BY created_at DESC, rowid DESC LIMIT ?",
            (limit,))

    def load_profile_seen(self) -> set[str]:
        with self.connect() as conn:
            rows = conn.execute("SELECT row_sig FROM profile_seen").fetchall()
        return {r["row_sig"] for r in rows}

    def load_profile_fields(self) -> dict:
        """按客户分组的当前档案：{customer_id: {field: 字段状态}}。"""
        rows = self._all(
            "SELECT * FROM profile_fields ORDER BY customer_id, field", ())
        out: dict[str, dict[str, dict]] = {}
        for row in rows:
            out.setdefault(row["customer_id"], {})[row["field"]] = {
                "value": row["value"],
                "source": row["source"],
                "confirmed": bool(row["confirmed"]),
                "updated_event_time": row["updated_event_time"],
                "updated_batch_id": row["updated_batch_id"],
            }
        return out

    def load_profile_decisions(self, limit: int = 200,
                               batch_id: str | None = None) -> list[dict]:
        if batch_id:
            rows = self._all(
                "SELECT * FROM profile_decisions WHERE batch_id=? "
                "ORDER BY id DESC LIMIT ?", (batch_id, limit))
        else:
            rows = self._all(
                "SELECT * FROM profile_decisions ORDER BY id DESC LIMIT ?", (limit,))
        for row in rows:
            row["detail"] = json.loads(row.pop("detail_json") or "{}")
        return rows

    def load_profile_conflicts(self, status: str | None = None) -> list[dict]:
        if status:
            return self._all(
                "SELECT * FROM profile_conflicts WHERE status=? "
                "ORDER BY updated_at DESC, id DESC", (status,))
        return self._all(
            "SELECT * FROM profile_conflicts ORDER BY status, updated_at DESC, id DESC",
            ())

    def save_profile_run(self, *, batch: dict, decisions: list[dict],
                         fields: list[dict], conflicts: list[dict],
                         seen: list[dict]) -> None:
        """一次裁决运行落库：批次 + 裁决记录 + 档案字段 + 冲突 + 已见签名，单事务。

        任何一步失败整体回滚，档案保持旧版（失败后保留旧版）。
        """
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO profile_batches (batch_id,batch_key,run_id,source,status,
                        created_at,finished_at,input_digest,counters_json,note)
                   VALUES (?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(batch_id) DO UPDATE SET
                     status=excluded.status, finished_at=excluded.finished_at,
                     counters_json=excluded.counters_json, note=excluded.note""",
                (batch["batch_id"], batch["batch_key"], batch["run_id"],
                 batch["source"], batch["status"], batch["created_at"],
                 batch.get("finished_at"), batch["input_digest"],
                 json.dumps(batch.get("counters", {}), ensure_ascii=False),
                 batch.get("note", "")))
            for row in decisions:
                conn.execute(
                    """INSERT INTO profile_decisions (batch_id,run_id,customer_id,source,
                            event_time,row_ref,row_sig,verdict,reason_code,detail_json,
                            created_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (batch["batch_id"], batch["run_id"], row["customer_id"],
                     row["source"], row.get("event_time"), row.get("row_ref", ""),
                     row["row_sig"], row["verdict"], row.get("reason_code"),
                     json.dumps(row.get("detail", {}), ensure_ascii=False),
                     batch["created_at"]))
            for row in fields:
                conn.execute(
                    """INSERT INTO profile_fields (customer_id,field,value,source,
                            confirmed,updated_event_time,updated_batch_id)
                       VALUES (?,?,?,?,?,?,?)
                       ON CONFLICT(customer_id, field) DO UPDATE SET
                         value=excluded.value, source=excluded.source,
                         confirmed=excluded.confirmed,
                         updated_event_time=excluded.updated_event_time,
                         updated_batch_id=excluded.updated_batch_id""",
                    (row["customer_id"], row["field"], row.get("value"),
                     row.get("source"), 1 if row.get("confirmed") else 0,
                     row.get("updated_event_time"), batch["batch_id"]))
            for row in conflicts:
                conn.execute(
                    """INSERT INTO profile_conflicts (customer_id,field,old_value,
                            old_source,new_value,new_source,resolution,status,note,
                            first_batch_id,last_batch_id,times,created_at,updated_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(customer_id, field, old_value, new_value)
                       DO UPDATE SET last_batch_id=excluded.last_batch_id,
                         times=profile_conflicts.times+1,
                         updated_at=excluded.updated_at""",
                    (row["customer_id"], row["field"], row.get("old_value"),
                     row.get("old_source"), row.get("new_value"),
                     row.get("new_source"), row.get("resolution", "kept"),
                     "pending", "", batch["batch_id"], batch["batch_id"], 1,
                     batch["created_at"], batch["created_at"]))
            for row in seen:
                conn.execute(
                    """INSERT INTO profile_seen (row_sig,customer_id,source,
                            first_batch_id,created_at)
                       VALUES (?,?,?,?,?)
                       ON CONFLICT(row_sig) DO NOTHING""",
                    (row["row_sig"], row["customer_id"], row["source"],
                     batch["batch_id"], batch["created_at"]))

    def update_profile_batch_counters(self, batch_id: str, counters: dict) -> None:
        """事务落库后回写批次计数（如待核对总数需按库内实际状态补齐）。"""
        with self.connect() as conn:
            conn.execute(
                "UPDATE profile_batches SET counters_json=? WHERE batch_id=?",
                (json.dumps(counters, ensure_ascii=False), batch_id))

    def ack_profile_conflict(self, conflict_id: int, note: str = "") -> dict | None:
        """标记冲突已核对；不存在的 id 返回 None。只改状态，不改档案值。"""
        with self.connect() as conn:
            found = conn.execute(
                "SELECT id FROM profile_conflicts WHERE id=?", (conflict_id,)).fetchone()
            if found is None:
                return None
            conn.execute(
                "UPDATE profile_conflicts SET status='acknowledged', note=?, "
                "updated_at=? WHERE id=?",
                (note, dt_now_iso(), conflict_id))
        rows = self._all("SELECT * FROM profile_conflicts WHERE id=?", (conflict_id,))
        return rows[0] if rows else None

    def count_profile_rows(self) -> dict:
        out = {}
        for table in ("profile_batches", "profile_fields", "profile_decisions",
                      "profile_conflicts", "profile_seen"):
            out[table] = self._all(f"SELECT COUNT(*) AS n FROM {table}", ())[0]["n"]
        return out

    def _all(self, sql: str, args: tuple) -> list[dict]:
        with self.connect() as conn:
            return [dict(r) for r in conn.execute(sql, args).fetchall()]
