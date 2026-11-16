"""M8 入口：档案采信、对外档案、裁决记录、待核对冲突。"""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request

from ... import config, errors
from ...web import Ctx, get_ctx, render
from . import service

router = APIRouter(tags=["M8 客户档案裁决"])


@router.post("/api/profile/import")
def api_profile_import(payload: dict | None = None,
                       ctx: Ctx = Depends(get_ctx)) -> dict:
    source = (payload or {}).get("source", "all")
    result = service.run(settings=ctx.settings, store=ctx.store, source=source)
    return {
        "status": result["status"],
        "batch_id": result["batch"]["batch_id"],
        "batch_key": result["batch_key"],
        "run_id": result["run_id"],
        "idempotent_replay": result["idempotent_replay"],
        "counters": result["counters"],
        "note": result.get("note", ""),
    }


@router.get("/api/profile")
def api_profile(ctx: Ctx = Depends(get_ctx)) -> dict:
    return service.profile_payload(ctx.store)


@router.get("/api/profile/decisions")
def api_profile_decisions(limit: int = 100, batch_id: str | None = None,
                          ctx: Ctx = Depends(get_ctx)) -> dict:
    return service.decisions_payload(ctx.store, limit=max(1, min(limit, 1000)),
                                     batch_id=batch_id)


@router.get("/api/profile/conflicts")
def api_profile_conflicts(status: str | None = None,
                          ctx: Ctx = Depends(get_ctx)) -> dict:
    if status and status not in config.PROFILE_CONFLICT_STATUS:
        raise errors.BadRequest(f"未知冲突状态 {status!r}", code="E_BAD_STATUS")
    return service.conflicts_payload(ctx.store, status)


@router.post("/api/profile/conflicts/{conflict_id}/ack")
def api_profile_conflict_ack(conflict_id: int, payload: dict | None = None,
                             ctx: Ctx = Depends(get_ctx)) -> dict:
    note = str((payload or {}).get("note", ""))[:200]
    row = ctx.store.ack_profile_conflict(conflict_id, note)
    if row is None:
        raise errors.NotFound(f"冲突 {conflict_id} 不存在", code="E_NOT_FOUND")
    return {"status": "acknowledged", "conflict": row}


@router.get("/profile")
def page_profile(request: Request, ctx: Ctx = Depends(get_ctx)):
    payload = service.profile_payload(ctx.store)
    conflicts = service.conflicts_payload(ctx.store)
    decisions = service.decisions_payload(ctx.store, limit=50)
    batches = service.batches_payload(ctx.store)
    return render(request, "profile.html", page="profile",
                  customers=payload["customers"], summary=payload["summary"],
                  conflicts=conflicts["conflicts"],
                  decisions=decisions["decisions"],
                  profile_batches=batches["batches"],
                  profile_fields=config.PROFILE_FIELDS,
                  field_order=config.PROFILE_FIELD_ORDER,
                  verdict_labels=config.PROFILE_VERDICT_LABELS,
                  conflict_status_labels=config.PROFILE_CONFLICT_STATUS_LABELS,
                  source_kind_labels=config.PROFILE_SOURCE_KIND_LABELS,
                  profile_source_labels=config.PROFILE_SOURCE_LABELS,
                  profile_source_keys=("all", "order", "settlement"),
                  page_title="客户档案")
