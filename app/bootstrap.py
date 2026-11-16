"""启动引导：库里还没有完成批次时，用自带样例跑一次主流程。

这样 `docker compose up` 之后页面直接有水位刻度、事件流、冲突与快照可看；
重复启动命中 batch_key，走幂等重放，不新增行。
"""
from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger("cdcwater.bootstrap")


def needs_seed(store) -> bool:
    try:
        return store.latest_batch("done") is None
    except Exception:  # noqa: BLE001 - 库还没建表时按需要初始化处理
        return True


def sample_ready(settings) -> bool:
    base = Path(settings.sample_dir)
    if not base.is_dir():
        return False
    return any((base / name).exists() for name in
               ("events_main.jsonl", "events_v1_gb.csv", "events_late.jsonl"))


def ensure_seed(app) -> dict | None:
    settings = app.state.settings
    store = app.state.store
    if not sample_ready(settings):
        logger.info("样例目录为空，跳过首次采信：%s", settings.sample_dir)
        return None
    result = None
    if needs_seed(store):
        from .modules.m6_batches import service as batch_service

        try:
            result = batch_service.run(settings=settings, store=store, source="all")
        except Exception as exc:  # noqa: BLE001 - 首次采信失败不应阻断服务启动
            logger.warning("首次采信失败：%s", exc)
            result = None
        else:
            logger.info("首次采信完成：batch=%s applied=%s",
                        result["batch"]["batch_id"], result["counters"]["applied"])
    _ensure_profile_seed(settings, store)
    return result


def _ensure_profile_seed(settings, store) -> None:
    """档案模块同样首启播种：没有已完成档案批次时跑一次双来源裁决。"""
    try:
        if store.latest_profile_batch() is not None:
            return
        from .modules.m8_profile import service as profile_service

        result = profile_service.run(settings=settings, store=store, source="all")
        logger.info("首次档案裁决完成：batch=%s adopted=%s",
                    result["batch"]["batch_id"], result["counters"]["adopted"])
    except Exception as exc:  # noqa: BLE001 - 档案播种失败不应阻断服务启动
        logger.warning("首次档案裁决失败：%s", exc)
