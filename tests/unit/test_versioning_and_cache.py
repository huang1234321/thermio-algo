"""algo_version 指纹机制 + 最新值缓存去重（algo.md §9 / §4.2-4.3）。"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from algo.fdd.rules import RULES
from algo.fdd.thresholds import ThresholdStore
from algo.kafka.consumer import LatestCache
from algo.kafka.topics import TelemetryRow
from algo.versioning import FDD_ENGINE_SEMVER, fdd_algo_version, optimizer_algo_version


def row(point_id: int, ts: datetime, value: float | None = 1.0) -> TelemetryRow:
    return TelemetryRow(
        point_id=point_id,
        gateway_id="GW-1",
        tenant_id="t-1",
        ts=ts,
        value=value,
        value_text=None if value is not None else "running",
    )


T0 = datetime(2026, 9, 27, 12, 0, tzinfo=UTC)


def test_version_format_and_stability(tmp_path) -> None:  # type: ignore[no-untyped-def]
    f = tmp_path / "th.yaml"
    f.write_text("version: 1\nrules: {}\n", encoding="utf-8")
    store = ThresholdStore(f)
    v1 = fdd_algo_version(RULES, store)
    assert v1.startswith(f"{FDD_ENGINE_SEMVER}+")
    assert len(v1.split("+")[1]) == 8  # fp8
    assert fdd_algo_version(RULES, store) == v1  # 输入不变 → 指纹稳定


def test_version_changes_with_thresholds(tmp_path) -> None:  # type: ignore[no-untyped-def]
    f = tmp_path / "th.yaml"
    f.write_text("version: 1\nrules: {}\n", encoding="utf-8")
    store = ThresholdStore(f)
    v1 = fdd_algo_version(RULES, store)
    import os
    import time

    time.sleep(0.01)
    tmp = f.with_suffix(".tmp")
    tmp.write_text("version: 1\ndefaults: {min_good_ratio: 0.7}\nrules: {}\n", encoding="utf-8")
    os.replace(tmp, f)
    store.maybe_reload()
    v2 = fdd_algo_version(RULES, store)
    assert v2 != v1  # 阈值热调改变判定结果 → 新版本（归因面覆盖热调，§9）


def test_version_changes_with_rule_list(tmp_path) -> None:  # type: ignore[no-untyped-def]
    f = tmp_path / "th.yaml"
    f.write_text("version: 1\nrules: {}\n", encoding="utf-8")
    store = ThresholdStore(f)
    full = fdd_algo_version(RULES, store)
    subset = fdd_algo_version(RULES[:3], store)
    assert full != subset  # 规则清单 = 发版动作


def test_optimizer_version_bare_semver() -> None:
    assert optimizer_algo_version() == "0.1.0"  # 无规则包指纹（§9 能力独立）


def test_latest_cache_dedup_and_freshness() -> None:
    c = LatestCache(dedup_capacity=4)
    assert c.offer(row(1, T0, 10.0))
    assert not c.offer(row(1, T0, 11.0))  # (point_id, ts) 重复：忽略（at-least-once 幂等）
    r1 = c.get(1)
    assert r1 is not None and r1.value == 10.0
    assert c.offer(row(1, T0 + timedelta(seconds=60), 12.0))  # 新 ts 更新
    r1b = c.get(1)
    assert r1b is not None and r1b.value == 12.0
    # 质量位打标不丢弃（M&V 红线：数据永不丢，§4.3）
    assert c.offer(row(2, T0, None))  # value_text 路径
    r2 = c.get(2)
    assert r2 is not None and r2.value_text == "running"
