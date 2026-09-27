"""optimizer.yaml 配置面单测：合并序 / 热调容错 / cfg_fp8（optimizer.md §10/§11）。"""

from __future__ import annotations

from pathlib import Path

import yaml
from algo.optimizer.config import STRATEGY_DEFAULTS, OptimizerConfigStore, cfg_fp8

YAML_FULL = """
version: 1
defaults:
  min_expected_saving_kw: 3.0
  proposal_ttl_min: 30
  deadband_c: 0.4
strategies:
  chw.temp_reset:
    enabled: true
    step_c: 0.3
buildings:
  b-1:
    strategies:
      chw.temp_reset:
        delta_t_low_c: 2.2
    chillers:
      "1#冷机":
        rated_cooling_capacity_kw: 2110
        rated_input_power_kw: 420
        min_unload_ratio: 0.25
"""


def write(tmp_path: Path, name: str, text: str) -> Path:
    p = tmp_path / name
    p.write_text(text, encoding="utf-8")
    return p


def test_merge_order_code_defaults_yaml_building(tmp_path: Path) -> None:
    store = OptimizerConfigStore(write(tmp_path, "o.yaml", YAML_FULL))
    eff = store.effective("chw.temp_reset", "b-1")
    assert eff.p("step_c", 9.9) == 0.3  # strategies 段覆盖代码默认
    assert eff.p("delta_t_low_c", 9.9) == 2.2  # buildings 段后胜前
    assert eff.p("load_ratio_max", 9.9) == 0.55  # 未覆盖 → 代码默认
    assert store.defaults.min_expected_saving_kw == 3.0
    assert store.defaults.proposal_ttl_min == 30


def test_building_absent_uses_strategy_layer(tmp_path: Path) -> None:
    store = OptimizerConfigStore(write(tmp_path, "o.yaml", YAML_FULL))
    eff = store.effective("chw.temp_reset", None)
    assert eff.p("delta_t_low_c", 9.9) == 2.0  # 代码默认
    eff_other = store.effective("chw.temp_reset", "b-2")  # 未配置的楼
    assert eff_other.p("delta_t_low_c", 9.9) == 2.0


def test_chiller_rated_fallback(tmp_path: Path) -> None:
    store = OptimizerConfigStore(write(tmp_path, "o.yaml", YAML_FULL))
    fb = store.chiller_rated_fallback("b-1", "1#冷机")
    assert fb["rated_cooling_capacity_kw"] == 2110
    assert store.chiller_rated_fallback("b-1", "9#冷机") == {}
    assert store.chiller_rated_fallback(None, "1#冷机") == {}


def test_bad_yaml_at_startup_falls_to_code_defaults(tmp_path: Path) -> None:
    store = OptimizerConfigStore(
        write(tmp_path, "bad.yaml", "defaults: {min_expected_saving_kw: [}}")
    )
    assert store.defaults.min_expected_saving_kw == 5.0  # 代码默认兜底


def test_hot_reload_invalid_keeps_previous(tmp_path: Path) -> None:
    p = write(tmp_path, "o.yaml", YAML_FULL)
    store = OptimizerConfigStore(p)
    assert store.effective("chw.temp_reset", None).p("step_c", 9.9) == 0.3
    p.write_text("strategies: {chw.temp_reset: {step_c: 'oops-not-float'}}", encoding="utf-8")
    store.maybe_reload()
    assert store.effective("chw.temp_reset", None).p("step_c", 9.9) == 0.3  # 沿用上一份有效值


def test_hot_reload_valid_applies_next_round(tmp_path: Path) -> None:
    import os
    import time

    p = write(tmp_path, "o.yaml", YAML_FULL)
    store = OptimizerConfigStore(p)
    # 触碰 mtime（内容变更）
    time.sleep(0.02)
    data = yaml.safe_load(YAML_FULL)
    data["strategies"]["chw.temp_reset"]["step_c"] = 0.7
    p.write_text(yaml.safe_dump(data), encoding="utf-8")
    os.utime(p)
    store.maybe_reload()
    assert store.effective("chw.temp_reset", None).p("step_c", 9.9) == 0.7


def test_missing_file_uses_code_defaults(tmp_path: Path) -> None:
    store = OptimizerConfigStore(tmp_path / "nonexistent.yaml")
    for sid, params in STRATEGY_DEFAULTS.items():
        eff = store.effective(sid, None)
        assert eff.enabled  # enabled 是独立字段（非 params 键）
        for k, v in params.items():
            if k == "enabled":
                continue
            got = eff.params.get(k)
            assert got == v, (sid, k, got, v)


def test_unknown_strategy_key_rejected(tmp_path: Path) -> None:
    store = OptimizerConfigStore(
        write(tmp_path, "o.yaml", "strategies: {chw.temp_reset: {bogus_key: 1}}")
    )
    # 坏段整体拒收 → 代码默认（extra=forbid 命中 ValidationError → startup 回落）
    assert store.effective("chw.temp_reset", None).p("step_c", 9.9) == 0.5


def test_cfg_fp8_stable_and_sensitive(tmp_path: Path) -> None:
    store = OptimizerConfigStore(write(tmp_path, "o.yaml", YAML_FULL))
    eff1 = store.effective("chw.temp_reset", "b-1")
    eff2 = store.effective("chw.temp_reset", "b-1")
    assert cfg_fp8(eff1) == cfg_fp8(eff2)  # 确定性
    assert len(cfg_fp8(eff1)) == 8
    assert cfg_fp8(eff1) != cfg_fp8(store.effective("chw.temp_reset", None))  # 楼段差异可见
    assert cfg_fp8(eff1) != cfg_fp8(store.effective("cw.temp_reset", "b-1"))  # 策略差异可见
