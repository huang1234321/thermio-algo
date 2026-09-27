"""阈值 YAML：合并优先级 / mtime 热调 / 坏 YAML 容错（algo.md §7.4）。"""

from __future__ import annotations

import os
import time
from pathlib import Path

from algo.fdd.rules import RULES
from algo.fdd.thresholds import CODE_DEFAULTS, ThresholdStore

RULE_BY_ID = {r.rule_id: r for r in RULES}


def write(tmp_path: Path, text: str) -> Path:
    f = tmp_path / "thresholds.yaml"
    atomic_write(f, text)
    return f


def atomic_write(f: Path, text: str) -> None:
    """§7.4 写入纪律：临时文件 + 原子 rename（防读到半写状态）。"""
    tmp = f.with_suffix(".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, f)


BASE = """
version: 1
defaults:
  min_good_ratio: 0.9
  confirm_windows: 2
  clear_windows: 4
  window_minutes: 30
rules:
  chiller.delta_t_low:
    severity: major
    window_minutes: 60
    params:
      delta_t_min_c: 2.0
"""


def test_effective_merging_priority(tmp_path: Path) -> None:
    store = ThresholdStore(write(tmp_path, BASE))
    rule = RULE_BY_ID["chiller.delta_t_low"]
    t = store.effective(rule)
    # 规则段 > defaults 段 > 代码默认
    assert t.severity == "major"  # rules 段覆盖
    assert t.window_minutes == 60  # rules 段覆盖
    assert t.min_good_ratio == 0.9  # defaults 段（规则段未覆盖）
    assert t.confirm_windows == 2  # defaults 段
    assert t.clear_windows == 4  # defaults 段
    assert t.params["delta_t_min_c"] == 2.0
    # 未配置的规则 → 全 defaults
    other = store.effective(RULE_BY_ID["cooling_tower.approach_high"])
    assert other.window_minutes == 30
    assert other.severity == RULE_BY_ID["cooling_tower.approach_high"].default_severity
    assert other.params == {}


def test_code_defaults_when_file_missing(tmp_path: Path) -> None:
    store = ThresholdStore(tmp_path / "absent.yaml")
    t = store.effective(RULE_BY_ID["chiller.delta_t_low"])
    assert t.min_good_ratio == CODE_DEFAULTS.min_good_ratio
    assert t.confirm_windows == CODE_DEFAULTS.confirm_windows


def test_hot_reload_applies_next_round(tmp_path: Path) -> None:
    f = write(tmp_path, BASE)
    store = ThresholdStore(f)
    before = store.effective(RULE_BY_ID["chiller.delta_t_low"]).params["delta_t_min_c"]
    assert before == 2.0
    time.sleep(0.01)
    atomic_write(f, BASE.replace("delta_t_min_c: 2.0", "delta_t_min_c: 3.5"))
    store.maybe_reload()  # 每轮评估前调用
    after = store.effective(RULE_BY_ID["chiller.delta_t_low"]).params["delta_t_min_c"]
    assert after == 3.5


def test_hot_reload_invalid_yaml_keeps_previous(tmp_path: Path) -> None:
    """热调永不把服务打挂：坏 YAML = 沿用上一份有效配置。"""
    f = write(tmp_path, BASE)
    store = ThresholdStore(f)
    time.sleep(0.01)
    atomic_write(f, "version: 1\nrules: {!!!: [")  # 语法坏
    store.maybe_reload()
    t = store.effective(RULE_BY_ID["chiller.delta_t_low"])
    assert t.params["delta_t_min_c"] == 2.0  # 旧值仍生效

    time.sleep(0.01)
    atomic_write(
        f, "version: 1\nrules:\n  chiller.delta_t_low:\n    window_minutes: -5\n"
    )  # 校验坏
    store.maybe_reload()
    assert store.effective(RULE_BY_ID["chiller.delta_t_low"]).params["delta_t_min_c"] == 2.0


def test_canonical_payload_stable_and_sensitive(tmp_path: Path) -> None:
    f = write(tmp_path, BASE)
    s1 = ThresholdStore(f)
    c1 = s1.canonical_payload()
    assert s1.canonical_payload() == c1  # 幂等
    assert c1 == ThresholdStore(f).canonical_payload()  # 同文同码
    time.sleep(0.01)
    atomic_write(f, BASE.replace("confirm_windows: 2", "confirm_windows: 3"))
    s2 = ThresholdStore(f)
    assert s2.canonical_payload() != c1  # 阈值变化 → 指纹面变化（§9）
