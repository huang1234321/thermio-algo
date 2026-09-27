"""algo_version 机制（algo.md §9，ADR-008「所有输出带 algo_version」）。

按能力独立版本：fdd = <engine_semver>+<rule_pack_fp8>（指纹纳入生效阈值——
阈值热调改变判定结果，归因面必须覆盖）；optimizer/forecast = 裸 semver。
"""

from __future__ import annotations

import hashlib
import inspect
from pathlib import Path

from algo.fdd.base import Rule
from algo.fdd.thresholds import ThresholdStore

FDD_ENGINE_SEMVER = "0.1.0"
OPTIMIZER_SEMVER = "0.1.0"
FORECAST_SEMVER = "0.1.0"


def rule_pack_fingerprint(rules: tuple[Rule, ...], thresholds: ThresholdStore) -> str:
    """sha256(规则清单 + 各规则源码摘要 + 生效阈值全文) 前 8 位（§9）。

    - 规则清单与源码：新增/修改规则 = 新指纹（发版动作 §7.7）；
    - 生效阈值全文：YAML 热调 → 下一轮输出即带新版本（热调可见性闭环 §7.4）。
    """
    parts: list[str] = [f"rules:{len(rules)}"]
    for rule in sorted(rules, key=lambda r: r.rule_id):
        src_file = inspect.getsourcefile(type(rule))
        assert src_file is not None  # 规则类必来自源文件（发版件，非动态执行）
        src = Path(src_file)
        digest = hashlib.sha256(src.read_bytes()).hexdigest()[:16]
        parts.append(f"{rule.rule_id}@{digest}:{type(rule).__name__}")
    parts.append(f"thresholds:{thresholds.canonical_payload()}")
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()[:8]


def fdd_algo_version(rules: tuple[Rule, ...], thresholds: ThresholdStore) -> str:
    """FDD 能力版本，落 fdd_finding.algo_version / fdd_report.algo_version / 日志绑定字段。"""
    return f"{FDD_ENGINE_SEMVER}+{rule_pack_fingerprint(rules, thresholds)}"


def optimizer_algo_version() -> str:
    """optimizer 能力裸 semver（DAT-126 引用；无规则包指纹）。"""
    return OPTIMIZER_SEMVER
