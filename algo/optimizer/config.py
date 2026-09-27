"""optimizer.yaml 加载 + mtime 热调（optimizer.md §10/§11；复用 fdd/thresholds.py 机制）。

合并序：defaults ← strategies.<id> ← buildings.<b>.strategies.<id>（后胜前，逐字段）。
热调纪律与 thresholds 一致：mtime/size 变更才重读；解析失败沿用上一份有效配置
+ ERROR（热调永不把服务打挂）；生效即 INFO。
归因：热调不进 algo_version——生效配置段指纹 sha256 前 8 位进 evidence.cfg_fp8（§2）。
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from algo.obs.logging import get_logger

log = get_logger(__name__)


class _DefaultsSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    min_expected_saving_kw: float = 5.0  # 噪声下限（R3 豁免）
    proposal_ttl_min: int = Field(default=45, ge=5, le=1440)  # = 冷却窗（3 槽位）
    deadband_c: float = Field(default=0.5, ge=0.0)  # 温度类值死区
    systems: list[str] = ["chilled_water"]  # system_type 扫描范围
    min_good_ratio: float = Field(default=0.8, ge=0.0, le=1.0)  # 窗口质量门控（algo.md §5.2）
    window_min: int = Field(default=60, ge=15, le=240)  # 装配取窗宽度（覆盖最宽 sustain）
    cop_default: float = Field(default=5.0, gt=0.0)  # proxy 负荷的额定 COP 兜底
    idle_power_ratio: float = Field(default=0.3, gt=0.0, le=1.0)  # §8.2 额定两点线性化的怠机功率比


class _StrategySection(BaseModel):
    """策略参数段（三层合并的任一层；全字段可空 = 该层不覆盖）。extra=forbid 防键名漂移。"""

    model_config = ConfigDict(extra="forbid")

    enabled: bool | None = None
    step_c: float | None = Field(default=None, gt=0.0)
    max_total_reset_c: float | None = Field(default=None, gt=0.0)
    delta_t_low_c: float | None = None
    load_ratio_max: float | None = None
    sustain_min: int | None = Field(default=None, ge=5, le=240)
    sensitivity_pct_per_c: float | None = Field(default=None, gt=0.0)
    confidence_cap: float | None = Field(default=None, gt=0.0, le=1.0)
    stage_down_ratio: float | None = None
    stage_headroom: float | None = None
    stage_up_ratio: float | None = None
    exempt_min_saving: bool | None = None
    approach_max_c: float | None = None
    fan_headroom_pct: float | None = None
    fan_penalty_pct_per_c: float | None = None
    cw_sensitivity_pct_per_c: float | None = Field(default=None, gt=0.0)


class _BuildingSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    strategies: dict[str, _StrategySection] = Field(default_factory=dict)
    chillers: dict[str, dict[str, float]] = Field(default_factory=dict)  # local_id → rated 兜底


class OptimizerConfig(BaseModel):
    """optimizer.yaml 全文模型（结构全量示例见 config/optimizer.yaml）。"""

    model_config = ConfigDict(extra="forbid")

    version: int = 1
    defaults: _DefaultsSection = Field(default_factory=_DefaultsSection)
    strategies: dict[str, _StrategySection] = Field(default_factory=dict)
    buildings: dict[str, _BuildingSection] = Field(default_factory=dict)


@dataclass(frozen=True)
class EffectiveConfig:
    """某策略在某楼的生效视图（三层合并完成；evidence.cfg_fp8 的输入）。"""

    strategy_id: str
    building_id: str | None
    enabled: bool = True
    params: dict[str, float | int | bool | str] = field(default_factory=dict)

    def p(self, key: str, default: float) -> float:
        v = self.params.get(key, default)
        return float(v)

    def canonical_payload(self) -> str:
        data = {
            "strategy_id": self.strategy_id,
            "building_id": self.building_id,
            "enabled": self.enabled,
            "params": self.params,
        }
        return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def cfg_fp8(cfg: EffectiveConfig) -> str:
    """生效配置段 sha256 前 8 位（§8.4：热调参数的归因指纹）。"""
    return hashlib.sha256(cfg.canonical_payload().encode()).hexdigest()[:8]


@dataclass
class _FileState:
    mtime_ns: int
    size: int


# 各策略的代码默认参数（优先级最低；§6 表与 §11 YAML 对齐）
STRATEGY_DEFAULTS: dict[str, dict[str, float | int | bool]] = {
    "chw.temp_reset": {
        "enabled": True,
        "step_c": 0.5,
        "max_total_reset_c": 1.5,
        "delta_t_low_c": 2.0,
        "load_ratio_max": 0.55,
        "sustain_min": 30,
        "sensitivity_pct_per_c": 0.02,
        "confidence_cap": 0.85,
    },
    "chiller.stage_down": {
        "enabled": True,
        "stage_down_ratio": 0.45,
        "stage_headroom": 0.85,
        "sustain_min": 30,
        "confidence_cap": 0.80,
    },
    "chiller.stage_up": {
        "enabled": True,
        "stage_up_ratio": 0.92,
        "sustain_min": 20,
        "confidence_cap": 0.75,
        "exempt_min_saving": True,
    },
    "cw.temp_reset": {
        "enabled": True,
        "step_c": 1.0,
        "approach_max_c": 5.0,
        "fan_headroom_pct": 0.80,
        "fan_penalty_pct_per_c": 0.50,
        "cw_sensitivity_pct_per_c": 0.01,
        "confidence_cap": 0.80,
    },
}


class OptimizerConfigStore:
    """单文件配置仓库：maybe_reload() 每轮寻优前调用；effective() 纯查询。"""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._config = OptimizerConfig()
        self._state: _FileState | None = None
        self._load(first=True)

    @property
    def defaults(self) -> _DefaultsSection:
        return self._config.defaults

    def chiller_rated_fallback(self, building_id: str | None, local_id: str) -> dict[str, float]:
        """buildings.<b>.chillers.<local_id> 段（rated_params 缺键兜底，§6.6-2）。"""
        if building_id is None:
            return {}
        sec = self._config.buildings.get(building_id)
        if sec is None:
            return {}
        rated = sec.chillers.get(local_id, {})
        return {k: v for k, v in rated.items() if math.isfinite(v)}

    def effective(self, strategy_id: str, building_id: str | None) -> EffectiveConfig:
        """代码默认 ← strategies.<id> ← buildings.<b>.strategies.<id>（逐字段后胜前）。"""
        merged: dict[str, float | int | bool | str] = dict(STRATEGY_DEFAULTS.get(strategy_id, {}))
        layers: list[_StrategySection] = []
        if (sec := self._config.strategies.get(strategy_id)) is not None:
            layers.append(sec)
        if building_id is not None:
            b = self._config.buildings.get(building_id)
            if b is not None and (sec := b.strategies.get(strategy_id)) is not None:
                layers.append(sec)
        for layer in layers:
            merged.update({k: v for k, v in layer.model_dump().items() if v is not None})
        enabled = bool(merged.pop("enabled", True))
        # NaN/Inf 防御（thresholds.py 同款）：非有限浮点视为配置噪声剔除
        merged = {
            k: v for k, v in merged.items() if not (isinstance(v, float) and not math.isfinite(v))
        }
        return EffectiveConfig(
            strategy_id=strategy_id, building_id=building_id, enabled=enabled, params=merged
        )

    def building_ids(self) -> list[str]:
        return sorted(self._config.buildings)

    # ── 装载与热调（thresholds.py 同款纪律）─────────────────────────────────

    def _load(self, *, first: bool) -> None:
        if not self._path.exists():
            if first:
                log.warning("optimizer_yaml_missing_use_code_defaults", path=str(self._path))
            return  # 热调期文件消失 = 保持现值
        try:
            stat = self._path.stat()
            state = _FileState(mtime_ns=stat.st_mtime_ns, size=stat.st_size)
            if not first and state == self._state:
                return
            raw = self._path.read_text(encoding="utf-8")
            cfg = OptimizerConfig.model_validate(yaml.safe_load(raw) or {})
        except OSError:
            raise
        except (yaml.YAMLError, ValidationError, ValueError) as exc:
            if first:
                log.error(
                    "optimizer_yaml_invalid_at_startup_use_code_defaults",
                    path=str(self._path),
                    error=str(exc)[:300],
                )
                self._config = OptimizerConfig()
                self._state = None
                return
            log.error(
                "optimizer_yaml_reload_invalid_keep_previous",
                path=str(self._path),
                error=str(exc)[:300],
            )
            return
        self._config = cfg
        self._state = state
        log.info(
            "optimizer_yaml_loaded",
            path=str(self._path),
            strategies=len(cfg.strategies),
            buildings=len(cfg.buildings),
        )

    def maybe_reload(self) -> None:
        try:
            self._load(first=False)
        except OSError as exc:
            log.warning("optimizer_yaml_reload_io_error_keep_previous", error=str(exc)[:200])
