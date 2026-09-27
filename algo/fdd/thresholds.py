"""阈值 YAML 加载 + mtime 热调（algo.md §7.4，ADR-008）。

优先级：代码默认值 ← defaults 段 ← rules.<key> 段（后胜前）。
热调纪律：每轮 stat() mtime，变更则重读 + 校验（pydantic）；解析失败 = 沿用上一份
有效配置 + ERROR（**热调永不把服务打挂**）；生效即 INFO 并进 §9 规则包指纹。
删整个规则段 ≠ 停用规则（停用走 §7.7 清单管理，防误操作静默关规则）。
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from algo.fdd.base import Rule, ThresholdSet
from algo.obs.logging import get_logger

log = get_logger(__name__)

# 代码默认值（优先级最低；YAML defaults 段可整体覆盖）
CODE_DEFAULTS = ThresholdSet(
    severity="info",
    window_minutes=30,
    min_good_ratio=0.8,
    confirm_windows=3,
    clear_windows=6,
    params={},
)


class _RuleSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    severity: str | None = None
    window_minutes: int | None = Field(default=None, ge=1, le=1440)
    min_good_ratio: float | None = Field(default=None, ge=0.0, le=1.0)
    confirm_windows: int | None = Field(default=None, ge=1, le=100)
    clear_windows: int | None = Field(default=None, ge=1, le=100)
    params: dict[str, float | int | str] = Field(default_factory=dict)


class _DefaultsSection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    min_good_ratio: float = 0.8
    confirm_windows: int = 3
    clear_windows: int = 6
    window_minutes: int = 30


class ThresholdConfig(BaseModel):
    """thresholds.yaml 全文模型（结构全量示例见 config/thresholds.yaml）。"""

    model_config = ConfigDict(extra="forbid")

    version: int = 1
    defaults: _DefaultsSection = Field(default_factory=_DefaultsSection)
    rules: dict[str, _RuleSection] = Field(default_factory=dict)


@dataclass
class _FileState:
    mtime_ns: int
    size: int


class ThresholdStore:
    """单文件阈值仓库：maybe_reload() 每轮评估前调用；effective(rule) 纯查询。"""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._config = ThresholdConfig()
        self._state: _FileState | None = None
        self._load(first=True)

    # ── 装载与热调 ────────────────────────────────────────────────────────

    def _load(self, *, first: bool) -> None:
        if not self._path.exists():
            if first:
                log.warning("thresholds_file_missing_use_code_defaults", path=str(self._path))
            return  # 热调期文件消失 = 保持现值（等效冻结）
        try:
            stat = self._path.stat()
            state = _FileState(mtime_ns=stat.st_mtime_ns, size=stat.st_size)
            if not first and state == self._state:
                return  # 未变更
            raw = self._path.read_text(encoding="utf-8")
            cfg = ThresholdConfig.model_validate(yaml.safe_load(raw) or {})
        except OSError:
            raise
        except (yaml.YAMLError, ValidationError, ValueError) as exc:
            if first:
                log.error(
                    "thresholds_invalid_at_startup_use_code_defaults",
                    path=str(self._path),
                    error=str(exc)[:300],
                )
                self._config = ThresholdConfig()
                self._state = None
                return
            # 热调失败：沿用上一份有效配置（§7.4：热调永不把服务打挂）
            log.error(
                "thresholds_reload_invalid_keep_previous",
                path=str(self._path),
                error=str(exc)[:300],
            )
            return
        self._config = cfg
        self._state = state
        log.info("thresholds_loaded", path=str(self._path), rules=len(cfg.rules))

    def maybe_reload(self) -> None:
        """每轮评估前调用：mtime/size 变更才重读。"""
        try:
            self._load(first=False)
        except OSError as exc:
            log.warning("thresholds_reload_io_error_keep_previous", error=str(exc)[:200])

    # ── 查询 ─────────────────────────────────────────────────────────────

    def effective(self, rule: Rule) -> ThresholdSet:
        """代码默认 ← defaults ← rules.<rule_id>（全量解析为 ThresholdSet）。"""
        d = self._config.defaults
        sec = self._config.rules.get(rule.rule_id)
        params: dict[str, float | int | str] = dict(sec.params) if sec else {}
        # NaN/Inf 防御：阈值面出现非有限浮点视为配置噪声，剔除该键（回落规则代码默认）
        params = {
            k: v for k, v in params.items() if not (isinstance(v, float) and not math.isfinite(v))
        }
        return ThresholdSet(
            severity=(sec.severity if sec and sec.severity else rule.default_severity),
            window_minutes=(
                sec.window_minutes if sec and sec.window_minutes is not None else d.window_minutes
            ),
            min_good_ratio=(
                sec.min_good_ratio if sec and sec.min_good_ratio is not None else d.min_good_ratio
            ),
            confirm_windows=(
                sec.confirm_windows
                if sec and sec.confirm_windows is not None
                else d.confirm_windows
            ),
            clear_windows=(
                sec.clear_windows if sec and sec.clear_windows is not None else d.clear_windows
            ),
            params=params,
        )

    def canonical_payload(self) -> str:
        """生效配置规范化全文（§9 指纹输入：排序键、紧凑分隔、无空白噪声）。"""
        data = self._config.model_dump(mode="json")
        return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
