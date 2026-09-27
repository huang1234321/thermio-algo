"""AssetSnapshot / EquipmentView / PointView wire 模型（algo.md §6.2）。

字段来源 = PG equipment/point 列投影；extra="ignore"（字段只增不删，API-CT-02）。
设备类型/量类型枚举源头 = shared-types（platform.md §6.2/§6.3）——消费侧按字面量
比较，未知值由 registry 的 UNKNOWN 兜底纪律处理（WARN + 跳过，不崩不静默吞）。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict

# ── 量类型（本仓消费面）─────────────────────────────────────────────────────
# 前 3 值 = shared-types QUANTITY_TYPES 现行全集（platform.md §6.2，commit 8b8297f）。
# 其余为 FDD 规则库所需扩充值（algo.md §7.7/§15 开放项）：扩充 = shared-types 发版动作，
# TS 侧发版前本仓按下列字面量消费（DB 侧 text + 应用层校验，不阻塞链路）。
QUANTITY_TYPES: frozenset[str] = frozenset(
    {
        "chw_supply_temp",  # 冷冻水供水温度
        "power",  # 功率
        "run_status",  # 运行状态（枚态量）
        "chw_return_temp",  # 冷冻水回水温度（扩充待发版）
        "cooling_water_supply_temp",  # 冷却水供水温度（扩充待发版）
        "cooling_water_return_temp",  # 冷却水回水温度（扩充待发版）
    }
)

# 设备类型 = shared-types EQUIPMENT_TYPES 现行全集（platform.md §6.2）。
EQUIPMENT_TYPES: frozenset[str] = frozenset(
    {
        "chiller",
        "chwp_pump",
        "cwp_pump",
        "cooling_tower",
        "ahu",
        "valve",
        "sensor",
        "energy_meter",
    }
)


class PointView(BaseModel):
    """不可变投影（frozen=hashable：registry 的参与点位集为 frozenset，§7.2）。"""

    model_config = ConfigDict(extra="ignore", frozen=True)

    point_id: int
    equipment_id: str | None = None
    quantity_type: str
    unit_std: str | None = None
    valid_range_min: float | None = None
    valid_range_max: float | None = None
    is_controllable: bool = False
    clamp_min: float | None = None
    clamp_max: float | None = None
    control_mode: str = "advisory"


class EquipmentView(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)

    equipment_id: str
    equipment_type: str
    system_id: str | None = None
    building_id: str | None = None
    tenant_id: str | None = None  # 随快照带出（§6.3）：提交侧无需自带租户
    local_id: str | None = None
    name: str | None = None  # 快照投影前瞻字段（报告健康度排名用，缺失时回落 local_id）
    rated_params: dict[str, Any] = {}


class AssetSnapshot(BaseModel):
    model_config = ConfigDict(extra="ignore")

    generated_at: datetime
    equipments: list[EquipmentView] = []
    points: list[PointView] = []


def group_points(snapshot: AssetSnapshot) -> dict[str, list[PointView]]:
    """equipment_id → 挂接点位（无设备归属的点不参与规则路由，§6.1）。"""
    out: dict[str, list[PointView]] = {}
    for p in snapshot.points:
        if p.equipment_id is not None:
            out.setdefault(p.equipment_id, []).append(p)
    return out
