"""冷热源 advisory 寻优（optimizer.md v1.0 / DAT-126 详设，IMPL-19 实现）。

铁律继承（algo.md §1）：算法只输出 proposal、止于收到 201；PG 零直连；
TSDB 只读 + Kafka 消费；所有输出带 algo_version。MVP advisory-only——
策略只对快照中 control_mode='advisory' 的点位产提案（§0 前置闸）。

版本：能力 optimizer 起版 0.1.0 裸 semver（algo.md §16）；热调参数不进
版本字符串，归因缺口由 evidence.cfg_fp8 补齐（§2/§8.4）。
"""

from __future__ import annotations

from algo.versioning import OPTIMIZER_SEMVER, optimizer_algo_version

__all__ = ["OPTIMIZER_SEMVER", "optimizer_algo_version"]
