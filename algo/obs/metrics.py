"""prometheus-client 指标（algo.md §10 全表；OBS-MT-01/04：RED 基线 + 命名域/单位）。

全部指标模块级注册（import 即定义），main.py 起 /metrics 端口由 deploy.md 监控栈刮取。
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

_ns = "algo"  # 命名域前缀（algo.md §10 表内名逐字对齐）

# RED 基线 + 任务面（OBS-MT-01）
JOB_DURATION_MS = Histogram(
    f"{_ns}_job_duration_ms",
    "每轮调度任务耗时（毫秒）",
    ["job"],
    buckets=(10, 50, 100, 250, 500, 1_000, 2_500, 5_000, 15_000, 60_000),
)
JOB_RUNS_TOTAL = Counter(
    f"{_ns}_job_runs_total",
    "任务成败计数（insufficient 单列：数据不足非错误）",
    ["job", "result"],  # result ∈ ok | error | skipped_insufficient
)

# FDD 规则面
FDD_RULES_EVALUATED_TOTAL = Counter(f"{_ns}_fdd_rules_evaluated_total", "FDD 规则评估吞吐", [])
FDD_FINDINGS_TOTAL = Counter(
    f"{_ns}_fdd_findings_total", "FDD 发现产出（hit=新命中/续报，cleared=消除）", ["action"]
)

# proposal 面（提交去向；采纳率由 api 侧统计，不归 algo）
PROPOSAL_SUBMISSIONS_TOTAL = Counter(
    f"{_ns}_proposal_submissions_total",
    "proposal 提交去向",
    ["result"],  # result ∈ created | rejected | dropped | expired
)

# 优化器面（optimizer.md §9；与 algo_proposal_submissions_total 两层不重复计数：
# 本表计「本轮仲裁结果」，提交结果计上方既有指标）
OPTIMIZER_PROPOSALS_TOTAL = Counter(
    f"{_ns}_optimizer_proposals_total",
    "优化器候选去向（仲裁层）",
    ["strategy", "result"],  # result ∈ emitted | deduped | throttled | selfcheck_failed
)
OPTIMIZER_FORECAST_SOURCE = Gauge(
    f"{_ns}_optimizer_forecast_source",
    "负荷预测降级链当前生效源（1=当前源）",
    ["source"],  # source ∈ model | persistence | none
)
OPTIMIZER_STRATEGY_APPLICABLE = Gauge(
    f"{_ns}_optimizer_strategy_applicable",
    "策略适用性判定结果（0=量类型缺失/禁用——点表配齐度验收抓手）",
    ["strategy", "system_id"],
)
OPTIMIZER_SAVING_ESTIMATED_KW = Histogram(
    f"{_ns}_optimizer_saving_estimated_kw",
    "节能量估算分布（回测对照的线上影子）",
    ["strategy"],
    buckets=(1, 2, 5, 10, 20, 50, 100, 200, 500, 1000),
)

# 数据入口健康面
KAFKA_CONSUMER_LAG = Gauge(
    f"{_ns}_kafka_consumer_lag", "消费 lag（ADR-017 监控项）", ["group", "topic"]
)
TSDB_QUERY_LATENCY_MS = Histogram(
    f"{_ns}_tsdb_query_latency_ms",
    "TSDB 读延迟",
    [],
    buckets=(1, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500),
)
SNAPSHOT_AGE_S = Gauge(f"{_ns}_snapshot_age_s", "资产快照陈旧度（刷新失败可见，algo.md §6.3）", [])

# 天气与学习闭环
WEATHER_FETCH_TOTAL = Counter(f"{_ns}_weather_fetch_total", "天气拉取成败", ["result"])
ATTRIBUTION_EVENTS_TOTAL = Counter(
    f"{_ns}_attribution_events_total", "thermio.control.executed 事件消费量（§4.4）", []
)

# 内部通道凭证健康
PLATFORM_AUTH_FAILURES_TOTAL = Counter(
    f"{_ns}_platform_auth_failures_total", "internal 401/403（凭证问题告警源，§8.1）", []
)
