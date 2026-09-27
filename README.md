# thermio-algo

thermio 算法服务（Python，ADR-008/016）：FDD 规则引擎 + APScheduler 多调度任务 + proposal 生成器接口。

- 服务详设：伞仓 `docs/design/algo.md`（本仓结构 = 其 §2 目录蓝图逐件落地）
- 建仓卡片：`docs/design/implementation-plan.md` IMPL-16（DAT-154）
- 规范基线：`docs/conventions/company/`（PY-01..07、SEC-KEY、CODE-LOG/ST/TST、OBS-MT、FLOW-GIT）

## 边界纪律（结构性，非约定）

- **对 PG 零直连**（ADR-016 / DM §2）：`algo/config.py` 不存在任何 PG 配置项——业务数据
  （proposal、FDD 发现/报告）一律经中台 `POST /internal/*`；语义映射经
  `GET /internal/algo/asset-snapshot`。`tests/unit/test_config.py` 钉死该结构性质。
- TSDB 只读（`tsdb_algo` 角色）+ 天气域读写；启动时逐对象校验权限面（防配置漂移）。
- 算法只输出 proposal，经中台 control-safety 仲裁后下发（ADR-009）——algo 的职责在
  「提交 proposal、收到 201」处截断。
- 所有输出带 `algo_version`（ADR-008）：FDD 版本 = `<semver>+<rule_pack_fp8>`，
  指纹纳入生效阈值（热调归因闭环，algo.md §9）。

## 快速开始

```bash
uv sync                                    # 依赖锁定（uv.lock 入库，PY-03）
cp config/algo.example.env .env            # 占位值 → 按部署面注入（SEC-KEY-01/06）
uv run python -m algo.main                 # 宿主进程 + compose 中间件（deploy.md §1 形态）
uv run python -m algo.main --once fdd_eval # 单轮评估（运维/联调）
```

中间件一律用伞仓 `deploy/docker-compose.dev.yml` 独立栈（thermio- 前缀容器）；
**禁止复用宿主机或其他项目既有 PG/Kafka/EMQX/TimescaleDB**（环境隔离纪律）。

## 质量门禁

```bash
uv run ruff check . && uv run ruff format --check .   # lint + format（PY-07）
uv run mypy algo                                       # strict（PY-01）
uv run pytest                                          # unit + contract（CODE-TST 权威层）
scripts/it-fdd.sh                                      # integration：compose 回放链路
```

- **unit**：规则求值纯函数（正/负/边界矩阵）、迟滞、阈值热调容错、信封约束——零 mock。
- **contract**：proposal 信封字段/约束快照（对照 shared-types @ 8b8297f）、FDD wire 快照。
  **改信封先改 shared-types（发版），本仓快照随对照 PR 同步。**
- **integration**：gw-sim → EMQX → ingestd → Kafka/TSDB → fdd_eval → mock internal 断言
  findings upsert 载荷 + YAML 热调 → cleared（需独立栈，脚本一键编排）。

## 首批规则集（12 条，algo.md §7.7）

冷机 4（供回水温差偏低 / 供水超温 / 功率占比偏高 / 供回水传感器反接）、
水泵 5（chwp/cwp 功率双侧占比 + chwp 波动率）、冷却塔 3（逼近温度 / 出水超温 / 供回温差）。
阈值见 `config/thresholds.yaml`（改后下一评估周期生效，无需重启）。
新增规则 = 发版动作：`rules/*.py` 一个类 + `rules/__init__.py` 清单一行 + YAML 一段。

## 已知边界与跨仓协调项（IMPL-16 交付时点）

- `quantity_type` 扩充值（`chw_return_temp` / `cooling_water_supply_temp` /
  `cooling_water_return_temp`）待 shared-types 发版（platform.md §6.2 治理口）；
  发版前本仓按上述字面量消费（DB 侧 text，不阻塞链路）。
- 中台 `/internal/fdd/*`、`/internal/algo/asset-snapshot`、`GET /internal/fdd/findings`
  端点随 platform 侧落地（platform.md §11 表 + algo.md §15 跨仓协调项）——落地前
  integration 以 mock 替身验证 algo 侧契约（algo.md §14 既定方式）。
