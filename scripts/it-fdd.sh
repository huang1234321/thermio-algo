#!/usr/bin/env bash
# it-fdd.sh —— FDD 引擎集成自测编排（DAT-154 / IMPL-16，algo.md §14 integration 层）。
#
# 链路：gw-sim → EMQX → ingestd → Kafka/TSDB → algo fdd_eval →（mock internal）断言载荷。
#
# 环境隔离纪律：一律用伞仓 deploy/docker-compose.dev.yml 独立栈（thermio- 前缀容器，
# compose project thermio-dev），禁止复用宿主机或其他项目既有 PG/Kafka/EMQX/TimescaleDB。
# 宿主端口冲突用本项目端口映射覆盖（build/compose.env，只落本仓 build/）。
#
# 用法：scripts/it-fdd.sh
#   可覆盖：THERMIO_UMBRELLA / THERMIO_INGEST / THERMIO_PLATFORM（兄弟仓路径）
# 幂等：standing 栈可复用（角色/迁移/种子均幂等）；build/it.env 持久化口令。
set -euo pipefail
cd "$(dirname "$0")/.."
ALGO_REPO="$(pwd)"
BUILD="$ALGO_REPO/build"
mkdir -p "$BUILD"

log() { printf '\033[1;36m[it-fdd]\033[0m %s\n' "$*"; }
die() { printf '\033[1;31m[it-fdd FAIL]\033[0m %s\n' "$*" >&2; exit 1; }

# ── 0.5 真实 internal 面开关（DAT-163/IMPL-17 并入项）────────────────────────
# 平台侧 internal 三端点（asset-snapshot / fdd findings GET·POST / reports POST）
# 已落地（platform 迁移 0007/0008）：起真实 api 进程，test_internal_real.py 直连
# 断言（ALGO_IT_API_BASE_URL 门控）；不设 IT_REAL_API=1 时跳过本段——
# algo.md §14 mock 回放链路保持既定方式不受影响（mock 是默认，真实面是增量衔接）。
IT_REAL_API="${IT_REAL_API:-0}"
API_PORT="${IT_API_PORT:-18080}"
IT_SVC_TOKEN="${IT_SVC_TOKEN:-it-svc-token-$(openssl rand -hex 8)}"
API_PIDFILE="$BUILD/api.pid"
API_LOG="$BUILD/api.log"
api_running() {
  [[ -f "$API_PIDFILE" ]] && kill -0 "$(cat "$API_PIDFILE")" 2>/dev/null
}

# ── 0. 定位兄弟仓（向上找伞仓；本仓常检出在伞仓内或其旁）───────────────────
UMBRELLA="${THERMIO_UMBRELLA:-}"
if [[ -z "$UMBRELLA" ]]; then
  d="$ALGO_REPO"
  while [[ "$d" != "/" ]]; do
    if [[ -f "$d/deploy/docker-compose.dev.yml" && "$d" != "$ALGO_REPO" ]]; then
      UMBRELLA="$d"
      break
    fi
    d="$(dirname "$d")"
  done
  [[ -n "$UMBRELLA" ]] || die "未定位伞仓（deploy/docker-compose.dev.yml）；设 THERMIO_UMBRELLA 覆盖"
fi
INGEST="${THERMIO_INGEST:-$UMBRELLA/thermio-ingest}"
PLATFORM="${THERMIO_PLATFORM:-$UMBRELLA/thermio-platform}"
[[ -d "$INGEST" ]] || die "thermio-ingest 不存在: $INGEST"
[[ -d "$PLATFORM" ]] || die "thermio-platform 不存在: $PLATFORM"
DEPLOY="$UMBRELLA/deploy"

command -v docker >/dev/null || die "需要 docker"
command -v go >/dev/null || die "需要 go"
command -v nc >/dev/null || die "需要 nc"

# ── 1. 端口规划：已有容器沿用在用映射；新容器避让宿主占用 ──────────────────
ctr_mapped_port() { docker port "$1" "$2/tcp" 2>/dev/null | head -1 | sed 's/.*://'; }
port_free() { ! nc -z 127.0.0.1 "$1" 2>/dev/null; }
pick() {
  local p="$1"
  if port_free "$p"; then echo "$p"; else die "端口 $p 被占且无既定避让位"; fi
}

if [[ -n "$(ctr_mapped_port thermio-pg 5432)" ]]; then
  PG_PORT="$(ctr_mapped_port thermio-pg 5432)"
else
  PG_PORT="$(pick 5432)"
fi
if [[ -n "$(ctr_mapped_port thermio-tsdb 5432)" ]]; then
  TSDB_PORT="$(ctr_mapped_port thermio-tsdb 5432)"
else
  TSDB_PORT="$(pick 5434)" # 5433 常被宿主其他栈占用，默认避让
fi
if [[ -n "$(ctr_mapped_port thermio-emqx 1883)" ]]; then
  EMQX_PORT="$(ctr_mapped_port thermio-emqx 1883)"
else
  EMQX_PORT="$(pick 1883)"
fi
if [[ -n "$(ctr_mapped_port thermio-kafka 9092)" ]]; then
  KAFKA_PORT="$(ctr_mapped_port thermio-kafka 9092)"
else
  KAFKA_PORT=9092
fi
log "ports: pg=$PG_PORT tsdb=$TSDB_PORT emqx=$EMQX_PORT kafka=$KAFKA_PORT"

# ── 2. 起栈（thermio- 前缀独立栈；幂等）────────────────────────────────────
cat >"$BUILD/compose.env" <<EOF
PG_PORT=$PG_PORT
TSDB_PORT=$TSDB_PORT
EMQX_MQTT_PORT=$EMQX_PORT
EOF
( cd "$DEPLOY" && docker compose --env-file "$BUILD/compose.env" -f docker-compose.dev.yml up -d --wait )
log "栈就绪：$(docker ps --format '{{.Names}}' | grep '^thermio-' | grep -v dt118 | tr '\n' ' ')"

CTR_PG=thermio-pg
CTR_TSDB=thermio-tsdb

# ── 3. 口令（standing 栈复用 build/it.env；本机生成、600、不入库）──────────
IT_ENV="$BUILD/it.env"
role_exists() {
  docker exec "$CTR_PG" psql -U thermio -d thermio -tAc \
    "SELECT 1 FROM pg_roles WHERE rolname='$1'" | grep -q 1
}
if [[ -f "$IT_ENV" ]] && role_exists thermio_api; then
  log "复用既有角色/口令（standing 栈）"
  # shellcheck disable=SC1090
  source "$IT_ENV"
else
  THERMIO_API_PASSWORD="it_$(openssl rand -hex 8)"
  THERMIO_INGEST_PASSWORD="it_$(openssl rand -hex 8)"
  THERMIO_AUTH_PASSWORD="it_$(openssl rand -hex 8)"
  TSDB_INGEST_PASSWORD="it_$(openssl rand -hex 8)"
  TSDB_API_PASSWORD="it_$(openssl rand -hex 8)"
  TSDB_ALGO_PASSWORD="it_$(openssl rand -hex 8)"
  printf 'PG_PORT=%s\nTSDB_PORT=%s\nTHERMIO_API_PASSWORD=%s\nTHERMIO_INGEST_PASSWORD=%s\nTHERMIO_AUTH_PASSWORD=%s\nTSDB_INGEST_PASSWORD=%s\nTSDB_API_PASSWORD=%s\nTSDB_ALGO_PASSWORD=%s\n' \
    "$PG_PORT" "$TSDB_PORT" "$THERMIO_API_PASSWORD" "$THERMIO_INGEST_PASSWORD" \
    "$THERMIO_AUTH_PASSWORD" "$TSDB_INGEST_PASSWORD" "$TSDB_API_PASSWORD" \
    "$TSDB_ALGO_PASSWORD" >"$IT_ENV"
  chmod 600 "$IT_ENV"
fi

# ── 4. PG：角色 bootstrap（首次）+ 迁移 + 种子（幂等）──────────────────────
if ! role_exists thermio_api; then
  log "PG 角色 bootstrap"
  docker exec -i "$CTR_PG" psql -U thermio -d thermio -v ON_ERROR_STOP=1 \
    -v api_password="$THERMIO_API_PASSWORD" \
    -v ingest_password="$THERMIO_INGEST_PASSWORD" \
    -v auth_password="$THERMIO_AUTH_PASSWORD" \
    <"$PLATFORM/db/bootstrap/pg-roles.sql" >/dev/null
fi
log "PG 迁移 0001–0005（goose，幂等）"
( cd "$INGEST" && go run github.com/pressly/goose/v3/cmd/goose@v3.24.0 \
  -dir "$PLATFORM/db/migrations/pg" \
  postgres "postgres://thermio:thermio_dev_pg@127.0.0.1:$PG_PORT/thermio?sslmode=disable&options=-c role=thermio_owner" up >/dev/null )

# e2e-seed 幂等闸：point 无 (gateway, raw_name) 唯一约束，重复灌会翻倍
SIM_POINTS=$(docker exec "$CTR_PG" psql -U thermio -d thermio -tAc \
  "SELECT count(*) FROM point WHERE raw_name LIKE 'SIM%'")
if [[ "$SIM_POINTS" -eq 0 ]]; then
  log "种子 e2e-seed（首次；匿名 EMQX 下凭证列为装饰，哈希为占位值）"
  docker exec -i "$CTR_PG" psql -U thermio -d thermio -v ON_ERROR_STOP=1 \
    -v gwsim_hash='$argon2id$v=19$m=65536,t=3,p=4$aXQtcGxhY2Vob2xkZXI$URdel7XbMP5kYmZ0YmF0' \
    <"$DEPLOY/e2e/seed/e2e-seed.sql" >/dev/null
else
  log "种子已就位（SIM 点位 ${SIM_POINTS}）"
fi

log "FDD 夹具种子（幂等）"
docker exec -i "$CTR_PG" psql -U thermio -d thermio -v ON_ERROR_STOP=1 \
  <tests/integration/fixtures/algo-fdd-seed.sql | tail -3

# ── 5. TSDB：角色（首次）+ 迁移（幂等）─────────────────────────────────────
tsdb_role_exists() {
  docker exec "$CTR_TSDB" psql -U thermio_ts -d thermio_ts -tAc \
    "SELECT 1 FROM pg_roles WHERE rolname='tsdb_algo'" | grep -q 1
}
if ! tsdb_role_exists; then
  log "TSDB 角色 bootstrap"
  docker exec -i "$CTR_TSDB" psql -U thermio_ts -d thermio_ts -v ON_ERROR_STOP=1 \
    -v ingest_password="$TSDB_INGEST_PASSWORD" \
    -v api_password="$TSDB_API_PASSWORD" \
    -v algo_password="$TSDB_ALGO_PASSWORD" <<'SQL' >/dev/null
CREATE ROLE tsdb_ingest LOGIN PASSWORD :'ingest_password';
CREATE ROLE tsdb_api    LOGIN PASSWORD :'api_password';
CREATE ROLE tsdb_algo   LOGIN PASSWORD :'algo_password';
GRANT CONNECT ON DATABASE thermio_ts TO tsdb_ingest, tsdb_api, tsdb_algo;
SQL
fi
log "TSDB 迁移 0001–0004（goose，幂等）"
( cd "$INGEST" && go run github.com/pressly/goose/v3/cmd/goose@v3.24.0 \
  -dir db/migrations/tsdb \
  postgres "postgres://thermio_ts:thermio_dev_ts@127.0.0.1:$TSDB_PORT/thermio_ts?sslmode=disable" up >/dev/null )

# ── 6. 构建产物（ingestd / gw-sim）─────────────────────────────────────────
log "构建 ingestd / gw-sim"
( cd "$INGEST" && CGO_ENABLED=0 go build -o "$BUILD/ingestd" ./cmd/ingestd \
  && CGO_ENABLED=0 go build -o "$BUILD/gw-sim" ./tools/gw-sim )

# ── 7. ingestd 宿主进程（已在跑则复用）─────────────────────────────────────
INGESTD_PIDFILE="$BUILD/ingestd.pid"
ingestd_running() {
  [[ -f "$INGESTD_PIDFILE" ]] && kill -0 "$(cat "$INGESTD_PIDFILE")" 2>/dev/null
}
if ! ingestd_running; then
  log "启动 ingestd"
  MQTT_BROKER_URL="tcp://127.0.0.1:$EMQX_PORT" \
    MQTT_USERNAME="svc-ingest-it" MQTT_PASSWORD="it-dev-anonymous" \
    KAFKA_BROKERS="127.0.0.1:$KAFKA_PORT" \
    PG_DSN="postgres://thermio_ingest:$THERMIO_INGEST_PASSWORD@127.0.0.1:$PG_PORT/thermio?sslmode=disable" \
    TSDB_DSN="postgres://tsdb_ingest:$TSDB_INGEST_PASSWORD@127.0.0.1:$TSDB_PORT/thermio_ts?sslmode=disable" \
    "$BUILD/ingestd" >"$BUILD/ingestd.log" 2>&1 &
  echo $! >"$INGESTD_PIDFILE"
  sleep 2
  kill -0 "$(cat "$INGESTD_PIDFILE")" || {
    tail -20 "$BUILD/ingestd.log"
    die "ingestd 启动失败"
  }
  log "ingestd pid=$(cat "$INGESTD_PIDFILE")（日志 build/ingestd.log）"
else
  log "ingestd 已在跑（pid $(cat "$INGESTD_PIDFILE")）"
fi

# ── 7.5 真实 api 进程（internal 面衔接自测用；IT_REAL_API=1 启用）────────────
if [[ "$IT_REAL_API" == "1" ]]; then
  command -v pnpm >/dev/null || die "IT_REAL_API=1 需要 pnpm"
  if ! api_running; then
    log "构建并启动 thermio-api（internal 面：SVC_TOKEN_ALGO + PG 双池）"
    ( cd "$PLATFORM" && pnpm install --prefer-offline >/dev/null \
      && pnpm --filter @thermio/shared-types build >/dev/null \
      && pnpm --filter api build >/dev/null ) || die "api 构建失败"
    SVC_TOKEN_ALGO="$IT_SVC_TOKEN" \
    PG_API_URL="postgres://thermio_api:$THERMIO_API_PASSWORD@127.0.0.1:$PG_PORT/thermio?sslmode=disable" \
    PG_AUTH_URL="postgres://thermio_auth:$THERMIO_AUTH_PASSWORD@127.0.0.1:$PG_PORT/thermio?sslmode=disable" \
    PORT="$API_PORT" \
    AUTH_JWT_SECRET="it-api-jwt-secret-0123456789abcdef-0123456789abcdef" \
    PROPOSAL_MOCK_EXECUTOR=off \
    node "$PLATFORM/apps/api/dist/main.js" >"$API_LOG" 2>&1 &
    echo $! >"$API_PIDFILE"
    for _ in $(seq 1 30); do
      curl -sf "http://127.0.0.1:$API_PORT/healthz" >/dev/null 2>&1 && break
      sleep 1
    done
    api_running || { tail -20 "$API_LOG"; die "api 启动失败"; }
    log "api pid=$(cat "$API_PIDFILE")（日志 build/api.log，端口 $API_PORT）"
  else
    log "api 已在跑（pid $(cat "$API_PIDFILE")）"
  fi
  export ALGO_IT_API_BASE_URL="http://127.0.0.1:$API_PORT"
fi

# ── 8. 跑 integration 用例 ─────────────────────────────────────────────────
log "pytest -m integration"
set +e
ALGO_IT=1 \
  ${ALGO_IT_API_BASE_URL:+ALGO_IT_API_BASE_URL="$ALGO_IT_API_BASE_URL"} \
  ALGO_IT_TSDB_DSN="postgres://tsdb_algo:$TSDB_ALGO_PASSWORD@127.0.0.1:$TSDB_PORT/thermio_ts?sslmode=disable" \
  ALGO_IT_TSDB_ADMIN_DSN="postgres://thermio_ts:thermio_dev_ts@127.0.0.1:$TSDB_PORT/thermio_ts?sslmode=disable" \
  ALGO_IT_PG_DSN="postgres://thermio:thermio_dev_pg@127.0.0.1:$PG_PORT/thermio?sslmode=disable" \
  ALGO_IT_KAFKA_BROKERS="127.0.0.1:$KAFKA_PORT" \
  ALGO_IT_MQTT_URL="tcp://127.0.0.1:$EMQX_PORT" \
  ALGO_IT_MQTT_PASSWORD="it-dev-anonymous" \
  ALGO_IT_GWSIM="$BUILD/gw-sim" \
  ALGO_IT_SVC_TOKEN="$IT_SVC_TOKEN" \
  uv run pytest -m integration -v 2>&1 | tee "$BUILD/it-report.txt"
RC=${PIPESTATUS[0]}
set -e
log "报告：$BUILD/it-report.txt"
if [[ $RC -eq 0 ]]; then
  log "integration 全绿"
else
  die "integration 失败（详见 $BUILD/it-report.txt）"
fi
