#!/usr/bin/env bash
# 端到端冒烟脚本：起整套栈 → 提交 Flink Job → 触发数据 → 验证目标库 + 看板 API → 清理
# 退出码：0 = 通过，非 0 = 任意一步失败
#
# 用法：
#   bash scripts/smoke.sh             # 默认行为
#   bash scripts/smoke.sh --keep      # 不停容器，方便事后排查
set -euo pipefail

KEEP=false
if [[ "${1:-}" == "--keep" ]]; then KEEP=true; fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE="docker compose -f ${ROOT}/docker/docker-compose.yml"

step() { printf "\n\033[1;36m[smoke] %s\033[0m\n" "$*"; }
ok()   { printf "  \033[32m✓\033[0m %s\n" "$*"; }
fail() { printf "  \033[31m✗\033[0m %s\n" "$*"; exit 1; }

cleanup() {
    if [[ "${KEEP}" == "false" ]]; then
        step "清理"
        $COMPOSE down -v 2>&1 | tail -3 || true
    else
        printf "\n[smoke] --keep: 容器保留，停掉用 \`docker compose -f docker/docker-compose.yml down -v\`\n"
    fi
}
trap cleanup EXIT

# ------------------------------------------------------------------
step "1) 检查依赖产物"
JAR=${ROOT}/flink-jobs/target/gis-sync-app-1.0-SNAPSHOT.jar
BACKEND_JAR=${ROOT}/backend/target/gis-sync-backend-1.0-SNAPSHOT.jar
[[ -f $JAR ]] || fail "缺 ${JAR}，先 make build-flink"
[[ -f $BACKEND_JAR ]] || fail "缺 ${BACKEND_JAR}，先 make build-backend"
ok "Flink jar + backend jar 都在"

# ------------------------------------------------------------------
step "2) 起栈（包括 backend，但不含 prod profile 的 frontend）"
$COMPOSE down -v 2>&1 | tail -2 || true
$COMPOSE up -d 2>&1 | tail -3
ok "compose up 完成"

step "3) 等待 backend healthy"
for i in {1..30}; do
    s=$(curl -fsS http://localhost:8090/actuator/health 2>/dev/null \
        | python3 -c "import json,sys; print(json.load(sys.stdin).get('status','?'))" 2>/dev/null || echo "?")
    if [[ "$s" == "UP" ]]; then ok "backend UP (try $i)"; break; fi
    sleep 4
    if [[ $i == 30 ]]; then fail "backend 30 次都没 UP"; fi
done

# ------------------------------------------------------------------
step "4) 提交 Source + Sink Job"
docker exec gis-flink-jobmanager flink run -d -c com.example.gis.source.SourceCdcToKafkaJob \
    /opt/flink/usrlib/gis-sync-app-1.0-SNAPSHOT.jar 2>&1 | grep -E "JobID" | tail -1
sleep 3
docker exec gis-flink-jobmanager flink run -d -c com.example.gis.sink.KafkaToSinkJob \
    /opt/flink/usrlib/gis-sync-app-1.0-SNAPSHOT.jar 2>&1 | grep -E "JobID" | tail -1
ok "两个 Job 已提交"

step "5) 等 Flink Jobs 进入 RUNNING"
for i in {1..15}; do
    running=$(curl -fsS http://localhost:8081/jobs/overview \
        | python3 -c "import json,sys; print(sum(1 for j in json.load(sys.stdin)['jobs'] if j['state']=='RUNNING'))" 2>/dev/null || echo "0")
    if [[ "$running" == "2" ]]; then ok "2 个 Job RUNNING (try $i)"; break; fi
    sleep 3
    if [[ $i == 15 ]]; then fail "Job 没都 RUNNING"; fi
done

# ------------------------------------------------------------------
step "6) 插一行测试数据 + 故意一行坏数据"
docker exec gis-postgis-src psql -U postgres -d geodb_src -c "
INSERT INTO spatial_data (id, name, geom) VALUES
  (90001, 'smoke-good', ST_SetSRID(ST_MakePoint(116.404, 39.915), 4326));
INSERT INTO spatial_data (id, name, geom) VALUES
  (90002, 'smoke-bad', ST_SetSRID(ST_MakePoint(110.0, 30.0), 4326));
ALTER TABLE spatial_data DISABLE TRIGGER spatial_data_set_ewkt;
UPDATE spatial_data SET geom_ewkt = 'NOT-EWKT' WHERE id = 90002;
ALTER TABLE spatial_data ENABLE TRIGGER spatial_data_set_ewkt;
" 2>&1 | tail -2
ok "插入完成"

step "7) 等好数据落 spatial_data_xfm"
for i in {1..30}; do
    cnt=$(docker exec gis-postgis-dst psql -U postgres -d geodb_dst -tAc \
        "SELECT count(*) FROM spatial_data_xfm WHERE id=90001" 2>/dev/null || echo "0")
    if [[ "$cnt" == "1" ]]; then ok "good row 已同步 (try $i)"; break; fi
    sleep 2
    if [[ $i == 30 ]]; then fail "good row 60 秒未同步"; fi
done

step "8) 等坏数据落 cdc_dlq"
for i in {1..15}; do
    cnt=$(docker exec gis-postgis-dst psql -U postgres -d geodb_dst -tAc \
        "SELECT count(*) FROM cdc_dlq WHERE src_id=90002" 2>/dev/null || echo "0")
    if [[ "$cnt" -ge 1 ]]; then ok "bad row 进 DLQ (try $i)"; break; fi
    sleep 2
    if [[ $i == 15 ]]; then fail "DLQ 没拿到坏数据"; fi
done

# ------------------------------------------------------------------
step "9) 验证 backend API 各端点"
for url in \
    'http://localhost:8090/api/sync/status' \
    'http://localhost:8090/api/slot/health' \
    'http://localhost:8090/api/dlq?limit=5' \
    'http://localhost:8090/api/sync/live-points?limit=5' \
    ; do
    code=$(curl -fsS -o /dev/null -w "%{http_code}" "$url" || echo "000")
    [[ "$code" == "200" ]] && ok "$url → 200" || fail "$url → $code"
done

# slot 应该 active
slot_active=$(curl -fsS http://localhost:8090/api/slot/health \
    | python3 -c "import json,sys; r=json.load(sys.stdin); print('yes' if r and r[0].get('active') else 'no')")
[[ "$slot_active" == "yes" ]] && ok "复制槽 active=true" || fail "复制槽未激活"

step "10) 验证 sync_metrics 表有窗口数据"
# Sink Job 的窗口是 10 秒滚动，插入后要等至少一个窗口关闭
for i in {1..15}; do
    metrics_cnt=$(docker exec gis-postgis-dst psql -U postgres -d geodb_dst -tAc \
        "SELECT count(*) FROM sync_metrics WHERE records_out > 0" 2>/dev/null || echo "0")
    if [[ "$metrics_cnt" -ge 1 ]]; then ok "sync_metrics 至少 1 个非空窗口 (try $i)"; break; fi
    sleep 3
    if [[ $i == 15 ]]; then fail "sync_metrics 45 秒内未出现非空窗口"; fi
done

step "11) Prometheus 指标暴露检查"
# 业务指标先要在 sync_metrics 表里，再被 pgexp 暴露，再被 prom scrape (15s 间隔)
# 给一定耐心
for q in \
    "pg_replication_slots_active" \
    "gis_sync_records_out_total" \
    "flink_jobmanager_job_uptime" \
    ; do
    seen=0
    for i in {1..15}; do
        n=$(curl -fsS "http://localhost:9090/api/v1/query?query=${q}" \
            | python3 -c "import json,sys; print(len(json.load(sys.stdin)['data']['result']))" 2>/dev/null || echo "0")
        if [[ "$n" -ge 1 ]]; then ok "metric $q → $n series (try $i)"; seen=1; break; fi
        sleep 3
    done
    [[ "$seen" == "1" ]] || fail "metric $q 45 秒内未采到"
done

printf "\n\033[1;32m[smoke] ALL PASSED\033[0m\n"
