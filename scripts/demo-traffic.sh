#!/usr/bin/env bash
# 持续模拟同步流量：每 N 秒插一条新城市坐标，偶尔做 UPDATE / DELETE / 坏数据
# 用于让看板（http://localhost:8080）出现持续的吞吐曲线 + 地图实时打点 + WS 流。
#
# 用法：
#   bash scripts/demo-traffic.sh                # 默认间隔 2s，无限跑
#   INTERVAL=1 bash scripts/demo-traffic.sh     # 1s 一条（更密）
#   INTERVAL=5 DURATION=300 bash scripts/demo-traffic.sh   # 5 分钟后自动停
#
# Ctrl-C 中断。

set -uo pipefail

INTERVAL=${INTERVAL:-2}
DURATION=${DURATION:-0}             # 0 = 无限跑
START_ID=${START_ID:-2000}          # 不跟之前 1001-1008 / 9999 冲突

PSQL="docker exec -i gis-postgis-src psql -U postgres -d geodb_src -q"

# 中国主要城市坐标范围（lon, lat, name 前缀）
declare -a CITIES=(
  "116.40 39.90 北京"
  "121.47 31.23 上海"
  "113.32 23.13 广州"
  "114.05 22.55 深圳"
  "120.15 30.27 杭州"
  "104.07 30.66 成都"
  "108.95 34.27 西安"
  "114.30 30.59 武汉"
  "118.78 32.04 南京"
  "117.20 39.13 天津"
  "106.55 29.56 重庆"
  "112.93 28.23 长沙"
  "117.21 31.84 合肥"
  "113.65 34.76 郑州"
  "111.75 40.84 呼和浩特"
)

step() { printf "\e[36m[%s]\e[0m %s\n" "$(date +%H:%M:%S)" "$*"; }
ok()   { printf "  \e[32m+\e[0m %s\n" "$*"; }
warn() { printf "  \e[33m!\e[0m %s\n" "$*"; }

# 检查容器是否在跑
if ! docker ps --format '{{.Names}}' | grep -q '^gis-postgis-src$'; then
    echo "ERROR: gis-postgis-src 容器没在跑。先 make up 起栈"
    exit 1
fi

step "开始模拟流量 (INTERVAL=${INTERVAL}s, DURATION=${DURATION}s, 0=无限)"
step "看板：http://localhost:8080  会看到吞吐 + 地图打点 + WebSocket 流实时增长"
echo ""

trap 'echo ""; step "停止"; exit 0' INT TERM

i=0
deadline=$(( $(date +%s) + DURATION ))
nextId=$START_ID

while true; do
    [[ $DURATION -gt 0 && $(date +%s) -ge $deadline ]] && break

    # 选个随机城市 + 在它附近 ±0.05° 抖动
    city_line="${CITIES[$RANDOM % ${#CITIES[@]}]}"
    read -r baseLon baseLat baseName <<<"$city_line"
    jitterLon=$(awk -v b="$baseLon" 'BEGIN { srand(); printf "%.5f", b + (rand()-0.5)*0.1 }')
    jitterLat=$(awk -v b="$baseLat" 'BEGIN { srand(); printf "%.5f", b + (rand()-0.5)*0.1 }')

    # 80% INSERT, 10% UPDATE 已存在 ID, 5% DELETE 已存在 ID, 5% 坏数据
    roll=$((RANDOM % 100))

    if [[ $roll -lt 80 ]]; then
        # INSERT
        name="${baseName}-生成-${i}"
        $PSQL -c "
            INSERT INTO spatial_data (id, name, geom)
              VALUES ($nextId, '${name}', ST_SetSRID(ST_MakePoint($jitterLon, $jitterLat), 4326))
              ON CONFLICT (id) DO NOTHING;" >/dev/null 2>&1 \
          && ok "INSERT id=$nextId  $name @ ($jitterLon,$jitterLat)" \
          || warn "INSERT failed id=$nextId"
        ((nextId++))

    elif [[ $roll -lt 90 ]]; then
        # UPDATE 已有 ID（取最近 30 个里面随机一个）
        target=$(( START_ID + (RANDOM % (nextId - START_ID + 1)) ))
        $PSQL -c "
            UPDATE spatial_data
               SET geom = ST_SetSRID(ST_MakePoint($jitterLon, $jitterLat), 4326)
             WHERE id = $target;" >/dev/null 2>&1 \
          && ok "UPDATE id=$target → ($jitterLon,$jitterLat)"

    elif [[ $roll -lt 95 ]]; then
        # DELETE 一个已有 ID
        if [[ $nextId -gt $((START_ID + 5)) ]]; then
            target=$(( START_ID + (RANDOM % (nextId - START_ID - 4)) ))
            $PSQL -c "DELETE FROM spatial_data WHERE id = $target;" >/dev/null 2>&1 \
              && ok "DELETE id=$target"
        fi

    else
        # 坏数据：trigger 暂时禁用，写非法 EWKT
        name="坏数据-${nextId}"
        $PSQL -c "
            INSERT INTO spatial_data (id, name, geom)
              VALUES ($nextId, '${name}', ST_SetSRID(ST_MakePoint($jitterLon, $jitterLat), 4326));
            ALTER TABLE spatial_data DISABLE TRIGGER spatial_data_set_ewkt;
            UPDATE spatial_data SET geom_ewkt = 'INVALID-GEOMETRY-${nextId}' WHERE id = $nextId;
            ALTER TABLE spatial_data ENABLE TRIGGER spatial_data_set_ewkt;
        " >/dev/null 2>&1 \
          && warn "BAD-DATA id=$nextId（应该会进 DLQ）"
        ((nextId++))
    fi

    ((i++))
    sleep "$INTERVAL"
done

step "完成，共触发 $i 次操作"
