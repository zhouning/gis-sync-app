#!/usr/bin/env bash
# 可参数化的 CRUD 全场景测试：加载 N 条 polygon + UPDATE/DELETE/坏数据 + 4 层验证
#
# 用法：
#   bash scripts/run-crud-test.sh                          # 默认 2000 条，id=300000+
#   COUNT=200 FROM=400000 bash scripts/run-crud-test.sh    # 200 条，id=400000+
#   COUNT=10000 FROM=500000 bash scripts/run-crud-test.sh
#
# 内部按比例分配 CRUD 量：
#   UPDATE = COUNT / 20    (5%)
#   DELETE = COUNT / 40    (2.5%)
#   BAD    = max(COUNT/200, 2)  (≥0.5%, 至少 2 条)
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FROM=${FROM:-300000}
COUNT=${COUNT:-2000}
TO=$((FROM + COUNT - 1))

# 按比例分配（小批量保证至少几条）
UPDATE_N=$(( COUNT / 20 ))
[[ $UPDATE_N -lt 5 ]] && UPDATE_N=5
DELETE_N=$(( COUNT / 40 ))
[[ $DELETE_N -lt 2 ]] && DELETE_N=2
BAD_N=$(( COUNT / 200 ))
[[ $BAD_N -lt 2 ]] && BAD_N=2

step() { printf "\n\033[1;36m=== %s ===\033[0m\n" "$*"; }
ok()   { printf "  \033[32m✓\033[0m %s\n" "$*"; }

SRC_PSQL="docker exec -i gis-postgis-src psql -U postgres -d geodb_src -q"
DST_PSQL="docker exec -i gis-postgis-dst psql -U postgres -d geodb_dst -q -tAc"

echo "参数：COUNT=$COUNT  FROM=$FROM  TO=$TO"
echo "      UPDATE=$UPDATE_N  DELETE=$DELETE_N  BAD=$BAD_N"

# ============================================================
step "Phase 1 — INSERT $COUNT 条 bishan polygon (id=$FROM..$TO)"
python3 "$ROOT/scripts/load-shp-to-src.py" /Users/zhouning/Downloads/shp/bishan.shp \
  --limit $COUNT --interval 0.05 --start-id $FROM 2>&1 | tail -3

ok "INSERT 完成，等 15 秒让同步追上"
sleep 15

# ============================================================
step "Phase 2 — UPDATE $UPDATE_N 条（随机改 name 和 geom）"
UPDATE_IDS=$($SRC_PSQL -tAc "SELECT id FROM spatial_data
  WHERE id BETWEEN $FROM AND $TO ORDER BY random() LIMIT $UPDATE_N")
update_cnt=0
for id in $UPDATE_IDS; do
  $SRC_PSQL -c "UPDATE spatial_data
    SET name = 'UPDATED-' || name,
        geom = ST_SetSRID(ST_MakePoint(
            106.0 + random()*0.4,
            29.3  + random()*0.5
          ), 4326)
    WHERE id = $id;" >/dev/null 2>&1 && ((update_cnt++))
done
ok "UPDATE 完成 $update_cnt 条"

# ============================================================
step "Phase 3 — DELETE $DELETE_N 条（随机，跟 UPDATE 不重复）"
DELETE_IDS=$($SRC_PSQL -tAc "SELECT id FROM spatial_data
  WHERE id BETWEEN $FROM AND $TO
    AND id NOT IN ($(echo $UPDATE_IDS | tr ' ' ','))
  ORDER BY random() LIMIT $DELETE_N")
delete_cnt=0
for id in $DELETE_IDS; do
  $SRC_PSQL -c "DELETE FROM spatial_data WHERE id = $id;" >/dev/null 2>&1 && ((delete_cnt++))
done
ok "DELETE 完成 $delete_cnt 条"

# ============================================================
step "Phase 4 — 在 UPDATE 集中混入 $BAD_N 条坏数据测 DLQ 路径"
BAD_IDS=$(echo $UPDATE_IDS | tr ' ' '\n' | head -$BAD_N)
bad_cnt=0
for id in $BAD_IDS; do
  $SRC_PSQL -c "
    ALTER TABLE spatial_data DISABLE TRIGGER spatial_data_set_ewkt;
    UPDATE spatial_data SET geom_ewkt = 'BAD-EWKT-${id}' WHERE id = $id;
    ALTER TABLE spatial_data ENABLE TRIGGER spatial_data_set_ewkt;" >/dev/null 2>&1 && ((bad_cnt++))
done
ok "故意制造 $bad_cnt 条坏数据"

# ============================================================
step "等 25 秒让 Flink 把所有变更（含 UPDATE 拆出的 d+c, DELETE, 坏数据→DLQ）追完"
sleep 25

# ============================================================
step "Phase 5 — 中间快照统计"
src_total=$($SRC_PSQL -tAc "SELECT count(*) FROM spatial_data WHERE id BETWEEN $FROM AND $TO")
dst_total=$($DST_PSQL "SELECT count(*) FROM spatial_data_xfm WHERE id BETWEEN $FROM AND $TO" </dev/null)
dlq_total=$($DST_PSQL "SELECT count(*) FROM cdc_dlq WHERE src_id BETWEEN $FROM AND $TO" </dev/null)
updated_dst=$($DST_PSQL "SELECT count(*) FROM spatial_data_xfm
  WHERE id BETWEEN $FROM AND $TO AND name LIKE 'UPDATED-%'" </dev/null)
printf "  src 行数（删后）          : %s （预期 %d - %d = %d）\n" "$src_total" $COUNT $delete_cnt $((COUNT - delete_cnt))
printf "  dst 行数                   : %s\n" "$dst_total"
printf "  cdc_dlq 行数（坏数据）     : %s （预期 ≈%d）\n" "$dlq_total" $bad_cnt
printf "  dst 中已 UPDATE 名称的     : %s （预期 ≈%d）\n" "$updated_dst" $((update_cnt - bad_cnt))

# ============================================================
step "Phase 6 — 4 层完整性验证"
SAMPLE_N=$(( COUNT / 20 ))
[[ $SAMPLE_N -lt 10 ]] && SAMPLE_N=10
[[ $SAMPLE_N -gt 50 ]] && SAMPLE_N=50
bash "$ROOT/scripts/verify-sync.sh" --expected-from $FROM --expected-to $TO --sample $SAMPLE_N

