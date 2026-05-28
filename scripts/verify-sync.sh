#!/usr/bin/env bash
# verify-sync.sh — 多层完整性验证：判断 PG_src → Kafka → PG_dst 同步链路是否成功落库
#
# 用法：
#   bash scripts/verify-sync.sh                                  # 验证全部
#   bash scripts/verify-sync.sh --expected-from 100000 --expected-to 100499
#                                                                # 只验证某个 id 区间
#   bash scripts/verify-sync.sh --sample 20                      # 几何抽样数（默认 10）
#   bash scripts/verify-sync.sh --json                           # 机器可读输出
#
# 退出码：0 = 全部通过，非 0 = 任意一层失败
#
# ===========================================================================
# 验证策略（4 层证据）
# ===========================================================================
# Layer 1  数量对账：源端 = 目标端 + DLQ + 在途 (Kafka lag)
# Layer 2  ID 对账：源端有但目标端缺失的 id（精确到条）
# Layer 3  几何对等：抽 N 条比对源/目标几何 ST_AsBinary（exact match）
# Layer 4  指标对账：sync_metrics 表 sum(records_in) ≥ 源端总变更数
# ===========================================================================

set -uo pipefail

# ---------- 参数 ----------
FROM_ID=""
TO_ID=""
SAMPLE=10
JSON_OUT=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --expected-from) FROM_ID="$2"; shift 2;;
        --expected-to)   TO_ID="$2"; shift 2;;
        --sample)        SAMPLE="$2"; shift 2;;
        --json)          JSON_OUT=true; shift;;
        *) echo "unknown arg: $1" >&2; exit 2;;
    esac
done

# ---------- 工具 ----------
step()  { $JSON_OUT || printf "\n\033[1;36m=== %s ===\033[0m\n" "$*"; }
ok()    { $JSON_OUT || printf "  \033[32m✓\033[0m %s\n" "$*"; }
bad()   { $JSON_OUT || printf "  \033[31m✗\033[0m %s\n" "$*"; }
warn()  { $JSON_OUT || printf "  \033[33m!\033[0m %s\n" "$*"; }
info()  { $JSON_OUT || printf "    %s\n" "$*"; }

src_q() {
    docker exec gis-postgis-src psql -U postgres -d geodb_src -tAc "$1" 2>/dev/null
}
dst_q() {
    docker exec gis-postgis-dst psql -U postgres -d geodb_dst -tAc "$1" 2>/dev/null
}

# WHERE 子句根据 from/to 拼接
WHERE_SRC=""
WHERE_DST=""
if [[ -n "$FROM_ID" && -n "$TO_ID" ]]; then
    WHERE_SRC=" WHERE id BETWEEN $FROM_ID AND $TO_ID"
    WHERE_DST=" WHERE id BETWEEN $FROM_ID AND $TO_ID"
fi

FAILS=0
LAYER_RESULTS=()

# ============================================================
# Layer 1: 数量对账
# ============================================================
step "Layer 1 — 数量对账"
SRC_COUNT=$(src_q "SELECT count(*) FROM spatial_data${WHERE_SRC}")
DST_COUNT=$(dst_q "SELECT count(*) FROM spatial_data_xfm${WHERE_DST}")

if [[ -n "$FROM_ID" ]]; then
    DLQ_COUNT=$(dst_q "SELECT count(*) FROM cdc_dlq WHERE src_id BETWEEN $FROM_ID AND $TO_ID")
else
    DLQ_COUNT=$(dst_q "SELECT count(*) FROM cdc_dlq")
fi

# Kafka 在途 = consumer group lag（暂时简化：把它当作 0，实际 active 同步时短暂差几条是正常的）
# 若 Kafka exporter 暴露指标，这里可改成 Prometheus 查询。简化版本只看 src vs dst+dlq。

info "src.spatial_data       : $SRC_COUNT"
info "dst.spatial_data_xfm   : $DST_COUNT"
info "dst.cdc_dlq            : $DLQ_COUNT"

EXPECTED=$((DST_COUNT + DLQ_COUNT))
DIFF=$((SRC_COUNT - EXPECTED))

if [[ "$DIFF" == "0" ]]; then
    ok "src ($SRC_COUNT) = dst ($DST_COUNT) + dlq ($DLQ_COUNT) — 数量完全对得上"
    LAYER_RESULTS+=('"layer1":"pass"')
elif [[ "$DIFF" -gt 0 ]]; then
    if [[ "$DIFF" -le 5 ]]; then
        warn "src 比 dst+dlq 多 $DIFF 条（在途/checkpoint 间隙正常，等几秒重试）"
        LAYER_RESULTS+=('"layer1":"in-flight"')
    else
        bad "src 比 dst+dlq 多 $DIFF 条（>5，疑似真有数据丢失）"
        FAILS=$((FAILS + 1))
        LAYER_RESULTS+=('"layer1":"fail"')
    fi
else
    bad "目标端比源端还多 $((-DIFF)) 条（异常：可能是 DLQ 重投或脏数据）"
    FAILS=$((FAILS + 1))
    LAYER_RESULTS+=('"layer1":"fail"')
fi

# ============================================================
# Layer 2: ID 对账
# ============================================================
step "Layer 2 — ID 对账（哪些 id 源端有但目标端缺）"

MISSING_LIST=$(
    {
        src_q "SELECT id FROM spatial_data${WHERE_SRC} ORDER BY id" | awk '{print "S " $0}'
        dst_q "SELECT id FROM spatial_data_xfm${WHERE_DST} ORDER BY id" | awk '{print "D " $0}'
        dst_q "SELECT src_id FROM cdc_dlq $( [[ -n \"$FROM_ID\" ]] && echo \"WHERE src_id BETWEEN $FROM_ID AND $TO_ID\")" | awk '{print "L " $0}'
    } | awk '
        $1=="S" { src[$2]=1 }
        $1=="D" { dst[$2]=1 }
        $1=="L" { dlq[$2]=1 }
        END {
            for (i in src) {
                if (!(i in dst) && !(i in dlq)) print i
            }
        }
    ' | sort -n
)

MISSING_COUNT=$(echo -n "$MISSING_LIST" | grep -c . || true)

if [[ "$MISSING_COUNT" == "0" ]]; then
    ok "源端所有 id 都在 dst 或 dlq 里出现"
    LAYER_RESULTS+=('"layer2":"pass"')
elif [[ "$MISSING_COUNT" -le 5 ]]; then
    warn "$MISSING_COUNT 条 id 缺失（可能 in-flight），等下面 Layer 3 几何抽样验证"
    info "缺失 id：$(echo $MISSING_LIST | tr '\n' ' ')"
    LAYER_RESULTS+=('"layer2":"in-flight"')
else
    bad "$MISSING_COUNT 条 id 缺失（前 10 个）：$(echo "$MISSING_LIST" | head -10 | tr '\n' ' ')"
    FAILS=$((FAILS + 1))
    LAYER_RESULTS+=('"layer2":"fail"')
fi

# ============================================================
# Layer 3: 几何对等（抽样）
# ============================================================
step "Layer 3 — 几何对等抽样（$SAMPLE 条）"

SAMPLE_IDS=$(src_q "SELECT id FROM spatial_data${WHERE_SRC} ORDER BY random() LIMIT $SAMPLE")
GEOM_PASS=0
GEOM_FAIL=0
GEOM_NOTFOUND=0

for id in $SAMPLE_IDS; do
    SRC_HASH=$(src_q "SELECT md5(ST_AsBinary(geom)) FROM spatial_data WHERE id=$id")
    DST_HASH=$(dst_q "SELECT md5(ST_AsBinary(geom_4326)) FROM spatial_data_xfm WHERE id=$id")

    if [[ -z "$DST_HASH" ]]; then
        warn "id=$id  目标端缺失（in-flight 或在 DLQ）"
        GEOM_NOTFOUND=$((GEOM_NOTFOUND + 1))
    elif [[ "$SRC_HASH" == "$DST_HASH" ]]; then
        GEOM_PASS=$((GEOM_PASS + 1))
    else
        bad "id=$id  几何 hash 不一致  src=${SRC_HASH:0:8} dst=${DST_HASH:0:8}"
        GEOM_FAIL=$((GEOM_FAIL + 1))
    fi
done

info "几何对等：通过 ${GEOM_PASS} / 不一致 ${GEOM_FAIL} / 目标缺 ${GEOM_NOTFOUND}（共 ${SAMPLE} 条抽样）"

if [[ "$GEOM_FAIL" == "0" && "$GEOM_NOTFOUND" -le 1 ]]; then
    ok "抽样中所有匹配上的几何 hash 完全一致（即 ST_AsBinary 字节级相等）"
    LAYER_RESULTS+=('"layer3":"pass"')
elif [[ "$GEOM_FAIL" == "0" ]]; then
    warn "抽样中没有 hash 不一致，但 $GEOM_NOTFOUND 条目标端缺失（in-flight）"
    LAYER_RESULTS+=('"layer3":"in-flight"')
else
    bad "$GEOM_FAIL 条几何 hash 不一致（数据真损坏，不是 in-flight）"
    FAILS=$((FAILS + 1))
    LAYER_RESULTS+=('"layer3":"fail"')
fi

# ============================================================
# Layer 4: 指标对账
# ============================================================
step "Layer 4 — sync_metrics 累计指标 vs 源端变更数"

METRIC_IN=$(dst_q "SELECT coalesce(sum(records_in)::bigint, 0) FROM sync_metrics")
METRIC_OUT=$(dst_q "SELECT coalesce(sum(records_out)::bigint, 0) FROM sync_metrics")
METRIC_DLQ=$(dst_q "SELECT coalesce(sum(records_dlq)::bigint, 0) FROM sync_metrics")

info "sync_metrics 累计：records_in=$METRIC_IN  records_out=$METRIC_OUT  records_dlq=$METRIC_DLQ"
info "对账标准：records_in ≥ src 总变更（含 UPDATE 拆出的 d/c 双记）"
info "          records_out + records_dlq ≈ records_in（误差 ≤ 一个窗口的 in-flight）"

if [[ "$METRIC_IN" -ge "$SRC_COUNT" ]]; then
    ok "records_in ($METRIC_IN) ≥ src 当前行数 ($SRC_COUNT) — 所有事件都被聚合算子见过"
    LAYER_RESULTS+=('"layer4":"pass"')
else
    GAP=$((SRC_COUNT - METRIC_IN))
    if [[ "$GAP" -le 10 ]]; then
        warn "records_in ($METRIC_IN) < src ($SRC_COUNT)，差 $GAP（最近窗口可能还没关闭）"
        LAYER_RESULTS+=('"layer4":"in-flight"')
    else
        bad "records_in ($METRIC_IN) < src ($SRC_COUNT)，差 $GAP（窗口聚合断了？）"
        FAILS=$((FAILS + 1))
        LAYER_RESULTS+=('"layer4":"fail"')
    fi
fi

# ============================================================
# 总结
# ============================================================
if $JSON_OUT; then
    LAYERS_JSON=$(IFS=,; echo "${LAYER_RESULTS[*]}")
    cat <<EOF
{
  "src_count": $SRC_COUNT,
  "dst_count": $DST_COUNT,
  "dlq_count": $DLQ_COUNT,
  "missing_ids_count": $MISSING_COUNT,
  "sample_geom_pass": $GEOM_PASS,
  "sample_geom_fail": $GEOM_FAIL,
  "sample_geom_missing": $GEOM_NOTFOUND,
  "metric_records_in": $METRIC_IN,
  "metric_records_out": $METRIC_OUT,
  "metric_records_dlq": $METRIC_DLQ,
  $LAYERS_JSON,
  "overall": "$([ $FAILS -eq 0 ] && echo pass || echo fail)",
  "fails": $FAILS
}
EOF
else
    step "汇总"
    if [[ "$FAILS" == "0" ]]; then
        printf "\n  \033[1;32m同步链路完整性 —— PASS\033[0m （4/4 层都通过或仅有 in-flight 容差）\n\n"
    else
        printf "\n  \033[1;31m同步链路完整性 —— FAIL\033[0m （%d 层 fail）\n\n" "$FAILS"
    fi
fi

exit $FAILS
