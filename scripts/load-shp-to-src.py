#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
load-shp-to-src.py

把 shapefile 的每个 feature 按节奏 INSERT 到 geodb_src.spatial_data。
配合实时同步看板（http://localhost:8080）观察 PG → Flink → Kafka → PG_dst 全链路。

执行流程：
  1. 用 docker 跑 ogr2ogr 把 shp 投影到 EPSG:4326，输出 GeoJSON 到 /tmp/<name>.geojson
     （如果 GeoJSON 已存在且比 shp 新，跳过这一步）
  2. 流式读 GeoJSON，每个 feature 按 --interval 秒间隔 INSERT 到 source DB
  3. 几何用 ST_GeomFromGeoJSON(?) 写入；id 从 --start-id 开始递增；
     name 取属性表里第一个非空字符串字段（DLMC / QSDWMC / 等）

用法：
  python3 scripts/load-shp-to-src.py /Users/zhouning/Downloads/shp/bishan.shp
  python3 scripts/load-shp-to-src.py /path/to.shp --interval 0.1 --limit 1000
  python3 scripts/load-shp-to-src.py /path/to.shp --start-id 100000 --limit 500

依赖：本机 python3、docker（用于跑 ogr2ogr）、psql 容器内可达。
"""
import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

GDAL_IMAGE = "ghcr.io/osgeo/gdal:alpine-small-latest"
PG_CONTAINER = "gis-postgis-src"
PG_USER = "postgres"
PG_DB = "geodb_src"


def step(msg: str) -> None:
    print(f"\033[36m[{time.strftime('%H:%M:%S')}]\033[0m {msg}", flush=True)


def ensure_geojson(shp_path: Path) -> Path:
    """用 docker GDAL 投影 shp → 4326 GeoJSON。已存在且新于 shp 时跳过。"""
    geojson_path = Path("/tmp") / (shp_path.stem + ".4326.geojson")
    if geojson_path.exists() and geojson_path.stat().st_mtime > shp_path.stat().st_mtime:
        step(f"GeoJSON 已存在: {geojson_path} ({geojson_path.stat().st_size // 1024} KiB)")
        return geojson_path

    step(f"用 ogr2ogr 把 {shp_path.name} 投影到 EPSG:4326 → {geojson_path}")
    shp_dir = shp_path.parent
    cmd = [
        "docker", "run", "--rm",
        "-v", f"{shp_dir}:/in:ro",
        "-v", "/tmp:/out",
        GDAL_IMAGE,
        "ogr2ogr",
        "-f", "GeoJSON",
        "-t_srs", "EPSG:4326",
        "-skipfailures",
        f"/out/{geojson_path.name}",
        f"/in/{shp_path.name}",
    ]
    subprocess.run(cmd, check=True)
    step(f"ogr2ogr 完成 ({geojson_path.stat().st_size // 1024} KiB)")
    return geojson_path


def pick_name(props: dict, fallback_id: int) -> str:
    """从属性表挑一个最像 "名称" 的字符串字段；都没的话用 id 作为 name。"""
    name_keys = ["DLMC", "QSDWMC", "ZLDWMC", "NAME", "name", "BSM"]
    for k in name_keys:
        if k in props and props[k] not in (None, "", "0"):
            return f"{k}={props[k]}"
    # 没合适字段，找第一个非空 string
    for k, v in props.items():
        if isinstance(v, str) and v.strip():
            return f"{k}={v}"
    return f"feature-{fallback_id}"


def insert_one(feature_id: int, name: str, geojson_geom: str) -> None:
    """用 docker exec psql 单条 INSERT。SQL 注入：name 单引号转义。"""
    safe_name = name.replace("'", "''")
    safe_geom = geojson_geom.replace("'", "''")
    sql = (
        f"INSERT INTO spatial_data (id, name, geom) "
        f"VALUES ({feature_id}, '{safe_name}', "
        f"        ST_SetSRID(ST_GeomFromGeoJSON('{safe_geom}'), 4326)) "
        f"ON CONFLICT (id) DO NOTHING;"
    )
    cmd = ["docker", "exec", "-i", PG_CONTAINER, "psql", "-U", PG_USER, "-d", PG_DB, "-q", "-c", sql]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        sys.stderr.write(f"\033[31m  ✗\033[0m id={feature_id} INSERT failed: {res.stderr.strip()[:120]}\n")


def stream_features(geojson_path: Path):
    """GeoJSON FeatureCollection 用 ijson 流式解析比较稳；这里数据量万级，json.load 也够。"""
    with open(geojson_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("features", [])


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("shp", type=Path, help="shapefile 路径")
    p.add_argument("--interval", type=float, default=0.1, help="INSERT 间隔（秒），默认 0.1")
    p.add_argument("--limit", type=int, default=0, help="加载多少条，0 = 全部")
    p.add_argument("--start-id", type=int, default=100000, help="id 起始值（避免与 demo 数据冲突）")
    args = p.parse_args()

    if not args.shp.exists():
        sys.exit(f"shp 不存在: {args.shp}")

    # 1. ogr2ogr → 4326 GeoJSON
    geojson_path = ensure_geojson(args.shp)

    # 2. 加载 features
    step("读 GeoJSON...")
    features = stream_features(geojson_path)
    total = len(features)
    if args.limit > 0:
        features = features[: args.limit]
    step(f"feature 总数 {total}, 实际加载 {len(features)}, 间隔 {args.interval}s")
    step(f"id 从 {args.start_id} 开始递增")
    print("")
    step("看板：http://localhost:8080  会看到吞吐 + 地图区域不断填充")
    step("完整性验证：bash scripts/verify-sync.sh " +
         f"--expected-from {args.start_id} --expected-to {args.start_id + len(features) - 1}")
    print("")

    inserted = 0
    failed = 0
    started = time.time()

    try:
        for i, feature in enumerate(features):
            feature_id = args.start_id + i
            geom = feature.get("geometry")
            if geom is None:
                failed += 1
                continue
            name = pick_name(feature.get("properties") or {}, feature_id)
            geom_str = json.dumps(geom, ensure_ascii=False)

            insert_one(feature_id, name, geom_str)
            inserted += 1

            if inserted % 50 == 0 or inserted == 1:
                elapsed = time.time() - started
                rate = inserted / max(elapsed, 0.001)
                step(f"已 INSERT {inserted}/{len(features)} 条  ({rate:.1f} cps)")

            time.sleep(args.interval)
    except KeyboardInterrupt:
        print()
        step(f"用户中断 (Ctrl-C)")

    elapsed = time.time() - started
    print("")
    step(f"完成。INSERT={inserted}, FAILED={failed}, 用时 {elapsed:.1f}s")
    step(f"id 区间: {args.start_id} ~ {args.start_id + inserted - 1}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
