-- ==============================================================
-- 源库（业务库）：geodb_src
-- 模拟一个对外服务的业务系统，存储真实空间数据
-- ==============================================================

CREATE EXTENSION IF NOT EXISTS postgis;

-- 业务表：使用 PostGIS 原生 geometry 类型（WGS84 / EPSG:4326）
CREATE TABLE IF NOT EXISTS spatial_data (
    id           INT PRIMARY KEY,
    name         TEXT,
    geom         GEOMETRY(Point, 4326) NOT NULL,
    update_time  TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- GiST 空间索引：业务侧空间查询的基础
CREATE INDEX IF NOT EXISTS idx_spatial_data_geom ON spatial_data USING GIST (geom);

-- Flink CDC 在解码端会读取整行；下游希望拿到稳定的文本表达。
-- ⚠️ PG 16 的逻辑复制不发布 GENERATED 列（PG 17+ 才有 publish_generated_columns）。
-- 因此用 TRIGGER 维护一个普通 TEXT 列 geom_ewkt：源表仍以 geometry 为权威，
-- 但 CDC 链路看到的是稳定的 EWKT 文本。
ALTER TABLE spatial_data
    ADD COLUMN IF NOT EXISTS geom_ewkt TEXT;

CREATE OR REPLACE FUNCTION trg_spatial_data_set_ewkt()
RETURNS TRIGGER AS $$
BEGIN
    NEW.geom_ewkt := ST_AsEWKT(NEW.geom);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS spatial_data_set_ewkt ON spatial_data;
CREATE TRIGGER spatial_data_set_ewkt
    BEFORE INSERT OR UPDATE OF geom ON spatial_data
    FOR EACH ROW EXECUTE FUNCTION trg_spatial_data_set_ewkt();

-- 逻辑复制要求：UPDATE/DELETE 拿到完整 before-image
ALTER TABLE spatial_data REPLICA IDENTITY FULL;

-- 显式创建 publication，只覆盖 spatial_data 表，避免 FOR ALL TABLES 的隐患
DROP PUBLICATION IF EXISTS gis_pub;
CREATE PUBLICATION gis_pub FOR TABLE spatial_data;

-- 种子数据
INSERT INTO spatial_data (id, name, geom) VALUES
    (1, '北京天安门',  ST_SetSRID(ST_MakePoint(116.404, 39.915), 4326)),
    (2, '上海外滩',    ST_SetSRID(ST_MakePoint(121.473, 31.230), 4326)),
    (3, '广州塔',      ST_SetSRID(ST_MakePoint(113.264, 23.129), 4326))
ON CONFLICT (id) DO NOTHING;
