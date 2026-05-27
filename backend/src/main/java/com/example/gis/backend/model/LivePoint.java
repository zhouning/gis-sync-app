package com.example.gis.backend.model;

import java.time.Instant;

/** 目标库实际同步到的最新空间记录（地图打点用）。 */
public record LivePoint(
    int id,
    String name,
    double lon,    // 4326
    double lat,
    Instant syncTime
) {}
