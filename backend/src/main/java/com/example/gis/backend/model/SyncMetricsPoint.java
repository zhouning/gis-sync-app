package com.example.gis.backend.model;

import java.time.Instant;

public record SyncMetricsPoint(
    Instant windowStart,
    Instant windowEnd,
    String jobName,
    long recordsIn,
    long recordsOut,
    long recordsDlq,
    long p50LatencyMs,
    long p99LatencyMs
) {}
