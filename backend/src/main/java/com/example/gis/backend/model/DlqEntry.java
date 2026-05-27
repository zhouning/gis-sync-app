package com.example.gis.backend.model;

import java.time.Instant;

public record DlqEntry(
    long id,
    Integer srcId,
    String op,
    String errorClass,
    String errorMessage,
    String rawPayload,         // jsonb 序列化成字符串原样回传
    Instant occurredAt,
    Instant replayedAt,
    int replayCount
) {}
