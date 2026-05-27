package com.example.gis.backend.model;

public record SlotHealth(
    String slotName,
    String plugin,
    boolean active,
    long walRetainedBytes,
    long confirmedFlushLagBytes
) {}
