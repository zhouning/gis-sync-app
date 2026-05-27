package com.example.gis.backend.model;

import java.util.List;
import java.util.Map;

/**
 * 顶部状态卡聚合视图：把多个数据源的关键 KPI 拼成一个响应，前端一次获取。
 * 字段都是 nullable —— 任何子系统不可达时，响应仍然 200，对应字段为 null。
 */
public record SyncStatus(
    Map<String, Object> flinkOverview,    // taskmanagers / slots / jobs-running
    List<Map<String, Object>> flinkJobs,
    List<SlotHealth> slots
) {}
