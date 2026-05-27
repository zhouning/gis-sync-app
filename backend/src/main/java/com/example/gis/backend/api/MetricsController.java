package com.example.gis.backend.api;

import com.example.gis.backend.model.LivePoint;
import com.example.gis.backend.model.SyncMetricsPoint;
import com.example.gis.backend.service.DashboardService;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;

@RestController
@RequestMapping("/api/sync")
@CrossOrigin(origins = "*")
public class MetricsController {

    private final DashboardService dashboard;

    public MetricsController(DashboardService dashboard) {
        this.dashboard = dashboard;
    }

    /**
     * 时序指标。
     * @param from epoch ms（默认 30 分钟前）
     * @param to   epoch ms（默认现在）
     */
    @GetMapping("/metrics")
    public List<SyncMetricsPoint> metrics(
            @RequestParam(required = false) Long from,
            @RequestParam(required = false) Long to) {
        long now = Instant.now().toEpochMilli();
        long fromMs = from != null ? from : now - 30 * 60_000L;
        long toMs = to != null ? to : now;
        return dashboard.metrics(fromMs, toMs);
    }

    @GetMapping("/live-points")
    public List<LivePoint> livePoints(@RequestParam(defaultValue = "200") int limit) {
        if (limit > 1000) limit = 1000;   // 简单上限
        return dashboard.recentPoints(limit);
    }
}
