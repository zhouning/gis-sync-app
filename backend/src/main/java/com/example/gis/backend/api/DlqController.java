package com.example.gis.backend.api;

import com.example.gis.backend.kafka.DlqReplayer;
import com.example.gis.backend.model.DlqEntry;
import com.example.gis.backend.service.DashboardService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/dlq")
@CrossOrigin(origins = "*")
public class DlqController {

    private static final Logger LOG = LoggerFactory.getLogger(DlqController.class);

    private final DashboardService dashboard;
    private final DlqReplayer replayer;

    public DlqController(DashboardService dashboard, DlqReplayer replayer) {
        this.dashboard = dashboard;
        this.replayer = replayer;
    }

    @GetMapping
    public List<DlqEntry> list(
            @RequestParam(defaultValue = "100") int limit,
            @RequestParam(defaultValue = "false") boolean unreplayed) {
        if (limit > 500) limit = 500;
        return dashboard.listDlq(limit, unreplayed);
    }

    @PostMapping("/{id}/replay")
    public ResponseEntity<?> replay(@PathVariable long id) {
        String raw = dashboard.fetchRawPayload(id);
        if (raw == null) {
            return ResponseEntity.status(404).body(Map.of("error", "dlq id not found"));
        }
        try {
            String result = replayer.replay(raw);
            dashboard.markReplayed(id);
            LOG.info("Replayed DLQ id={} → {}", id, result);
            return ResponseEntity.ok(Map.of("status", "ok", "delivery", result));
        } catch (Exception e) {
            LOG.error("Replay failed for DLQ id={}", id, e);
            return ResponseEntity.status(500)
                .body(Map.of("error", e.getClass().getSimpleName(), "message", e.getMessage()));
        }
    }
}
