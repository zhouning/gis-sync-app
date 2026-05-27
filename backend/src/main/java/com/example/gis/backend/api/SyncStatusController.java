package com.example.gis.backend.api;

import com.example.gis.backend.model.SlotHealth;
import com.example.gis.backend.model.SyncStatus;
import com.example.gis.backend.service.FlinkClient;
import com.example.gis.backend.service.SlotHealthService;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/sync")
@CrossOrigin(origins = "*")  // 本地 dev 简化跨域，生产用 Spring Security + 白名单
public class SyncStatusController {

    private final FlinkClient flink;
    private final SlotHealthService slots;

    public SyncStatusController(FlinkClient flink, SlotHealthService slots) {
        this.flink = flink;
        this.slots = slots;
    }

    @GetMapping("/status")
    public SyncStatus status() {
        List<SlotHealth> slotList;
        try { slotList = slots.list(); } catch (Exception e) { slotList = List.of(); }
        return new SyncStatus(flink.overview(), flink.listJobs(), slotList);
    }
}
