package com.example.gis.backend.api;

import com.example.gis.backend.model.SlotHealth;
import com.example.gis.backend.service.SlotHealthService;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/slot")
@CrossOrigin(origins = "*")
public class SlotController {

    private final SlotHealthService service;

    public SlotController(SlotHealthService service) {
        this.service = service;
    }

    @GetMapping("/health")
    public List<SlotHealth> health() {
        return service.list();
    }
}
