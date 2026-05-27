package com.example.gis.backend.service;

import com.example.gis.backend.config.GisProperties;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 调 Flink JobManager REST API：列作业、查 uptime 等。
 * 失败时返回空列表（看板需要"降级展示"，不能因为 Flink 不通就 500）。
 */
@Service
public class FlinkClient {

    private final RestTemplate restTemplate;
    private final String baseUrl;

    public FlinkClient(RestTemplate restTemplate, GisProperties props) {
        this.restTemplate = restTemplate;
        this.baseUrl = props.getFlink().getRestUrl();
    }

    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> listJobs() {
        try {
            Map<String, Object> resp = restTemplate.getForObject(baseUrl + "/jobs/overview", Map.class);
            if (resp == null) return Collections.emptyList();
            Object jobs = resp.get("jobs");
            if (jobs instanceof List<?> list) {
                return (List<Map<String, Object>>) list;
            }
            return Collections.emptyList();
        } catch (RestClientException e) {
            return Collections.emptyList();
        }
    }

    /** 集群整体概览 — taskmanagers/slots/jobs 数。 */
    @SuppressWarnings("unchecked")
    public Map<String, Object> overview() {
        try {
            Map<String, Object> resp = restTemplate.getForObject(baseUrl + "/overview", Map.class);
            return resp != null ? resp : Collections.emptyMap();
        } catch (RestClientException e) {
            return Collections.emptyMap();
        }
    }
}
