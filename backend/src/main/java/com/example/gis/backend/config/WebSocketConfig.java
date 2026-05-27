package com.example.gis.backend.config;

import com.example.gis.backend.kafka.CdcBroadcaster;
import com.example.gis.backend.kafka.CdcWebSocketHandler;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    private final CdcBroadcaster broadcaster;

    public WebSocketConfig(CdcBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(new CdcWebSocketHandler(broadcaster), "/ws/cdc")
            .setAllowedOrigins("*"); // 本地 dev，生产需要白名单
    }
}
