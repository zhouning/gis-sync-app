package com.example.gis.backend.kafka;

import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

/**
 * /ws/cdc 端点的 handler：纯订阅模式，不接受客户端消息。
 * 客户端连上后即开始收推送，断开自动从 broadcaster 摘除。
 */
public class CdcWebSocketHandler extends TextWebSocketHandler {

    private final CdcBroadcaster broadcaster;

    public CdcWebSocketHandler(CdcBroadcaster broadcaster) {
        this.broadcaster = broadcaster;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        broadcaster.register(session);
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        broadcaster.unregister(session);
    }
}
