package com.example.gis.backend.kafka;

import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.TextMessage;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 进程内单实例：维护当前所有 WebSocket session，
 * Kafka consumer 拿到一条消息后，扇出广播给所有 session。
 *
 * <p>本地多浏览器开多个标签页测试时，每个标签页都是一个 session，会各自收到消息。
 */
@Component
public class CdcBroadcaster {

    private final Set<WebSocketSession> sessions = ConcurrentHashMap.newKeySet();

    public void register(WebSocketSession s) {
        sessions.add(s);
    }

    public void unregister(WebSocketSession s) {
        sessions.remove(s);
    }

    public int sessionCount() {
        return sessions.size();
    }

    public void broadcast(String text) {
        TextMessage msg = new TextMessage(text);
        for (WebSocketSession s : sessions) {
            if (!s.isOpen()) {
                sessions.remove(s);
                continue;
            }
            try {
                s.sendMessage(msg);
            } catch (IOException e) {
                // 写失败就丢弃这个 session，下一条消息时被过滤
                sessions.remove(s);
            }
        }
    }
}
