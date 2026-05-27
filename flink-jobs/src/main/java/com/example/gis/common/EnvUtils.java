package com.example.gis.common;

/**
 * 环境变量读取工具，支持默认值。
 * 优先从 -D 系统属性读（测试时方便覆盖），否则从环境变量读，否则用默认值。
 */
public final class EnvUtils {

    private EnvUtils() {}

    public static String get(String key, String defaultValue) {
        String v = System.getProperty(key);
        if (v == null || v.isEmpty()) v = System.getenv(key);
        return (v == null || v.isEmpty()) ? defaultValue : v;
    }

    public static int getInt(String key, int defaultValue) {
        String v = get(key, null);
        if (v == null) return defaultValue;
        try { return Integer.parseInt(v); } catch (NumberFormatException e) { return defaultValue; }
    }

    public static long getLong(String key, long defaultValue) {
        String v = get(key, null);
        if (v == null) return defaultValue;
        try { return Long.parseLong(v); } catch (NumberFormatException e) { return defaultValue; }
    }
}
