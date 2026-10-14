package com.yzl.consume;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 基于 JDK HttpURLConnection 的任务拉取客户端，解析形如
 * [{"id":"task-0001","durationMs":1234},...] 的 JSON 响应
 */
public class HttpTaskClient implements TaskClient {

    private static final Pattern OBJ = Pattern.compile("\\{[^{}]*\\}");
    private static final Pattern ID = Pattern.compile("\"id\"\\s*:\\s*\"([^\"]+)\"");
    private static final Pattern DURATION = Pattern.compile("\"durationMs\"\\s*:\\s*(\\d+)");

    private final String baseUrl;
    private final int connectTimeoutMs;
    private final int readTimeoutMs;

    public HttpTaskClient(String baseUrl) {
        this(baseUrl, 1000, 5000);
    }

    public HttpTaskClient(String baseUrl, int connectTimeoutMs, int readTimeoutMs) {
        this.baseUrl = baseUrl;
        this.connectTimeoutMs = connectTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
    }

    @Override
    public List<Task> fetch(int max) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(baseUrl + "/tasks?limit=" + max).openConnection();
        try {
            conn.setConnectTimeout(connectTimeoutMs);
            conn.setReadTimeout(readTimeoutMs);
            conn.setRequestMethod("GET");
            int status = conn.getResponseCode();
            if (status != 200) {
                throw new IOException("fetch tasks failed, HTTP " + status);
            }
            return parse(readBody(conn));
        } finally {
            conn.disconnect();
        }
    }

    static List<Task> parse(String body) {
        List<Task> tasks = new ArrayList<>();
        Matcher m = OBJ.matcher(body);
        while (m.find()) {
            String obj = m.group();
            String id = matchFirst(ID, obj);
            String duration = matchFirst(DURATION, obj);
            if (id != null && duration != null) {
                tasks.add(new Task(id, Long.parseLong(duration)));
            }
        }
        return tasks;
    }

    private static String matchFirst(Pattern p, String s) {
        Matcher m = p.matcher(s);
        return m.find() ? m.group(1) : null;
    }

    private static String readBody(HttpURLConnection conn) throws IOException {
        InputStream in = conn.getInputStream();
        try {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            byte[] tmp = new byte[4096];
            int n;
            while ((n = in.read(tmp)) != -1) {
                buf.write(tmp, 0, n);
            }
            return new String(buf.toByteArray(), StandardCharsets.UTF_8);
        } finally {
            in.close();
        }
    }
}
