package com.provectus.fds.reports;

import java.util.HashMap;
import java.util.Map;

public class ApiResponse {
    private final int statusCode;
    private final String body;
    private final boolean isBase64Encoded = false;
    private final Map<String,String> headers = new HashMap<>();

    public ApiResponse(int statusCode, String body) {
        this.statusCode = statusCode;
        this.body = body;
    }

    public ApiResponse addHeader(String key, String value) {
        this.headers.put(key, value);
        return this;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getBody() {
        return body;
    }

    public boolean isBase64Encoded() {
        return isBase64Encoded;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }
}
