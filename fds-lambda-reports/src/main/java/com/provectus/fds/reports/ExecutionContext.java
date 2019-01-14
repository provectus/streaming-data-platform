package com.provectus.fds.reports;

import java.util.Objects;

public class ExecutionContext {
    private final String method;
    private final String path;

    public ExecutionContext(String method, String path) {
        this.method = method;
        this.path = path;
    }

    public ExecutionContext(ExecutionValues values) {
        this.method = values.getHttpMethod();
        this.path = values.getResource();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExecutionContext that = (ExecutionContext) o;
        return Objects.equals(method, that.method) &&
                Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(method, path);
    }
}
