package com.provectus.fds.ml;

public class Configuration {
    protected String get(String key) {
        return System.getenv(key);
    }

    protected String getOrElse(String key, String defaultValue) {
        String result = get(key);

        if (result == null) {
            return defaultValue;
        }

        return result;
    }

    protected String getOrThrow(String key) {
        String result = get(key);

        if (result == null) {
            throw new IllegalStateException(String.format("The variable '%s' is not defined with current environment"));
        }

        return result;
    }

    private String region;
    String getRegion() {
        if (region == null)
            region = getOrThrow("REGION");
        return region;
    }

    private String bucket;

    String getBucket() {
        if (bucket == null)
            bucket = getOrThrow("S3_BUCKET");
        return bucket;
    }

    private String endpoint;
    String getEndpoint() {
        if (endpoint == null)
            endpoint = getOrThrow("ENDPOINT");
        return endpoint;
    }

    private String sagemakerRole;
    public String getSagemakerRole() {
        if (sagemakerRole == null)
            sagemakerRole = getOrThrow("SAGEMAKER_ROLE_ARN");
        return sagemakerRole;
    }
}
