package com.provectus.fds.ml;

public class Configuration {

    private String region;
    private String bucket;
    private String endpoint;
    private String sagemakerRole;

    private String get(String key) {
        return System.getenv(key);
    }

    String getOrElse(String key, String defaultValue) {
        String result = get(key);

        if (result == null) {
            return defaultValue;
        }

        return result;
    }

    String getOrThrow(String key) {
        String result = get(key);

        if (result == null) {
            throw new IllegalStateException(String.format("The variable '%s' is not defined with current environment", key));
        }

        return result;
    }

    String getRegion() {
        if (region == null)
            region = getOrThrow("REGION");
        return region;
    }
    String getBucket() {
        if (bucket == null)
            bucket = getOrThrow("S3_BUCKET");
        return bucket;
    }

    String getEndpoint() {
        if (endpoint == null)
            endpoint = getOrThrow("ENDPOINT");
        return endpoint;
    }
    public String getSagemakerRole() {
        if (sagemakerRole == null)
            sagemakerRole = getOrThrow("SAGEMAKER_ROLE_ARN");
        return sagemakerRole;
    }
}
