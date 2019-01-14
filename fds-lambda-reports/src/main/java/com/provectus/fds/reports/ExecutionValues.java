package com.provectus.fds.reports;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ExecutionValues {
    private String resource;
    private String httpMethod;
    private JsonNode headers;
    private JsonNode queryStringParameters;
    private JsonNode pathParameters;
    private JsonNode body;

    public ExecutionValues() {
    }

    public String getResource() {
        return resource;
    }

    public void setResource(String resource) {
        this.resource = resource;
    }

    public String getHttpMethod() {
        return httpMethod;
    }

    public void setHttpMethod(String httpMethod) {
        this.httpMethod = httpMethod;
    }

    public Optional<JsonNode> getHeaders() {
        return Optional.ofNullable(headers);
    }

    public void setHeaders(JsonNode headers) {
        this.headers = headers;
    }

    public Optional<JsonNode> getQueryStringParameters() {
        return Optional.ofNullable(queryStringParameters);
    }

    public void setQueryStringParameters(JsonNode queryStringParameters) {
        this.queryStringParameters = queryStringParameters;
    }

    public Optional<JsonNode> getPathParameters() {
        return Optional.ofNullable(pathParameters);
    }

    public void setPathParameters(JsonNode pathParameters) {
        this.pathParameters = pathParameters;
    }

    public Optional<JsonNode> getBody() {
        return Optional.ofNullable(body);
    }

    public void setBody(JsonNode body) {
        this.body = body;
    }

    public Optional<ZoneId> getPathZone(String name) {
        return getZone(this.getPathParameters(), name);
    }

    public Optional<Boolean> getPathBoolean(String name) {
        return getBoolean(this.getPathParameters(), name);
    }

    public Optional<ZonedDateTime> getPathZoneDateTime(ZoneId zoneId, String name) {
        return this.getZoneDateTime(this.getPathParameters(), zoneId, name);
    }

    public Optional<ChronoUnit> getPathChronoUnit(String name) {
        return this.getChronoUnit(this.getPathParameters(), name);
    }

    public Optional<String> getPathString(String name) {
        return this.getString(this.getPathParameters(), name);
    }

    public Optional<Long> getPathLong(String name) {
        return this.getLong(this.getPathParameters(), name);
    }


    public Optional<ZoneId> getQueryZone(String name) {
        return getZone(this.getQueryStringParameters(), name);
    }

    public Optional<Boolean> getQueryBoolean(String name) {
        return getBoolean(this.getQueryStringParameters(), name);
    }

    public Optional<ZonedDateTime> getQueryZoneDateTime(ZoneId zoneId, String name) {
        return this.getZoneDateTime(this.getQueryStringParameters(), zoneId, name);
    }

    public Optional<ChronoUnit> getQueryChronoUnit(String name) {
        return this.getChronoUnit(this.getQueryStringParameters(), name);
    }

    public Optional<String> getQueryString(String name) {
        return this.getString(this.getQueryStringParameters(), name);
    }

    public Optional<Long> geQuerytLong(String name) {
        return this.getLong(this.getQueryStringParameters(), name);
    }


    private Optional<ZoneId> getZone(Optional<JsonNode> params, String name) {
        return params
                .flatMap(p -> Optional.ofNullable(p.get(name)))
                .filter(JsonNode::isTextual)
                .map(JsonNode::asText)
                .map(ZoneId::of);
    }

    private Optional<Boolean> getBoolean(Optional<JsonNode> params, String name) {
        return params
                .flatMap(p -> Optional.ofNullable(p.get(name)))
                .filter(JsonNode::isBoolean)
                .map(JsonNode::asBoolean);
    }

    private Optional<ZonedDateTime> getZoneDateTime(Optional<JsonNode> params, ZoneId zoneId, String name) {
        return this.getLong(params,name)
                .map( l -> Instant.ofEpochSecond(l).atZone(zoneId));
    }

    private Optional<ChronoUnit> getChronoUnit(Optional<JsonNode> params, String name) {
        return this.getString(params,name)
                .map(ChronoUnit::valueOf);
    }

    private Optional<String> getString(Optional<JsonNode> params, String name) {
        return params
                .flatMap(p -> Optional.ofNullable(p.get(name)))
                .filter(JsonNode::isTextual)
                .map(JsonNode::asText);
    }

    private Optional<Long> getLong(Optional<JsonNode> params, String name) {
        return getString(params, name).map(Long::parseLong);
    }

    public  <T> T orThrow(Optional<T> value, String name) {
        return value.orElseThrow(() -> new RuntimeException(name+" not found"));
    }

    @Override
    public String toString() {
        return "ExecutionValues{" +
                "resource='" + resource + '\'' +
                ", httpMethod='" + httpMethod + '\'' +
                ", headers=" + headers +
                ", queryStringParameters=" + queryStringParameters +
                ", pathParameters=" + pathParameters +
                ", body=" + body +
                '}';
    }
}
