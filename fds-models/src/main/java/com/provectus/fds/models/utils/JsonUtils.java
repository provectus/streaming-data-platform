package com.provectus.fds.models.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Reader;

public class JsonUtils {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static JsonNode readTree(Reader reader) throws IOException {
        return objectMapper.readTree(reader);
    }

    public static <T> byte[] write(T value) throws JsonProcessingException {
        return objectMapper.writeValueAsBytes(value);
    }

    public static <T> T read(Reader reader, Class clazz) throws IOException {
        return objectMapper.readerFor(clazz).readValue(reader);
    }

    public static <T> T read(byte[] bytes, Class<T> clazz) throws IOException {
        return objectMapper.readerFor(clazz).readValue(bytes);
    }

    public static <T> String writeAsString(T object) throws IOException {
        return objectMapper.writeValueAsString(object);
    }
}