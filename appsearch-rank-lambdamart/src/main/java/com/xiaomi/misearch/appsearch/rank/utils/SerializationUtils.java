package com.xiaomi.misearch.appsearch.rank.utils;

import java.io.IOException;
import java.io.OutputStream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

public class SerializationUtils {

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static <T> String toJson(T t) throws IOException {
        return objectMapper.writeValueAsString(t);
    }

    public static <T> void writeValue(T t, OutputStream out) throws IOException {
        objectMapper.writeValue(out, t);
    }

    public static <T> T fromJson(String value, Class<T> valueType) throws IOException {
        return objectMapper.readValue(value, valueType);
    }

    public static <T> T fromJson(String value, TypeReference valueTypeRef) throws IOException {
        return objectMapper.readValue(value, valueTypeRef);
    }
}