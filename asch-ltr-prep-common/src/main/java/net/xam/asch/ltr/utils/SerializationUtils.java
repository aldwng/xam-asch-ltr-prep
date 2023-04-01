package net.xam.asch.ltr.utils;

import java.io.IOException;
import java.io.OutputStream;

import com.fasterxml.jackson.databind.ObjectMapper;


public class SerializationUtils {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static <T> String toJson(T t) throws IOException {
        return OBJECT_MAPPER.writeValueAsString(t);
    }

    public static <T> void writeValue(T t, OutputStream out) throws IOException {
        OBJECT_MAPPER.writeValue(out, t);
    }

    public static <T> T fromJson(String value, Class<T> valueType) throws IOException {
        return OBJECT_MAPPER.readValue(value, valueType);
    }

//    public static <T> T fromJson(String value, TypeReference valueTypeRef) throws IOException {
//        return objectMapper.readValue(value, valueTypeRef);
//    }
}