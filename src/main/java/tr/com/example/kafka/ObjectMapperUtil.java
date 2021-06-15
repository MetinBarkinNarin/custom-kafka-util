package tr.com.example.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import reactor.core.Exceptions;

import java.io.IOException;
import java.util.LinkedHashMap;

public final class ObjectMapperUtil {
    private static final ObjectMapper objectMapper  = new ObjectMapper();

    public static Object convert(byte[] bytes, Class<?> clazz) throws IOException {
        return objectMapper.readValue(bytes, clazz);
    }
    public static <T> T readObject(String line, Class<T> tClass) {
        try {
            return objectMapper.readValue(line, tClass);
        } catch (JsonProcessingException ex) {
            throw Exceptions.propagate(ex);
        }
    }

    public static LinkedHashMap<?, ?> objectToMap(Object obj) {
        try {
            return readObject(objectMapper.writeValueAsString(obj), LinkedHashMap.class);
        } catch (JsonProcessingException e) {
            throw Exceptions.propagate(e);
        }
    }
    public static String writeToString(Object obj){
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw Exceptions.propagate(e);
        }
    }
}
