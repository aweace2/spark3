package com.convertlab.data.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Optional;

public class JsonUtils {

    private JsonUtils() {}

    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.configure(MapperFeature.PROPAGATE_TRANSIENT_MARKER, true);
        mapper.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'"));
    }

    public static ObjectMapper getMapper() {
        return mapper;
    }

    public static <T> Optional<String> objToStr(T obj) {
        return Optional.ofNullable(obj).map(o -> {
            try {
                return o instanceof String ? (String) o : mapper.writeValueAsString(o);
            } catch (JsonProcessingException e) {
                return null;
            }
        });
    }

    public static <T> Optional<T> strToObj(String str, Class<T> clz) {
        return Optional.ofNullable(str).map(s -> {
            try {
                //noinspection unchecked
                return clz == String.class ? (T) s : mapper.readValue(s, clz);
            } catch (IOException e) {
                return null;
            }
        });
    }
}
