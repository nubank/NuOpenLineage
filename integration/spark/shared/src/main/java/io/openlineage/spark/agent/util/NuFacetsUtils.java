package io.openlineage.spark.agent.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

@Slf4j
public class NuFacetsUtils {
    public static Map<String, String> parseJsonToMap(String jsonString) throws JsonProcessingException {
        if (Objects.isNull(jsonString)) {
            return null;
        }
        ObjectMapper mapper = new ObjectMapper();
        TypeReference<HashMap<String, String>> typeRef = new TypeReference<HashMap<String, String>>() {};
        return mapper.readValue(jsonString, typeRef);
    }

    public static String getConfigValue(String key, SparkSession sparkSession) {
        try {
            return sparkSession.conf().get(key);
        } catch (NoSuchElementException e) {
            log.warn("Property {} not found in the context", key);
            return null;
        }
    }
}
