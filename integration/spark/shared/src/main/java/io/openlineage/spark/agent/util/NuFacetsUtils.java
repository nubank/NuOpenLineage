package io.openlineage.spark.agent.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;

import java.util.*;
import java.util.stream.Collectors;

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

    public static Map<String, String> getConfigValues(String prefix, RuntimeConfig configs) {
        return JavaConverters.mapAsJavaMap(configs.getAll()).entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(prefix))
                .map(entry -> {
                    String datasetName = entry.getKey().replace(prefix, "");
                    String datasetPath = entry.getValue();
                    return new AbstractMap.SimpleEntry<>(datasetName, datasetPath);
                }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static Map<String, String> keyToValue(Map<String, String> map) {
        return map.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    }
}
