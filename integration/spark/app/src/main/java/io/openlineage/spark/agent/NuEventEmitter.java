package io.openlineage.spark.agent;

import io.openlineage.client.OpenLineage;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.openlineage.client.OpenLineage.RunEvent.EventType;
import static io.openlineage.client.OpenLineage.RunEvent;
import static io.openlineage.client.OpenLineage.RunEvent.EventType.*;
import static java.util.Objects.isNull;

@Slf4j
public class NuEventEmitter {

    private static final Set<String> WANTED_JOB_TYPES = Set.of(
            "SQL_JOB" // as defined in SparkSQLExecutionContext.SPARK_JOB_TYPE
    );

    private static final Set<String> WANTED_EVENT_NAME_SUBSTRINGS = Set.of(
            ".execute_insert_into_hadoop_fs_relation_command.",
            ".adaptive_spark_plan."
    );

    private static Boolean isPermittedJobType(RunEvent event) {
        String jobType = event.getJob().getFacets().getJobType().getJobType();
        if (WANTED_JOB_TYPES.stream().noneMatch(jobType::equals)) {
            log.info("OpenLineage event with job type {} has no lineage value and should not be emitted", jobType);
            return false;
        }
        return true;
    }

    private static Boolean isPermitedEventType(RunEvent event) {
        if (RUNNING.equals(event.getEventType())) {
            log.info("OpenLineage event is {} and should not be emitted", RUNNING);
            return false;
        }
        return true;
    }

    private static Boolean isPermittedJobName(RunEvent event) {
        String jobName = event.getJob().getName();
        if (isNull(jobName)) {
            log.info("OpenLineage event has no job name and should not be emitted");
            return false;
        }
        if (WANTED_EVENT_NAME_SUBSTRINGS.stream().noneMatch(jobName::contains)) {
            log.info("OpenLineage event job name has no permitted substring and should not be emitted");
            return false;
        }
        return true;
    }

    private static Boolean shouldEmit(RunEvent event) {
        return Stream.of(
                isPermittedJobType(event),
                isPermitedEventType(event),
                isPermittedJobName(event)
        ).noneMatch(Boolean.FALSE::equals);
    }

    private static Boolean shouldDiscardColumnLineageFacet(EventType eventType) {
        return !COMPLETE.equals(eventType);
    }

    private static void discardColumnLineageFacet(RunEvent event) {
        try {
            Field columnLineageFacetField = OpenLineage.DatasetFacets.class.getDeclaredField("columnLineage");
            columnLineageFacetField.setAccessible(true);
            Stream
                    .concat(event.getInputs().stream(), event.getOutputs().stream())
                    .collect(Collectors.toList())
                    .forEach(dataset -> {
                        try {
                            log.info("Discarding column lineage facet for dataset {} {} {}",
                                    dataset.getClass().getName(), dataset.getNamespace(), dataset.getName());
                            columnLineageFacetField.set(dataset.getFacets(), null);
                        } catch (IllegalAccessException e) {
                            log.warn("Failed to discard column lineage facet", e);
                        }
                    });
        } catch (NoSuchFieldException e) {
            log.error("Failed to discard column lineage facet: columnLineage field not found at OpenLineage.DatasetFacets", e);
        }
    }

    public static void emit(RunEvent event, EventEmitter eventEmitter) {
        if (!shouldEmit(event)) {
            return;
        }

        if (shouldDiscardColumnLineageFacet(event.getEventType())) {
            discardColumnLineageFacet(event);
        }

        eventEmitter.emit(event);
    }
}
