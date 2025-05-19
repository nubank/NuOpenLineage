package io.openlineage.spark.agent;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.openlineage.client.OpenLineage.RunEvent.EventType;
import static io.openlineage.client.OpenLineage.RunEvent;
import static io.openlineage.client.OpenLineage.RunEvent.EventType.*;
import static java.util.Objects.isNull;

@Slf4j
public class NuEventEmitter {

    public static long estimateShallowSize() {
        return 24;
    }

    public static long estimateDeepSize(String str) {
        long shallowSize = estimateShallowSize();
        boolean isLatin1 = str.chars().allMatch(ch -> ch <= 0xFF);
        long dataSize = isLatin1 ? str.length() : str.length() * 2L;
        long arraySize = 12 + 4 + dataSize;
        arraySize = (arraySize + 7) & ~7;
        return shallowSize + arraySize;
    }

    private static final Set<String> WANTED_JOB_TYPES = new HashSet<>(
            Collections.singletonList(
                    "SQL_JOB" // as defined in SparkSQLExecutionContext.SPARK_JOB_TYPE
            )
    );

    private static final Set<String> WANTED_EVENT_NAME_SUBSTRINGS = new HashSet<>(
            Arrays.asList(
                    ".execute_insert_into_hadoop_fs_relation_command.",
                    ".adaptive_spark_plan."
            )
    );

    private static Boolean isPermittedJobType(RunEvent event) {
        String jobType = event.getJob().getFacets().getJobType().getJobType();
        if (WANTED_JOB_TYPES.stream().noneMatch(jobType::equals)) {
            log.info("NuOpenLineageLog: NuEventEmitter: isPermittedJobType: OpenLineage event with job type {} has no lineage value and should not be emitted", jobType);
            return false;
        }
        return true;
    }

    private static Boolean isPermittedEventType(RunEvent event) {
        if (RUNNING.equals(event.getEventType())) {
            log.info("NuOpenLineageLog: NuEventEmitter: isPermittedEventType: OpenLineage event is {} and should not be emitted", RUNNING);
            return false;
        }
        return true;
    }

    private static Boolean isPermittedJobName(RunEvent event) {
        String jobName = event.getJob().getName();
        if (isNull(jobName)) {
            log.info("NuOpenLineageLog: NuEventEmitter: isPermittedJobName: OpenLineage event has no job name and should not be emitted");
            return false;
        }
        if (WANTED_EVENT_NAME_SUBSTRINGS.stream().noneMatch(jobName::contains)) {
            log.info("NuOpenLineageLog: NuEventEmitter: isPermittedJobName: OpenLineage event job name {} has no permitted substring and should not be emitted", jobName);
            return false;
        }
        return true;
    }

    private static Boolean shouldEmit(RunEvent event) {
        return Stream.of(
                isPermittedJobType(event),
                isPermittedEventType(event),
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
                            log.info("NuOpenLineageLog: NuEventEmitter: discardColumnLineageFacet: Discarding column lineage facet for dataset {} {} {}",
                                    dataset.getClass().getSimpleName(), dataset.getNamespace(), dataset.getName());
                            columnLineageFacetField.set(dataset.getFacets(), null);
                        } catch (IllegalAccessException e) {
                            log.info("NuOpenLineageLog: NuEventEmitter: discardColumnLineageFacet:  Failed to discard column lineage facet", e);
                        }
                    });
        } catch (NoSuchFieldException e) {
            log.info("NuOpenLineageLog: NuEventEmitter: discardColumnLineageFacet: Failed to discard column lineage facet: columnLineage field not found at OpenLineage.DatasetFacets", e);
        }
    }

    public static void emit(RunEvent event, EventEmitter eventEmitter) {
        String jsonEvent = OpenLineageClientUtils.toJson(event);
        double estimatedEventSize = estimateDeepSize(jsonEvent) / 1024.0;

        log.info("NuOpenLineageLog: NuEventEmitter: emit:  Begin: Emitting OpenLineage event {} with job name {} and job type {} and size {} KB",
                event.getEventType(), event.getJob().getName(), event.getJob().getFacets().getJobType().getJobType(), estimatedEventSize);
        if (!shouldEmit(event)) {
            log.info("NuOpenLineageLog: NuEventEmitter: emit: OpenLineage event {} has no lineage value and should not be emitted", event.getEventType());
            return;
        }

        if (shouldDiscardColumnLineageFacet(event.getEventType())) {
            log.info("NuOpenLineageLog: NuEventEmitter: emit: Discarding column lineage facet for event {}", event.getEventType());
            discardColumnLineageFacet(event);
        }

        eventEmitter.emit(event);

        log.info("NuOpenLineageLog: NuEventEmitter: emit: End: Emitting OpenLineage event {} with job name {} and job type {} and size {} KB",
                event.getEventType(), event.getJob().getName(), event.getJob().getFacets().getJobType().getJobType(), estimatedEventSize);
    }

}
