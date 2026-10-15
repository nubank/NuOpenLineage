package io.openlineage.spark.agent;

import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static io.openlineage.client.OpenLineage.RunEvent;
import static io.openlineage.client.OpenLineage.RunEvent.EventType.COMPLETE;
import static java.util.Objects.isNull;

@Slf4j
public class NuEventEmitter {

    private static final Set<String> WANTED_JOB_TYPES = new HashSet<>(
            Collections.singletonList(
                    "SQL_JOB" // as defined in SparkSQLExecutionContext.SPARK_JOB_TYPE
            )
    );

    private static final Set<String> WANTED_EVENT_NAME_SUBSTRINGS = new HashSet<>(
            Arrays.asList(
                    ".execute_insert_into_hadoop_fs_relation_command.",
                    ".adaptive_spark_plan.",
                    ".execute_save_into_data_source_command.",
                    ".execute_merge_into_command."
            )
    );

    private static Boolean isPermittedJobType(RunEvent event) {
        String jobType = event.getJob().getFacets().getJobType().getJobType();
        if (WANTED_JOB_TYPES.stream().noneMatch(jobType::equals)) {
            log.debug("OpenLineage event with job type {} has no lineage value and should not be emitted", jobType);
            return false;
        }
        return true;
    }

    private static Boolean isPermitedEventType(RunEvent event) {
        if (!COMPLETE.equals(event.getEventType())) {
            log.debug("OpenLineage event is {} and only {} events are emitted", event.getEventType(), COMPLETE);
            return false;
        }
        return true;
    }

    private static Boolean isPermittedJobName(RunEvent event) {
        return isPermittedJobName(event.getJob().getName());
    }

    public static boolean isPermittedJobName(String jobName) {
        if (isNull(jobName)) {
            log.debug("OpenLineage event has no job name and should not be emitted");
            return false;
        }
        // Allowlist entries are dot-delimited on both sides (`.command.`), but a real Delta MERGE
        // job name is suffix-less (ends right at the command, no trailing dot). Pad with a trailing
        // dot so both `…execute_merge_into_command` and `…execute_merge_into_command.table` match.
        String normalizedJobName = jobName + ".";
        if (WANTED_EVENT_NAME_SUBSTRINGS.stream().noneMatch(normalizedJobName::contains)) {
            log.debug("OpenLineage event job name {} has no permitted substring and should not be emitted", jobName);
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

    public static void emit(RunEvent event, EventEmitter eventEmitter) {
        if (!shouldEmit(event)) {
            return;
        }

        eventEmitter.emit(event);
    }
}
