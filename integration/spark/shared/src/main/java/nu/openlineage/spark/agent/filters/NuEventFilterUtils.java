package nu.openlineage.spark.agent.filters;

import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.naming.JobNameBuilder;
import lombok.extern.slf4j.Slf4j;
import io.openlineage.client.OpenLineage.RunEvent.EventType;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Objects.nonNull;

@Slf4j
public class NuEventFilterUtils {

    private static final Set<String> WANTED_EVENT_NAME_SUBSTRINGS = new HashSet<>(Arrays.asList(
            ".execute_insert_into_hadoop_fs_relation_command.",
            ".adaptive_spark_plan."
    ));

    private static final Set<String> WANTED_JOB_TYPES = new HashSet<>(Collections.singletonList(
            "SQL_JOB"
    ));

    private static boolean isEventTypeStart(EventType eventType) {
        //We could create spark context parameters to define what kind of events we want to emit,
        //but for now we will just emit the start events
        return EventType.START == eventType;
    }

    private static boolean isJobTypeWanted(String jobType) {
        return WANTED_JOB_TYPES.stream().anyMatch(jobType::equals);
    }

    private static boolean isJobNamePermitted(String jobName) {
        return nonNull(jobName) && WANTED_EVENT_NAME_SUBSTRINGS.stream().anyMatch(jobName::contains);
    }

    public static boolean isDisabled(OpenLineageContext context, EventType eventType, String jobType) {
        String jobName = JobNameBuilder.build(context);
        log.info("isDisabled called with eventType: {}, jobType: {}, jobName: {}", eventType, jobType, jobName);

        if (Stream.of(
                isEventTypeStart(eventType),
                isJobTypeWanted(jobType),
                isJobNamePermitted(jobName)
        ).allMatch(Boolean.TRUE::equals)) {
            log.info("OpenLineage event is not disabled and should be emitted.");
            return false;
        }

        return true;
    }
}
