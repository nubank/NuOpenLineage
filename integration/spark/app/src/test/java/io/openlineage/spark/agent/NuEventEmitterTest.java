/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.openlineage.client.OpenLineage;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * MDP-566: a Delta MERGE (job name segment `.execute_merge_into_command.`) was silently dropped
 * here because it wasn't in WANTED_EVENT_NAME_SUBSTRINGS, independent of the identical allowlist in
 * nubank/data-lineage's enhancement app. Locally reproduced that this is exactly the job-name
 * segment a real Delta 3.3.2 / Spark 3.5.3 MERGE produces (see MDP-566 for the reproduction).
 */
class NuEventEmitterTest {

  private static final OpenLineage OL =
      new OpenLineage(URI.create("https://github.com/nubank/NuOpenLineage"));

  private OpenLineage.RunEvent buildEvent(
      String jobName, String jobType, OpenLineage.RunEvent.EventType eventType) {
    OpenLineage.JobTypeJobFacet jobTypeFacet =
        OL.newJobTypeJobFacetBuilder()
            .jobType(jobType)
            .processingType("BATCH")
            .integration("SPARK")
            .build();
    OpenLineage.JobFacets jobFacets = OL.newJobFacetsBuilder().jobType(jobTypeFacet).build();
    OpenLineage.Job job = OL.newJob("namespace", jobName, jobFacets);
    OpenLineage.Run run = OL.newRun(UUID.randomUUID(), OL.newRunFacetsBuilder().build());

    return OL.newRunEventBuilder()
        .eventTime(ZonedDateTime.now())
        .eventType(eventType)
        .run(run)
        .job(job)
        .build();
  }

  @Test
  void deltaMergeCompleteEventIsEmitted() {
    OpenLineage.RunEvent event =
        buildEvent(
            "app.execute_merge_into_command.target_table",
            "SQL_JOB",
            OpenLineage.RunEvent.EventType.COMPLETE);
    EventEmitter eventEmitter = mock(EventEmitter.class);

    NuEventEmitter.emit(event, eventEmitter);

    verify(eventEmitter, times(1)).emit(any());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "app.execute_insert_into_hadoop_fs_relation_command.target_table",
        "app.adaptive_spark_plan.target_table",
        "app.execute_save_into_data_source_command.target_table",
        "app.execute_merge_into_command.target_table"
      })
  void allowlistedCompleteEventsAreEmitted(String jobName) {
    OpenLineage.RunEvent event =
        buildEvent(jobName, "SQL_JOB", OpenLineage.RunEvent.EventType.COMPLETE);
    EventEmitter eventEmitter = mock(EventEmitter.class);

    NuEventEmitter.emit(event, eventEmitter);

    verify(eventEmitter, times(1)).emit(any());
  }

  @Test
  void nonAllowlistedJobNameIsDiscarded() {
    OpenLineage.RunEvent event =
        buildEvent(
            "app.some_other_command.target_table",
            "SQL_JOB",
            OpenLineage.RunEvent.EventType.COMPLETE);
    EventEmitter eventEmitter = mock(EventEmitter.class);

    NuEventEmitter.emit(event, eventEmitter);

    verify(eventEmitter, never()).emit(any());
  }

  @Test
  void nonSqlJobTypeIsDiscardedEvenWithAllowlistedName() {
    OpenLineage.RunEvent event =
        buildEvent(
            "app.execute_merge_into_command.target_table",
            "RDD_JOB",
            OpenLineage.RunEvent.EventType.COMPLETE);
    EventEmitter eventEmitter = mock(EventEmitter.class);

    NuEventEmitter.emit(event, eventEmitter);

    verify(eventEmitter, never()).emit(any());
  }

  @Test
  void runningEventIsDiscardedEvenWithAllowlistedName() {
    OpenLineage.RunEvent event =
        buildEvent(
            "app.execute_merge_into_command.target_table",
            "SQL_JOB",
            OpenLineage.RunEvent.EventType.RUNNING);
    EventEmitter eventEmitter = mock(EventEmitter.class);

    NuEventEmitter.emit(event, eventEmitter);

    verify(eventEmitter, never()).emit(any());
  }
}
