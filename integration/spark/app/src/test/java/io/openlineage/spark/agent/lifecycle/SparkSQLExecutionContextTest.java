/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.spark.agent.EventEmitter;
import io.openlineage.spark.agent.JobMetricsHolder;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import io.openlineage.spark.api.OpenLineageRunStatus;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark.api.VisitedNodes;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.spark.scheduler.JobFailed;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

@ExtendWith(SparkAgentTestExtension.class)
class SparkSQLExecutionContextTest {

  private static final int JOB_ID = 3;

  private final long executionId = 1L;
  private final OpenLineageContext olContext = mock(OpenLineageContext.class);
  private final OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
  private final EventEmitter eventEmitter = mock(EventEmitter.class);
  private final OpenLineageRunEventBuilder runEventBuilder =
      spy(new OpenLineageRunEventBuilder(olContext, mock(OpenLineageEventHandlerFactory.class)));
  private final SparkSQLExecutionContext context =
      new SparkSQLExecutionContext(executionId, eventEmitter, olContext, runEventBuilder);
  private final QueryExecution queryExecution = mock(QueryExecution.class, RETURNS_DEEP_STUBS);

  @AfterEach
  void reset() {
    Mockito.reset(olContext, eventEmitter, queryExecution);
  }

  @BeforeEach
  void setup(SparkSession spark) {
    when(olContext.getQueryExecution()).thenReturn(Optional.of(queryExecution));
    when(olContext.getSparkContext()).thenReturn(Optional.of(spark.sparkContext()));
    when(olContext.getLineageRunStatus()).thenReturn(new OpenLineageRunStatus());
    when(queryExecution.sparkPlan())
        .thenReturn(mock(org.apache.spark.sql.execution.SparkPlan.class));
    when(queryExecution.sparkPlan().sparkContext()).thenReturn(spark.sparkContext());
    when(olContext.getOpenLineage()).thenReturn(openLineage);
    when(olContext.getOpenLineageConfig()).thenReturn(new SparkOpenLineageConfig());
    when(olContext.getMeterRegistry()).thenReturn(new SimpleMeterRegistry());
    when(olContext.getVisitedNodes()).thenReturn(new VisitedNodes());
    when(olContext.getRunUuid())
        .thenReturn(UUID.fromString("8d99e33e-2a1c-4254-9600-18f23435fc3b"));
    AtomicReference<String> jobName = new AtomicReference<>();
    doAnswer(invocation -> {
          jobName.set(invocation.getArgument(0));
          return null;
        })
        .when(olContext)
        .setJobName(any());
    when(olContext.getJobName()).thenAnswer(invocation -> jobName.get());

    when(eventEmitter.getOverriddenAppName()).thenReturn(Optional.of("test-rdd"));
    // Resolves to the allowlisted `setup.execute_insert_into_hadoop_fs_relation_command`.
    when(queryExecution.executedPlan().nodeName())
        .thenReturn("Execute InsertIntoHadoopFsRelationCommand");

    when(eventEmitter.getJobNamespace()).thenReturn("ns_name");
    when(eventEmitter.getApplicationRunId())
        .thenReturn(UUID.fromString("8d99e33e-bbbb-cccc-dddd-18f2343aaaaa"));
    when(eventEmitter.getApplicationJobName()).thenReturn("app-name");
  }

  private SparkListenerJobEnd jobEnd() {
    SparkListenerJobEnd jobEnd = mock(SparkListenerJobEnd.class);
    when(jobEnd.jobId()).thenReturn(JOB_ID);
    return jobEnd;
  }

  private SparkListenerJobEnd failedJobEnd() {
    SparkListenerJobEnd jobEnd = jobEnd();
    when(jobEnd.jobResult()).thenReturn(mock(JobFailed.class));
    return jobEnd;
  }

  private RunEvent singleEmittedEvent() {
    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(RunEvent.class);
    verify(eventEmitter, times(1)).emit(lineageEvent.capture());
    return lineageEvent.getValue();
  }

  private void runFullExecution() {
    context.start(mock(SparkListenerJobStart.class));
    context.start(mock(SparkListenerSQLExecutionStart.class));
    context.end(mock(SparkListenerSQLExecutionEnd.class));
    context.end(jobEnd());
  }

  @Test
  void startEventsAreNeitherBuiltNorEmitted() {
    context.start(mock(SparkListenerSQLExecutionStart.class));
    context.start(mock(SparkListenerJobStart.class));

    verify(runEventBuilder, never()).buildRun(any());
    verify(eventEmitter, never()).emit(any());
  }

  @Test
  void singleCompleteIsSentWhenSqlEndGoesFirst() {
    runFullExecution();

    verify(runEventBuilder, times(1)).buildRun(any());
    assertThat(singleEmittedEvent()).hasFieldOrPropertyWithValue("eventType", EventType.COMPLETE);
  }

  @Test
  void singleCompleteIsSentWhenJobEndGoesFirst() {
    context.start(mock(SparkListenerJobStart.class));
    context.start(mock(SparkListenerSQLExecutionStart.class));
    context.end(jobEnd());
    context.end(mock(SparkListenerSQLExecutionEnd.class));

    verify(runEventBuilder, times(1)).buildRun(any());
    assertThat(singleEmittedEvent()).hasFieldOrPropertyWithValue("eventType", EventType.COMPLETE);
  }

  @Test
  void completeIsSentWhenNoSqlStart() {
    context.start(mock(SparkListenerJobStart.class));
    context.end(jobEnd());

    assertThat(singleEmittedEvent()).hasFieldOrPropertyWithValue("eventType", EventType.COMPLETE);
  }

  @Test
  void completeIsSentWhenNoJob() {
    context.start(mock(SparkListenerSQLExecutionStart.class));
    context.end(mock(SparkListenerSQLExecutionEnd.class));

    assertThat(singleEmittedEvent()).hasFieldOrPropertyWithValue("eventType", EventType.COMPLETE);
  }

  @Test
  void failedJobIsNeitherBuiltNorEmitted() {
    context.start(mock(SparkListenerJobStart.class));
    context.end(failedJobEnd());

    verify(runEventBuilder, never()).buildRun(any());
    verify(eventEmitter, never()).emit(any());
  }

  @Test
  void failedJobFollowedBySqlEndSendsComplete() {
    context.start(mock(SparkListenerJobStart.class));
    context.start(mock(SparkListenerSQLExecutionStart.class));
    context.end(failedJobEnd());
    context.end(mock(SparkListenerSQLExecutionEnd.class));

    assertThat(singleEmittedEvent()).hasFieldOrPropertyWithValue("eventType", EventType.COMPLETE);
  }

  @Test
  void nonAllowlistedJobIsNotBuiltButMetricsAreCleanedUp() {
    when(queryExecution.executedPlan().nodeName()).thenReturn("SomeOtherCommand");
    JobMetricsHolder jobMetrics = mock(JobMetricsHolder.class);
    try (MockedStatic<JobMetricsHolder> holder = mockStatic(JobMetricsHolder.class)) {
      holder.when(JobMetricsHolder::getInstance).thenReturn(jobMetrics);

      context.start(mock(SparkListenerJobStart.class));
      context.end(jobEnd());
    }

    verify(runEventBuilder, never()).buildRun(any());
    verify(eventEmitter, never()).emit(any());
    verify(jobMetrics).cleanUp(JOB_ID);
  }

  @Test
  void plainAdaptiveSparkPlanReadIsNotBuilt() {
    when(queryExecution.executedPlan().nodeName()).thenReturn("AdaptiveSparkPlan");

    context.start(mock(SparkListenerSQLExecutionStart.class));
    context.end(mock(SparkListenerSQLExecutionEnd.class));

    verify(runEventBuilder, never()).buildRun(any());
    verify(eventEmitter, never()).emit(any());
  }

  @Test
  void plainAdaptiveSparkPlanIsBuiltWhenDatasetNamesAreNotAppended() {
    SparkOpenLineageConfig config = new SparkOpenLineageConfig();
    config.getJobName().setAppendDatasetName(false);
    when(olContext.getOpenLineageConfig()).thenReturn(config);
    when(queryExecution.executedPlan().nodeName()).thenReturn("AdaptiveSparkPlan");

    context.start(mock(SparkListenerSQLExecutionStart.class));
    context.end(mock(SparkListenerSQLExecutionEnd.class));

    verify(runEventBuilder, times(1)).buildRun(any());
  }

  @Test
  void metricsAreCleanedUpAfterCompleteIsBuilt() {
    JobMetricsHolder jobMetrics = mock(JobMetricsHolder.class);
    try (MockedStatic<JobMetricsHolder> holder = mockStatic(JobMetricsHolder.class)) {
      holder.when(JobMetricsHolder::getInstance).thenReturn(jobMetrics);

      context.start(mock(SparkListenerJobStart.class));
      context.end(jobEnd());
    }

    InOrder inOrder = Mockito.inOrder(runEventBuilder, jobMetrics, eventEmitter);
    inOrder.verify(runEventBuilder).buildRun(any());
    inOrder.verify(jobMetrics).cleanUp(JOB_ID);
    inOrder.verify(eventEmitter).emit(any());
  }

  @Test
  void jobNameIsResolvedWhenTheRunStarts() {
    context.start(mock(SparkListenerSQLExecutionStart.class));
    when(queryExecution.executedPlan().nodeName()).thenReturn("SomeOtherCommand");
    context.end(mock(SparkListenerSQLExecutionEnd.class));

    assertThat(singleEmittedEvent().getJob().getName())
        .isEqualTo("setup.execute_insert_into_hadoop_fs_relation_command");
  }

  @Test
  void completeHasSqlJobType() {
    runFullExecution();

    OpenLineage.JobTypeJobFacet jobType = singleEmittedEvent().getJob().getFacets().getJobType();
    assertThat(jobType.getJobType()).isEqualTo("SQL_JOB");
    assertThat(jobType.getIntegration()).isEqualTo("SPARK");
    assertThat(jobType.getProcessingType()).isEqualTo("BATCH");
  }

  @Test
  void completeHasStreamingProcessingType() {
    when(queryExecution.optimizedPlan().isStreaming()).thenReturn(true);

    runFullExecution();

    assertThat(singleEmittedEvent().getJob().getFacets().getJobType().getProcessingType())
        .isEqualTo("STREAMING");
  }

  @Test
  void runAndJobAreSet() {
    runFullExecution();

    RunEvent runEvent = singleEmittedEvent();
    assertThat(runEvent.getJob().getName())
        .isEqualTo("setup.execute_insert_into_hadoop_fs_relation_command");
    assertThat(runEvent.getJob().getNamespace()).isEqualTo("ns_name");
    assertThat(runEvent.getRun().getRunId())
        .isEqualTo(UUID.fromString("8d99e33e-2a1c-4254-9600-18f23435fc3b"));
  }

  @Test
  void parentRunFacetIsSet() {
    runFullExecution();

    OpenLineage.ParentRunFacet parentRunFacet =
        singleEmittedEvent().getRun().getFacets().getParent();
    assertThat(parentRunFacet.getJob().getName()).isEqualTo("app_name");
    assertThat(parentRunFacet.getJob().getNamespace()).isEqualTo("ns_name");
    assertThat(parentRunFacet.getRun().getRunId())
        .isEqualTo(UUID.fromString("8d99e33e-bbbb-cccc-dddd-18f2343aaaaa"));
    assertThat(parentRunFacet.getRoot().getJob().getName()).isEqualTo("app-name");
    assertThat(parentRunFacet.getRoot().getJob().getNamespace()).isEqualTo("ns_name");
    assertThat(parentRunFacet.getRoot().getRun().getRunId())
        .isEqualTo(UUID.fromString("8d99e33e-bbbb-cccc-dddd-18f2343aaaaa"));
  }

  @Test
  void parentRunFacetWithRootIsSet() {
    UUID rootUuid = UUID.randomUUID();
    when(eventEmitter.getRootParentRunId()).thenReturn(Optional.of(rootUuid));
    when(eventEmitter.getRootParentJobName()).thenReturn(Optional.of("root_job_name"));
    when(eventEmitter.getRootParentJobNamespace()).thenReturn(Optional.of("root_job_namespace"));

    runFullExecution();

    OpenLineage.ParentRunFacet parentRunFacet =
        singleEmittedEvent().getRun().getFacets().getParent();
    assertThat(parentRunFacet.getJob().getName()).isEqualTo("app_name");
    assertThat(parentRunFacet.getRun().getRunId())
        .isEqualTo(UUID.fromString("8d99e33e-bbbb-cccc-dddd-18f2343aaaaa"));
    assertThat(parentRunFacet.getRoot().getJob().getName()).isEqualTo("root_job_name");
    assertThat(parentRunFacet.getRoot().getJob().getNamespace()).isEqualTo("root_job_namespace");
    assertThat(parentRunFacet.getRoot().getRun().getRunId()).isEqualTo(rootUuid);
  }
}
