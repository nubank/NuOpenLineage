/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.facets.NuDefaultFacet;
import io.openlineage.spark.agent.facets.NuItaipuFacet;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.client.OpenLineage.RunFacet;
import io.openlineage.spark.agent.util.ScalaConversionUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import io.openlineage.spark.api.SparkOpenLineageConfig;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

class NuFacetBuilderTest {

  @Test
  void testBuildDefaultFacet() {
      SparkSession sparkSession = SparkSession.builder()
              .master("local[*]")
              .appName("testBuildDefaultFacet")
              .config("spark.openlineage.namespace", "spark_multirepo")
              .getOrCreate();

      OpenLineageContext openLineageContext = OpenLineageContext.builder()
              .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
              .sparkSession(sparkSession)
              .meterRegistry(new SimpleMeterRegistry())
              .openLineageConfig(new SparkOpenLineageConfig())
              .build();

    openLineageContext.getSparkSession().ifPresent(spark -> {
        spark.conf().set("spark.job.name", "nurn:blablabla");
        spark.conf().set("spark.job.resolvedInputsMap", "{\"s3a://path/to/datasetName\":\"datasetName\"}");
    });

    Map<String, String> expectedResolvedInputs = new HashMap<>();
    expectedResolvedInputs.put("s3a://path/to/datasetName", "datasetName");

    NuFacetBuilder builder = new NuFacetBuilder(openLineageContext);
    Map<String, RunFacet> runFacetMap = new HashMap<>();
    builder.build(
            new SparkListenerJobStart(1, 1L, ScalaConversionUtils.asScalaSeqEmpty(), new Properties()),
            runFacetMap::put);
    assertThat(runFacetMap)
            .hasEntrySatisfying(
                    "nu_facets",
                    facet ->
                            assertThat(facet)
                                    .isInstanceOf(NuDefaultFacet.class)
                                    .hasFieldOrPropertyWithValue("jobNurn", "nurn:blablabla")
                                    .hasFieldOrPropertyWithValue("resolvedInputs", expectedResolvedInputs));
  }

    @Test
    void testBuildItaipuFacet() {
        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("testBuildItaipuFacet")
                .config("spark.openlineage.namespace", "spark_itaipu")
                .getOrCreate();

        OpenLineageContext openLineageContext = OpenLineageContext.builder()
                .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
                .sparkSession(sparkSession)
                .meterRegistry(new SimpleMeterRegistry())
                .openLineageConfig(new SparkOpenLineageConfig())
                .build();

        openLineageContext.getSparkSession().ifPresent(spark -> {
            spark.conf().set("spark.nuOpenLineage.inputPath.inputName_1", "s3a://path/to/inputName_1");
            spark.conf().set("spark.nuOpenLineage.inputPath.inputName_2", "s3a://path/to/inputName_2");
            spark.conf().set("spark.nuOpenLineage.inputPath.inputName_3", "s3a://path/to/inputName_3");

            spark.conf().set("spark.nuOpenLineage.outputPath.outputName_1", "s3a://path/to/outputName_1");
            spark.conf().set("spark.nuOpenLineage.outputPath.outputName_2", "s3a://path/to/outputName_2");
            spark.conf().set("spark.nuOpenLineage.outputPath.outputName_3", "s3a://path/to/outputName_3");
        });

        Map<String, String> expectedResolvedInputs = new HashMap<>();
        expectedResolvedInputs.put("inputName_1", "s3a://path/to/inputName_1");
        expectedResolvedInputs.put("inputName_2", "s3a://path/to/inputName_2");
        expectedResolvedInputs.put("inputName_3", "s3a://path/to/inputName_3");


        Map<String, String> expectedResolvedOutputs = new HashMap<>();
        expectedResolvedOutputs.put("outputName_1", "s3a://path/to/outputName_1");
        expectedResolvedOutputs.put("outputName_2", "s3a://path/to/outputName_2");
        expectedResolvedOutputs.put("outputName_3", "s3a://path/to/outputName_3");


        NuFacetBuilder builder = new NuFacetBuilder(openLineageContext);
        Map<String, RunFacet> runFacetMap = new HashMap<>();
        builder.build(
                new SparkListenerJobStart(1, 1L, ScalaConversionUtils.asScalaSeqEmpty(), new Properties()),
                runFacetMap::put);
        assertThat(runFacetMap)
                .hasEntrySatisfying(
                        "nu_facets",
                        facet ->
                                assertThat(facet)
                                        .isInstanceOf(NuItaipuFacet.class)
                                        .hasFieldOrPropertyWithValue("resolvedInputs", expectedResolvedInputs)
                                        .hasFieldOrPropertyWithValue("resolvedOutputs", expectedResolvedOutputs));
    }
}
