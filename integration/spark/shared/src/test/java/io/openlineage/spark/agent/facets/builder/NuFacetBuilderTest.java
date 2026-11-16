/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.facets.NuDefaultFacet;
import io.openlineage.spark.agent.facets.NuItaipuFacet;
import io.openlineage.spark.agent.facets.NuRunFacet;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.agent.util.ScalaConversionUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import io.openlineage.spark.api.SparkOpenLineageConfig;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class NuFacetBuilderTest {

  private static final String DAG_RUN_TYPE_KEY = "spark.job.dagRunType";

  private SparkSession sparkSession;

  // getOrCreate shares one SparkSession across tests, so this key would otherwise leak into later tests.
  @AfterEach
  void unsetDagRunType() {
    if (sparkSession != null) {
      sparkSession.conf().unset(DAG_RUN_TYPE_KEY);
    }
  }

  @Test
  void testBuildDefaultFacet() {
    sparkSession = sparkSession("testBuildDefaultFacet", "spark_multirepo");
    sparkSession.conf().set("spark.job.name", "nurn:blablabla");
    sparkSession.conf().set(DAG_RUN_TYPE_KEY, "production");
    sparkSession.conf().set("spark.job.resolvedInputsMap", "{\"s3a://path/to/datasetName\":\"datasetName\"}");

    Map<String, String> expectedResolvedInputs = new HashMap<>();
    expectedResolvedInputs.put("s3a://path/to/datasetName", "datasetName");

    assertThat(buildNuFacet(sparkSession))
            .isInstanceOf(NuDefaultFacet.class)
            .hasFieldOrPropertyWithValue("jobNurn", "nurn:blablabla")
            .hasFieldOrPropertyWithValue("dagRunType", "production")
            .hasFieldOrPropertyWithValue("resolvedInputs", expectedResolvedInputs);
  }

  @ParameterizedTest
  @ValueSource(strings = {"staging", "production"})
  void testBuildDefaultFacetWithDagRunType(String dagRunType) {
    sparkSession = sparkSession("testBuildDefaultFacetWithDagRunType", "spark_multirepo");
    sparkSession.conf().set(DAG_RUN_TYPE_KEY, dagRunType);

    NuRunFacet facet = buildNuFacet(sparkSession);

    assertThat(facet)
            .isInstanceOf(NuDefaultFacet.class)
            .hasFieldOrPropertyWithValue("dagRunType", dagRunType);
    assertThat(OpenLineageClientUtils.toJson(facet))
            .contains("\"dagRunType\":\"" + dagRunType + "\"");
  }

  @Test
  void testBuildDefaultFacetWithoutDagRunType() {
    sparkSession = sparkSession("testBuildDefaultFacetWithoutDagRunType", "spark_multirepo");
    sparkSession.conf().unset(DAG_RUN_TYPE_KEY);

    NuRunFacet facet = buildNuFacet(sparkSession);

    assertThat(facet)
            .isInstanceOf(NuDefaultFacet.class)
            .hasFieldOrPropertyWithValue("dagRunType", null);
    assertThat(OpenLineageClientUtils.toJson(facet)).doesNotContain("dagRunType");
  }

  @Test
  void testBuildItaipuFacet() {
    sparkSession = sparkSession("testBuildItaipuFacet", "spark_itaipu");
    sparkSession.conf().set("spark.nuOpenLineage.inputPath.inputName_1", "s3a://path/to/inputName_1");
    sparkSession.conf().set("spark.nuOpenLineage.inputPath.inputName_2", "s3a://path/to/inputName_2");
    sparkSession.conf().set("spark.nuOpenLineage.inputPath.inputName_3", "s3a://path/to/inputName_3");

    sparkSession.conf().set("spark.nuOpenLineage.outputPath.outputName_1", "s3a://path/to/outputName_1");
    sparkSession.conf().set("spark.nuOpenLineage.outputPath.outputName_2", "s3a://path/to/outputName_2");
    sparkSession.conf().set("spark.nuOpenLineage.outputPath.outputName_3", "s3a://path/to/outputName_3");

    Map<String, String> expectedResolvedInputs = new HashMap<>();
    expectedResolvedInputs.put("s3a://path/to/inputName_1", "inputName_1");
    expectedResolvedInputs.put("s3a://path/to/inputName_2", "inputName_2");
    expectedResolvedInputs.put("s3a://path/to/inputName_3", "inputName_3");

    Map<String, String> expectedResolvedOutputs = new HashMap<>();
    expectedResolvedOutputs.put("s3a://path/to/outputName_1", "outputName_1");
    expectedResolvedOutputs.put("s3a://path/to/outputName_2", "outputName_2");
    expectedResolvedOutputs.put("s3a://path/to/outputName_3", "outputName_3");

    assertThat(buildNuFacet(sparkSession))
            .isInstanceOf(NuItaipuFacet.class)
            .hasFieldOrPropertyWithValue("resolvedInputs", expectedResolvedInputs)
            .hasFieldOrPropertyWithValue("resolvedOutputs", expectedResolvedOutputs);
  }

  private static SparkSession sparkSession(String appName, String lineageNamespace) {
    return SparkSession.builder()
            .master("local[*]")
            .appName(appName)
            .config("spark.openlineage.namespace", lineageNamespace)
            .getOrCreate();
  }

  private static NuRunFacet buildNuFacet(SparkSession sparkSession) {
    OpenLineageContext openLineageContext = OpenLineageContext.builder()
            .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
            .sparkSession(sparkSession)
            .meterRegistry(new SimpleMeterRegistry())
            .openLineageConfig(new SparkOpenLineageConfig())
            .build();
    Map<String, NuRunFacet> runFacetMap = new HashMap<>();
    new NuFacetBuilder(openLineageContext).build(
            new SparkListenerJobStart(1, 1L, ScalaConversionUtils.asScalaSeqEmpty(), new Properties()),
            runFacetMap::put);
    return runFacetMap.get("nu_facets");
  }
}
