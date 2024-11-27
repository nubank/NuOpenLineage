/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;

import java.util.*;

import lombok.Getter;
import lombok.NonNull;
import io.openlineage.spark.api.OpenLineageContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

import static io.openlineage.spark.agent.util.NuFacetsUtils.getConfigValue;
import static io.openlineage.spark.agent.util.NuFacetsUtils.parseJsonToMap;

/** Captures information related to the Apache Spark job. */
@Getter
@Slf4j
public class NuFacet extends OpenLineage.DefaultRunFacet {

  @JsonProperty("jobNurn")
  private String jobNurn;

  /**
   * Resolved inputs for the job.
   * Map of input dataset NURNs by their location path
   */
  @JsonProperty("resolvedInputs")
  private Map<String, String> resolvedInputs;

  private String getJobNurn(SparkSession sparkSession) {
    return getConfigValue("spark.job.name", sparkSession);
  }

  private Map<String, String> getResolvedInputs(SparkSession sparkSession) {
    String resolvedInputsJson = getConfigValue("spark.job.resolvedInputsMap", sparkSession);
    try {
      return parseJsonToMap(resolvedInputsJson);
    } catch (JsonProcessingException e) {
      log.warn("Error parsing resolvedInputsJson JSON", e);
      return null;
    }
  }

  public NuFacet(@NonNull OpenLineageContext olContext) {
    super(Versions.OPEN_LINEAGE_PRODUCER_URI);
    if (olContext.getSparkSession().isPresent()) {
      SparkSession sparkSession = olContext.getSparkSession().get();
      this.jobNurn = getJobNurn(sparkSession);
      this.resolvedInputs = getResolvedInputs(sparkSession);
    }
  }
}
