/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;

import java.util.NoSuchElementException;
import java.util.Properties;
import lombok.Getter;
import lombok.NonNull;
import io.openlineage.spark.api.OpenLineageContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

/** Captures information related to the Apache Spark job. */
@Getter
@Slf4j
public class NuFacet extends OpenLineage.DefaultRunFacet {
  // @JsonProperty("jobId")
  // @NonNull
  // private Integer jobId;

  // @JsonProperty("jobDescription")
  // private String jobDescription;

  @JsonProperty("jobNurn")
  private String jobNurn;

  private String fetchJobNurn(OpenLineageContext olContext) {
    if (olContext.getSparkSession().isPresent()) {
      SparkSession sparkSession = olContext.getSparkSession().get();
      try {
        return sparkSession.conf().get("spark.job.name");
      } catch (NoSuchElementException e) {
        log.warn("spark.job.name property not found in the context");
        return null;
      }
    }

    log.warn("spark.job.name property not found because the SparkContext could not be retrieved from OpenLineageContext");
    return null;
  }

  public NuFacet(@NonNull OpenLineageContext olContext) {
    super(Versions.OPEN_LINEAGE_PRODUCER_URI);
    this.jobNurn = fetchJobNurn(olContext);
  }
}
