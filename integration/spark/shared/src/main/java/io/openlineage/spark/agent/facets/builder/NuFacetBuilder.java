/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import io.openlineage.spark.agent.facets.NuDefaultFacet;
import io.openlineage.spark.agent.facets.NuItaipuFacet;
import io.openlineage.spark.agent.facets.NuRunFacet;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.function.BiConsumer;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SparkSession;


public class NuFacetBuilder
    extends CustomFacetBuilder<SparkListenerEvent, NuRunFacet> {

  private static final String ITAIPU_LINEAGE_NAMESPACE = "spark_itaipu";

  private final OpenLineageContext olContext;

  public NuFacetBuilder(OpenLineageContext olContext) {
    this.olContext = olContext;
  }

  private NuRunFacet getFacet(SparkSession spark) {
   String lineageNamespace = spark.conf().get("spark.openlineage.namespace", null);
    if (ITAIPU_LINEAGE_NAMESPACE.equals(lineageNamespace)) {
      return new NuItaipuFacet(spark);
    }
    return new NuDefaultFacet(spark);
  }

  @Override
  protected void build(SparkListenerEvent event, BiConsumer<String, ? super NuRunFacet> consumer) {
    olContext.getSparkSession().ifPresent(spark -> {
      consumer.accept("nu_facets", getFacet(spark));
    });
  }
}
