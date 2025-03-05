package io.openlineage.spark.agent.facets;

import io.openlineage.client.OpenLineage;

import java.net.URI;

public abstract class NuRunFacet extends  OpenLineage.DefaultRunFacet{
    public NuRunFacet(URI _producer) {
        super(_producer);
    }
}
