package io.openlineage.spark.agent.facets;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

import static io.openlineage.spark.agent.util.NuFacetsUtils.getConfigValues;

@Getter
@Slf4j
public class NuItaipuFacet extends NuRunFacet{

    private static final String RESOLVED_INPUTS_PREFIX = "spark.nuOpenLineage.inputPath.";
    private static final String RESOLVED_OUTPUTS_PREFIX = "spark.nuOpenLineage.outputPath.";

    @JsonProperty("resolvedInputs")
    private Map<String, String> resolvedInputs;

    @JsonProperty("resolvedOutputs")
    private Map<String, String> resolvedOutputs;

    public NuItaipuFacet(@NonNull SparkSession sparkSession) {
        super(Versions.OPEN_LINEAGE_PRODUCER_URI);
        this.resolvedInputs = getConfigValues(RESOLVED_INPUTS_PREFIX, sparkSession.conf());
        this.resolvedOutputs = getConfigValues(RESOLVED_OUTPUTS_PREFIX, sparkSession.conf());
    }
}
