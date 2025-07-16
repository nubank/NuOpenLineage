/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.spark.agent.Versions;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Parser for static analysis of Spark query execution plans.
 * This class can parse JSON representations of Spark execution plans and generate OpenLineage events.
 */
@Slf4j
public class StaticQueryExecutionParser {
    
    private final OpenLineage openLineage;
    private final ObjectMapper objectMapper;
    private final Map<String, String> datasetIdCache;
    
    public StaticQueryExecutionParser() {
        // Create producer URI without relying on static initialization
        URI producerUri = createProducerUri();
        this.openLineage = new OpenLineage(producerUri);
        this.objectMapper = new ObjectMapper();
        this.datasetIdCache = new HashMap<>();
    }
    
    private URI createProducerUri() {
        try {
            String version = getVersionSafely();
            return URI.create(
                String.format("https://github.com/OpenLineage/OpenLineage/tree/%s/integration/spark", version));
        } catch (Exception e) {
            log.warn("Failed to load version from properties, using default URI: {}", e.getMessage());
            return URI.create("https://github.com/OpenLineage/OpenLineage/tree/main/integration/spark");
        }
    }
    
    private String getVersionSafely() {
        try {
            Properties properties = new Properties();
            InputStream is = this.getClass().getResourceAsStream("/version.properties");
            if (is != null) {
                properties.load(is);
                return properties.getProperty("version", "main");
            }
        } catch (Exception e) {
            log.debug("Could not load version properties: {}", e.getMessage());
        }
        return "main";
    }
    
    /**
     * Main entry point to parse a JSON execution plan file and generate OpenLineage event
     */
    public OpenLineage.RunEvent parseExecutionPlanFile(String jsonFilePath) throws IOException {
        log.info("Parsing execution plan from file: {}", jsonFilePath);
        
        File file = new File(jsonFilePath);
        if (!file.exists()) {
            throw new IOException("File not found: " + jsonFilePath);
        }
        
        JsonNode planRoot = objectMapper.readTree(file);
        return parseExecutionPlan(planRoot, extractJobNameFromPath(jsonFilePath));
    }
    
    /**
     * Parse a JSON execution plan and generate OpenLineage event
     */
    public OpenLineage.RunEvent parseExecutionPlan(JsonNode planRoot, String jobName) {
        try {
            log.info("Starting static execution plan analysis for job: {}", jobName);
            
            // Clear cache for new analysis
            datasetIdCache.clear();
            
            // Parse the execution plan structure
            ExecutionPlanContext context = analyzePlan(planRoot);
            
            // Extract input datasets
            List<OpenLineage.InputDataset> inputDatasets = extractInputDatasets(context);
            
            // Extract output dataset
            OpenLineage.OutputDataset outputDataset = extractOutputDataset(context, jobName);
            
            // Build column lineage
            OpenLineage.ColumnLineageDatasetFacet columnLineage = buildColumnLineage(context);
            
            // Create the complete OpenLineage event
            OpenLineage.RunEvent event = createOpenLineageEvent(
                jobName, inputDatasets, outputDataset, columnLineage);
            
            log.info("Successfully generated OpenLineage event for job: {}", jobName);
            return event;
            
        } catch (Exception e) {
            log.error("Error parsing execution plan for job: {}", jobName, e);
            throw new RuntimeException("Failed to parse execution plan", e);
        }
    }
    
    /**
     * Emit the OpenLineage event using the provided client
     */
    public void emitEvent(OpenLineage.RunEvent event, OpenLineageClient client) {
        try {
            log.info("Emitting OpenLineage event for job: {}", event.getJob().getName());
            client.emit(event);
            log.info("Successfully emitted OpenLineage event");
        } catch (Exception e) {
            log.error("Failed to emit OpenLineage event", e);
            throw new RuntimeException("Event emission failed", e);
        }
    }
    
    /**
     * Analyze the execution plan and build context
     */
    private ExecutionPlanContext analyzePlan(JsonNode planRoot) {
        ExecutionPlanContext context = new ExecutionPlanContext();
        
        // Handle array-based plan structure where nodes reference each other by index
        if (planRoot.isArray()) {
            // Store all nodes in context for reference resolution
            List<JsonNode> allNodes = new ArrayList<>();
            for (JsonNode node : planRoot) {
                allNodes.add(node);
            }
            context.setAllNodesArray(allNodes);
            
            // Analyze all nodes in the array
            for (int i = 0; i < allNodes.size(); i++) {
                analyzeNodeWithIndex(allNodes.get(i), context, 0, i);
            }
        } else {
            // Handle single node or nested structure
            JsonNode mainPlan = findMainPlan(planRoot);
            if (mainPlan != null) {
                analyzeNode(mainPlan, context, 0);
            }
        }
        
        log.info("Analysis complete. Found {} input sources, {} total nodes", 
                context.getInputSources().size(), context.getAllNodes().size());
        
        return context;
    }
    
    /**
     * Find the main execution plan node in the JSON structure
     */
    private JsonNode findMainPlan(JsonNode root) {
        // Handle different JSON structures
        if (root.has("class")) {
            return root; // Direct plan node
        }
        
        // Look for nested plan structures
        if (root.isArray() && root.size() > 0) {
            return root.get(0);
        }
        
        // Look for specific keys that might contain the plan
        String[] planKeys = {"plan", "logicalPlan", "optimizedPlan", "executedPlan"};
        for (String key : planKeys) {
            if (root.has(key)) {
                return root.get(key);
            }
        }
        
        return root;
    }
    
    /**
     * Recursively analyze a plan node
     */
    private void analyzeNode(JsonNode node, ExecutionPlanContext context, int depth) {
        if (node == null || node.isNull()) return;
        
        String nodeClass = node.path("class").asText();
        if (nodeClass.isEmpty()) return;
        
        PlanNode planNode = new PlanNode(nodeClass, node, depth);
        context.addNode(planNode);
        
        // Check if this is an input source
        if (isInputSource(nodeClass)) {
            context.addInputSource(planNode);
            log.debug("Found input source at depth {}: {}", depth, nodeClass);
        }
        
        // Set root node (typically the first/top-level node)
        if (context.getRootNode() == null || depth == 0) {
            context.setRootNode(planNode);
        }
        
        // Recursively analyze children based on node structure
        analyzeChildNodes(node, context, depth + 1);
    }
    
    /**
     * Analyze a plan node with support for array-based index references
     */
    private void analyzeNodeWithIndex(JsonNode node, ExecutionPlanContext context, int depth, int nodeIndex) {
        if (node == null || node.isNull()) return;
        
        String nodeClass = node.path("class").asText();
        if (nodeClass.isEmpty()) return;
        
        PlanNode planNode = new PlanNode(nodeClass, node, depth);
        context.addNode(planNode);
        
        // Check if this is an input source
        if (isInputSource(nodeClass)) {
            context.addInputSource(planNode);
            log.debug("Found input source at depth {}: {}", depth, nodeClass);
        }
        
        // Set root node (first node in array)
        if (context.getRootNode() == null && nodeIndex == 0) {
            context.setRootNode(planNode);
        }
        
        // Handle indexed references for array-based plans
        analyzeIndexedChildren(node, context, depth + 1);
    }
    
    /**
     * Check if a node class represents an input source
     */
    private boolean isInputSource(String nodeClass) {
        return nodeClass.equals("org.apache.spark.sql.execution.LogicalRDD") ||
               nodeClass.contains("LogicalRDD") ||
               nodeClass.contains("DataSource") ||
               nodeClass.contains("HadoopRDD") ||
               nodeClass.contains("FileScanRDD");
    }
    
    /**
     * Analyze child nodes of a plan node
     */
    private void analyzeChildNodes(JsonNode node, ExecutionPlanContext context, int depth) {
        // Handle different child node patterns
        String[] childKeys = {"children", "child", "left", "right", "input", "inputs"};
        
        for (String key : childKeys) {
            JsonNode childNode = node.path(key);
            if (childNode.isArray()) {
                for (JsonNode child : childNode) {
                    analyzeNode(child, context, depth);
                }
            } else if (!childNode.isMissingNode()) {
                analyzeNode(childNode, context, depth);
            }
        }
    }
    
    /**
     * Analyze child nodes with support for indexed references
     */
    private void analyzeIndexedChildren(JsonNode node, ExecutionPlanContext context, int depth) {
        // For array-based structures, we only need to handle traditional nested structures
        // since all nodes in the array are already processed individually
        analyzeChildNodes(node, context, depth);
    }
    
    /**
     * Extract input datasets from the execution plan
     */
    private List<OpenLineage.InputDataset> extractInputDatasets(ExecutionPlanContext context) {
        List<OpenLineage.InputDataset> inputs = new ArrayList<>();
        
        for (PlanNode inputSource : context.getInputSources()) {
            String datasetId = generateDatasetId(inputSource);
            OpenLineage.SchemaDatasetFacet schema = extractSchemaFromNode(inputSource.getNode());
            
            OpenLineage.InputDataset dataset = openLineage.newInputDatasetBuilder()
                .namespace("memory://dataframes")
                .name(datasetId)
                .facets(openLineage.newDatasetFacetsBuilder()
                    .schema(schema)
                    .build())
                .build();
            
            inputs.add(dataset);
            log.debug("Created input dataset: {}", datasetId);
        }
        
        // If no input sources found, create a mock input
        if (inputs.isEmpty()) {
            log.warn("No input sources found, creating mock input dataset");
            inputs.add(createMockInputDataset());
        }
        
        return inputs;
    }
    
    /**
     * Create a mock input dataset when none are found
     */
    private OpenLineage.InputDataset createMockInputDataset() {
        return openLineage.newInputDatasetBuilder()
            .namespace("memory://dataframes")
            .name("mock_input_dataset")
            .facets(openLineage.newDatasetFacetsBuilder()
                .schema(openLineage.newSchemaDatasetFacetBuilder()
                    .fields(Collections.emptyList())
                    .build())
                .build())
            .build();
    }
    
    /**
     * Extract output dataset from the execution plan
     */
    private OpenLineage.OutputDataset extractOutputDataset(ExecutionPlanContext context, String jobName) {
        String outputId = jobName + "_output";
        
        // Extract schema from the final output (first node in array-based plans)
        OpenLineage.SchemaDatasetFacet schema = extractOutputSchema(context);
        
        return openLineage.newOutputDatasetBuilder()
            .namespace("memory://dataframes")
            .name(outputId)
            .facets(openLineage.newDatasetFacetsBuilder()
                .schema(schema)
                .build())
            .build();
    }
    
    /**
     * Build column lineage facet
     */
    private OpenLineage.ColumnLineageDatasetFacet buildColumnLineage(ExecutionPlanContext context) {
        OpenLineage.ColumnLineageDatasetFacetFieldsBuilder fieldsBuilder = 
            openLineage.newColumnLineageDatasetFacetFieldsBuilder();
        
        // Extract output columns from the final transformation (first node)
        List<String> outputColumns = extractOutputColumnNames(context);
        
        // For each output column, trace back to input columns
        for (String outputColumn : outputColumns) {
            OpenLineage.ColumnLineageDatasetFacetFieldsAdditional field = traceColumnLineageImproved(
                outputColumn, context);
            if (field != null) {
                fieldsBuilder.put(outputColumn, field);
            }
        }
        
        OpenLineage.ColumnLineageDatasetFacetFields fields = fieldsBuilder.build();
        if (fields.getAdditionalProperties().isEmpty()) {
            return null;
        }
        
        return openLineage.newColumnLineageDatasetFacetBuilder()
            .fields(fields)
            .build();
    }
    
    /**
     * Improved column lineage tracing for a specific column
     */
    private OpenLineage.ColumnLineageDatasetFacetFieldsAdditional traceColumnLineageImproved(
            String columnName, ExecutionPlanContext context) {
        
        List<OpenLineage.InputField> inputFields = new ArrayList<>();
        Set<String> processedDatasets = new HashSet<>();
        
        // Trace through input sources to find columns with matching names
        for (PlanNode inputSource : context.getInputSources()) {
            String datasetId = generateDatasetId(inputSource);
            
            // Avoid duplicates for the same dataset
            if (processedDatasets.contains(datasetId)) {
                continue;
            }
            processedDatasets.add(datasetId);
            
            List<String> inputColumns = extractColumnNames(inputSource.getNode());
            if (inputColumns.contains(columnName)) {
                OpenLineage.InputField inputField = 
                    openLineage.newInputFieldBuilder()
                        .namespace("memory://dataframes")
                        .name(datasetId)
                        .field(columnName)
                        .build();
                
                inputFields.add(inputField);
            }
        }
        
        // If no direct matches found, try to trace through transformations
        if (inputFields.isEmpty()) {
            inputFields = traceColumnThroughTransformations(columnName, context);
        }
        
        if (inputFields.isEmpty()) {
            return null;
        }
        
        return openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
            .inputFields(inputFields)
            .transformationDescription("Column lineage traced through Spark execution plan")
            .transformationType(inputFields.size() == 1 ? "DIRECT" : "INDIRECT")
            .build();
    }
    
    /**
     * Trace column lineage through transformations when direct matching fails
     */
    private List<OpenLineage.InputField> traceColumnThroughTransformations(String columnName, ExecutionPlanContext context) {
        List<OpenLineage.InputField> inputFields = new ArrayList<>();
        Set<String> processedDatasets = new HashSet<>();
        
        // For complex transformations, fall back to mapping all input columns
        // This is a simplified approach - in practice, you'd analyze the transformation logic
        for (PlanNode inputSource : context.getInputSources()) {
            String datasetId = generateDatasetId(inputSource);
            
            if (processedDatasets.contains(datasetId)) {
                continue;
            }
            processedDatasets.add(datasetId);
            
            List<String> inputColumns = extractColumnNames(inputSource.getNode());
            if (!inputColumns.isEmpty()) {
                // Use the first column as a representative (simplified approach)
                String firstColumn = inputColumns.get(0);
                
                OpenLineage.InputField inputField = 
                    openLineage.newInputFieldBuilder()
                        .namespace("memory://dataframes")
                        .name(datasetId)
                        .field(firstColumn)
                        .build();
                
                inputFields.add(inputField);
            }
        }
        
        return inputFields;
    }
    
    /**
     * Trace lineage for a specific column (legacy method)
     */
    private OpenLineage.ColumnLineageDatasetFacetFieldsAdditional traceColumnLineage(
            String columnName, PlanNode node, ExecutionPlanContext context) {
        
        List<OpenLineage.InputField> inputFields = new ArrayList<>();
        
        // Simple lineage tracing - can be enhanced for complex transformations
        for (PlanNode inputSource : context.getInputSources()) {
            List<String> inputColumns = extractColumnNames(inputSource.getNode());
            if (inputColumns.contains(columnName)) {
                String datasetId = generateDatasetId(inputSource);
                
                OpenLineage.InputField inputField = 
                    openLineage.newInputFieldBuilder()
                        .namespace("memory://dataframes")
                        .name(datasetId)
                        .field(columnName)
                        .build();
                
                inputFields.add(inputField);
            }
        }
        
        if (inputFields.isEmpty()) {
            return null;
        }
        
        return openLineage.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
            .inputFields(inputFields)
            .transformationDescription("Column lineage from execution plan")
            .transformationType("DIRECT")
            .build();
    }
    
    /**
     * Extract output schema from the execution plan context
     */
    private OpenLineage.SchemaDatasetFacet extractOutputSchema(ExecutionPlanContext context) {
        List<OpenLineage.SchemaDatasetFacetFields> fields = new ArrayList<>();
        
        // For array-based plans, extract schema from the first node (final output)
        if (context.getAllNodesArray() != null && !context.getAllNodesArray().isEmpty()) {
            JsonNode outputNode = context.getAllNodesArray().get(0);
            extractSchemaFromProjectList(outputNode, fields);
        } else if (context.getRootNode() != null) {
            // Fallback to root node
            extractSchemaFromOutput(context.getRootNode().getNode(), fields);
        }
        
        return openLineage.newSchemaDatasetFacetBuilder()
            .fields(fields)
            .build();
    }
    
    /**
     * Extract schema from a plan node
     */
    private OpenLineage.SchemaDatasetFacet extractSchemaFromNode(JsonNode node) {
        List<OpenLineage.SchemaDatasetFacetFields> fields = new ArrayList<>();
        
        // Try different ways to extract schema
        extractSchemaFromOutput(node, fields);
        
        if (fields.isEmpty()) {
            extractSchemaFromAttributes(node, fields);
        }
        
        return openLineage.newSchemaDatasetFacetBuilder()
            .fields(fields)
            .build();
    }
    
    /**
     * Extract schema from projectList field (for Project nodes)
     */
    private void extractSchemaFromProjectList(JsonNode node, List<OpenLineage.SchemaDatasetFacetFields> fields) {
        JsonNode projectList = node.path("projectList");
        if (projectList.isArray()) {
            for (JsonNode projectionArray : projectList) {
                if (projectionArray.isArray() && projectionArray.size() > 0) {
                    // Each projection is an array, get the first element which describes the output column
                    JsonNode columnNode = projectionArray.get(0);
                    OpenLineage.SchemaDatasetFacetFields field = extractFieldFromAttributeReference(columnNode);
                    if (field != null) {
                        fields.add(field);
                    }
                }
            }
        }
    }
    
    /**
     * Extract schema from output field
     */
    private void extractSchemaFromOutput(JsonNode node, List<OpenLineage.SchemaDatasetFacetFields> fields) {
        JsonNode output = node.path("output");
        if (output.isArray()) {
            for (JsonNode outputItem : output) {
                if (outputItem.isArray()) {
                    for (JsonNode fieldNode : outputItem) {
                        OpenLineage.SchemaDatasetFacetFields field = extractFieldFromAttributeReference(fieldNode);
                        if (field != null) {
                            fields.add(field);
                        }
                    }
                } else {
                    OpenLineage.SchemaDatasetFacetFields field = extractFieldFromAttributeReference(outputItem);
                    if (field != null) {
                        fields.add(field);
                    }
                }
            }
        }
    }
    
    /**
     * Extract schema from attributes field
     */
    private void extractSchemaFromAttributes(JsonNode node, List<OpenLineage.SchemaDatasetFacetFields> fields) {
        JsonNode attributes = node.path("attributes");
        if (attributes.isArray()) {
            for (JsonNode attr : attributes) {
                OpenLineage.SchemaDatasetFacetFields field = extractFieldFromAttributeReference(attr);
                if (field != null) {
                    fields.add(field);
                }
            }
        }
    }
    
    /**
     * Extract field information from an AttributeReference node
     */
    private OpenLineage.SchemaDatasetFacetFields extractFieldFromAttributeReference(JsonNode fieldNode) {
        // Handle both direct AttributeReference and Alias nodes
        String name = fieldNode.path("name").asText();
        String dataType = fieldNode.path("dataType").asText();
        boolean nullable = fieldNode.path("nullable").asBoolean(true);
        
        // Handle Alias nodes that wrap AttributeReference
        String nodeClass = fieldNode.path("class").asText();
        if (nodeClass.contains("Alias") && fieldNode.has("child")) {
            // For Alias nodes, use the alias name but get type from the child
            JsonNode childRef = fieldNode.path("child");
            if (childRef.isInt()) {
                // This is a reference to another node by index - we'll keep the alias name
                // but use a generic type for now
                dataType = "unknown";
            }
        }
        
        if (name.isEmpty()) {
            return null;
        }
        
        return openLineage.newSchemaDatasetFacetFieldsBuilder()
            .name(name)
            .type(dataType.isEmpty() ? "unknown" : dataType)
            .description("Column extracted from execution plan")
            .build();
    }
    
    /**
     * Extract output column names from the execution plan context
     */
    private List<String> extractOutputColumnNames(ExecutionPlanContext context) {
        List<String> columnNames = new ArrayList<>();
        
        // For array-based plans, extract column names from the first node (final output)
        if (context.getAllNodesArray() != null && !context.getAllNodesArray().isEmpty()) {
            JsonNode outputNode = context.getAllNodesArray().get(0);
            extractColumnNamesFromProjectList(outputNode, columnNames);
        } else if (context.getRootNode() != null) {
            // Fallback to root node
            extractColumnNamesFromOutput(context.getRootNode().getNode(), columnNames);
        }
        
        return columnNames;
    }
    
    /**
     * Extract column names from projectList field
     */
    private void extractColumnNamesFromProjectList(JsonNode node, List<String> columnNames) {
        JsonNode projectList = node.path("projectList");
        if (projectList.isArray()) {
            for (JsonNode projectionArray : projectList) {
                if (projectionArray.isArray() && projectionArray.size() > 0) {
                    // Each projection is an array, get the first element which describes the output column
                    JsonNode columnNode = projectionArray.get(0);
                    String name = columnNode.path("name").asText();
                    if (!name.isEmpty()) {
                        columnNames.add(name);
                    }
                }
            }
        }
    }
    
    /**
     * Extract column names from a node
     */
    private List<String> extractColumnNames(JsonNode node) {
        List<String> columnNames = new ArrayList<>();
        
        // Try output first
        extractColumnNamesFromOutput(node, columnNames);
        
        // Try attributes if no output found
        if (columnNames.isEmpty()) {
            extractColumnNamesFromAttributes(node, columnNames);
        }
        
        return columnNames;
    }
    
    /**
     * Extract column names from output field
     */
    private void extractColumnNamesFromOutput(JsonNode node, List<String> columnNames) {
        JsonNode output = node.path("output");
        if (output.isArray()) {
            for (JsonNode outputItem : output) {
                if (outputItem.isArray()) {
                    for (JsonNode fieldNode : outputItem) {
                        String name = fieldNode.path("name").asText();
                        if (!name.isEmpty()) {
                            columnNames.add(name);
                        }
                    }
                } else {
                    String name = outputItem.path("name").asText();
                    if (!name.isEmpty()) {
                        columnNames.add(name);
                    }
                }
            }
        }
    }
    
    /**
     * Extract column names from attributes field
     */
    private void extractColumnNamesFromAttributes(JsonNode node, List<String> columnNames) {
        JsonNode attributes = node.path("attributes");
        if (attributes.isArray()) {
            for (JsonNode attr : attributes) {
                String name = attr.path("name").asText();
                if (!name.isEmpty()) {
                    columnNames.add(name);
                }
            }
        }
    }
    
    /**
     * Generate a unique dataset ID for input sources
     */
    private String generateDatasetId(PlanNode inputSource) {
        // Create a deterministic ID based on node structure
        String nodeClass = inputSource.getNodeClass();
        JsonNode node = inputSource.getNode();
        
        // Use output schema hash for uniqueness
        List<String> columns = extractColumnNames(node);
        String schemaSignature = String.join(",", columns.stream().sorted().collect(Collectors.toList()));
        
        // Create a hash-based ID
        int hash = (nodeClass + schemaSignature + inputSource.getDepth()).hashCode();
        String datasetId = "input_dataset_" + Math.abs(hash);
        
        // Cache for consistency
        datasetIdCache.put(schemaSignature, datasetId);
        
        return datasetId;
    }
    
    /**
     * Create the complete OpenLineage event
     */
    private OpenLineage.RunEvent createOpenLineageEvent(
            String jobName,
            List<OpenLineage.InputDataset> inputDatasets,
            OpenLineage.OutputDataset outputDataset,
            OpenLineage.ColumnLineageDatasetFacet columnLineage) {
        
        String runId = UUID.randomUUID().toString();
        ZonedDateTime eventTime = ZonedDateTime.now();
        
        // Add column lineage to output dataset if available
        if (columnLineage != null) {
            outputDataset = openLineage.newOutputDatasetBuilder()
                .namespace(outputDataset.getNamespace())
                .name(outputDataset.getName())
                .facets(openLineage.newDatasetFacetsBuilder()
                    .schema(outputDataset.getFacets().getSchema())
                    .columnLineage(columnLineage)
                    .build())
                .build();
        }
        
        return openLineage.newRunEventBuilder()
            .eventType(OpenLineage.RunEvent.EventType.COMPLETE)
            .eventTime(eventTime)
            .run(openLineage.newRunBuilder()
                .runId(UUID.fromString(runId))
                .build())
            .job(openLineage.newJobBuilder()
                .namespace("static_analysis")
                .name(jobName)
                .build())
            .inputs(inputDatasets)
            .outputs(Arrays.asList(outputDataset))
            .build();
    }
    
    /**
     * Extract job name from file path
     */
    private String extractJobNameFromPath(String filePath) {
        String fileName = new File(filePath).getName();
        // Remove .json extension and clean up
        return fileName.replaceAll("\\.json$", "").replaceAll("[^a-zA-Z0-9_-]", "_");
    }
    
    /**
     * Context class to hold execution plan analysis results
     */
    private static class ExecutionPlanContext {
        private final List<PlanNode> allNodes = new ArrayList<>();
        private final List<PlanNode> inputSources = new ArrayList<>();
        private PlanNode rootNode;
        private List<JsonNode> allNodesArray; // For array-based plan structures
        
        public void addNode(PlanNode node) { allNodes.add(node); }
        public void addInputSource(PlanNode node) { inputSources.add(node); }
        public void setRootNode(PlanNode node) { this.rootNode = node; }
        public void setAllNodesArray(List<JsonNode> nodes) { this.allNodesArray = nodes; }
        
        public List<PlanNode> getAllNodes() { return allNodes; }
        public List<PlanNode> getInputSources() { return inputSources; }
        public PlanNode getRootNode() { return rootNode; }
        public List<JsonNode> getAllNodesArray() { return allNodesArray; }
    }
    
    /**
     * Represents a node in the execution plan
     */
    private static class PlanNode {
        private final String nodeClass;
        private final JsonNode node;
        private final int depth;
        
        public PlanNode(String nodeClass, JsonNode node, int depth) {
            this.nodeClass = nodeClass;
            this.node = node;
            this.depth = depth;
        }
        
        public String getNodeClass() { return nodeClass; }
        public JsonNode getNode() { return node; }
        public int getDepth() { return depth; }
    }
} 