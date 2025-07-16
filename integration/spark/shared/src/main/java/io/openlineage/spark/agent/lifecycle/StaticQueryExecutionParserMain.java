/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.StdDateFormat;
import io.openlineage.client.OpenLineage;
import java.io.File;
import java.net.URI;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;

/**
 * Main runner class to demonstrate the StaticQueryExecutionParser.
 * This class processes Spark query execution plans from JSON files and generates OpenLineage events.
 */
@Slf4j
public class StaticQueryExecutionParserMain {
    
    private static final String QUERY_EXAMPLES_DIR = "query_execution_examples";
    
    public static void main(String[] args) {
        try {
            StaticQueryExecutionParserMain runner = new StaticQueryExecutionParserMain();
            
            if (args.length > 0) {
                // Process specific file
                String filePath = args[0];
                runner.processFile(filePath);
            } else {
                // Process all example files
                runner.processExampleFiles();
            }
            
        } catch (Exception e) {
            log.error("Error in main execution", e);
            System.exit(1);
        }
    }
    
    /**
     * Process a specific JSON file
     */
    public void processFile(String filePath) {
        try {
            log.info("Processing file: {}", filePath);
            
            StaticQueryExecutionParser parser = new StaticQueryExecutionParser();
            
            OpenLineage.RunEvent event = parser.parseExecutionPlanFile(filePath);
            
            // Print the event details instead of JSON serialization (to avoid ZonedDateTime issues)
            log.info("Generated OpenLineage Event:");
            printEventDetails(event);
            
            // Create a simplified JSON representation
            System.out.println(createSimpleEventJson(event));
            
            // Print event details
            printEventDetails(event);
            
            // Validate event
            boolean isValid = validateEvent(event);
            log.info("Event validation: {}", isValid ? "PASSED" : "FAILED");
            
        } catch (Exception e) {
            log.error("Error processing file: {}", filePath, e);
            throw new RuntimeException("Failed to process file: " + filePath, e);
        }
    }
    
    /**
     * Process all example JSON files in the query_execution_examples directory
     */
    public void processExampleFiles() {
        try {
            File examplesDir = new File(QUERY_EXAMPLES_DIR);
            if (!examplesDir.exists()) {
                log.warn("Query examples directory not found: {}", QUERY_EXAMPLES_DIR);
                return;
            }
            
            File[] jsonFiles = examplesDir.listFiles((dir, name) -> name.endsWith(".json"));
            if (jsonFiles == null || jsonFiles.length == 0) {
                log.warn("No JSON files found in: {}", QUERY_EXAMPLES_DIR);
                return;
            }
            
            log.info("Found {} JSON files to process", jsonFiles.length);
            
            for (File jsonFile : jsonFiles) {
                try {
                    log.info("Processing: {}", jsonFile.getName());
                    processFile(jsonFile.getAbsolutePath());
                    System.out.println("\n" + "=".repeat(80) + "\n");
                } catch (Exception e) {
                    log.error("Error processing file: {}", jsonFile.getName(), e);
                }
            }
            
        } catch (Exception e) {
            log.error("Error processing example files", e);
            throw new RuntimeException("Failed to process example files", e);
        }
    }
    

    
    /**
     * Demonstrate parsing a query execution plan programmatically
     */
    public OpenLineage.RunEvent demonstrateStaticParsing() {
        try {
            // Example of using the parser programmatically
            String exampleFilePath = QUERY_EXAMPLES_DIR + "/nu-br-dataset-savings-svr-paid_query_plan.json";
            
            StaticQueryExecutionParser parser = new StaticQueryExecutionParser();
            OpenLineage.RunEvent event = parser.parseExecutionPlanFile(exampleFilePath);
            
            log.info("Successfully parsed execution plan into OpenLineage event");
            log.info("Job: {}", event.getJob().getName());
            log.info("Inputs: {}", event.getInputs().size());
            log.info("Outputs: {}", event.getOutputs().size());
            
            return event;
            
        } catch (Exception e) {
            log.error("Error in demonstration", e);
            throw new RuntimeException("Demonstration failed", e);
        }
    }
    
    /**
     * Validate that the generated event conforms to OpenLineage specification
     */
    public boolean validateEvent(OpenLineage.RunEvent event) {
        try {
            // Basic validation checks
            if (event == null) {
                log.error("Event is null");
                return false;
            }
            
            if (event.getEventType() == null) {
                log.error("Event type is null");
                return false;
            }
            
            if (event.getJob() == null) {
                log.error("Job is null");
                return false;
            }
            
            if (event.getRun() == null) {
                log.error("Run is null");
                return false;
            }
            
            if (event.getInputs() == null) {
                log.error("Inputs is null");
                return false;
            }
            
            if (event.getOutputs() == null) {
                log.error("Outputs is null");
                return false;
            }
            
            log.info("Event validation passed");
            return true;
            
        } catch (Exception e) {
            log.error("Error validating event", e);
            return false;
        }
    }
    
    /**
     * Create a simplified JSON representation of the event
     */
    private String createSimpleEventJson(OpenLineage.RunEvent event) {
        try {
            StringBuilder json = new StringBuilder();
            json.append("{\n");
            json.append("  \"eventType\": \"").append(event.getEventType()).append("\",\n");
            json.append("  \"eventTime\": \"").append(event.getEventTime()).append("\",\n");
            json.append("  \"producer\": \"").append(event.getProducer()).append("\",\n");
            json.append("  \"job\": {\n");
            json.append("    \"name\": \"").append(event.getJob().getName()).append("\",\n");
            json.append("    \"namespace\": \"").append(event.getJob().getNamespace()).append("\"\n");
            json.append("  },\n");
            json.append("  \"run\": {\n");
            json.append("    \"runId\": \"").append(event.getRun().getRunId()).append("\"\n");
            json.append("  },\n");
            json.append("  \"inputs\": [\n");
            for (int i = 0; i < event.getInputs().size(); i++) {
                OpenLineage.InputDataset input = event.getInputs().get(i);
                json.append("    {\n");
                json.append("      \"name\": \"").append(input.getName()).append("\",\n");
                json.append("      \"namespace\": \"").append(input.getNamespace()).append("\"\n");
                json.append("    }");
                if (i < event.getInputs().size() - 1) json.append(",");
                json.append("\n");
            }
            json.append("  ],\n");
            json.append("  \"outputs\": [\n");
            for (int i = 0; i < event.getOutputs().size(); i++) {
                OpenLineage.OutputDataset output = event.getOutputs().get(i);
                json.append("    {\n");
                json.append("      \"name\": \"").append(output.getName()).append("\",\n");
                json.append("      \"namespace\": \"").append(output.getNamespace()).append("\"\n");
                json.append("    }");
                if (i < event.getOutputs().size() - 1) json.append(",");
                json.append("\n");
            }
            json.append("  ]\n");
            json.append("}");
            return json.toString();
        } catch (Exception e) {
            return "Error creating JSON representation: " + e.getMessage();
        }
    }

    /**
     * Print detailed information about the parsed event
     */
    public void printEventDetails(OpenLineage.RunEvent event) {
        try {
            log.info("=== OpenLineage Event Details ===");
            log.info("Event Type: {}", event.getEventType());
            log.info("Event Time: {}", event.getEventTime());
            log.info("Job Name: {}", event.getJob().getName());
            log.info("Job Namespace: {}", event.getJob().getNamespace());
            log.info("Run ID: {}", event.getRun().getRunId());
            
            log.info("Input Datasets ({}): ", event.getInputs().size());
            for (OpenLineage.InputDataset input : event.getInputs()) {
                log.info("  - {}/{}", input.getNamespace(), input.getName());
                if (input.getFacets() != null && input.getFacets().getSchema() != null) {
                    log.info("    Schema fields: {}", input.getFacets().getSchema().getFields().size());
                }
            }
            
            log.info("Output Datasets ({}): ", event.getOutputs().size());
            for (OpenLineage.OutputDataset output : event.getOutputs()) {
                log.info("  - {}/{}", output.getNamespace(), output.getName());
                if (output.getFacets() != null && output.getFacets().getSchema() != null) {
                    log.info("    Schema fields: {}", output.getFacets().getSchema().getFields().size());
                }
                if (output.getFacets() != null && output.getFacets().getColumnLineage() != null) {
                    log.info("    Column lineage fields: {}", output.getFacets().getColumnLineage().getFields().getAdditionalProperties().size());
                }
            }
            
        } catch (Exception e) {
            log.error("Error printing event details", e);
        }
    }
} 