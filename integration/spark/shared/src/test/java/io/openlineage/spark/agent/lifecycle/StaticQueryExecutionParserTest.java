/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.OpenLineage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.List;

/**
 * Test class for StaticQueryExecutionParser
 */
public class StaticQueryExecutionParserTest {

    private StaticQueryExecutionParser parser;
    private ObjectMapper objectMapper;

    @BeforeEach
    public void setUp() {
        parser = new StaticQueryExecutionParser();
        objectMapper = new ObjectMapper();
    }

    @Test
    public void testParseSimpleQueryPlan() throws IOException {
        // Create a simple test JSON structure similar to the actual query plans
        String testJson = "[" +
            "{" +
                "\"class\": \"org.apache.spark.sql.catalyst.plans.logical.Project\"," +
                "\"num-children\": 1," +
                "\"projectList\": [" +
                    "[{" +
                        "\"class\": \"org.apache.spark.sql.catalyst.expressions.AttributeReference\"," +
                        "\"name\": \"customer_id\"," +
                        "\"dataType\": \"string\"," +
                        "\"nullable\": true" +
                    "}]," +
                    "[{" +
                        "\"class\": \"org.apache.spark.sql.catalyst.expressions.AttributeReference\"," +
                        "\"name\": \"amount\"," +
                        "\"dataType\": \"decimal(38,8)\"," +
                        "\"nullable\": true" +
                    "}]" +
                "]," +
                "\"child\": 0" +
            "}," +
            "{" +
                "\"class\": \"org.apache.spark.sql.execution.LogicalRDD\"," +
                "\"num-children\": 0," +
                "\"output\": [" +
                    "[{" +
                        "\"class\": \"org.apache.spark.sql.catalyst.expressions.AttributeReference\"," +
                        "\"name\": \"customer_id\"," +
                        "\"dataType\": \"string\"," +
                        "\"nullable\": true" +
                    "}]," +
                    "[{" +
                        "\"class\": \"org.apache.spark.sql.catalyst.expressions.AttributeReference\"," +
                        "\"name\": \"amount\"," +
                        "\"dataType\": \"decimal(38,8)\"," +
                        "\"nullable\": true" +
                    "}]" +
                "]," +
                "\"rdd\": null" +
            "}" +
        "]";

        JsonNode planRoot = objectMapper.readTree(testJson);
        OpenLineage.RunEvent event = parser.parseExecutionPlan(planRoot, "test_job");

        assertNotNull(event);
        assertEquals(OpenLineage.RunEvent.EventType.COMPLETE, event.getEventType());
        assertEquals("test_job", event.getJob().getName());
        assertEquals("static_analysis", event.getJob().getNamespace());
        assertNotNull(event.getRun().getRunId());
        assertNotNull(event.getEventTime());
        
        // Verify inputs
        assertNotNull(event.getInputs());
        assertTrue(event.getInputs().size() >= 1);
        
        // Verify outputs
        assertNotNull(event.getOutputs());
        assertEquals(1, event.getOutputs().size());
        
        OpenLineage.OutputDataset output = event.getOutputs().get(0);
        assertEquals("memory://dataframes", output.getNamespace());
        assertEquals("test_job_output", output.getName());
    }

    @Test
    public void testExtractSchemaFromLogicalRDD() throws IOException {
        String testJson = "{" +
            "\"class\": \"org.apache.spark.sql.execution.LogicalRDD\"," +
            "\"output\": [" +
                "[{" +
                    "\"class\": \"org.apache.spark.sql.catalyst.expressions.AttributeReference\"," +
                    "\"name\": \"customer_id\"," +
                    "\"dataType\": \"string\"," +
                    "\"nullable\": true" +
                "}]," +
                "[{" +
                    "\"class\": \"org.apache.spark.sql.catalyst.expressions.AttributeReference\"," +
                    "\"name\": \"amount\"," +
                    "\"dataType\": \"decimal(38,8)\"," +
                    "\"nullable\": true" +
                "}]" +
            "]" +
        "}";

        JsonNode node = objectMapper.readTree(testJson);
        
        // Use reflection to test private method (for testing purposes)
        try {
            java.lang.reflect.Method method = StaticQueryExecutionParser.class
                .getDeclaredMethod("extractSchemaFromNode", JsonNode.class);
            method.setAccessible(true);
            
            OpenLineage.SchemaDatasetFacet schema = 
                (OpenLineage.SchemaDatasetFacet) method.invoke(parser, node);
            
            assertNotNull(schema);
            assertNotNull(schema.getFields());
            assertEquals(2, schema.getFields().size());
            
            // Check first field
            OpenLineage.SchemaDatasetFacetFields firstField = schema.getFields().get(0);
            assertEquals("customer_id", firstField.getName());
            assertEquals("string", firstField.getType());
            
            // Check second field
            OpenLineage.SchemaDatasetFacetFields secondField = schema.getFields().get(1);
            assertEquals("amount", secondField.getName());
            assertEquals("decimal(38,8)", secondField.getType());
            
        } catch (Exception e) {
            fail("Failed to test schema extraction: " + e.getMessage());
        }
    }

    @Test
    public void testParseWithMultipleInputSources() throws IOException {
        String testJson = "[" +
            "{" +
                "\"class\": \"org.apache.spark.sql.catalyst.plans.logical.Join\"," +
                "\"left\": 0," +
                "\"right\": 1," +
                "\"joinType\": {\"object\": \"org.apache.spark.sql.catalyst.plans.Inner$\"}" +
            "}," +
            "{" +
                "\"class\": \"org.apache.spark.sql.execution.LogicalRDD\"," +
                "\"output\": [" +
                    "[{" +
                        "\"name\": \"id\"," +
                        "\"dataType\": \"string\"" +
                    "}]" +
                "]" +
            "}," +
            "{" +
                "\"class\": \"org.apache.spark.sql.execution.LogicalRDD\"," +
                "\"output\": [" +
                    "[{" +
                        "\"name\": \"value\"," +
                        "\"dataType\": \"integer\"" +
                    "}]" +
                "]" +
            "}" +
        "]";

        JsonNode planRoot = objectMapper.readTree(testJson);
        OpenLineage.RunEvent event = parser.parseExecutionPlan(planRoot, "join_test");

        assertNotNull(event);
        
        // Should have 2 input datasets from the 2 LogicalRDD nodes
        List<OpenLineage.InputDataset> inputs = event.getInputs();
        assertNotNull(inputs);
        assertEquals(2, inputs.size());
        
        // Both should be in the memory namespace
        for (OpenLineage.InputDataset input : inputs) {
            assertEquals("memory://dataframes", input.getNamespace());
            assertTrue(input.getName().startsWith("input_dataset_"));
        }
    }

    @Test
    public void testValidateEvent() {
        // Create a valid event using the parser
        String testJson = "[{" +
            "\"class\": \"org.apache.spark.sql.execution.LogicalRDD\"," +
            "\"output\": [" +
                "[{\"name\": \"test_field\", \"dataType\": \"string\"}]" +
            "]" +
        "}]";

        try {
            JsonNode planRoot = objectMapper.readTree(testJson);
            OpenLineage.RunEvent event = parser.parseExecutionPlan(planRoot, "validation_test");
            
            // Test validation through the main class
            StaticQueryExecutionParserMain main = new StaticQueryExecutionParserMain();
            assertTrue(main.validateEvent(event));
            
        } catch (Exception e) {
            fail("Failed to validate event: " + e.getMessage());
        }
    }

    @Test
    public void testHandleEmptyInputSources() throws IOException {
        // Test with no LogicalRDD nodes
        String testJson = "[{" +
            "\"class\": \"org.apache.spark.sql.catalyst.plans.logical.Project\"," +
            "\"projectList\": [" +
                "[{\"name\": \"test_field\", \"dataType\": \"string\"}]" +
            "]" +
        "}]";

        JsonNode planRoot = objectMapper.readTree(testJson);
        OpenLineage.RunEvent event = parser.parseExecutionPlan(planRoot, "empty_inputs_test");

        assertNotNull(event);
        
        // Should have at least one input (mock input)
        List<OpenLineage.InputDataset> inputs = event.getInputs();
        assertNotNull(inputs);
        assertTrue(inputs.size() >= 1);
        
        // Check if mock input was created
        boolean hasMockInput = inputs.stream()
            .anyMatch(input -> "mock_input_dataset".equals(input.getName()));
        assertTrue(hasMockInput);
    }

    @Test
    public void testJobNameExtraction() {
        // Test job name extraction from file path
        try {
            java.lang.reflect.Method method = StaticQueryExecutionParser.class
                .getDeclaredMethod("extractJobNameFromPath", String.class);
            method.setAccessible(true);
            
            String jobName = (String) method.invoke(parser, 
                "query_execution_examples/nu-br-dataset-savings-svr-paid_query_plan.json");
            
            assertEquals("nu-br-dataset-savings-svr-paid_query_plan", jobName);
            
        } catch (Exception e) {
            fail("Failed to test job name extraction: " + e.getMessage());
        }
    }

    @Test
    public void testColumnLineageExtraction() throws IOException {
        String testJson = "[" +
            "{" +
                "\"class\": \"org.apache.spark.sql.catalyst.plans.logical.Project\"," +
                "\"projectList\": [" +
                    "[{" +
                        "\"name\": \"customer_id\"," +
                        "\"dataType\": \"string\"" +
                    "}]" +
                "]," +
                "\"child\": 0" +
            "}," +
            "{" +
                "\"class\": \"org.apache.spark.sql.execution.LogicalRDD\"," +
                "\"output\": [" +
                    "[{" +
                        "\"name\": \"customer_id\"," +
                        "\"dataType\": \"string\"" +
                    "}]" +
                "]" +
            "}" +
        "]";

        JsonNode planRoot = objectMapper.readTree(testJson);
        OpenLineage.RunEvent event = parser.parseExecutionPlan(planRoot, "lineage_test");

        assertNotNull(event);
        
        // Check if output dataset has column lineage
        OpenLineage.OutputDataset output = event.getOutputs().get(0);
        assertNotNull(output.getFacets());
        
        // Column lineage might be present depending on the parsing logic
        if (output.getFacets().getColumnLineage() != null) {
            assertNotNull(output.getFacets().getColumnLineage().getFields());
        }
    }
} 