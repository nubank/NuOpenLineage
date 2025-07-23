package io.openlineage.spark.agent.util;

import io.openlineage.client.OpenLineage;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.apache.spark.sql.functions.*;

public class ManualEventCreationTest {
    
    private SparkSession spark;

    @BeforeEach
    void setUp() {
        spark = SparkSession.builder()
            .appName("ManualEventCreationTest")
            .master("local[*]")
            .config("spark.ui.enabled", "false")
            .config("spark.openlineage.namespace", "test_namespace")
            .getOrCreate();
    }

    @AfterEach
    void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    void testCreateCompletedEventWithDefaults() {
        // Create a simple DataFrame with transformations
        StructType schema = new StructType(new StructField[]{
            DataTypes.createStructField("id", DataTypes.IntegerType, false),
            DataTypes.createStructField("name", DataTypes.StringType, false),
            DataTypes.createStructField("age", DataTypes.IntegerType, false)
        });

        Dataset<Row> df = spark.createDataFrame(Arrays.asList(
            org.apache.spark.sql.RowFactory.create(1, "Alice", 25),
            org.apache.spark.sql.RowFactory.create(2, "Bob", 30),
            org.apache.spark.sql.RowFactory.create(3, "Charlie", 35)
        ), schema);

        // Perform transformations to create lineage
        Dataset<Row> transformedDf = df
            .filter(col("age").gt(25))
            .select(col("id"), col("name"), col("age").plus(5).alias("adjusted_age"));

        // Trigger action to create QueryExecution
        transformedDf.collect();

        // Create OpenLineage event
        OpenLineage.RunEvent event = OpenLineageManualEventCreator.createCompletedEvent(
            transformedDf.queryExecution()
        );

        // Verify event properties
        assertThat(event).isNotNull();
        assertThat(event.getEventType()).isEqualTo(OpenLineage.RunEvent.EventType.COMPLETE);
        assertThat(event.getJob()).isNotNull();
        assertThat(event.getJob().getName()).isNotEmpty();
        assertThat(event.getJob().getNamespace()).isEqualTo("test_namespace");
        assertThat(event.getRun()).isNotNull();
        assertThat(event.getRun().getRunId()).isNotNull();
        
        System.out.println("✅ Event created successfully!");
        System.out.println("Job Name: " + event.getJob().getName());
        System.out.println("Namespace: " + event.getJob().getNamespace());
        System.out.println("Event Type: " + event.getEventType());
        System.out.println("Number of outputs: " + event.getOutputs().size());
    }

    @Test
    void testCreateCompletedEventWithCustomParameters() {
        // Create a simple DataFrame
        Dataset<Row> df = spark.range(10).toDF("number");
        Dataset<Row> result = df.select(col("number").multiply(2).alias("doubled"));
        
        // Trigger action
        result.collect();

        String customJobName = "test_custom_job";
        ZonedDateTime customTime = ZonedDateTime.now();

        // Create event with custom parameters
        OpenLineage.RunEvent event = OpenLineageManualEventCreator.createCompletedEvent(
            result.queryExecution(),
            Optional.of(customJobName),
            Optional.of(customTime)
        );

        // Verify custom parameters
        assertThat(event.getJob().getName()).isEqualTo(customJobName);
        assertThat(event.getEventTime()).isEqualTo(customTime);
        
        System.out.println("✅ Custom event created successfully!");
        System.out.println("Custom Job Name: " + event.getJob().getName());
        System.out.println("Custom Event Time: " + event.getEventTime());
    }

    @Test
    void testEventContainsLineageInformation() {
        // Create DataFrame with clear lineage
        Dataset<Row> source = spark.range(5).toDF("id");
        Dataset<Row> transformed = source
            .withColumn("doubled_id", col("id").multiply(2))
            .withColumn("squared_id", col("id").multiply(col("id")))
            .select(col("id"), col("doubled_id"), col("squared_id"));
        
        // Trigger action
        transformed.collect();

        // Create event
        OpenLineage.RunEvent event = OpenLineageManualEventCreator.createCompletedEvent(
            transformed.queryExecution()
        );

        // Verify lineage information
        assertThat(event.getOutputs()).isNotEmpty();
        
        if (!event.getOutputs().isEmpty()) {
            OpenLineage.OutputDataset output = event.getOutputs().get(0);
            
            // Check schema information
            if (output.getFacets().getSchema() != null) {
                assertThat(output.getFacets().getSchema().getFields()).isNotEmpty();
                System.out.println("✅ Schema facet present with " + 
                    output.getFacets().getSchema().getFields().size() + " fields");
            }
            
            // Check column lineage information
            if (output.getFacets().getColumnLineage() != null) {
                assertThat(output.getFacets().getColumnLineage().getFields().getAdditionalProperties())
                    .isNotEmpty();
                System.out.println("✅ Column lineage present for " + 
                    output.getFacets().getColumnLineage().getFields().getAdditionalProperties().size() + " columns");
            }
        }
        
        System.out.println("✅ Lineage information verified!");
    }
} 