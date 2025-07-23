package io.openlineage.spark.agent.util;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Optional;

import static org.apache.spark.sql.functions.*;

public class ManualEventCreationExample {
    public static void main(String[] args) {
        System.out.println("Starting ManualEventCreationExample...");
        
        SparkSession spark = SparkSession.builder()
            .appName("ManualEventCreationExample")
            .master("local[*]")
            .config("spark.openlineage.namespace", "example_namespace")
            .getOrCreate();

        try {
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

            // Perform some transformations to create lineage
            Dataset<Row> transformedDf = df
                .filter(col("age").gt(25))
                .select(col("id"), col("name"), col("age").plus(5).alias("adjusted_age"));

            // Trigger action to create QueryExecution
            transformedDf.collect();

            System.out.println("DataFrame transformations completed. Creating OpenLineage events...");

            // Method 1: Create event with default job name and current time
            OpenLineage.RunEvent event1 = OpenLineageManualEventCreator.createCompletedEvent(
                transformedDf.queryExecution()
            );

            // Method 2: Create event with custom job name and time
            OpenLineage.RunEvent event2 = OpenLineageManualEventCreator.createCompletedEvent(
                transformedDf.queryExecution(),
                Optional.of("my_custom_lineage_job"),
                Optional.of(ZonedDateTime.now())
            );

            System.out.println("OpenLineage events created successfully!");
            System.out.println("Event 1 Job Name: " + event1.getJob().getName());
            System.out.println("Event 2 Job Name: " + event2.getJob().getName());
            System.out.println("Number of inputs: " + event1.getInputs().size());
            System.out.println("Number of outputs: " + event1.getOutputs().size());

            // Print basic event information
            if (!event1.getOutputs().isEmpty()) {
                OpenLineage.OutputDataset output = event1.getOutputs().get(0);
                System.out.println("Output dataset namespace: " + output.getNamespace());
                System.out.println("Output dataset name: " + output.getName());
                
                if (output.getFacets().getSchema() != null) {
                    System.out.println("Output schema fields: " + output.getFacets().getSchema().getFields().size());
                }
                
                if (output.getFacets().getColumnLineage() != null) {
                    System.out.println("Column lineage available: " + 
                        output.getFacets().getColumnLineage().getFields().getAdditionalProperties().size() + " columns");
                }
            }

            // Optional: Print abbreviated event JSON for inspection
            System.out.println("\n=== Event Summary ===");
            System.out.println("Event Type: " + event1.getEventType());
            System.out.println("Event Time: " + event1.getEventTime());
            System.out.println("Run ID: " + event1.getRun().getRunId());
            
            // Print full JSON if requested via system property
            if ("true".equals(System.getProperty("printFullJson"))) {
                System.out.println("\n=== Full Event JSON ===");
                System.out.println(OpenLineageClientUtils.toJson(event1));
            } else {
                System.out.println("\nTo see full JSON, run with -DprintFullJson=true");
            }

        } catch (Exception e) {
            System.err.println("Error running example: " + e.getMessage());
            e.printStackTrace();
        } finally {
            spark.stop();
            System.out.println("Spark session stopped.");
        }
    }
} 