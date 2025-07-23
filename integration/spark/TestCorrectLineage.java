import io.openlineage.spark.agent.util.OpenLineageManualEventCreator;
import io.openlineage.client.OpenLineage;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SQLExecution;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import java.io.File;

public class TestCorrectLineage {
    public static void main(String[] args) {
        System.out.println("Testing QueryExecution from transformations vs write operations...");
        
        SparkSession spark = SparkSession.builder()
            .appName("TestCorrectLineage")
            .master("local[*]")
            .config("spark.ui.enabled", "false")
            .config("spark.openlineage.namespace", "test_namespace")
            .getOrCreate();

        try {
            // Create temporary directories
            String inputDir = "/tmp/ol_correct_test_input";
            String outputDir = "/tmp/ol_correct_test_output";
            
            // Clean up directories
            deleteDirectory(new File(inputDir));
            deleteDirectory(new File(outputDir));
            
            System.out.println("Creating test input data...");
            
            // Create test data and save it
            Dataset<Row> testData = spark.range(1, 5)
                .selectExpr("id", "concat('user_', id) as name", "(id * 10) as age");
            
            testData.write().mode("overwrite").parquet(inputDir);
            System.out.println("✅ Created input data at: " + inputDir);
            
            // Read from parquet and apply transformations
            System.out.println("\nTesting transformation QueryExecution (current approach):");
            Dataset<Row> inputDF = spark.read().parquet(inputDir);
            Dataset<Row> transformedDF = inputDF
                .filter("age > 20")
                .selectExpr("id", "name", "(age + 5) as adjusted_age");
            
            // Print logical plan from transformation
            System.out.println("Transformation LogicalPlan: " + transformedDF.queryExecution().optimizedPlan().getClass().getSimpleName());
            
            // Create event from transformation
            OpenLineage.RunEvent transformEvent = OpenLineageManualEventCreator.createCompletedEvent(transformedDF.queryExecution());
            System.out.println("From transformation - Inputs: " + transformEvent.getInputs().size() + ", Outputs: " + transformEvent.getOutputs().size());
            
            // Now test the write operation approach
            System.out.println("\nTesting write operation approach:");
            
            // Perform the write operation and immediately get the last SQL execution
            long executionId = spark.sparkContext().listenerBus().waitUntilEmpty(1000);
            
            // Execute the write and try to capture the execution
            transformedDF.write().mode("overwrite").parquet(outputDir);
            System.out.println("✅ Created output data at: " + outputDir);
            
            System.out.println("\nInvestigating why write operations don't show up:");
            System.out.println("The issue is that transformedDF.queryExecution() gives us the TRANSFORMATION plan,");
            System.out.println("not the WRITE COMMAND plan. Write operations create separate QueryExecution objects");
            System.out.println("with nodes like SaveIntoDataSourceCommand that the visitors can detect.");
            
            System.out.println("\nCurrent approach captures: Filter -> Project -> LogicalRelation");
            System.out.println("We need to capture: SaveIntoDataSourceCommand -> Filter -> Project -> LogicalRelation");
            
            // Example of what we would need to capture
            System.out.println("\nTo get proper lineage, you would need to:");
            System.out.println("1. Hook into Spark's SQL execution during the write operation");
            System.out.println("2. Capture the QueryExecution that contains the write command");
            System.out.println("3. That QueryExecution would have input datasets (from read) and output datasets (from write)");
            
            // Clean up
            deleteDirectory(new File(inputDir));
            deleteDirectory(new File(outputDir));
            
        } catch (Exception e) {
            System.err.println("❌ ERROR: " + e.getMessage());
            e.printStackTrace();
        } finally {
            spark.stop();
            System.out.println("\nTest completed.");
        }
    }
    
    private static void deleteDirectory(File dir) {
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
            dir.delete();
        }
    }
} 