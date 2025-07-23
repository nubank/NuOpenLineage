import io.openlineage.spark.agent.util.OpenLineageManualEventCreator;
import io.openlineage.client.OpenLineage;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.io.File;

public class TestFileLineage {
    public static void main(String[] args) {
        System.out.println("Testing OpenLineageManualEventCreator with file operations...");
        
        SparkSession spark = SparkSession.builder()
            .appName("TestFileLineage")
            .master("local[*]")
            .config("spark.ui.enabled", "false")
            .config("spark.openlineage.namespace", "test_namespace")
            .getOrCreate();

        try {
            // Create temporary directories
            String inputDir = "/tmp/ol_test_input";
            String outputDir = "/tmp/ol_test_output";
            
            // Clean up directories
            deleteDirectory(new File(inputDir));
            deleteDirectory(new File(outputDir));
            
            System.out.println("Creating test input data...");
            
            // Create test data in memory first
            Dataset<Row> testData = spark.range(1, 5)
                .selectExpr("id", "concat('user_', id) as name", "(id * 10) as age");
            
            // Save to parquet (this creates input data)
            testData.write().mode("overwrite").parquet(inputDir);
            System.out.println("✅ Created input data at: " + inputDir);
            
            // Now read from the parquet file (this should be detected as input)
            System.out.println("Reading from input file and transforming...");
            Dataset<Row> inputDF = spark.read().parquet(inputDir);
            
            // Apply transformations
            Dataset<Row> transformedDF = inputDF
                .filter("age > 20")
                .selectExpr("id", "name", "(age + 5) as adjusted_age");
            
            // Save to another parquet file (this should be detected as output)
            transformedDF.write().mode("overwrite").parquet(outputDir);
            System.out.println("✅ Created output data at: " + outputDir);
            
            // Create OpenLineage event from the transformation query execution
            System.out.println("Creating OpenLineage event...");
            OpenLineage.RunEvent event = OpenLineageManualEventCreator.createCompletedEvent(transformedDF.queryExecution());
            
            System.out.println("✅ SUCCESS: Event created!");
            System.out.println("Job Name: " + event.getJob().getName());
            System.out.println("Namespace: " + event.getJob().getNamespace());
            System.out.println("Run ID: " + event.getRun().getRunId());
            
            // Check inputs
            System.out.println("\n📥 Input datasets: " + event.getInputs().size());
            for (OpenLineage.InputDataset input : event.getInputs()) {
                System.out.println("  - " + input.getNamespace() + "/" + input.getName());
                if (input.getFacets() != null && input.getFacets().getSchema() != null) {
                    System.out.println("    Schema fields: " + input.getFacets().getSchema().getFields().size());
                }
            }
            
            // Check outputs
            System.out.println("\n📤 Output datasets: " + event.getOutputs().size());
            for (OpenLineage.OutputDataset output : event.getOutputs()) {
                System.out.println("  - " + output.getNamespace() + "/" + output.getName());
                if (output.getFacets() != null && output.getFacets().getSchema() != null) {
                    System.out.println("    Schema fields: " + output.getFacets().getSchema().getFields().size());
                }
                if (output.getFacets() != null && output.getFacets().getColumnLineage() != null) {
                    System.out.println("    Column lineage present: " + 
                        output.getFacets().getColumnLineage().getFields().getAdditionalProperties().size() + " columns");
                }
            }
            
            // Test in-memory operations (should have no datasets)
            System.out.println("\n🧠 Testing in-memory operations...");
            Dataset<Row> memoryDF = spark.range(1, 5).selectExpr("id", "(id * 2) as doubled");
            OpenLineage.RunEvent memoryEvent = OpenLineageManualEventCreator.createCompletedEvent(memoryDF.queryExecution());
            
            System.out.println("In-memory event inputs: " + memoryEvent.getInputs().size());
            System.out.println("In-memory event outputs: " + memoryEvent.getOutputs().size());
            
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