import io.openlineage.spark.agent.util.OpenLineageManualEventCreator;
import io.openlineage.client.OpenLineage;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import java.io.File;

public class TestInputOutputDebug {
    public static void main(String[] args) {
        System.out.println("=== Testing Input/Output Dataset Detection ===\n");
        
        SparkSession spark = SparkSession.builder()
            .appName("TestInputOutputDebug")
            .master("local[*]")
            .config("spark.ui.enabled", "false")
            .config("spark.openlineage.namespace", "test_namespace")
            .getOrCreate();

        try {
            testInMemoryOperations(spark);
            testFileBasedOperations(spark);
            
        } catch (Exception e) {
            System.err.println("❌ ERROR: " + e.getMessage());
            e.printStackTrace();
        } finally {
            spark.stop();
            System.out.println("\n🔍 Debug analysis completed.");
        }
    }
    
    private static void testInMemoryOperations(SparkSession spark) {
        System.out.println("🧠 Testing In-Memory Operations:");
        System.out.println("================================");
        
        // Create purely in-memory DataFrame
        Dataset<Row> memoryDF = spark.range(1, 10)
            .selectExpr("id", "id * 2 as doubled", "concat('item_', id) as name");
        
        Dataset<Row> transformedDF = memoryDF
            .filter("id > 5")
            .selectExpr("id", "doubled", "upper(name) as upper_name");
        
        // Trigger execution
        transformedDF.collect();
        
        // Analyze the logical plan
        System.out.println("Logical Plan Analysis:");
        LogicalPlan logicalPlan = transformedDF.queryExecution().logical();
        System.out.println("Root node type: " + logicalPlan.getClass().getSimpleName());
        System.out.println("Plan structure:");
        System.out.println(logicalPlan.treeString());
        
        // Create OpenLineage event
        OpenLineage.RunEvent event = OpenLineageManualEventCreator.createCompletedEvent(transformedDF.queryExecution());
        
        System.out.println("\n📊 Event Results:");
        System.out.println("Inputs: " + event.getInputs().size());
        System.out.println("Outputs: " + event.getOutputs().size());
        
        if (!event.getOutputs().isEmpty()) {
            OpenLineage.OutputDataset output = event.getOutputs().get(0);
            System.out.println("Output dataset: " + output.getNamespace() + "/" + output.getName());
            
            if (output.getFacets() != null && output.getFacets().getColumnLineage() != null) {
                System.out.println("Column lineage fields: " + 
                    output.getFacets().getColumnLineage().getFields().getAdditionalProperties().size());
            }
        }
        
        System.out.println("\n🔍 Why no inputs?");
        System.out.println("- In-memory DataFrames (spark.range(), createDataFrame()) don't have underlying storage");
        System.out.println("- OpenLineage visitors look for LogicalRelation nodes with file/database sources");
        System.out.println("- Range operations create LocalRelation nodes, not input datasets");
        System.out.println();
    }
    
    private static void testFileBasedOperations(SparkSession spark) {
        System.out.println("📁 Testing File-Based Operations:");
        System.out.println("=================================");
        
        try {
            // Create temporary input file
            String inputDir = "/tmp/debug_input_test";
            String outputDir = "/tmp/debug_output_test";
            
            // Clean up
            deleteDirectory(new File(inputDir));
            deleteDirectory(new File(outputDir));
            
            // Create input data
            Dataset<Row> sourceData = spark.range(1, 5)
                .selectExpr("id", "concat('user_', id) as username", "(id * 100) as score");
            
            sourceData.write().mode("overwrite").parquet(inputDir);
            System.out.println("✅ Created input file at: " + inputDir);
            
            // Read from file and transform
            Dataset<Row> inputDF = spark.read().parquet(inputDir);
            Dataset<Row> transformedDF = inputDF
                .filter("score > 200")
                .selectExpr("id", "username", "(score + 50) as bonus_score");
            
            // Write to output file  
            transformedDF.write().mode("overwrite").parquet(outputDir);
            System.out.println("✅ Created output file at: " + outputDir);
            
            // Analyze logical plan for READ operation
            System.out.println("\nRead Operation Logical Plan:");
            LogicalPlan readPlan = inputDF.queryExecution().logical();
            System.out.println("Root node type: " + readPlan.getClass().getSimpleName());
            System.out.println("Plan structure:");
            System.out.println(readPlan.treeString());
            
            // Test the transformation plan (this will show inputs!)
            OpenLineage.RunEvent transformEvent = OpenLineageManualEventCreator.createCompletedEvent(transformedDF.queryExecution());
            
            System.out.println("\n📊 Transform Event Results:");
            System.out.println("Inputs: " + transformEvent.getInputs().size());
            System.out.println("Outputs: " + transformEvent.getOutputs().size());
            
            if (!transformEvent.getInputs().isEmpty()) {
                OpenLineage.InputDataset input = transformEvent.getInputs().get(0);
                System.out.println("Input dataset: " + input.getNamespace() + "/" + input.getName());
            } else {
                System.out.println("❗ Still no inputs detected in transformation plan");
                System.out.println("This is because we're capturing the TRANSFORMATION plan, not the READ plan");
            }
            
            System.out.println("\n🔍 Key Insight:");
            System.out.println("- transformedDF.queryExecution() captures the transformation logical plan");
            System.out.println("- The transformation plan shows: Filter -> Project -> LogicalRelation");
            System.out.println("- LogicalRelation contains the file source, which visitors can detect");
            System.out.println("- But for write operations, you need the WRITE command plan, not transformation plan");
            
            // Clean up
            deleteDirectory(new File(inputDir));
            deleteDirectory(new File(outputDir));
            
        } catch (Exception e) {
            System.err.println("File operation test failed: " + e.getMessage());
        }
        
        System.out.println();
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