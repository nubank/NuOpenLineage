import io.openlineage.spark.agent.util.OpenLineageManualEventCreator
import org.apache.spark.sql.SaveMode

object TestTransformVsWritePlan {
  def main(args: Array[String]): Unit = {
    val spark = org.apache.spark.sql.SparkSession.builder()
      .appName("TestTransformVsWritePlan")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.openlineage.namespace", "test_namespace")
      .getOrCreate()

    try {
      println("=== Demonstrating Transformation Plan vs Write Plan ===\n")
      
      // Create test data first
      println("📝 Creating test input data...")
      val sourceData = spark.range(1, 10).select(
        spark.col("id"),
        (spark.col("id") + 20).as("age"),
        spark.concat(spark.lit("user_"), spark.col("id")).as("name")
      )
      sourceData.write.mode("overwrite").parquet("/tmp/ages")
      println("✅ Created input file at /tmp/ages")
      
      // Read from file and create transformation
      val ages = spark.read.format("parquet").load("/tmp/ages")
      val transformedDf = ages.filter(spark.col("age") > 25)
        .select(spark.col("id"), spark.col("name"), (spark.col("age") + 5).as("adjusted_age"))
      
      println("\n🔍 ANALYSIS 1: Transformation Plan (transformedDf.queryExecution)")
      println("===============================================================")
      
      // Print the logical plan structure
      println("Logical Plan:")
      println(transformedDf.queryExecution.logical.treeString)
      
      // Create event from transformation plan
      val transformEvent = OpenLineageManualEventCreator.createCompletedEvent(transformedDf.queryExecution)
      println(s"Inputs: ${transformEvent.getInputs.size}")
      println(s"Outputs: ${transformEvent.getOutputs.size}")
      
      if (transformEvent.getInputs.size > 0) {
        transformEvent.getInputs.forEach(input => 
          println(s"  Input: ${input.getNamespace}/${input.getName}")
        )
      } else {
        println("  ❌ NO INPUTS DETECTED")
        println("  Why? Transformation plan doesn't include write command context")
      }
      
      if (transformEvent.getOutputs.size > 0) {
        transformEvent.getOutputs.forEach(output => 
          println(s"  Output: ${output.getNamespace}/${output.getName}")
        )
      }

      println("\n🔍 ANALYSIS 2: Write Operation Context")
      println("=====================================")
      
      // The problem: We need to capture the write operation's QueryExecution
      // But Spark doesn't expose this easily through DataFrame API
      
      println("🔧 SOLUTION APPROACHES:")
      println("1. Hook into Spark SQL execution listener (what OpenLineage normally does)")
      println("2. Use SQL commands instead of DataFrame API")
      println("3. Create a custom method that captures write operations")
      
      // Let's try approach 2: SQL commands
      println("\n🔍 ANALYSIS 3: Using SQL Commands")
      println("=================================")
      
      // Register temporary view and use SQL
      transformedDf.createOrReplaceTempView("transformed_data")
      
      // Execute write via SQL (this might capture better lineage)
      spark.sql("""
        INSERT OVERWRITE DIRECTORY '/tmp/ages_over_25_sql'
        USING PARQUET
        SELECT * FROM transformed_data
      """)
      
      println("✅ Executed write via SQL command")
      println("Note: SQL execution might provide better lineage context")

      // Cleanup
      cleanup()
      
    } catch {
      case e: Exception =>
        println(s"❌ Error: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
  
  def cleanup(): Unit = {
    import java.io.File
    import scala.util.Try
    
    def deleteDirectory(file: File): Unit = {
      if (file.exists()) {
        if (file.isDirectory) {
          file.listFiles().foreach(deleteDirectory)
        }
        file.delete()
      }
    }
    
    Try(deleteDirectory(new File("/tmp/ages")))
    Try(deleteDirectory(new File("/tmp/ages_over_25")))
    Try(deleteDirectory(new File("/tmp/ages_over_25_sql")))
  }
} 