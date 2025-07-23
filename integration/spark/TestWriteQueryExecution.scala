import io.openlineage.spark.agent.util.OpenLineageManualEventCreator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.execution.QueryExecution

object TestWriteQueryExecution {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TestWriteQueryExecution")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.openlineage.namespace", "test_namespace")
      .getOrCreate()

    import spark.implicits._

    try {
      println("=== How to Capture WRITE QueryExecution ===\n")
      
      // Setup: Create test data
      println("📝 Setting up test data...")
      val sourceData = spark.range(1, 10).select(
        $"id",
        ($"id" + 20).as("age"),
        concat(lit("user_"), $"id").as("name")
      )
      sourceData.write.mode("overwrite").parquet("/tmp/ages")
      println("✅ Created input file at /tmp/ages")
      
      // Read and prepare transformation
      val ages = spark.read.format("parquet").load("/tmp/ages")
      val transformedDf = ages.filter($"age" > 25).select($"id", $"name", ($"age" + 5).as("adjusted_age"))
      
      println("\n" + "="*60)
      println("APPROACH 1: SQL Commands (RECOMMENDED)")
      println("="*60)
      
      // Register the DataFrame as a temporary view
      transformedDf.createOrReplaceTempView("transformed_data")
      
      // Method 1A: Use INSERT OVERWRITE (captures both input and output)
      println("\n🔧 Method 1A: INSERT OVERWRITE")
      val insertSql = """
        INSERT OVERWRITE DIRECTORY '/tmp/ages_over_25_sql' 
        USING PARQUET
        SELECT * FROM transformed_data
      """
      
      println(s"Executing SQL: $insertSql")
      spark.sql(insertSql)
      
      // The challenge: SQL execution doesn't directly expose QueryExecution
      // But we can capture it through the query plan
      val sqlPlan = spark.sql("SELECT * FROM transformed_data").queryExecution
      println(s"📊 SQL approach - Inputs: Would need SparkListener to capture write QE")
      
      println("\n" + "="*60)
      println("APPROACH 2: Custom Spark Listener (MOST ACCURATE)")
      println("="*60)
      
      // Method 2: Use a custom SparkListener to capture QueryExecution during write
      setupQueryExecutionCapture(spark)
      
      // Perform write operation
      transformedDf.write.mode("overwrite").format("parquet").save("/tmp/ages_over_25_listener")
      
      // The captured QueryExecution would be available here
      // (Implementation shown below)
      
      println("\n" + "="*60)
      println("APPROACH 3: Wrapper Method (PRACTICAL)")
      println("="*60)
      
      // Method 3: Create a wrapper that captures QueryExecution during write
      val capturedEvent = captureWriteEvent(transformedDf, "/tmp/ages_over_25_wrapper", "parquet")
      println(s"📊 Wrapper approach - Inputs: ${capturedEvent.getInputs.size}")
      println(s"📊 Wrapper approach - Outputs: ${capturedEvent.getOutputs.size}")
      
      println("\n" + "="*60)
      println("APPROACH 4: DataFrame Write Hook (EXPERIMENTAL)")
      println("="*60)
      
      // Method 4: Hook into DataFrame.write operation
      val writeEvent = captureFromWriteOperation {
        transformedDf.write.mode("overwrite").format("parquet").save("/tmp/ages_over_25_hook")
      }
      
      if (writeEvent.isDefined) {
        println(s"📊 Write hook - Inputs: ${writeEvent.get.getInputs.size}")
        println(s"📊 Write hook - Outputs: ${writeEvent.get.getOutputs.size}")
      }
      
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
  
  /**
   * APPROACH 2: Setup a custom SparkListener to capture QueryExecution during writes
   */
  def setupQueryExecutionCapture(spark: SparkSession): Unit = {
    println("🔧 Method 2: Custom SparkListener")
    
    // This is how OpenLineage actually works - by listening to SQL execution events
    val listener = new org.apache.spark.sql.util.QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        // This is where you'd capture the write QueryExecution
        println(s"✅ SQL execution completed: $funcName")
        
        // Check if this is a write operation
        val plan = qe.logical.toString
        if (plan.contains("InsertIntoHadoopFsRelationCommand") || 
            plan.contains("CreateDataSourceTableAsSelectCommand")) {
          
          println("🎯 WRITE OPERATION DETECTED!")
          println(s"Logical plan: ${qe.logical.getClass.getSimpleName}")
          
          // Create OpenLineage event from write QueryExecution
          val event = OpenLineageManualEventCreator.createCompletedEvent(qe)
          println(s"📊 Write QE - Inputs: ${event.getInputs.size}")
          println(s"📊 Write QE - Outputs: ${event.getOutputs.size}")
        }
      }
      
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
        println(s"❌ SQL execution failed: $funcName - ${exception.getMessage}")
      }
    }
    
    spark.listenerManager.register(listener)
    println("✅ QueryExecutionListener registered")
  }
  
  /**
   * APPROACH 3: Wrapper method that performs write and captures lineage
   */
  def captureWriteEvent(df: org.apache.spark.sql.DataFrame, path: String, format: String): io.openlineage.client.OpenLineage.RunEvent = {
    println("🔧 Method 3: Write Wrapper")
    
    // The key insight: We need to capture the QueryExecution that includes the write command
    // This requires hooking into the actual write execution
    
    // For now, we'll use the enhanced transformation-based approach
    // In a real implementation, you'd intercept the write command
    
    df.write.mode("overwrite").format(format).save(path)
    
    // Use our enhanced method that can extract inputs from transformation plans
    val event = OpenLineageManualEventCreator.createCompletedEvent(df.queryExecution)
    
    println("✅ Write operation completed with lineage capture")
    return event
  }
  
  /**
   * APPROACH 4: Hook into DataFrame write operation (experimental)
   */
  def captureFromWriteOperation(writeOp: => Unit): Option[io.openlineage.client.OpenLineage.RunEvent] = {
    println("🔧 Method 4: Write Operation Hook")
    
    // This is conceptual - in practice you'd need to modify Spark internals
    // or use bytecode manipulation to intercept the write operation
    
    try {
      writeOp // Execute the write operation
      
      // In a real implementation, you would:
      // 1. Intercept the DataFrameWriter.save() call
      // 2. Extract the QueryExecution from the write command
      // 3. Create OpenLineage event from that QueryExecution
      
      println("⚠️  Write hook is conceptual - would need Spark internals access")
      return None
      
    } catch {
      case e: Exception =>
        println(s"❌ Write operation failed: ${e.getMessage}")
        return None
    }
  }
  
  def cleanup(): Unit = {
    import java.io.File
    
    def deleteDirectory(file: File): Unit = {
      if (file.exists()) {
        if (file.isDirectory) {
          file.listFiles().foreach(deleteDirectory)
        }
        file.delete()
      }
    }
    
    try {
      deleteDirectory(new File("/tmp/ages"))
      deleteDirectory(new File("/tmp/ages_over_25_sql"))
      deleteDirectory(new File("/tmp/ages_over_25_listener"))
      deleteDirectory(new File("/tmp/ages_over_25_wrapper"))
      deleteDirectory(new File("/tmp/ages_over_25_hook"))
      println("\n🧹 Cleaned up temporary files")
    } catch {
      case _: Exception => // Ignore cleanup errors
    }
  }
} 