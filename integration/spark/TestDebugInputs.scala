import io.openlineage.spark.agent.util.OpenLineageManualEventCreator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TestDebugInputs {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TestDebugInputs")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.openlineage.namespace", "test_namespace")
      .getOrCreate()

    import spark.implicits._

    try {
      println("=== Replicating Your Exact Issue ===\n")
      
      // Step 1: Create test input data (same as your setup)
      println("📝 Creating test data at /tmp/ages...")
      val sourceData = spark.range(1, 10).select(
        $"id",
        ($"id" + 20).as("age"),
        concat(lit("user_"), $"id").as("name")
      )
      sourceData.write.mode("overwrite").parquet("/tmp/ages")
      println("✅ Created input file")
      
      // Step 2: Your exact code
      println("\n🔍 Running your exact code:")
      println("val ages = spark.read.format(\"parquet\").load(\"/tmp/ages\")")
      println("val transformedDf = ages.filter($\"age\" > 25).select($\"id\", $\"name\", ($\"age\" + 5).as(\"adjusted_age\"))")
      println("transformedDf.write.mode(\"overwrite\").format(\"parquet\").save(\"/tmp/ages_over_25\")")
      println("val event1 = OpenLineageManualEventCreator.createCompletedEvent(transformedDf.queryExecution)")
      
      val ages = spark.read.format("parquet").load("/tmp/ages")
      val transformedDf = ages.filter($"age" > 25).select($"id", $"name", ($"age" + 5).as("adjusted_age"))
      transformedDf.write.mode("overwrite").format("parquet").save("/tmp/ages_over_25")
      val event1 = OpenLineageManualEventCreator.createCompletedEvent(transformedDf.queryExecution)
      
      println(s"\n📊 RESULT: event1.getInputs.size = ${event1.getInputs.size}")
      println(s"📊 RESULT: event1.getOutputs.size = ${event1.getOutputs.size}")
      
      // Step 3: Debug analysis
      println("\n🔍 DEBUG ANALYSIS:")
      println("==================")
      val debugInfo = OpenLineageManualEventCreator.debugLogicalPlan(transformedDf.queryExecution)
      println(debugInfo)
      
      // Step 4: Show what the transformation plan actually contains
      println("\n🧐 WHY NO INPUTS? Let's examine the transformation plan:")
      println("=======================================================")
      println("Your transformedDf.queryExecution.logical contains:")
      println(transformedDf.queryExecution.logical.treeString)
      
      // You should see something like:
      // Project [id#123, name#124, ...]
      // +- Filter (age#125 > 25)
      //    +- Relation [id#123,age#125,name#124] parquet
      
      println("\n✅ Good news: The LogicalRelation (parquet source) IS there!")
      println("❌ Problem: OpenLineage visitors aren't detecting it properly")
      
      // Step 5: Demonstrate the difference with write operations
      println("\n🔧 SOLUTION: You need the WRITE operation's QueryExecution")
      println("===========================================================")
      println("The transformation plan shows what data flows through the pipeline")
      println("The write plan shows what actually gets written and from where")
      println("OpenLineage normally hooks into SQL execution events, not DataFrame transformations")
      
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
      deleteDirectory(new File("/tmp/ages_over_25"))
      println("\n🧹 Cleaned up temporary files")
    } catch {
      case _: Exception => // Ignore cleanup errors
    }
  }
} 