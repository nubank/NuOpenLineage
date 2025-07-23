import io.openlineage.spark.agent.util.OpenLineageManualEventCreator
import org.apache.spark.sql.SparkSession

object TestManualEvent {
  def main(args: Array[String]): Unit = {
    println("Starting OpenLineageManualEventCreator test...")
    
    val spark = SparkSession.builder()
      .appName("TestManualEvent")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.openlineage.namespace", "test_namespace")
      .getOrCreate()

    try {
      println("Creating test DataFrame...")
      val df = spark.range(10).toDF("id")
      val result = df.select($"id" * 2 as "doubled_id")
      
      println("Executing DataFrame to create QueryExecution...")
      result.collect() // Trigger execution
      
      println("Creating OpenLineage event...")
      val event = OpenLineageManualEventCreator.createCompletedEvent(result.queryExecution)
      
      println("✅ SUCCESS: OpenLineage event created successfully!")
      println(s"Job Name: ${event.getJob.getName}")
      println(s"Namespace: ${event.getJob.getNamespace}")
      println(s"Event Type: ${event.getEventType}")
      println(s"Run ID: ${event.getRun.getRunId}")
      println(s"Number of outputs: ${event.getOutputs.size()}")
      
      if (!event.getOutputs.isEmpty) {
        val output = event.getOutputs.get(0)
        println(s"Output dataset: ${output.getNamespace}/${output.getName}")
        if (output.getFacets.getSchema != null) {
          println(s"Schema fields: ${output.getFacets.getSchema.getFields.size()}")
        }
        if (output.getFacets.getColumnLineage != null) {
          println(s"Column lineage fields: ${output.getFacets.getColumnLineage.getFields.getAdditionalProperties.size()}")
        }
      }
      
    } catch {
      case e: Exception =>
        println(s"❌ ERROR: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
      println("Test completed.")
    }
  }
} 