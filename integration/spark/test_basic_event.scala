import io.openlineage.spark.agent.util.OpenLineageManualEventCreator
import org.apache.spark.sql.SparkSession

println("Testing OpenLineageManualEventCreator basic functionality...")

val spark = SparkSession.builder()
  .appName("BasicEventTest")
  .master("local[*]")
  .config("spark.ui.enabled", "false")
  .config("spark.openlineage.namespace", "test_namespace")
  .getOrCreate()

try {
  println("Creating simple DataFrame...")
  val df = spark.range(5).toDF("id")
  val result = df.select($"id" * 2 as "doubled_id")
  
  println("Executing DataFrame...")
  result.collect() // Trigger execution
  
  println("Creating OpenLineage event...")
  val event = OpenLineageManualEventCreator.createCompletedEvent(result.queryExecution)
  
  println("✅ SUCCESS: Basic event creation working!")
  println(s"Job Name: ${event.getJob.getName}")
  println(s"Namespace: ${event.getJob.getNamespace}")
  println(s"Event Type: ${event.getEventType}")
  println(s"Run ID: ${event.getRun.getRunId}")
  
} catch {
  case e: Exception =>
    println(s"❌ ERROR: ${e.getMessage}")
    e.printStackTrace()
} finally {
  spark.stop()
  println("Test completed.")
} 