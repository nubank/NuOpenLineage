import io.openlineage.spark.agent.util.OpenLineageManualEventCreator
import org.apache.spark.sql.SparkSession
import java.io.File

println("Testing OpenLineageManualEventCreator with complete lineage...")

val spark = SparkSession.builder()
  .appName("CompleteLineageTest")
  .master("local[*]")
  .config("spark.ui.enabled", "false")
  .config("spark.openlineage.namespace", "test_namespace")
  .getOrCreate()

try {
  import spark.implicits._
  
  println("Creating test data and DataFrame with clear lineage...")
  
  // Create some test data
  val testData = Seq(
    (1, "Alice", 25, "Engineering"),
    (2, "Bob", 30, "Marketing"),
    (3, "Charlie", 35, "Engineering"),
    (4, "Diana", 28, "Sales")
  ).toDF("id", "name", "age", "department")
  
  // Create a temporary view (acts as input)
  testData.createOrReplaceTempView("employees")
  
  // Perform transformations that should create clear lineage
  val result = spark.sql("""
    SELECT 
      id,
      name,
      age + 5 as adjusted_age,
      UPPER(department) as dept_upper,
      CASE WHEN age > 30 THEN 'Senior' ELSE 'Junior' END as level
    FROM employees 
    WHERE department = 'Engineering'
  """)
  
  println("Executing query to create QueryExecution...")
  result.collect() // Trigger execution
  
  println("Creating OpenLineage event...")
  val event = OpenLineageManualEventCreator.createCompletedEvent(result.queryExecution)
  
  println("✅ SUCCESS: Complete lineage event created!")
  println(s"Job Name: ${event.getJob.getName}")
  println(s"Namespace: ${event.getJob.getNamespace}")
  println(s"Event Type: ${event.getEventType}")
  println(s"Run ID: ${event.getRun.getRunId}")
  
  println(s"\n📥 Input datasets: ${event.getInputs.size()}")
  event.getInputs.forEach { input =>
    println(s"  - ${input.getNamespace}/${input.getName}")
    if (input.getFacets.getSchema != null) {
      println(s"    Schema fields: ${input.getFacets.getSchema.getFields.size()}")
    }
  }
  
  println(s"\n📤 Output datasets: ${event.getOutputs.size()}")
  event.getOutputs.forEach { output =>
    println(s"  - ${output.getNamespace}/${output.getName}")
    if (output.getFacets.getSchema != null) {
      println(s"    Schema fields: ${output.getFacets.getSchema.getFields.size()}")
      output.getFacets.getSchema.getFields.forEach { field =>
        println(s"      • ${field.getName}: ${field.getType}")
      }
    }
    if (output.getFacets.getColumnLineage != null) {
      val columnLineage = output.getFacets.getColumnLineage.getFields.getAdditionalProperties
      println(s"    Column lineage: ${columnLineage.size()} columns")
      columnLineage.forEach { (columnName, lineageInfo) =>
        println(s"      • ${columnName}: ${lineageInfo.getInputFields.size()} input fields")
      }
    }
  }
  
  // Test with custom job name and time
  println("\n🔄 Testing with custom parameters...")
  val customEvent = OpenLineageManualEventCreator.createCompletedEvent(
    result.queryExecution,
    java.util.Optional.of("custom_lineage_job"),
    java.util.Optional.of(java.time.ZonedDateTime.now())
  )
  println(s"Custom job name: ${customEvent.getJob.getName}")
  
} catch {
  case e: Exception =>
    println(s"❌ ERROR: ${e.getMessage}")
    e.printStackTrace()
} finally {
  spark.stop()
  println("\nTest completed.")
} 