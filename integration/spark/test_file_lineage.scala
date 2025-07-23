import io.openlineage.spark.agent.util.OpenLineageManualEventCreator
import org.apache.spark.sql.SparkSession
import java.io.File
import java.nio.file.{Files, Paths}

println("Testing OpenLineageManualEventCreator with file-based operations...")

val spark = SparkSession.builder()
  .appName("FileLineageTest")
  .master("local[*]")
  .config("spark.ui.enabled", "false")
  .config("spark.openlineage.namespace", "test_namespace")
  .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
  .getOrCreate()

try {
  import spark.implicits._
  
  // Create temporary directories for input and output
  val inputDir = "/tmp/openlineage_test_input"
  val outputDir = "/tmp/openlineage_test_output"
  
  // Clean up any existing directories
  def deleteDirectory(path: String): Unit = {
    val dir = new File(path)
    if (dir.exists()) {
      dir.listFiles().foreach(_.delete())
      dir.delete()
    }
  }
  
  deleteDirectory(inputDir)
  deleteDirectory(outputDir)
  
  println("Creating test data files...")
  
  // Create some test data and save it as Parquet (this will be our input)
  val testData = Seq(
    (1, "Alice", 25, "Engineering"),
    (2, "Bob", 30, "Marketing"), 
    (3, "Charlie", 35, "Engineering"),
    (4, "Diana", 28, "Sales")
  ).toDF("id", "name", "age", "department")
  
  testData.write.mode("overwrite").parquet(inputDir)
  println(s"✅ Created input data at: $inputDir")
  
  // Now read from the parquet file and perform transformations
  println("Reading from input file and performing transformations...")
  val inputDF = spark.read.parquet(inputDir)
  
  val transformedDF = inputDF
    .filter($"department" === "Engineering")
    .select(
      $"id",
      $"name", 
      ($"age" + 5).alias("adjusted_age"),
      $"department".alias("dept")
    )
  
  // Write the result to another parquet file (this will be our output)
  transformedDF.write.mode("overwrite").parquet(outputDir)
  println(s"✅ Created output data at: $outputDir")
  
  // Now create an OpenLineage event from the write operation
  println("Creating OpenLineage event from QueryExecution...")
  val event = OpenLineageManualEventCreator.createCompletedEvent(transformedDF.queryExecution)
  
  println("✅ SUCCESS: File-based lineage event created!")
  println(s"Job Name: ${event.getJob.getName}")
  println(s"Namespace: ${event.getJob.getNamespace}")
  println(s"Event Type: ${event.getEventType}")
  println(s"Run ID: ${event.getRun.getRunId}")
  
  println(s"\n📥 Input datasets: ${event.getInputs.size()}")
  event.getInputs.forEach { input =>
    println(s"  - ${input.getNamespace}/${input.getName}")
    if (input.getFacets.getSchema != null) {
      println(s"    Schema fields: ${input.getFacets.getSchema.getFields.size()}")
      input.getFacets.getSchema.getFields.forEach { field =>
        println(s"      • ${field.getName}: ${field.getType}")
      }
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
        lineageInfo.getInputFields.forEach { inputField =>
          println(s"        - ${inputField.getNamespace}/${inputField.getName}/${inputField.getField}")
        }
      }
    }
  }
  
  // Test with an actual write operation to see if we get output datasets
  println("\n🔄 Testing with write operation...")
  val writeOutputDir = "/tmp/openlineage_test_write_output"
  deleteDirectory(writeOutputDir)
  
  val writeOperation = transformedDF.write.mode("overwrite").parquet(writeOutputDir)
  // The write operation itself might generate lineage
  
  // Clean up
  deleteDirectory(inputDir)
  deleteDirectory(outputDir) 
  deleteDirectory(writeOutputDir)
  
} catch {
  case e: Exception =>
    println(s"❌ ERROR: ${e.getMessage}")
    e.printStackTrace()
} finally {
  spark.stop()
  println("\nTest completed.")
} 