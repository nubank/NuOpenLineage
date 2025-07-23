package io.openlineage.spark.agent.util

import io.openlineage.spark.agent.util.OpenLineageManualEventCreator
import org.apache.spark.sql.SparkSession
import java.time.ZonedDateTime

object ManualEventCreationExample {
  def main(args: Array[String]): Unit = {
    println("Starting ManualEventCreationExample...")
    val spark = SparkSession.builder()
      .appName("ManualEventCreationExample")
      .master("local[*]")
      .config("spark.openlineage.namespace", "example_namespace")
      .getOrCreate()

    try {
      // Create a simple DataFrame with transformations
      import spark.implicits._

      Seq((1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)).toDF("id", "name", "age").write.mode("overwrite").format("parquet").save("/tmp/ages")

      val ages = spark.read.format("parquet").load("/tmp/ages")

      // Perform some transformations to create lineage
      val transformedDf = ages.filter($"age" > 25).select($"id", $"name", ($"age" + 5).as("adjusted_age"))

      // Trigger action to create QueryExecution
      transformedDf.write.mode("overwrite").format("parquet").save("/tmp/ages_over_25")

      // Method 1: Create event with default job name and current time
      val event1 = OpenLineageManualEventCreator.createCompletedEvent(transformedDf.queryExecution)

      // Method 2: Create event with custom job name and time
      val customJobName = Some("my_custom_lineage_job")
      val customTime = Some(ZonedDateTime.now())
      val event2 = OpenLineageManualEventCreator.createCompletedEvent(
        transformedDf.queryExecution,
        customJobName,
        customTime
      )

      println("OpenLineage events created successfully!")
      println(s"Event 1 Job Name: ${event1.getJob.getName}")
      println(s"Event 2 Job Name: ${event2.getJob.getName}")
      println(s"Number of inputs: ${event1.getInputs.size()}")
      println(s"Number of outputs: ${event1.getOutputs.size()}")

      // Optional: Print the event as JSON for inspection
      import io.openlineage.client.OpenLineageClientUtils
      println("Event JSON:")
      println(OpenLineageClientUtils.toJson(event1))

    } finally {
      spark.stop()
    }
  }
}
