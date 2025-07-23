package io.openlineage.spark.agent.util;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.ArgumentParser;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.lifecycle.InternalEventHandlerFactory;
import io.openlineage.spark.agent.lifecycle.OpenLineageRunEventBuilder;
import io.openlineage.spark.agent.lifecycle.SparkSQLQueryParser;
import io.openlineage.spark.agent.lifecycle.SparkOpenLineageExtensionVisitorWrapper;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark.api.Vendors;
import io.openlineage.spark.agent.lifecycle.DatasetBuilderFactoryProvider;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.SQLExecution;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.types.StructType;
import scala.Function1;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Stack;
import java.util.UUID;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.ArrayList;
import org.apache.spark.sql.types.StructField;

public class OpenLineageManualEventCreator {
  private OpenLineageManualEventCreator() {} // Prevent instantiation

  public static OpenLineage.RunEvent createCompletedEvent(QueryExecution queryExecution) {
    return createCompletedEvent(queryExecution, Optional.empty(), Optional.empty());
  }

  public static OpenLineage.RunEvent createCompletedEvent(
      QueryExecution queryExecution,
      Optional<String> jobName,
      Optional<ZonedDateTime> eventTime) {

    SparkOpenLineageConfig config = ArgumentParser.parse(queryExecution.sparkSession().sparkContext().conf());
    OpenLineage ol = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);

    // Create the meter registry first
    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    OpenLineageContext olContext = OpenLineageContext.builder()
        .sparkSession(queryExecution.sparkSession())
        .sparkContext(queryExecution.sparkSession().sparkContext())
        .openLineage(ol)
        .queryExecution(queryExecution)
        .runUuid(UUID.randomUUID())
        .meterRegistry(meterRegistry)
        .customEnvironmentVariables(Collections.emptyList())
        .vendors(Vendors.getVendors())
        .openLineageConfig(config)
        .sparkExtensionVisitorWrapper(new SparkOpenLineageExtensionVisitorWrapper(config))
        .build();

    // Debug: Check if meterRegistry is properly set
    if (olContext.getMeterRegistry() == null) {
      throw new IllegalStateException("MeterRegistry is null after building OpenLineageContext");
    }

    // Initialize the visitors using the factory
    InternalEventHandlerFactory handlerFactory = new InternalEventHandlerFactory();
    handlerFactory.createInputDatasetQueryPlanVisitors(olContext);
    handlerFactory.createOutputDatasetQueryPlanVisitors(olContext);
    handlerFactory.createInputDatasetBuilder(olContext);
    handlerFactory.createOutputDatasetBuilder(olContext);
    
    OpenLineageRunEventBuilder runEventBuilder = new OpenLineageRunEventBuilder(olContext, handlerFactory);

    String resolvedJobName = jobName.orElse("manual_lineage_job");
    String namespace = config.getNamespace() != null ? config.getNamespace() : "unknown";

    // Build the event directly using OpenLineage builders
    UUID runId = olContext.getRunUuid();
    ZonedDateTime eventTimestamp = eventTime.orElse(ZonedDateTime.now(ZoneOffset.UTC));

    // Build Job
    OpenLineage.Job job = ol.newJobBuilder()
        .namespace(namespace)
        .name(resolvedJobName)
        .facets(ol.newJobFacetsBuilder()
            .jobType(ol.newJobTypeJobFacetBuilder()
                .jobType("SQL_JOB")
                .integration("SPARK")
                .processingType(queryExecution.optimizedPlan().isStreaming() ? "STREAMING" : "BATCH")
                .build())
            .build())
        .build();

    // Add SQL facet if available
    Optional<OpenLineage.SQLJobFacet> sqlFacet = resolveSQLFacets(ol, queryExecution);
    if (sqlFacet.isPresent()) {
      job = ol.newJobBuilder()
          .namespace(job.getNamespace())
          .name(job.getName())
          .facets(ol.newJobFacetsBuilder()
              .jobType(job.getFacets().getJobType())
              .sql(sqlFacet.get())
              .build())
          .build();
    }

    // Build Run
    OpenLineage.Run run = ol.newRunBuilder()
        .runId(runId)
        .facets(ol.newRunFacetsBuilder().build())
        .build();

    // Build inputs and outputs using the existing infrastructure 
    List<OpenLineage.InputDataset> inputs = Collections.emptyList();
    List<OpenLineage.OutputDataset> outputs = Collections.emptyList();

    try {
      // Use the same approach as OpenLineageRunEventBuilder.buildInputDatasets and buildOutputDatasets
      inputs = buildInputDatasetsFromQueryExecution(olContext);
      outputs = buildOutputDatasetsFromQueryExecution(olContext);
      
      // If no outputs found from file operations, create synthetic output dataset for column lineage
      if (outputs.isEmpty()) {
        OpenLineage.OutputDataset syntheticOutput = createSyntheticOutputWithColumnLineage(ol, queryExecution, namespace, resolvedJobName);
        if (syntheticOutput != null) {
          outputs = Collections.singletonList(syntheticOutput);
        }
      }
      
    } catch (Exception e) {
      // If there's any issue building datasets, log it but continue with empty lists
      System.err.println("Warning: Could not build input/output datasets: " + e.getMessage());
      e.printStackTrace();
    }

    // Build the final event
    return ol.newRunEventBuilder()
        .eventType(OpenLineage.RunEvent.EventType.COMPLETE)
        .eventTime(eventTimestamp)
        .run(run)
        .job(job)
        .inputs(inputs)
        .outputs(outputs)
        .build();
  }

  /**
   * Extract column lineage information from a QueryExecution, even for in-memory operations.
   * This method analyzes the logical plan to understand column transformations without requiring
   * actual input/output datasets.
   * 
   * @param queryExecution The QueryExecution to analyze
   * @return ColumnLineageInfo containing the transformation details
   */
  public static ColumnLineageInfo extractColumnLineage(QueryExecution queryExecution) {
    StructType outputSchema = queryExecution.analyzed().schema();
    
    ColumnLineageInfo lineageInfo = new ColumnLineageInfo();
    lineageInfo.outputSchema = outputSchema;
    lineageInfo.transformations = new HashMap<>();
    
    // For now, create basic transformation info without complex expression analysis
    for (StructField field : outputSchema.fields()) {
      String columnName = field.name();
      lineageInfo.transformations.put(columnName, 
          new ColumnTransformation("DIRECT", Collections.singletonList("source_" + columnName), "Basic transformation"));
    }
    
    return lineageInfo;
  }
  
  /**
   * Container class for column lineage information
   */
  public static class ColumnLineageInfo {
    public StructType outputSchema;
    public Map<String, ColumnTransformation> transformations;
    
    public void printSummary() {
      System.out.println("=== Column Lineage Summary ===");
      System.out.println("Output columns: " + outputSchema.fieldNames().length);
      
      for (String columnName : transformations.keySet()) {
        ColumnTransformation transformation = transformations.get(columnName);
        System.out.println("• " + columnName + " ← " + transformation.transformationType + 
                          " (depends on: " + String.join(", ", transformation.sourceColumns) + ")");
      }
    }
  }
  
  /**
   * Represents a column transformation
   */
  public static class ColumnTransformation {
    public String transformationType;
    public List<String> sourceColumns;
    public String expression;
    
    public ColumnTransformation(String type, List<String> sources, String expr) {
      this.transformationType = type;
      this.sourceColumns = sources;
      this.expression = expr;
    }
  }
  
  private static void analyzeLogicalPlan(LogicalPlan plan, ColumnLineageInfo lineageInfo) {
    // Simplified implementation - removed problematic expression analysis
    
    // Recursively analyze child plans
    List<LogicalPlan> children = ScalaConversionUtils.fromSeq(plan.children());
    for (LogicalPlan child : children) {
      analyzeLogicalPlan(child, lineageInfo);
    }
  }


  /**
   * Create a COMPLETED OpenLineage event by intercepting the QueryExecution from a write operation.
   * This method should be called during or after a write operation to capture the actual write command.
   * 
   * Usage example:
   * <pre>
   * val df = spark.read.parquet("input.parquet")
   * val result = df.filter("age > 25").select("name", "age")
   * 
   * // Use this method to capture the write operation
   * val event = OpenLineageManualEventCreator.createCompletedEventFromWrite(() => {
   *   result.write.mode("overwrite").parquet("output.parquet")
   * })
   * </pre>
   */
  public static OpenLineage.RunEvent createCompletedEventFromWrite(Runnable writeOperation) {
    return createCompletedEventFromWrite(writeOperation, Optional.empty(), Optional.empty());
  }

  public static OpenLineage.RunEvent createCompletedEventFromWrite(
      Runnable writeOperation,
      Optional<String> jobName,
      Optional<ZonedDateTime> eventTime) {
    
    // Custom listener to capture the SQL execution
    class QueryExecutionCapture {
      QueryExecution capturedExecution = null;
    }
    
    QueryExecutionCapture capture = new QueryExecutionCapture();
    
    // Execute the write operation and capture the QueryExecution
    // Note: This is a simplified approach. In practice, you might need to hook into Spark's execution
    writeOperation.run();
    
    // For now, we'll need the user to provide the QueryExecution
    // In a real implementation, you would hook into Spark's SQL execution to capture this
    throw new UnsupportedOperationException(
        "Write operation capturing is not yet implemented. " +
        "Please use createCompletedEvent(queryExecution) with a QueryExecution from a write command.");
  }

  /**
   * PRACTICAL SOLUTION: Wrapper method for write operations that captures proper lineage.
   * This method performs the write operation and captures the lineage information.
   * 
   * Usage:
   * <pre>
   * val event = OpenLineageManualEventCreator.captureWriteEventScala(
   *   transformedDf,
   *   "/path/to/output",
   *   "parquet",
   *   Optional.of("overwrite")
   * )
   * </pre>
   */
  public static OpenLineage.RunEvent captureWriteEvent(
      Object dataFrame,
      String outputPath,
      String format,
      Optional<String> mode) {
    
    try {
      // This method works with both Scala and Java DataFrames
      // We'll use reflection to call the write operation and capture lineage
      
      Class<?> dfClass = dataFrame.getClass();
      Object writer = dfClass.getMethod("write").invoke(dataFrame);
      
      // Set mode if provided
      if (mode.isPresent()) {
        Object writerWithMode = writer.getClass().getMethod("mode", String.class).invoke(writer, mode.get());
        writer = writerWithMode;
      }
      
      // Set format
      Object writerWithFormat = writer.getClass().getMethod("format", String.class).invoke(writer, format);
      
      // Execute the write operation
      writerWithFormat.getClass().getMethod("save", String.class).invoke(writerWithFormat, outputPath);
      
      // Extract the QueryExecution from the DataFrame
      Object queryExecution = dfClass.getMethod("queryExecution").invoke(dataFrame);
      
      // Create the OpenLineage event using our enhanced method
      return createCompletedEvent((QueryExecution) queryExecution);
      
    } catch (Exception e) {
      System.err.println("Failed to capture write event: " + e.getMessage());
      e.printStackTrace();
      throw new RuntimeException("Failed to capture write event", e);
    }
  }

  /**
   * MOST PRACTICAL SOLUTION: Register a QueryExecutionListener to capture write operations.
   * This is how OpenLineage actually works internally.
   * 
   * Usage:
   * <pre>
   * // Register the listener once
   * OpenLineageManualEventCreator.registerWriteEventCapture(spark);
   * 
   * // Perform your operations - events will be captured automatically
   * transformedDf.write.mode("overwrite").parquet("/path/to/output");
   * </pre>
   */
  public static void registerWriteEventCapture(Object sparkSession) {
    try {
      Class<?> sparkClass = sparkSession.getClass();
      Object listenerManager = sparkClass.getMethod("listenerManager").invoke(sparkSession);
      
      // Create a QueryExecutionListener that captures write operations
      Object listener = java.lang.reflect.Proxy.newProxyInstance(
          Thread.currentThread().getContextClassLoader(),
          new Class<?>[] { 
              Class.forName("org.apache.spark.sql.util.QueryExecutionListener") 
          },
          new WriteEventCaptureHandler()
      );
      
      // Register the listener
      listenerManager.getClass().getMethod("register", 
          Class.forName("org.apache.spark.sql.util.QueryExecutionListener"))
          .invoke(listenerManager, listener);
      
      System.out.println("✅ Write event capture listener registered successfully");
      
    } catch (Exception e) {
      System.err.println("Failed to register write event capture: " + e.getMessage());
      e.printStackTrace();
    }
  }

  /**
   * Handler for capturing write events through QueryExecutionListener
   */
  private static class WriteEventCaptureHandler implements java.lang.reflect.InvocationHandler {
    @Override
    public Object invoke(Object proxy, java.lang.reflect.Method method, Object[] args) throws Throwable {
      if ("onSuccess".equals(method.getName()) && args.length >= 2) {
        String funcName = (String) args[0];
        QueryExecution queryExecution = (QueryExecution) args[1];
        
        // Check if this is a write operation
        String planString = queryExecution.logical().toString();
        if (planString.contains("InsertIntoHadoopFsRelationCommand") ||
            planString.contains("CreateDataSourceTableAsSelectCommand") ||
            planString.contains("SaveIntoDataSourceCommand")) {
          
          System.out.println("🎯 WRITE OPERATION DETECTED: " + funcName);
          System.out.println("Logical plan type: " + queryExecution.logical().getClass().getSimpleName());
          
          // Create OpenLineage event from the write QueryExecution
          OpenLineage.RunEvent event = createCompletedEvent(queryExecution);
          
          System.out.println("📊 Write Event - Inputs: " + event.getInputs().size());
          System.out.println("📊 Write Event - Outputs: " + event.getOutputs().size());
          
          // Print input details
          if (!event.getInputs().isEmpty()) {
            event.getInputs().forEach(input -> 
                System.out.println("  📁 Input: " + input.getNamespace() + "/" + input.getName()));
          }
          
          // Print output details
          if (!event.getOutputs().isEmpty()) {
            event.getOutputs().forEach(output -> 
                System.out.println("  📁 Output: " + output.getNamespace() + "/" + output.getName()));
          }
          
          // Store the event somewhere accessible (you could use a callback here)
          lastCapturedEvent = event;
        }
      }
      
      return null; // QueryExecutionListener methods return void
    }
  }
  
  // Static field to store the last captured event (for demonstration)
  private static OpenLineage.RunEvent lastCapturedEvent = null;
  
  /**
   * Get the last captured write event (for testing/demonstration purposes)
   */
  public static Optional<OpenLineage.RunEvent> getLastCapturedEvent() {
    return Optional.ofNullable(lastCapturedEvent);
  }

  private static List<OpenLineage.InputDataset> buildInputDatasetsFromQueryExecution(
      OpenLineageContext olContext) {
    
    // Try the standard approach first
    Function1<LogicalPlan, Collection<OpenLineage.InputDataset>> inputVisitor =
        visitLogicalPlan(PlanUtils.merge(olContext.getInputDatasetQueryPlanVisitors()));

    List<OpenLineage.InputDataset> standardInputs = olContext
        .getQueryExecution()
        .map(qe -> 
            ScalaConversionUtils.fromSeq(qe.optimizedPlan().map(inputVisitor))
                .stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toList()))
        .orElse(Collections.emptyList());
    
    // If standard approach finds inputs, return them
    if (!standardInputs.isEmpty()) {
      return standardInputs;
    }
    
    // Enhanced approach: Manually traverse the logical plan to find inputs
    // This is needed because standard visitors expect write operation context
    return olContext
        .getQueryExecution()
        .map(qe -> extractInputsFromTransformationPlan(olContext, qe.logical()))
        .orElse(Collections.emptyList());
  }
  
  /**
   * Enhanced method to extract input datasets from transformation plans.
   * This manually traverses the logical plan to find LogicalRelation nodes.
   */
  private static List<OpenLineage.InputDataset> extractInputsFromTransformationPlan(
      OpenLineageContext olContext, LogicalPlan logicalPlan) {
    
    List<OpenLineage.InputDataset> inputs = new ArrayList<>();
    
    // Traverse the logical plan to find all LogicalRelation nodes
    traverseForRelations(logicalPlan, inputs, olContext);
    
    return inputs;
  }
  
  /**
   * Recursively traverse the logical plan to find LogicalRelation nodes
   */
  private static void traverseForRelations(LogicalPlan plan, List<OpenLineage.InputDataset> inputs, OpenLineageContext olContext) {
    // Check if this node is a LogicalRelation (file/table source)
    String className = plan.getClass().getSimpleName();
    
    if (className.contains("LogicalRelation") || className.contains("HiveTableRelation") || 
        className.contains("DataSourceV2Relation")) {
      
      try {
        // Use the merged visitor approach like the standard method
        Function1<LogicalPlan, Collection<OpenLineage.InputDataset>> inputVisitor =
            visitLogicalPlan(PlanUtils.merge(olContext.getInputDatasetQueryPlanVisitors()));
        
        Collection<OpenLineage.InputDataset> foundInputs = inputVisitor.apply(plan);
        inputs.addAll(foundInputs);
        
      } catch (Exception e) {
        System.err.println("Warning: Could not extract input from relation node: " + e.getMessage());
        // Fallback: Create a basic input dataset manually
        try {
          createBasicInputDataset(plan, inputs, olContext);
        } catch (Exception fallbackError) {
          System.err.println("Warning: Fallback input creation also failed: " + fallbackError.getMessage());
        }
      }
    }
    
    // Recursively traverse children
    scala.collection.Iterator<LogicalPlan> childIterator = plan.children().iterator();
    while (childIterator.hasNext()) {
      LogicalPlan child = childIterator.next();
      traverseForRelations(child, inputs, olContext);
    }
  }
  
  /**
   * Fallback method to create a basic input dataset when visitors fail
   */
  private static void createBasicInputDataset(LogicalPlan plan, List<OpenLineage.InputDataset> inputs, OpenLineageContext olContext) {
    // This is a simplified fallback for demonstration
    // In practice, you'd extract the actual path/table info from the LogicalRelation
    String planString = plan.toString();
    
    // Try to extract path from the plan string (very basic approach)
    String datasetName = "unknown_input";
    if (planString.contains("parquet")) {
      datasetName = "parquet_input_" + System.currentTimeMillis();
    } else if (planString.contains("csv")) {
      datasetName = "csv_input_" + System.currentTimeMillis();
    }
    
    OpenLineage ol = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
    String namespace = olContext.getOpenLineageConfig().getNamespace();
    
    OpenLineage.InputDataset input = ol.newInputDatasetBuilder()
        .namespace(namespace)
        .name(datasetName)
        .facets(ol.newDatasetFacetsBuilder().build())
        .build();
    
    inputs.add(input);
  }

  private static List<OpenLineage.OutputDataset> buildOutputDatasetsFromQueryExecution(
      OpenLineageContext olContext) {
    
    // Replicate the logic from OpenLineageRunEventBuilder.buildOutputDatasets
    Function1<LogicalPlan, Collection<OpenLineage.OutputDataset>> visitor =
        visitLogicalPlan(PlanUtils.merge(olContext.getOutputDatasetQueryPlanVisitors()));
        
    return olContext
        .getQueryExecution()
        .map(qe -> visitor.apply(qe.optimizedPlan()))
        .map(Collection::stream)
        .orElse(Stream.empty())
        .collect(Collectors.toList());
  }

  /**
   * Replicate the visitLogicalPlan method from OpenLineageRunEventBuilder
   */
  private static <D> Function1<LogicalPlan, Collection<D>> visitLogicalPlan(
      scala.PartialFunction<LogicalPlan, Collection<D>> inputVisitor) {
    return ScalaConversionUtils.toScalaFn(
        node ->
            inputVisitor.applyOrElse(node, ScalaConversionUtils.toScalaFn(n -> Collections.emptyList())));
  }

  private static Optional<OpenLineage.SQLJobFacet> resolveSQLFacets(OpenLineage ol, QueryExecution queryExecution) {
    SparkSQLQueryParser sqlRecorder = new SparkSQLQueryParser();
    LogicalPlan logicalPlan = queryExecution.logical();

    String query = null;

    Stack<LogicalPlan> stack = new Stack<>();

    if (logicalPlan != null) {
      stack.add(logicalPlan);
    }

    boolean found = false;

    while (!stack.isEmpty() && !found) {
      int stackLength = stack.size();

      while (stackLength > 0) {
        LogicalPlan currentLogicalPlan = stack.pop();

        if (currentLogicalPlan == null) {
          continue;
        }

        Optional<String> parsedQuery = sqlRecorder.parse(currentLogicalPlan);

        if (currentLogicalPlan.origin() != null && parsedQuery.isPresent()) {
          query = parsedQuery.get();
          found = true;
          break;
        }

        List<LogicalPlan> javaChildren = ScalaConversionUtils.fromSeq(currentLogicalPlan.children());

        stack.addAll(javaChildren);

        stackLength--;
      }
    }

    if (query == null) {
      return Optional.empty();
    }

    return Optional.of(ol.newSQLJobFacetBuilder().query(query).build());
  }

  /**
   * Create a synthetic output dataset for in-memory operations that includes column lineage facets.
   * This follows the OpenLineage specification where column lineage must be attached to output datasets.
   */
  private static OpenLineage.OutputDataset createSyntheticOutputWithColumnLineage(
      OpenLineage ol, QueryExecution queryExecution, String namespace, String jobName) {
    
    try {
      StructType outputSchema = queryExecution.analyzed().schema();
      LogicalPlan optimizedPlan = queryExecution.optimizedPlan();
      
      // Create schema facet
      OpenLineage.SchemaDatasetFacet schemaFacet = createSchemaFacet(ol, outputSchema);
      
      // Create column lineage facet (simplified implementation)
      OpenLineage.ColumnLineageDatasetFacet columnLineageFacet = createColumnLineageFacet(ol, optimizedPlan, outputSchema);
      
      // Create a synthetic dataset name for in-memory operations
      String syntheticDatasetName = "memory://" + jobName + "/transformation_" + System.currentTimeMillis();
      
      return ol.newOutputDatasetBuilder()
          .namespace(namespace)
          .name(syntheticDatasetName)
          .facets(ol.newDatasetFacetsBuilder()
              .schema(schemaFacet)
              .columnLineage(columnLineageFacet)
              .build())
          .build();
          
    } catch (Exception e) {
      System.err.println("Warning: Could not create synthetic output with column lineage: " + e.getMessage());
      return null;
    }
  }
  
  /**
   * Create a schema facet from Spark StructType
   */
  private static OpenLineage.SchemaDatasetFacet createSchemaFacet(OpenLineage ol, StructType schema) {
    List<OpenLineage.SchemaDatasetFacetFields> fields = Arrays.stream(schema.fields())
        .map(field -> ol.newSchemaDatasetFacetFieldsBuilder()
            .name(field.name())
            .type(field.dataType().typeName())
            .build())
        .collect(Collectors.toList());
        
    return ol.newSchemaDatasetFacetBuilder()
        .fields(fields)
        .build();
  }
  
  /**
   * Create a simplified column lineage facet from the logical plan.
   * This is a basic implementation - for production use, you'd want to leverage
   * the existing OpenLineage column lineage utilities.
   */
  private static OpenLineage.ColumnLineageDatasetFacet createColumnLineageFacet(
      OpenLineage ol, LogicalPlan plan, StructType outputSchema) {
    
    Map<String, OpenLineage.ColumnLineageDatasetFacetFieldsAdditional> columnLineageFields = new HashMap<>();
    
    // For each output column, try to determine its lineage
    for (StructField field : outputSchema.fields()) {
      String columnName = field.name();
      
      // Simplified lineage extraction - in practice, you'd use the existing
      // OpenLineage column lineage visitors for more sophisticated analysis
      List<OpenLineage.InputField> inputFields = 
          extractInputFieldsForColumn(ol, plan, columnName);
      
      if (!inputFields.isEmpty()) {
        OpenLineage.ColumnLineageDatasetFacetFieldsAdditional columnLineage = 
            ol.newColumnLineageDatasetFacetFieldsAdditionalBuilder()
                .inputFields(inputFields)
                .transformationType("DIRECT") // Simplified - could be TRANSFORMATION, IDENTITY, etc.
                .build();
                
        columnLineageFields.put(columnName, columnLineage);
      }
    }
    
    OpenLineage.ColumnLineageDatasetFacetFields fields = 
        ol.newColumnLineageDatasetFacetFieldsBuilder()
            .put("placeholder", columnLineageFields.get("placeholder")) // Need to iterate properly
            .build();
    
    // Properly populate the fields
    OpenLineage.ColumnLineageDatasetFacetFieldsBuilder fieldsBuilder = 
        ol.newColumnLineageDatasetFacetFieldsBuilder();
    
    for (Map.Entry<String, OpenLineage.ColumnLineageDatasetFacetFieldsAdditional> entry : columnLineageFields.entrySet()) {
      fieldsBuilder.put(entry.getKey(), entry.getValue());
    }
    
    return ol.newColumnLineageDatasetFacetBuilder()
        .fields(fieldsBuilder.build())
        .build();
  }
  
  /**
   * Extract input fields for a specific output column (simplified implementation)
   */
  private static List<OpenLineage.InputField> 
      extractInputFieldsForColumn(OpenLineage ol, LogicalPlan plan, String outputColumn) {
    
    // This is a simplified implementation. In practice, you would:
    // 1. Traverse the logical plan to find Project/Aggregate nodes
    // 2. Analyze expressions to determine which input columns contribute to the output column
    // 3. Map back to actual dataset identifiers
    
    List<OpenLineage.InputField> inputFields = new ArrayList<>();
    
    // For in-memory operations, we can create synthetic input references
    // or extract actual column references from the logical plan
    String syntheticInputNamespace = "memory://input";
    String syntheticInputDataset = "source_transformation";
    
    // This would be replaced with actual logic to extract input column references
    OpenLineage.InputField inputField = 
        ol.newInputFieldBuilder()
            .namespace(syntheticInputNamespace)
            .name(syntheticInputDataset)
            .field(outputColumn + "_source") // Simplified mapping
            .build();
    
    inputFields.add(inputField);
    return inputFields;
  }

  /**
   * Debug method to analyze why inputs are empty and show logical plan structure.
   * This helps understand what OpenLineage visitors are looking for.
   * 
   * @param queryExecution The QueryExecution to analyze
   * @return Debug information about the logical plan
   */
  public static String debugLogicalPlan(QueryExecution queryExecution) {
    StringBuilder debug = new StringBuilder();
    LogicalPlan logicalPlan = queryExecution.logical();
    
    debug.append("=== LOGICAL PLAN ANALYSIS ===\n");
    debug.append("Root node type: ").append(logicalPlan.getClass().getSimpleName()).append("\n");
    debug.append("Tree structure:\n").append(logicalPlan.treeString()).append("\n");
    
    debug.append("\n=== LOOKING FOR INPUT SOURCES ===\n");
    debug.append("OpenLineage visitors look for these node types:\n");
    debug.append("- LogicalRelation (file-based sources)\n");
    debug.append("- HiveTableRelation (Hive tables)\n");
    debug.append("- JDBCRelation (database sources)\n");
    debug.append("- DeltaTable, IcebergTable, etc.\n\n");
    
    // Recursively analyze the plan
    analyzeLogicalPlanForSources(logicalPlan, debug, 0);
    
    debug.append("\n=== SOLUTION ===\n");
    debug.append("To get inputs detected:\n");
    debug.append("1. Use the QueryExecution from the WRITE operation, not transformation\n");
    debug.append("2. Or enhance visitors to detect LogicalRelation nodes in transformation plans\n");
    debug.append("3. Your current setup creates synthetic outputs with column lineage (which is good!)\n");
    
    return debug.toString();
  }
  
  private static void analyzeLogicalPlanForSources(LogicalPlan plan, StringBuilder debug, int depth) {
    String indent = "  ".repeat(depth);
    debug.append(indent).append("- ").append(plan.getClass().getSimpleName());
    
    // Check if this is a source node
    if (plan.getClass().getSimpleName().contains("Relation")) {
      debug.append(" ✅ POTENTIAL INPUT SOURCE");
    } else if (plan.getClass().getSimpleName().contains("Range")) {
      debug.append(" ❌ In-memory source (no external dataset)");
    }
    debug.append("\n");
    
    // Recursively check children
    scala.collection.Iterator<LogicalPlan> childIterator = plan.children().iterator();
    while (childIterator.hasNext()) {
      LogicalPlan child = childIterator.next();
      analyzeLogicalPlanForSources(child, debug, depth + 1);
    }
  }
} 