#!/bin/bash

# Build the project first
echo "Building the project..."
./gradlew :shared:compileJava

# Set up classpath
CLASSPATH="shared/build/classes/java/main"

# Add dependencies
for jar in $(find ~/.gradle/caches/modules-2/files-2.1 -name "*.jar" 2>/dev/null | grep -E "(openlineage-client|jackson|slf4j)" | head -20); do
    CLASSPATH="$CLASSPATH:$jar"
done

# Run the static parser
echo "Running Static Query Execution Parser..."
echo "Processing files from query_execution_examples/"

if [ $# -eq 0 ]; then
    # Process all example files
    java -cp "$CLASSPATH" io.openlineage.spark.agent.lifecycle.StaticQueryExecutionParserMain
else
    # Process specific file
    java -cp "$CLASSPATH" io.openlineage.spark.agent.lifecycle.StaticQueryExecutionParserMain "$1"
fi 