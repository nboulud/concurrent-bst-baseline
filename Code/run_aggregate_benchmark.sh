#!/bin/bash

# Script to run the Aggregate Query Benchmark
# Usage: ./run_aggregate_benchmark.sh

cd "$(dirname "$0")"

echo "Building project..."
./gradlew clean build -x test

echo ""
echo "Starting Aggregate Query Benchmark..."
echo "This will take approximately 10-15 minutes..."
echo ""

java -cp "build/classes/java/main:build/classes/java/test" bench.AggregateQueryBenchmark

echo ""
echo "Benchmark complete!"
