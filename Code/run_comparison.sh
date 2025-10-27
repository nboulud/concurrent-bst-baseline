#!/bin/bash

# Comparison benchmark script for Baseline BST vs MyBST
# Usage: ./run_comparison.sh <threads> <seconds>
# Example: ./run_comparison.sh 16 10

THREADS=${1:-8}
SECONDS=${2:-10}

echo "=========================================="
echo "Benchmark Comparison: Baseline vs MyBST"
echo "Threads: $THREADS, Duration: $SECONDS seconds"
echo "=========================================="
echo

echo ">>> Running BASELINE BST..."
./gradlew run --quiet --args="$THREADS $SECONDS" -PmainClass=bench.MicroBench
echo

echo ">>> Running MyBST (with snapshot support)..."
./gradlew run --quiet --args="$THREADS $SECONDS" -PmainClass=bench.MicroBenchMyBST
echo

echo "=========================================="
echo "Comparison complete!"
echo "=========================================="

