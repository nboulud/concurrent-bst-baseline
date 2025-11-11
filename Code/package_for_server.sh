#!/bin/bash

# Script to package files for server transfer
# Creates a tarball with all necessary files

echo "Creating package for server transfer..."

# Create temporary directory structure
TEMP_DIR=$(mktemp -d)
PACKAGE_NAME="aggregate_bench_$(date +%Y%m%d_%H%M%S)"
PACKAGE_DIR="$TEMP_DIR/$PACKAGE_NAME"

mkdir -p "$PACKAGE_DIR"

# Copy necessary files
echo "Copying source files..."
cp -r src "$PACKAGE_DIR/"
cp -r gradle "$PACKAGE_DIR/"
cp build.gradle "$PACKAGE_DIR/"
cp gradlew "$PACKAGE_DIR/"
cp gradlew.bat "$PACKAGE_DIR/"
cp run_aggregate_benchmark.sh "$PACKAGE_DIR/"
cp SERVER_INSTRUCTIONS.md "$PACKAGE_DIR/"

# Create tarball
cd "$TEMP_DIR"
tar czf "${PACKAGE_NAME}.tar.gz" "$PACKAGE_NAME"

# Move to current directory
mv "${PACKAGE_NAME}.tar.gz" "$OLDPWD/"

# Cleanup
rm -rf "$TEMP_DIR"

echo ""
echo "✅ Package created: ${PACKAGE_NAME}.tar.gz"
echo ""
echo "To transfer to server:"
echo "  scp ${PACKAGE_NAME}.tar.gz boulud@lpdquad.epfl.ch:~/"
echo ""
echo "On the server:"
echo "  tar xzf ${PACKAGE_NAME}.tar.gz"
echo "  cd ${PACKAGE_NAME}"
echo "  ./run_aggregate_benchmark.sh"
echo ""
