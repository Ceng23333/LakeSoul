#!/usr/bin/env bash

# Script to update the Python scripts inside the Docker container

# Copy the original script
echo "Copying spark_native_script.py to the lakesoul-ann-spark container..."
docker cp spark_native_script.py lakesoul-ann-spark:/tmp/

# Copy the SQL-based LSH implementation
echo "Copying lakesoul_lsh_ann.py to the lakesoul-ann-spark container..."
docker cp lakesoul_lsh_ann.py lakesoul-ann-spark:/tmp/

# Verify the files were copied successfully
echo "Verifying files were copied..."
docker exec lakesoul-ann-spark ls -la /tmp/spark_native_script.py
docker exec lakesoul-ann-spark ls -la /tmp/lakesoul_lsh_ann.py

echo "Scripts updated in container!"
echo "You can now run queries with:"
echo "  - Original implementation: ./run_fixed_ann_query.sh"
echo "  - SQL-based implementation: ./run_sql_ann_query.sh" 