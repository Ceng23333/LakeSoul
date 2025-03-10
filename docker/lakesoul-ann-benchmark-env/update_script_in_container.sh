#!/usr/bin/env bash

# Script to update the fixed_ann_script.py inside the Docker container

echo "Copying updated fixed_ann_script.py to the lakesoul-ann-spark container..."
docker cp fixed_ann_script.py lakesoul-ann-spark:/tmp/

# Verify the file was copied successfully
echo "Verifying file was copied..."
docker exec lakesoul-ann-spark ls -la /tmp/fixed_ann_script.py

echo "Script updated in container!"
echo "You can now run queries with: ./run_fixed_ann_query.sh" 