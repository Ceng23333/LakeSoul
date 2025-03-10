#!/usr/bin/env bash

# Script to launch PySpark shell for interactive debugging

# MinIO configuration
S3_ENDPOINT="http://minio:9000"
ACCESS_KEY="admin"
SECRET_KEY="password"
WAREHOUSE="s3a://lakesoul-test-bucket/lakesoul-test"

echo "Launching PySpark shell for debugging..."
docker exec -it lakesoul-ann-spark /opt/bitnami/spark/bin/pyspark \
    --conf "spark.hadoop.fs.s3a.endpoint=${S3_ENDPOINT}" \
    --conf "spark.hadoop.fs.s3a.access.key=${ACCESS_KEY}" \
    --conf "spark.hadoop.fs.s3a.secret.key=${SECRET_KEY}" \
    --conf "spark.hadoop.fs.s3a.path.style.access=true" \
    --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
    --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false"

# This will open an interactive PySpark shell where you can:
# 1. Load the MNIST data
# 2. Test different vector transformations
# 3. Debug the data type issue
# 4. Try alternative save approaches 