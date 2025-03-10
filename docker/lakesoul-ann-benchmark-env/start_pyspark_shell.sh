#!/usr/bin/env bash

set -e

# Check if the container is running
if ! docker ps | grep -q lakesoul-ann-spark; then
  echo "The lakesoul-ann-spark container is not running"
  echo "Please run ./start_all.sh first"
  exit 1
fi

# Create a temporary Python script to launch a PySpark session
cat > temp_spark_init.py << 'EOL'
from pyspark.sql import SparkSession

# Create a SparkSession with LakeSoul configurations
spark = SparkSession.builder \
    .appName("LakeSoul-PySpark-Shell") \
    .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension") \
    .config("spark.sql.catalog.lakesoul", "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog") \
    .config("spark.sql.catalog.lakesoul.warehouse", "s3a://lakesoul-test-bucket/") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("\n" + "="*80)
print("LakeSoul PySpark Shell initialized successfully!")
print("The SparkSession is available as 'spark'")
print("="*80 + "\n")

# Keep the shell session open
import code
code.interact(local=locals())
EOL

# Copy the script to the container
docker cp temp_spark_init.py lakesoul-ann-spark:/tmp/

echo "Launching PySpark shell in lakesoul-ann-spark container..."
docker exec -it lakesoul-ann-spark /opt/bitnami/python/bin/python /tmp/temp_spark_init.py

# Clean up
rm temp_spark_init.py
echo "PySpark shell session ended."