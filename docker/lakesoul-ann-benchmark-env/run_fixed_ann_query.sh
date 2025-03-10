#!/usr/bin/env bash

# Script to run the fixed ANN query process

TABLE_NAME="mnist_ann_table"
QUERY_LIMIT=20
TOPK=10
WAREHOUSE="s3a://lakesoul-test-bucket/lakesoul-test"
BUCKET_LENGTH=2.0
NUM_HASH_TABLES=3
COMPUTE_RECALL=true  # Set to false to skip recall computation if it's too slow
DISTANCE_THRESHOLD=5000.0
# MinIO configuration
S3_ENDPOINT="http://minio:9000"
ACCESS_KEY="admin"
SECRET_KEY="password"

echo "Running fixed LSH ANN querying..."
docker exec -it lakesoul-ann-spark /opt/bitnami/spark/bin/spark-submit \
    --master "local[*]" \
    --driver-memory 4g \
    --executor-memory 4g \
    --conf "spark.memory.offHeap.enabled=true" \
    --conf "spark.memory.offHeap.size=1g" \
    --conf "spark.hadoop.fs.s3a.endpoint=${S3_ENDPOINT}" \
    --conf "spark.hadoop.fs.s3a.access.key=${ACCESS_KEY}" \
    --conf "spark.hadoop.fs.s3a.secret.key=${SECRET_KEY}" \
    --conf "spark.hadoop.fs.s3a.path.style.access=true" \
    --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
    --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
    /tmp/fixed_ann_script.py \
    --mode query \
    --table-name ${TABLE_NAME} \
    --warehouse ${WAREHOUSE} \
    --query-limit ${QUERY_LIMIT} \
    --topk ${TOPK} \
    --bucket-length ${BUCKET_LENGTH} \
    --num-hash-tables ${NUM_HASH_TABLES} \
    --distance-threshold ${DISTANCE_THRESHOLD} \
    ${COMPUTE_RECALL:+--compute-recall}

echo "Fixed ANN query completed!" 