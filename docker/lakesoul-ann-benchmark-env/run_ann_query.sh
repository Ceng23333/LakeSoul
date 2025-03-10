#!/usr/bin/env bash

# Script to run the ANN query process

TABLE_NAME="mnist_ann_table"
QUERY_LIMIT=20
TOPK=10
WAREHOUSE="s3a://lakesoul-test-bucket/lakesoul-test"

# The model path will be determined by the previous load step
MODEL_PATH="${WAREHOUSE}/${TABLE_NAME}_model"

# MinIO configuration
S3_ENDPOINT="http://minio:9000"
ACCESS_KEY="admin"
SECRET_KEY="password"

echo "Running LSH ANN querying..."
docker exec -it lakesoul-ann-spark /opt/bitnami/spark/bin/spark-submit \
    --master "local[*]" \
    --conf "spark.hadoop.fs.s3a.endpoint=${S3_ENDPOINT}" \
    --conf "spark.hadoop.fs.s3a.access.key=${ACCESS_KEY}" \
    --conf "spark.hadoop.fs.s3a.secret.key=${SECRET_KEY}" \
    --conf "spark.hadoop.fs.s3a.path.style.access=true" \
    --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
    --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
    /tmp/mnist_ann_bucketed_random_projection.py \
    --mode query \
    --table-name ${TABLE_NAME} \
    --warehouse ${WAREHOUSE} \
    --query-limit ${QUERY_LIMIT} \
    --topk ${TOPK}

echo "ANN query completed!" 