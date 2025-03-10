#!/usr/bin/env bash

# Script to run the fixed ANN data loading process

WAREHOUSE="s3a://lakesoul-test-bucket/lakesoul-test"
TABLE_NAME="mnist_ann_table"
HDF5_FILE="/data/embeddings/fashion-mnist-784-euclidean.hdf5"
EMBEDDING_DIM=784
BUCKET_LENGTH=2.0
NUM_HASH_TABLES=3
SAMPLE_RATIO=0.05  # Use only 5% of the data to avoid memory issues

# MinIO configuration
S3_ENDPOINT="http://minio:9000"
ACCESS_KEY="admin"
SECRET_KEY="password"

# Copy the fixed script to the container
echo "Copying fixed ANN script to container..."
docker cp fixed_ann_script.py lakesoul-ann-spark:/tmp/

echo "Running fixed LSH ANN model training and data loading..."
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
    --mode load \
    --hdf5-file ${HDF5_FILE} \
    --table-name ${TABLE_NAME} \
    --warehouse ${WAREHOUSE} \
    --embedding-dim ${EMBEDDING_DIM} \
    --bucket-length ${BUCKET_LENGTH} \
    --num-hash-tables ${NUM_HASH_TABLES} \
    --sample-ratio ${SAMPLE_RATIO}

echo "Fixed ANN data loading completed!" 