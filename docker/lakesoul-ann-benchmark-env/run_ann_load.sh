#!/usr/bin/env bash

# Script to run the ANN data loading process

WAREHOUSE="s3a://lakesoul-test-bucket/lakesoul-test"
TABLE_NAME="mnist_ann_table"
HDF5_FILE="/data/embeddings/fashion-mnist-784-euclidean.hdf5"
EMBEDDING_DIM=784
BUCKET_LENGTH=2.0
NUM_HASH_TABLES=3

# MinIO configuration
S3_ENDPOINT="http://minio:9000"
ACCESS_KEY="admin"
SECRET_KEY="password"

echo "Running LSH ANN model training and data loading..."
docker exec -it lakesoul-ann-spark /opt/bitnami/spark/bin/spark-submit \
    --master "local[*]" \
    --conf "spark.hadoop.fs.s3a.endpoint=${S3_ENDPOINT}" \
    --conf "spark.hadoop.fs.s3a.access.key=${ACCESS_KEY}" \
    --conf "spark.hadoop.fs.s3a.secret.key=${SECRET_KEY}" \
    --conf "spark.hadoop.fs.s3a.path.style.access=true" \
    --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
    --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
    /tmp/mnist_ann_bucketed_random_projection.py \
    --mode load \
    --hdf5-file ${HDF5_FILE} \
    --table-name ${TABLE_NAME} \
    --warehouse ${WAREHOUSE} \
    --embedding-dim ${EMBEDDING_DIM} \
    --bucket-length ${BUCKET_LENGTH} \
    --num-hash-tables ${NUM_HASH_TABLES}

echo "ANN data loading completed!" 