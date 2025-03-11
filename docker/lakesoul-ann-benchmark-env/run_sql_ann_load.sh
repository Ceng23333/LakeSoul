#!/usr/bin/env bash

# Script to run the SQL-based ANN data loading process

WAREHOUSE="s3a://lakesoul-test-bucket/lakesoul-test"
TABLE_NAME="mnist_sql_ann_table"
HDF5_FILE="/data/embeddings/fashion-mnist-784-euclidean.hdf5"
EMBEDDING_DIM=784
BUCKET_LENGTH=784.0
NUM_HASH_TABLES=10
SAMPLE_RATIO=1.0  # Use full dataset or reduce to avoid memory issues

# MinIO configuration
S3_ENDPOINT="http://minio:9000"
ACCESS_KEY="admin"
SECRET_KEY="password"

# Ensure script is executable
chmod +x /home/huazeng/Git/LakeSoul/docker/lakesoul-ann-benchmark-env/lakesoul_lsh_ann.py

# First, update the script inside the container
./update_script_in_container.sh

echo "Running SQL-based LSH ANN data loading..."
docker exec -it lakesoul-ann-spark /opt/bitnami/spark/bin/spark-submit \
    --master "local[*]" \
    --driver-memory 6g \
    --executor-memory 6g \
    --conf "spark.memory.offHeap.enabled=true" \
    --conf "spark.memory.offHeap.size=2g" \
    --conf "spark.hadoop.fs.s3a.endpoint=${S3_ENDPOINT}" \
    --conf "spark.hadoop.fs.s3a.access.key=${ACCESS_KEY}" \
    --conf "spark.hadoop.fs.s3a.secret.key=${SECRET_KEY}" \
    --conf "spark.hadoop.fs.s3a.path.style.access=true" \
    --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
    --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
    --conf "spark.driver.maxResultSize=1g" \
    --conf "spark.memory.fraction=0.8" \
    --conf "spark.sql.shuffle.partitions=8" \
    /tmp/lakesoul_lsh_ann.py \
    --mode load \
    --hdf5-file ${HDF5_FILE} \
    --table-name ${TABLE_NAME} \
    --warehouse ${WAREHOUSE} \
    --embedding-dim ${EMBEDDING_DIM} \
    --bucket-length ${BUCKET_LENGTH} \
    --num-hash-tables ${NUM_HASH_TABLES} \
    --sample-ratio ${SAMPLE_RATIO}

echo "SQL-based ANN data loading completed!" 