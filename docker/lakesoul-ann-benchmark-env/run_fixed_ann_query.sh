#!/usr/bin/env bash

# Script to run the fixed ANN query process

TABLE_NAME="mnist_ann_table"
HDF5_FILE="/data/embeddings/fashion-mnist-784-euclidean.hdf5"
QUERY_LIMIT=1
TOPK=10
WAREHOUSE="s3a://lakesoul-test-bucket/lakesoul-test"
BUCKET_LENGTH=784.0
NUM_HASH_TABLES=10
PRE_RANK_SIZE=1000
COMPUTE_RECALL=true  # Set to false to skip recall computation if it's too slow
DISTANCE_THRESHOLD=50000.0
# MinIO configuration
S3_ENDPOINT="http://minio:9000"
ACCESS_KEY="admin"
SECRET_KEY="password"

echo "Running fixed LSH ANN querying..."
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
    /tmp/spark_native_script.py \
    --mode query \
    --hdf5-file ${HDF5_FILE} \
    --table-name ${TABLE_NAME} \
    --warehouse ${WAREHOUSE} \
    --query-limit ${QUERY_LIMIT} \
    --topk ${TOPK} \
    --bucket-length ${BUCKET_LENGTH} \
    --pre-rank-size ${PRE_RANK_SIZE} \
    --distance-threshold ${DISTANCE_THRESHOLD} \
    ${COMPUTE_RECALL:+--compute-recall}

echo "Fixed ANN query completed!" 