from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, collect_list, row_number, col, array
from pyspark.sql.window import Window
from pyspark.ml.feature import BucketedRandomProjectionLSH
from pyspark.ml.linalg import Vectors, VectorUDT
import h5py
import math
import time
import argparse
from dataclasses import dataclass

@dataclass
class LoadConfig:
    hdf5_file: str = ""
    warehouse: str = ""
    table_name: str = "mnist_ann_table"
    embedding_dim: int = 784
    bucket_length: float = 2.0
    num_hash_tables: int = 3

@dataclass
class QueryConfig:
    table_name: str = ""
    query_limit: int = 20
    topk: int = 10

def create_spark_session():
    return (SparkSession.builder
            .appName("MNIST-ANN-BRP")
            .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
            .config("spark.sql.catalog.lakesoul", "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog")
            .config("spark.sql.defaultCatalog", "lakesoul")
            .getOrCreate())

def load_data(config: LoadConfig, spark: SparkSession):
    print(f"Loading data from {config.hdf5_file}...")
    
    with h5py.File(config.hdf5_file, 'r') as f:
        train_data = f['train'][:]
        test_data = f['test'][:]
        
        # If there are labels in the HDF5 file, extract them
        train_labels = f['train_labels'][:] if 'train_labels' in f else None
        test_labels = f['test_labels'][:] if 'test_labels' in f else None

    # Prepare train data with vector format
    train_rows = []
    for i, embedding in enumerate(train_data):
        # Flatten image if needed
        if len(embedding.shape) > 1:
            embedding = embedding.flatten()
        
        # Normalize data if needed
        if embedding.max() > 1.0:
            embedding = embedding / 255.0
            
        label = int(train_labels[i]) if train_labels is not None else None
        train_rows.append((i+1, Vectors.dense(embedding), "train", label))
    
    # Prepare test data with vector format
    test_rows = []
    for i, embedding in enumerate(test_data):
        # Flatten image if needed
        if len(embedding.shape) > 1:
            embedding = embedding.flatten()
            
        # Normalize data if needed
        if embedding.max() > 1.0:
            embedding = embedding / 255.0
            
        label = int(test_labels[i]) if test_labels is not None else None
        test_rows.append((i+1, Vectors.dense(embedding), "test", label))
    
    # Create DataFrame schema
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("features", VectorUDT(), True),
        StructField("split", StringType(), True),
        StructField("label", IntegerType(), True)
    ])
    
    # Create DataFrame
    df = spark.createDataFrame(train_rows + test_rows, schema)
    
    # Filter to get just train data for model fitting
    train_df = df.filter(df.split == "train")
    
    # Create and fit the LSH model
    brp = BucketedRandomProjectionLSH(
        inputCol="features",
        outputCol="hashes",
        bucketLength=config.bucket_length,
        numHashTables=config.num_hash_tables,
        seed=42
    )
    
    print("Fitting BucketedRandomProjectionLSH model...")
    model = brp.fit(train_df)
    
    # Transform the dataframes to include hash values
    train_with_hashes = model.transform(train_df)
    
    # Convert to array format for saving to LakeSoul
    @udf(returnType=ArrayType(FloatType()))
    def vector_to_array(v):
        return v.toArray().tolist()
    
    train_with_hashes = train_with_hashes.withColumn("features_array", vector_to_array(col("features")))
    
    # Save the model
    model_path = f"{config.warehouse}/{config.table_name}_model"
    model.write().overwrite().save(model_path)
    print(f"LSH model saved to {model_path}")
    
    # Write train data to LakeSoul table
    train_with_hashes.select(
        col("id"),
        col("features_array").alias("embedding"),
        col("hashes"),
        col("split"),
        col("label")
    ).write.format("lakesoul") \
        .option("hashPartitions", "id") \
        .option("hashBucketNum", 4) \
        .option("shortTableName", config.table_name) \
        .mode("overwrite") \
        .save(f"{config.warehouse}/{config.table_name}")
    
    # Transform test data
    test_df = df.filter(df.split == "test")
    test_with_hashes = model.transform(test_df)
    test_with_hashes = test_with_hashes.withColumn("features_array", vector_to_array(col("features")))
    
    # Add test data to the table
    test_with_hashes.select(
        col("id"),
        col("features_array").alias("embedding"),
        col("hashes"),
        col("split"),
        col("label")
    ).write.format("lakesoul") \
        .option("hashPartitions", "id") \
        .option("hashBucketNum", 4) \
        .option("shortTableName", config.table_name) \
        .mode("append") \
        .save(f"{config.warehouse}/{config.table_name}")
    
    print(f"Successfully loaded {len(train_rows)} train samples and {len(test_rows)} test samples to {config.warehouse}/{config.table_name}")
    
    return model, model_path

def query_ann(config: QueryConfig, spark: SparkSession, model_path):
    print(f"Loading LSH model from {model_path}...")
    model = BucketedRandomProjectionLSH.load(model_path)
    
    # Register UDF for Euclidean distance calculation
    @udf("double")
    def euclidean_distance(vec1, vec2):
        return math.sqrt(sum((a - b) ** 2 for a, b in zip(vec1, vec2)))
    
    spark.udf.register("calculateEuclideanDistance", euclidean_distance)
    
    # Load data from LakeSoul table
    df = spark.read.format("lakesoul").load(f"{config.table_name}")
    
    # Split into test and train
    test_df = df.filter(df.split == "test").limit(config.query_limit)
    train_df = df.filter(df.split == "train")
    
    # Convert arrays back to vectors for LSH
    @udf(returnType=VectorUDT())
    def array_to_vector(arr):
        return Vectors.dense(arr)
    
    test_df = test_df.withColumn("features", array_to_vector(col("embedding")))
    train_df = train_df.withColumn("features", array_to_vector(col("embedding")))
    
    # Start timing
    start_time = time.time()
    
    # Use the model to find approximate matches
    print(f"Finding approximate nearest neighbors for {config.query_limit} test samples...")
    
    # Process each test query
    results = []
    for test_row in test_df.collect():
        # Use LSH model to find candidates
        key = test_row["features"]
        approximate_neighbors = model.approxNearestNeighbors(train_df, key, config.topk * 2)
        # Get just the id and distance
        neighbors = approximate_neighbors.select(
            col("id").alias("neighbor_id"),
            col("distCol").alias("distance")
        ).orderBy("distance").limit(config.topk)
        
        results.append((test_row["id"], [row["neighbor_id"] for row in neighbors.collect()]))
    
    query_time = time.time() - start_time
    
    # Create result DataFrame
    result_schema = StructType([
        StructField("query_id", IntegerType(), True),
        StructField("neighbors", ArrayType(IntegerType()), True)
    ])
    
    result_df = spark.createDataFrame(results, result_schema)
    
    print(f"Query time: {query_time * 1000:.2f} milliseconds")
    print("\nSample of query results:")
    result_df.show(10, truncate=False)
    
    return result_df

def main():
    parser = argparse.ArgumentParser(description="MNIST ANN using BucketedRandomProjection")
    parser.add_argument("--mode", type=str, choices=["load", "query"], required=True, 
                        help="Operation mode: 'load' to load data, 'query' to query neighbors")
    parser.add_argument("--hdf5-file", type=str, help="Path to the HDF5 file containing the dataset")
    parser.add_argument("--table-name", type=str, default="mnist_ann_table", 
                        help="Name of the LakeSoul table")
    parser.add_argument("--warehouse", type=str, default="s3a://lakesoul-test-bucket", 
                        help="Warehouse location for storing the LakeSoul tables")
    parser.add_argument("--embedding-dim", type=int, default=784, 
                        help="Dimension of the embedding vectors")
    parser.add_argument("--bucket-length", type=float, default=2.0, 
                        help="Length of the hash bucket")
    parser.add_argument("--num-hash-tables", type=int, default=3, 
                        help="Number of hash tables to use")
    parser.add_argument("--query-limit", type=int, default=20, 
                        help="Number of test samples to query")
    parser.add_argument("--topk", type=int, default=10, 
                        help="Number of nearest neighbors to retrieve")
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    model_path = f"{args.warehouse}/{args.table_name}_model"
    
    if args.mode == "load":
        # Load data
        if not args.hdf5_file:
            parser.error("--hdf5-file is required for load mode")
            
        config = LoadConfig(
            hdf5_file=args.hdf5_file,
            warehouse=args.warehouse,
            table_name=args.table_name,
            embedding_dim=args.embedding_dim,
            bucket_length=args.bucket_length,
            num_hash_tables=args.num_hash_tables
        )
        _, model_path = load_data(config, spark)
    elif args.mode == "query":
        # Query data
        config = QueryConfig(
            table_name=args.table_name,
            query_limit=args.query_limit,
            topk=args.topk
        )
        query_ann(config, spark, model_path)

if __name__ == "__main__":
    main() 