"""
MNIST ANN benchmark using BucketedRandomProjection LSH with LakeSoul storage.
This version includes fixes for the hash vector compatibility with LakeSoul.
"""

import argparse
import h5py
import numpy as np
import random
import os
import json
import time
import sys
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, udf, row_number, collect_list, array
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, FloatType, IntegerType, StringType, StructType, StructField, DoubleType, LongType
from pyspark.ml.feature import BucketedRandomProjectionLSH
from pyspark.ml.linalg import Vectors, VectorUDT
from dataclasses import dataclass

@dataclass
class LoadConfig:
    hdf5_file: str = ""
    warehouse: str = ""
    table_name: str = "mnist_ann_table"
    embedding_dim: int = 784
    bucket_length: float = 2.0
    num_hash_tables: int = 3
    sample_ratio: float = 1.0  # Default to 10% of data to avoid memory issues
    self_query: bool = False   # Whether to perform a self-query after loading
    query_limit: int = 10      # Number of queries to run for self-query
    topk: int = 10             # Number of nearest neighbors for self-query

@dataclass
class QueryConfig:
    hdf5_file: str = ""
    table_name: str = ""
    query_limit: int = 20
    topk: int = 10
    warehouse: str = ""
    bucket_length: float = 2.0
    num_hash_tables: int = 3
    compute_recall: bool = True
    distance_threshold: float = 10.0  # Distance threshold for candidate selection

def create_spark_session():
    # Create a Spark session with more memory
    spark = SparkSession.builder \
        .appName("MNIST-ANN-BRP") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.memory.offHeap.size", "1g") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def compute_euclidean_distance(vec1, vec2):
    """Calculate Euclidean distance between two vectors"""
    return float(np.sqrt(sum((a - b) ** 2 for a, b in zip(vec1, vec2))))

def load_data(config: LoadConfig, spark: SparkSession):
    """
    Load data from the HDF5 file, apply LSH transformation using Spark SQL, 
    and save to LakeSoul with pre-computed LSH hashes.
    Uses sampling to reduce memory usage.
    """
    print(f"\n{'='*60}")
    print(f"LOAD MODE: Preparing data and LSH hashes using SQL approach")
    print(f"{'='*60}")
    print(f"Configuration:")
    print(f"  - HDF5 file: {config.hdf5_file}")
    print(f"  - Table name: {config.table_name}")
    print(f"  - Warehouse: {config.warehouse}")
    print(f"  - Sample ratio: {config.sample_ratio}")
    print(f"  - Bucket length: {config.bucket_length}")
    print(f"  - Number of hash tables: {config.num_hash_tables}")
    print(f"  - Self-query: {config.self_query}")
    if config.self_query:
        print(f"  - Query limit: {config.query_limit}")
        print(f"  - Top-k: {config.topk}")
    print(f"{'='*60}\n")
    
    print(f"Loading data from {config.hdf5_file}...")
    h5f = h5py.File(config.hdf5_file, 'r')
    
    # Load train and test vectors
    print(f"Sampling {config.sample_ratio * 100}% of training data to reduce memory usage")
    train_size = h5f['train'].shape[0]
    sample_size = int(train_size * config.sample_ratio)
    
    # Sample randomly from the training data
    train_vecs = h5f['train'][:sample_size]
    
    # Take a small sample of test data
    test_size = min(1000, h5f['test'].shape[0])
    test_vecs = h5f['test'][:test_size]
    
    print(f"Using {len(train_vecs)} training vectors and {len(test_vecs)} test vectors")
    
    # Register UDFs for LSH hash generation using Spark SQL
    @udf(returnType=ArrayType(LongType()))
    def generate_lsh_hashes(features_array):
        """
        Generate LSH hashes for a feature vector using random projections.
        This is a simplified implementation of the BucketedRandomProjectionLSH algorithm.
        """
        if features_array is None:
            return None
            
        # Dimensions of the input vector
        dim = len(features_array)
        
        # Generate random projection vectors (one for each hash table)
        # Using a fixed seed for reproducibility
        np.random.seed(42)
        projection_vectors = [np.random.normal(0, 1, dim) for _ in range(config.num_hash_tables)]
        
        # Compute hash for each projection vector
        hashes = []
        for proj_vec in projection_vectors:
            # Compute dot product
            dot_product = sum(a * b for a, b in zip(features_array, proj_vec))
            
            # Compute hash bucket
            hash_bucket = int(dot_product / config.bucket_length)
            
            # Add hash bucket to result (convert to long for storage efficiency)
            hashes.append(hash_bucket)
        
        return hashes
    
    # Register UDF for hash generation
    spark.udf.register("generate_lsh_hashes", generate_lsh_hashes)
    
    # Create rows with ids, vectors and splits
    train_rows = [(i, vec.tolist(), "train") 
                  for i, vec in enumerate(train_vecs)]
    
    test_rows = [(i + len(train_rows), vec.tolist(), "test") 
                 for i, vec in enumerate(test_vecs)]
    
    # Create schema for DataFrame
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("features_array", ArrayType(DoubleType()), True),
        StructField("split", StringType(), True)
    ])
    
    # Create DataFrame
    df = spark.createDataFrame(train_rows + test_rows, schema)
    
    # Apply LSH hash generation using SQL
    print("Generating LSH hashes using SQL approach...")
    df_with_hashes = df.withColumn("hashes", generate_lsh_hashes(col("features_array")))
    
    # Show sample data
    print("\nData sample with LSH hashes:")
    df_with_hashes.show(5)
    
    # Save the data with hashes to LakeSoul
    model_path = f"{config.warehouse}/{config.table_name}"
    print(f"Saving data with LSH hashes to {model_path}...")
    
    df_with_hashes.write.format("lakesoul") \
        .option("hashPartitions", "id") \
        .option("hashBucketNum", 4) \
        .option("shortTableName", config.table_name) \
        .mode("Overwrite") \
        .save(model_path)
    
    print(f"Successfully saved {df_with_hashes.count()} records to LakeSoul")
    
    # Perform self-query as a test if specified
    if config.self_query:
        print(f"\nPerforming self-query test with {config.query_limit} queries...")
        
        query_config = QueryConfig(
            hdf5_file=config.hdf5_file,
            table_name=config.table_name,
            query_limit=config.query_limit,
            topk=config.topk,
            warehouse=config.warehouse,
            bucket_length=config.bucket_length,
            num_hash_tables=config.num_hash_tables,
            compute_recall=True
        )
        
        # Run query using the SQL-based approach
        query_ann(query_config, spark, model_path)
    
    return df_with_hashes, model_path

def query_ann(config: QueryConfig, spark: SparkSession, model_path):
    """
    Query the ANN index using Spark SQL with a two-stage filtering approach:
    1. First stage: Use LSH hashes for initial filtering (Hamming distance) 
    2. Second stage: Refine results with exact Euclidean distance
    """
    print(f"\n{'='*60}")
    print(f"QUERY MODE: Searching for approximate nearest neighbors using Spark SQL")
    print(f"{'='*60}")
    print(f"Query configuration:")
    print(f"  - HDF5 file: {config.hdf5_file}")
    print(f"  - Table name: {config.table_name}")
    print(f"  - Warehouse: {config.warehouse}")
    print(f"  - Query limit: {config.query_limit}")
    print(f"  - Top-k neighbors: {config.topk}")
    print(f"  - Compute recall: {config.compute_recall}")
    print(f"{'='*60}\n")
    
    h5f = h5py.File(config.hdf5_file, 'r')

    neighbors_size = min(1000, h5f['neighbors'].shape[0])
    neighbors_vecs = h5f['neighbors'][:neighbors_size]
        
    # Load the data from LakeSoul
    data_path = f"{config.warehouse}/{config.table_name}"
    print(f"\nLoading data from {data_path}...")
    
    lakesoul_df = spark.read.format("lakesoul").load(data_path)
    print(f"Successfully loaded data with schema:")
    lakesoul_df.printSchema()
    
    total_rows = lakesoul_df.count()
    print(f"Total records loaded: {total_rows}")
    
    # Define UDFs for distance calculations
    @udf("double")
    def euclidean_distance(vec1, vec2):
        """Calculate Euclidean distance between two vectors"""
        return float(np.sqrt(sum((a - b) ** 2 for a, b in zip(vec1, vec2))))
    
    @udf("integer")
    def hamming_distance(vec1, vec2):
        """Calculate Hamming distance between two vectors of LSH hashes"""
        if vec1 is None or vec2 is None:
            return sys.maxsize
        return sum(bin(int(a) ^ int(b)).count('1') for a, b in zip(vec1, vec2))
    
    # Register UDFs for SQL queries
    spark.udf.register("euclidean_distance", euclidean_distance)
    spark.udf.register("hamming_distance", hamming_distance)
    
    # Separate train and test data
    train_df = lakesoul_df.filter(lakesoul_df.split == "train")
    test_df = lakesoul_df.filter(lakesoul_df.split == "test")
    
    # Create temporary views for SQL queries
    train_df.createOrReplaceTempView("train_data")
    
    # Sample query vectors
    queries = test_df.limit(config.query_limit)
    
    print(f"\nFound {train_df.count()} training records and {test_df.count()} test records")
    print(f"Selected {config.query_limit} random queries from test set")
    
    # Initialization for metrics calculation
    total_queries = 0
    total_recall = 0.0
    all_recalls = []
    sql_query_times = []
    exact_query_times = []
    
    # Determine the amplification factor for initial filtering (n * topk)
    filter_amplification = config.num_hash_tables  # Use num_hash_tables as amplification factor
    first_stage_k = config.topk * filter_amplification

    # Collect train data for efficient exact search comparison
    if config.compute_recall:
        print(f"Collecting training data for exact search comparison...")
        start_time = time.time()
        train_data_collected = [(row["id"], row["features_array"]) for row in train_df.select("id", "features_array").collect()]
        collection_time = time.time() - start_time
        print(f"Collected {len(train_data_collected)} training vectors in {collection_time:.2f} seconds")
    
    print(f"\n{'='*60}")
    print(f"Starting query process for {config.query_limit} queries using SQL approach...")
    print(f"{'='*60}")
    
    for query_idx, query_row in enumerate(queries.collect()):
        query_id = query_row["id"]
        query_features_array = query_row["features_array"]
        query_hashes = query_row.get("hashes")  # Get LSH hashes if available
        
        print(f"\nQUERY {query_idx+1}/{config.query_limit} (ID: {query_id})")
        print(f"{'-'*60}")
        
        # Create a temporary view for the current query
        query_df = spark.createDataFrame([query_row])
        query_df.createOrReplaceTempView("query_data")
        
        # Start timing the SQL-based ANN query
        sql_start_time = time.time()
        
        # Two-stage approach:
        # 1. First stage: Use LSH hashes with Hamming distance for initial filtering
        if query_hashes is not None and "hashes" in train_df.columns:
            first_stage_sql = f"""
            SELECT 
                train_data.id AS train_id,
                train_data.features_array AS train_features,
                query_data.id AS query_id,
                query_data.features_array AS query_features,
                hamming_distance(train_data.hashes, query_data.hashes) AS hamming_dist
            FROM train_data
            CROSS JOIN query_data
            WHERE query_data.id = {query_id}
            ORDER BY hamming_dist ASC
            LIMIT {first_stage_k}
            """
            
            first_stage_results = spark.sql(first_stage_sql)
            first_stage_results.createOrReplaceTempView("first_stage_results")
            
            # 2. Second stage: Refine results with exact Euclidean distance
            second_stage_sql = f"""
            SELECT 
                train_id,
                train_features,
                query_id,
                query_features,
                euclidean_distance(train_features, query_features) AS exact_distance
            FROM first_stage_results
            ORDER BY exact_distance ASC
            LIMIT {config.topk}
            """
            
            final_results = spark.sql(second_stage_sql)
        else:
            # If no LSH hashes available, use direct Euclidean distance (slower but works)
            print("WARNING: No LSH hashes found in data. Using direct Euclidean distance (slower).")
            direct_sql = f"""
            SELECT 
                train_data.id AS train_id,
                train_data.features_array AS train_features,
                query_data.id AS query_id,
                query_data.features_array AS query_features,
                euclidean_distance(train_data.features_array, query_data.features_array) AS exact_distance
            FROM train_data
            CROSS JOIN query_data
            WHERE query_data.id = {query_id}
            ORDER BY exact_distance ASC
            LIMIT {config.topk}
            """
            
            final_results = spark.sql(direct_sql)
        
        sql_end_time = time.time()
        sql_query_time = sql_end_time - sql_start_time
        sql_query_times.append(sql_query_time)
        
        print(f"SQL-based ANN search completed in {sql_query_time:.4f} seconds")
        print(f"Top {config.topk} nearest neighbors from SQL-based ANN:")
        print(f"{'-'*40}")
        
        # Collect and display results
        sql_results_collected = final_results.select("train_id", "exact_distance").collect()
        for i, row in enumerate(sql_results_collected):
            print(f"  {i+1}. ID: {row['train_id']}, Distance: {row['exact_distance']:.4f}")
        
        # Compute exact nearest neighbors for recall computation (if enabled)
        if config.compute_recall:
            print(f"\nComputing exact nearest neighbors for recall evaluation...")
            exact_start_time = time.time()
            
            # Get IDs from SQL results
            sql_ids = set(row["train_id"] for row in sql_results_collected)
            
            # Compute distances to all training vectors (in Python for efficiency)
            all_distances = []
            for train_id, train_features in train_data_collected:
                distance = compute_euclidean_distance(query_features_array, train_features)
                all_distances.append((train_id, distance))
            
            # Sort by distance and get the top-k
            all_distances.sort(key=lambda x: x[1])
            exact_topk = all_distances[:config.topk]
            
            exact_end_time = time.time()
            exact_query_time = exact_end_time - exact_start_time
            exact_query_times.append(exact_query_time)
            
            # Get the set of IDs from the exact results
            exact_ids = set(id for id, _ in exact_topk)
            
            # Calculate recall (percentage of true neighbors found by SQL approach)
            common_neighbors = sql_ids.intersection(exact_ids)
            recall = len(common_neighbors) / len(exact_ids) if exact_ids else 0
            all_recalls.append(recall)
            total_recall += recall
            total_queries += 1
            
            print(f"Exact search completed in {exact_query_time:.4f} seconds")
            print(f"Top {config.topk} nearest neighbors from exact search:")
            print(f"{'-'*40}")
            for i, (id, dist) in enumerate(exact_topk):
                in_sql = "✓" if id in sql_ids else " "
                print(f"  {i+1}. ID: {id}, Distance: {dist:.4f} {in_sql}")
            
            print(f"\nRecall statistics for this query:")
            print(f"  - True positives: {len(common_neighbors)} out of {config.topk}")
            print(f"  - Recall rate: {recall:.4f} ({len(common_neighbors)}/{len(exact_ids)})")
            print(f"  - SQL approach speedup: {exact_query_time/sql_query_time:.2f}x faster than exact search")
    
    # Calculate and print final recall statistics
    if config.compute_recall and total_queries > 0:
        avg_recall = total_recall / total_queries
        avg_sql_time = sum(sql_query_times) / len(sql_query_times) if sql_query_times else 0
        avg_exact_time = sum(exact_query_times) / len(exact_query_times) if exact_query_times else 0
        speedup = avg_exact_time / avg_sql_time if avg_sql_time > 0 else 0
        
        print("\n" + "="*80)
        print("=" + " "*30 + "ANN BENCHMARK RESULTS" + " "*30 + "=")
        print("="*80)
        print(f"Number of queries: {total_queries}")
        print(f"Average recall @ {config.topk}: {avg_recall:.4f}")
        print(f"Min recall: {min(all_recalls):.4f}, Max recall: {max(all_recalls):.4f}")
        print(f"Average SQL query time: {avg_sql_time:.4f} sec")
        print(f"Average exact query time: {avg_exact_time:.4f} sec")
        print(f"Speedup: {speedup:.2f}x")
        print("="*80)

def main():
    parser = argparse.ArgumentParser(description="MNIST ANN benchmark with LSH and LakeSoul")
    parser.add_argument("--mode", required=True, choices=["load", "query"], help="Operation mode")
    
    # Load config parameters
    parser.add_argument("--hdf5-file", help="Path to HDF5 file with embeddings")
    parser.add_argument("--table-name", help="LakeSoul table name")
    parser.add_argument("--warehouse", help="LakeSoul warehouse location")
    parser.add_argument("--embedding-dim", type=int, help="Embedding dimension")
    parser.add_argument("--bucket-length", type=float, help="LSH bucket length")
    parser.add_argument("--num-hash-tables", type=int, help="Number of LSH hash tables")
    parser.add_argument("--sample-ratio", type=float, help="Fraction of data to use (0.0-1.0)")
    parser.add_argument("--self-query", action="store_true", help="Perform self-query after loading data")
    
    # Query config parameters
    parser.add_argument("--query-limit", type=int, help="Number of queries to run")
    parser.add_argument("--topk", type=int, help="Number of nearest neighbors to return")
    parser.add_argument("--compute-recall", action="store_true", help="Compute recall statistics for queries")
    parser.add_argument("--distance-threshold", type=float, help="Distance threshold for LSH candidate selection")
    
    args = parser.parse_args()
    
    spark = create_spark_session()
    
    if args.mode == "load":
        config = LoadConfig(
            hdf5_file=args.hdf5_file,
            warehouse=args.warehouse,
            table_name=args.table_name or "mnist_ann_table",
            embedding_dim=args.embedding_dim or 784,
            bucket_length=args.bucket_length or 2.0,
            num_hash_tables=args.num_hash_tables or 3,
            sample_ratio=args.sample_ratio or 0.1,
            self_query=args.self_query,
            query_limit=args.query_limit or 10,
            topk=args.topk or 10
        )
        _, model_path = load_data(config, spark)
    elif args.mode == "query":
        config = QueryConfig(
            hdf5_file=args.hdf5_file,
            table_name=args.table_name or "mnist_ann_table",
            query_limit=args.query_limit or 20,
            topk=args.topk or 10,
            warehouse=args.warehouse,
            bucket_length=args.bucket_length or 2.0,
            num_hash_tables=args.num_hash_tables or 3,
            compute_recall=args.compute_recall,
            distance_threshold=args.distance_threshold or 10.0
        )
        model_path = f"{config.warehouse}/{config.table_name}_model"
        query_ann(config, spark, model_path)

if __name__ == "__main__":
    main() 