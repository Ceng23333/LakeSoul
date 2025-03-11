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
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, udf, row_number, collect_list, array
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, FloatType, IntegerType, StringType, StructType, StructField, DoubleType
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

def load_data(config: LoadConfig, spark: SparkSession):
    """
    Load data from the HDF5 file, apply LSH transformation, and save to LakeSoul.
    Uses sampling to reduce memory usage.
    """
    print(f"\n{'='*60}")
    print(f"LOAD MODE: Preparing data and LSH model")
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
    # train_indices = sorted(random.sample(range(train_size), sample_size))
    train_vecs = h5f['train'][:sample_size]
    
    # Take a small sample of test data
    test_size = min(1000, h5f['test'].shape[0])
    test_vecs = h5f['test'][:test_size]

    
    print(f"Using {len(train_vecs)} training vectors and {len(test_vecs)} test vectors")
    
    
    
    # Create rows with ids, vectors and splits
    train_rows = [(i, vec.tolist(), "train") 
                  for i, vec in enumerate(train_vecs)]
    
    test_rows = [(i, vec.tolist(), "test") 
                 for i, vec in enumerate(test_vecs)]
    
    # Create schema for DataFrame
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("features_array", ArrayType(DoubleType()), True),
        StructField("split", StringType(), True)
    ])
    
    # Create DataFrame
    df = spark.createDataFrame(train_rows + test_rows, schema)
    
    # Filter to get just train data for model fitting
    
    # Save the model
    model_path = f"{config.warehouse}/{config.table_name}"
    
    df.show()
    df.write.format("lakesoul") \
        .option("hashPartitions", "id") \
        .option("hashBucketNum", 4) \
        .option("shortTableName", "trainData") \
        .mode("Overwrite") \
        .save(model_path)
    
    return df, model_path

def compute_euclidean_distance(vec1, vec2):
    """Compute Euclidean distance between two vectors"""
    return np.sqrt(sum((a - b) ** 2 for a, b in zip(vec1, vec2)))

def query_ann(config: QueryConfig, spark: SparkSession, model_path):
    """
    Query the ANN index using the same LSH parameters.
    Instead of trying to load the saved model (which can be problematic),
    we recreate it with the same parameters.
    """
    print(f"\n{'='*60}")
    print(f"QUERY MODE: Searching for approximate nearest neighbors")
    print(f"{'='*60}")
    print(f"Query configuration:")
    print(f"  - HDF5 file: {config.hdf5_file}")
    print(f"  - Table name: {config.table_name}")
    print(f"  - Warehouse: {config.warehouse}")
    print(f"  - Query limit: {config.query_limit}")
    print(f"  - Top-k neighbors: {config.topk}")
    print(f"  - Compute recall: {config.compute_recall}")
    print(f"  - Model path: {model_path}")
    print(f"{'='*60}\n")
    
    print("Recreating LSH model with the same parameters...")

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
    
    # Define distance function for ranking
    @udf("double")
    def euclidean_distance(vec1, vec2):
        return float(np.sqrt(sum((a - b) ** 2 for a, b in zip(vec1, vec2))))
    
    # Convert arrays back to vectors for LSH queries
    @udf(returnType=VectorUDT())
    def array_to_vector(arr):
        return Vectors.dense(arr)
    
    # Add vector column back for LSH usage
    df_with_vectors = lakesoul_df.withColumn("features", array_to_vector(col("features_array")))
    
    # Get train data to fit model
    train_df = df_with_vectors.filter(df_with_vectors.split == "train")
    
    # Check if the 'hashes' column already exists from the load phase
    # If it does, temporarily rename it to avoid conflicts during LSH fitting
    column_names = train_df.columns
    has_hashes_column = "hashes" in column_names
    
    if has_hashes_column:
        print("Found existing 'hashes' column, temporarily renaming to 'hashes_original'")
        train_df = train_df.withColumnRenamed("hashes", "hashes_original")
    
    # Create a unique outputCol name for the LSH model to prevent conflicts
    lsh_output_col = "lsh_hashes"
    
    # Recreate the LSH model with same parameters but different output column
    brp = BucketedRandomProjectionLSH(
        inputCol="features",
        outputCol=lsh_output_col,
        bucketLength=config.bucket_length,
        numHashTables=config.num_hash_tables,
        seed=42
    )
    
    # Fit the model with our training data
    print(f"Fitting LSH model with outputCol='{lsh_output_col}'")

    brp_model = brp.fit(train_df)
    
    # Filter test data
    test_df = df_with_vectors.filter(df_with_vectors.split == "test")
    if has_hashes_column:
        test_df = test_df.withColumnRenamed("hashes", "hashes_original")
    
    test_count = test_df.count()
    print(f"\nFound {test_count} test records for querying")
    
    # Sample some queries
    queries = test_df.limit(config.query_limit)
    print(f"Selected {config.query_limit} random queries from test set")
    
    # Collect train data for efficient exact search
    print(f"Collecting training data for exact search comparison...")
    start_time = time.time()
    train_data_collected = [(row["id"], row["features_array"]) for row in train_df.select("id", "features_array").collect()]
    collection_time = time.time() - start_time
    print(f"Collected {len(train_data_collected)} training vectors in {collection_time:.2f} seconds")
    
    # Initialize metrics for recall calculation
    total_queries = 0
    total_recall = 0.0
    all_recalls = []
    lsh_query_times = []
    exact_query_times = []
    
    print(f"\n{'='*60}")
    print(f"Starting query process for {config.query_limit} queries...")
    print(f"{'='*60}")
    
    # For each query
    for query_idx, query_row in enumerate(queries.collect()):
        query_id = query_row["id"]
        query_vec = query_row["features"]
        query_features_array = query_row["features_array"]
        
        print(f"\nQUERY {query_idx+1}/{config.query_limit} (ID: {query_id})")
        print(f"{'-'*60}")

        # Time the LSH query
        lsh_start_time = time.time()
        
        # Use LSH model to approximately search for similar items
        approx_similar = brp_model.approxSimilarityJoin(
            train_df,
            queries.filter(col("id") == query_id),
            threshold=config.distance_threshold,  # Distance threshold for candidate selection
            distCol="distance"
        )
        approx_similar.show()
        
        # Compute exact distances and select top-k
        lsh_results = approx_similar.select(
            col("datasetA.id").alias("train_id"),
            col("datasetA.features_array").alias("train_features"),
            col("datasetB.id").alias("query_id"),
            col("datasetB.features_array").alias("query_features"),
            euclidean_distance(col("datasetA.features_array"), col("datasetB.features_array")).alias("exact_distance")
        ).orderBy("exact_distance").limit(config.topk)
        
        lsh_end_time = time.time()
        lsh_query_time = lsh_end_time - lsh_start_time
        lsh_query_times.append(lsh_query_time)
        
        print(f"LSH approximate search completed in {lsh_query_time:.4f} seconds")
        print(f"Top {config.topk} nearest neighbors from LSH:")
        print(f"{'-'*40}")
        lsh_results_collected = lsh_results.select("train_id", "exact_distance").collect()
        for i, row in enumerate(lsh_results_collected):
            print(f"  {i+1}. ID: {row['train_id']}, Distance: {row['exact_distance']:.4f}")
        
        # Compute exact nearest neighbors for recall computation (if enabled)
        if config.compute_recall:
            print(f"\nComputing exact nearest neighbors for recall evaluation...")
            exact_start_time = time.time()
            
            # Get neighbor vectors from LSH results
            neighbor_vecs = [(row["train_id"], row["train_features"]) for row in lsh_results.collect()]
            
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
            
            # Get the set of IDs from the LSH results (using neighbor_vecs)
            lsh_ids = set(id for id, _ in neighbor_vecs)
            
            # Get the set of IDs from the exact results
            exact_ids = set([id for id, _ in exact_topk])
            print(f"lsh_ids: {lsh_ids}")
            print(f"exact_ids: {exact_ids}")
            
            # Calculate recall (percentage of true neighbors found by LSH)
            common_neighbors = lsh_ids.intersection(exact_ids)
            recall = len(common_neighbors) / len(exact_ids) if exact_ids else 0
            all_recalls.append(recall)
            total_recall += recall
            total_queries += 1
            
            print(f"Exact search completed in {exact_query_time:.4f} seconds")
            print(f"Top {config.topk} nearest neighbors from exact search:")
            print(f"{'-'*40}")
            for i, (id, dist) in enumerate(exact_topk):
                in_lsh = "✓" if id in lsh_ids else " "
                print(f"  {i+1}. ID: {id}, Distance: {dist:.4f} {in_lsh}")
            
            print(f"\nRecall statistics for this query:")
            print(f"  - True positives: {len(common_neighbors)} out of {config.topk}")
            print(f"  - Recall rate: {recall:.4f} ({len(common_neighbors)}/{len(exact_ids)})")
            print(f"  - LSH speedup: {exact_query_time/lsh_query_time:.2f}x faster than exact search")
    
    print("config.compute_recall = {}, total_queries = {}, total_recall = {}".format(config.compute_recall, total_queries, total_recall))
    # Calculate and print final recall statistics
    if config.compute_recall and total_queries > 0:
        avg_recall = total_recall / total_queries
        avg_lsh_time = sum(lsh_query_times) / len(lsh_query_times) if lsh_query_times else 0
        avg_exact_time = sum(exact_query_times) / len(exact_query_times) if exact_query_times else 0
        speedup = avg_exact_time / avg_lsh_time if avg_lsh_time > 0 else 0
        
        print("\n" + "="*80)
        print("=" + " "*30 + "ANN BENCHMARK RESULTS" + " "*30 + "=")
        print("="*80)
        print(f"Number of queries: {total_queries}")
        print(f"Average recall @ {config.topk}: {avg_recall:.4f}")
        print(f"Min recall: {min(all_recalls):.4f}, Max recall: {max(all_recalls):.4f}")
        print(f"Average LSH query time: {avg_lsh_time:.4f} sec")
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