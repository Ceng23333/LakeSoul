# Debugging Guide for LSH Vector Data Type Issue

The error we're seeing suggests that LakeSoul doesn't support the complex vector data type used by the LSH model's hash values. Let's debug and fix this issue.

## 1. First, start the debugging environment

```bash
chmod +x debug_with_pyspark.sh
./debug_with_pyspark.sh
```

## 2. Inside the PySpark shell, let's load the data and examine the problem

```python
# Import required libraries
import h5py
import numpy as np
import random
from pyspark.ml.feature import BucketedRandomProjectionLSH
from pyspark.ml.linalg import Vectors
from pyspark.sql.functions import col, udf
from pyspark.sql.types import *
from pyspark.ml.linalg import VectorUDT

# Open the HDF5 file
file_path = "/data/embeddings/fashion-mnist-784-euclidean.hdf5"
h5f = h5py.File(file_path, 'r')

# Check the file structure
print("Available keys in HDF5 file:", list(h5f.keys()))

# Get the train and test vectors
train_vecs = h5f['train'][:100]  # First 100 training vectors for testing
test_vecs = h5f['test'][:20]  # First 20 test vectors for testing

# Generate random labels (since actual labels are not in the file)
train_labels = [random.randint(0, 9) for _ in range(len(train_vecs))]
test_labels = [random.randint(0, 9) for _ in range(len(test_vecs))]

# Convert to a DataFrame
train_rows = [(i, Vectors.dense(vec.tolist()), "train", label) 
               for i, (vec, label) in enumerate(zip(train_vecs, train_labels))]

# Create a schema for the DataFrame
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("features", VectorUDT(), True),
    StructField("split", StringType(), True),
    StructField("label", IntegerType(), True)
])

# Create a DataFrame
df = spark.createDataFrame(train_rows, schema)
df.show(5)

# Create and fit the LSH model
brp = BucketedRandomProjectionLSH(
    inputCol="features",
    outputCol="hashes",
    bucketLength=2.0,
    numHashTables=3,
    seed=42
)

model = brp.fit(df)

# Apply the transformation
hashed_df = model.transform(df)

# Examine the schema to see what's causing the problem
hashed_df.printSchema()

# Inspect the hashes column structure
hash_example = hashed_df.select("hashes").first()[0]
print("Hash type:", type(hash_example))
print("Hash structure:", hash_example)
```

## 3. The issue and potential solution

After examining the schema, you'll see that the "hashes" column is a complex type that LakeSoul cannot handle directly. Let's convert it to a more compatible format:

```python
# Define a function to convert the hashes to a string representation
@udf(returnType=StringType())
def hash_to_string(hash_array):
    if hash_array is None:
        return None
    return str(hash_array)

# Apply the transformation
simplified_df = hashed_df.withColumn("hashes_str", hash_to_string(col("hashes")))

# Drop the original complex hash column
simplified_df = simplified_df.drop("hashes")

# Verify the new schema
simplified_df.printSchema()

# Try saving to LakeSoul
simplified_df.write.format("lakesoul").save("s3a://lakesoul-test-bucket/lakesoul-test/mnist_ann_table")
```

## 4. Alternative approach using array type

If you find that the hashes are SparseVector objects, you can extract their array representation:

```python
@udf(returnType=ArrayType(DoubleType()))
def sparse_to_array(vec):
    if vec is None:
        return None
    # Check if the vector has toArray() method
    if hasattr(vec, "toArray"):
        return [float(x) for x in vec.toArray()]
    # If it's already an array-like object
    return [float(x) for x in vec]

# Apply the transformation to convert sparse vectors to arrays
hashed_array_df = hashed_df.withColumn("hash_arrays", sparse_to_array(col("hashes")))

# Verify the new schema
hashed_array_df.printSchema()

# Try saving this version (drop the original complex column)
hashed_array_df.drop("hashes").write.format("lakesoul").save("s3a://lakesoul-test-bucket/lakesoul-test/mnist_ann_table")
```

## 5. Fix for the main script

Based on your findings in the interactive shell, you'll need to update the `mnist_ann_bucketed_random_projection.py` script. Here's a template for the fix:

```python
# After applying the LSH transformation:
train_with_hashes = model.transform(train_df)

# Convert the hashes to a format LakeSoul can handle
@udf(returnType=StringType())  # or ArrayType(DoubleType()) based on what worked in debugging
def convert_hashes(hash_obj):
    if hash_obj is None:
        return None
    # Use the conversion method that worked in debugging
    return str(hash_obj)  # or hash_obj.toArray().tolist() or whatever worked

# Apply the conversion
train_with_hashes = train_with_hashes.withColumn("hashes_compatible", convert_hashes(col("hashes")))

# Drop the original complex hashes column
train_with_hashes = train_with_hashes.drop("hashes")

# Then continue with the save operation
train_with_hashes.select(
    col("id"), 
    col("features_array"),
    col("hashes_compatible").alias("hashes"),  # rename to keep original column name if needed
    col("split"), 
    col("label")
).write.format("lakesoul").save(f"{config.warehouse}/{config.table_name}")
```

This approach should allow you to save the LSH data in a format compatible with LakeSoul while still maintaining all the necessary information for ANN queries. 