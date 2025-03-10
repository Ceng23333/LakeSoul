from pyspark.sql import SparkSession

# Initialize Spark session with LakeSoul configurations
spark = SparkSession.builder \
    .appName("LakeSoul-PySpark-Demo") \
    .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension") \
    .config("spark.sql.catalog.lakesoul", "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog") \
    .config("spark.sql.catalog.lakesoul.warehouse", "s3a://lakesoul-test-bucket/") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("\n" + "="*80)
print("LakeSoul PySpark Demo Running!")
print("="*80 + "\n")

# Create a simple dataframe
data = [("John", 30), ("Alice", 25), ("Bob", 35)]
df = spark.createDataFrame(data, ["Name", "Age"])

print("Sample DataFrame:")
df.show()

# Write to LakeSoul table
print("\nWriting data to LakeSoul table...")
df.write.format("lakesoul").mode("overwrite").save("s3a://lakesoul-test-bucket/demo_table")

# Read from LakeSoul table
print("\nReading data from LakeSoul table...")
read_df = spark.read.format("lakesoul").load("s3a://lakesoul-test-bucket/demo_table")
read_df.show()

print("\n" + "="*80)
print("Demo completed successfully!")
print("="*80 + "\n")

# Stop the Spark session
spark.stop()