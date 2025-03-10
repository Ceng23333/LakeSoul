from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("Simple PySpark Demo") \
    .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension") \
    .config("spark.sql.catalog.lakesoul", "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog") \
    .config("spark.sql.defaultCatalog", "lakesoul") \
    .getOrCreate()

# Create a simple DataFrame
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Show the DataFrame
print("DataFrame Contents:")
df.show()

# Get Spark version
print(f"Spark version: {spark.version}")

# Stop the Spark session
spark.stop()

print("Simple PySpark demo completed successfully!") 