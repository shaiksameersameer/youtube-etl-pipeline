from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("YouTube_ETL").getOrCreate()

# Input and output paths
input_path = "/mnt/d/pratice/day 48/USvideos.csv"
output_path = "/home/sameer/projects/youtube/processed_youtube_data/"

print("🚀 Starting ETL Job...")

# Load data
df = spark.read.csv(input_path, header=True, inferSchema=True)

# Basic cleaning: remove nulls, drop duplicates
cleaned_df = df.dropna().dropDuplicates()

# Convert columns
cleaned_df = cleaned_df.withColumn("views", col("views").cast("long")) \
                       .withColumn("likes", col("likes").cast("long")) \
                       .withColumn("dislikes", col("dislikes").cast("long")) \
                       .withColumn("comment_count", col("comment_count").cast("long"))

# Save as Parquet
cleaned_df.write.mode("overwrite").parquet(output_path)

print("✅ ETL Job Completed — Clean data stored in:")
print(output_path)
spark.stop()

