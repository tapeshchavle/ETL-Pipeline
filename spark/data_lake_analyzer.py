"""
Foodingo Spark Data Lake Analyzer
=================================

This script demonstrates how Apache Spark connects directly to the S3 data lake,
bypassing PostgreSQL, to analyze raw Parquet files in a distributed manner.

Run this script inside the Jupyter Notebook container!
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, desc

def create_spark_session():
    print("Initializing Spark Session...")
    
    # We must include the Hadoop AWS and AWS Java SDK packages so Spark can read s3a://
    spark = SparkSession.builder \
        .appName("Foodingo Data Lake Analyzer") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "foodingo") \
        .config("spark.hadoop.fs.s3a.secret.key", "foodingo123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
        
    # Reduce logging verbosity
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def analyze_data_lake():
    spark = create_spark_session()
    
    try:
        print("Connected to Spark Master successfully!")
        print("Reading Parquet data lake files from S3 (s3a://foodingo-data-lake/raw/events/order/created/)...")
        
        # We read the entire folder of Parquet files. Spark automatically discovers the Hive partitions!
        # (e.g. year=2026/month=07/day=24)
        orders_df = spark.read.parquet("s3a://foodingo-data-lake/raw/events/order/created/")
        
        print("\n--- Raw Data Lake Schema ---")
        orders_df.printSchema()
        
        print(f"\nTotal Orders in Data Lake: {orders_df.count()}")
        
        print("\n--- Revenue Aggregation by Payment Status ---")
        revenue_df = orders_df.groupBy("paymentStatus") \
            .agg(
                sum("amount").alias("Total Revenue"),
                count("orderId").alias("Order Count")
            ) \
            .orderBy(desc("Total Revenue"))
            
        revenue_df.show()
        
        print("\nThis aggregation happened in a distributed manner across Spark Worker nodes,")
        print("completely bypassing your PostgreSQL database!")
        
    except Exception as e:
        print("\n[ERROR] Failed to read from Data Lake. Is there data in S3 yet?")
        print(f"Error details: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    analyze_data_lake()
