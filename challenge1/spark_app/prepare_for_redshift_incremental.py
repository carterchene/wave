from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder \
        .appName("PrepareRedshiftData") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    #### JUST FOR DEMO PURPOSES, THIS IS HARDCODED
    # in real situation, we'd store this in a table, then we would read/update that table on every run of this batch job
    last_processed_timestamp = "2025-01-12 00:00:00"

    incremental_df = spark.read \
        .format("hudi") \
        .load("s3a://transactions/transactions_table") \
        .filter(col("processing_timestamp") > last_processed_timestamp)

    # Write to staging area as parquet. this will then be pulled into redshift via COPY
    if not incremental_df.rdd.isEmpty():
        incremental_df.write \
            .mode("overwrite") \
            .parquet("s3a://transactions/redshift_stage")
        
        print(f"Staged {incremental_df.count()} records for Redshift loading")
    else:
        print("No new records to process")

    spark.stop()

if __name__ == "__main__":
    main()