from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("status", StringType(), True),
])

def main():
    # initialize spark sesh
    spark = SparkSession.builder \
        .appName("KafkaToHudi") \
        .config("spark.sql.shuffle.partitions", "1") \
        .master("local[2]") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.driver.host", "localhost") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # read from transactions topic
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "transactions") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # read the json, add two columns: processing_timestamp and partition_date
    parsed_df = df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("processing_timestamp", current_timestamp()) \
        .withColumn('partition_date', date_format(col("timestamp"), "yyyy-MM-dd"))
    

    # this was for debugging but leaving it here for visual indicator that things are working
    console_query = parsed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()

    hudi_options = {
        "hoodie.table.name": "transactions",
        "hoodie.datasource.write.recordkey.field": "transaction_id",
        "hoodie.datasource.write.partitionpath.field": "partition_date",
        "hoodie.datasource.write.precombine.field": "timestamp",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.SimpleKeyGenerator",
    }

    def write_to_hudi(batch_df, batch_id):
        if not batch_df.rdd.isEmpty():
            batch_df.write \
                .format("hudi") \
                .options(**hudi_options) \
                .mode("append") \
                .save("s3a://transactions/transactions_table")

    parsed_df.writeStream \
        .foreachBatch(write_to_hudi) \
        .option("checkpointLocation", "s3a://transactions/checkpoints") \
        .trigger(processingTime="10 seconds") \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
