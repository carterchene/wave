#!/bin/bash

# set up spark, kafka, minio
echo "Starting kafka, spark, minio..." 
docker-compose up -d

echo "waiting for kafka.. (20s)" 
sleep 20

# create "transacations" kafka topic
echo "creating 'transactions' kafka topic" 
docker exec kafka opt/bitnami/kafka/bin/kafka-topics.sh \
    --create --topic transactions \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1 \
    || echo "Topic may already exist"

# create "transactions" s3 bucket
echo "creating transactions bucket in minio (s3-based object storage). View at http://localhost:9001" 
docker exec minio mc alias set myminio http://localhost:9000 minioadmin minioadmin
docker exec minio mc mb myminio/transactions

# start the stream to kafka
echo "starting data stream to kafka..."
uv run stream_simulator.py &

# start the spark streaming job. this will write to minio bucket. takes a few seconds to show up in minio (has to download jars, etc) 
echo "starting spark streaming job... might take ~30s for the data to show up in the minio bucket"
docker exec spark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.apache.hudi:hudi-spark3.3-bundle_2.12:0.13.1 apps/spark_hudi.py


