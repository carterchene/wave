# Challenge 1 -- Hudi stream to S3

## SETUP

#### Python
I use uv as the package manager here. the version of python matters for this project and uv ensures its exactly the version i want. 

to get started: 

```
pip install uv
```

then, at the root of the repo: 

```
uv sync
```

#### Docker
I use docker here for portability and replication. if you don't have it, download it here: https://docs.docker.com/get-started/get-docker/

Then, in a BASH terminal (i use git bash on windows):

```
cd challenge1
```

```
bash setup_and_run_stream.sh
```

That will take a minute or two. It prints what's going onto console so you can follow. Once the initial docker containers are running, should be able to see the minio 
ui at 

http://localhost:9001

once the script fully spins up, there should be data populating in the 'transactions' bucket. (view in Object Browser on the left hand nav menu)

For batch processing (preparing for redshift incremental load):

```
bash redshift_incremental_batch.sh
```

that will submit a spark job which will result in a table at "transactions/redshift_stage" in minio.


## Summary

I've made a streaming-pipeline-in-a-box. I use kafka, minIO, and spark via docker-compose to ingest and process the simulated transcation data. 

I used minIO because it's s3-based and can be ran locally. essentially an on-premise s3. 
I use kafka to ingest the transaction records. kafka integrates nicely with spark streaming (and flink!), making it easy to process.
I use spark streaming because I believe wave uses spark. of course, flink or beam are other good options that are probably technically better than spark streaming. 

all of this is in docker because it makes it portable and easy to replicate. 

I edited the included supporting code just slightly so that it sends the transaction to a kafka producer (originally it was just printing the transaction)

my spark_app/spark_hudi.py file is the main streaming spark job. it reads from kafka and writes to a hudi table in minio (s3). 

## "Prepare the data for incremental loading into a downstream Redshift table."
with regards to the inceremental load for redshift, my understanding is that the easiest way to get data into redshift is via the copy command, which isn't very flexible. 
Therefore, i created a second spark batch job that incrementally pulls unloaded hudi table data into a staging layer in the data lake. it does this based off of a "processing_time" column that
I add in the spark stream. in a real environment, we'd store the processing time everytime we run the prepare_for_redshift_incremental.py batch job. on every run, it that time would be passed 
to the batch job, so that's only ever reading data that has yet to be loaded to redshift. we'd then use the copy command on that staging area to load to a table in redshift, then merge that incr data to the main redshift table via dbt, stored proc, sqlmesh etc 
NOTE: i just have a hardcoded processing_time in the batch job for sake of demo

## "Handle late-arriving data by updating records in the Hudi table."

the 'processing_time' column that i add ensures that we get arbitrarily late data sent to redshift. if we just used the timestamp or partition_date column to 
get incremental data sent to redshift, then we would potentially miss late data. 

also, if late-arriving transactions (same transaction id), hudi handles that wiht upsert:

my hudi options:
```
hudi_options = {
        "hoodie.table.name": "transactions",
        "hoodie.datasource.write.recordkey.field": "transaction_id",
        "hoodie.datasource.write.partitionpath.field": "partition_date",
        "hoodie.datasource.write.precombine.field": "timestamp",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.SimpleKeyGenerator",
    }
```

there we can see im setting the key to transaction_id and setting precombine.field to teh 'timestamp' column. this means if two records come in with the same
id, hudi takes the one with the highest 'timestamp' value.

## Fault Tolerance, Scalabilty

my choice of kafka and spark are meant for scalability. they are both distributed. rather than running them on one docker container, we could run them on kubernetes cluster (or managed via EMR, databricks, glue, etc). same is true of minio but in prod we'd use s3 which is very scalable 

for fault tolerance:

kafka: we can add more replication factors for the topic:

```
docker exec kafka opt/bitnami/kafka/bin/kafka-topics.sh \
    --create --topic transactions \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1 \ ## make this more
    || echo "Topic may already exist"
```
that ensure if a broker goes down for whatever reason, theres redundant ones to take over

we can also enable retries with the KafkaProducer class in the streaming script and we can spcify a wait time between the retries.

spark:
    we have a checkpoint location in the object storage that lets spark streaming resuming from where it left off in case of failure


# Challenge 2 -- dbt Transformations









