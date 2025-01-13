# 3 issues with the architecture

### 1. data loss at kafka ingestion

it's possible kafka experiences data loss if overwhelmed or interrupted. 

to mitigate: 
- use kafkas replication factors. this provides data redundancy in case of failure
- configure kafka 'acknowledgements' to make sure all replication factors get the message

tradeoffs: 
there's additional overhead (storage, compute) for acks and replication. but would almsot certainly still be worth the data loss prevention

### 2. redshift scalability

with large data volumes / complex queries, redshift can become slow.

mitigation:
- use built in featureslike disitrbution / sort keys so that data is stored efficiently. 
- use redshift 'concurrency scaling'. automatically scales out to accommadate many users reading/writing data. 
  you can configure which types of queries get considerd for concurrency scaling which helps limit unnecessary cost (via Workload Management)

tradeoffs: 
concurrency scaling can be expensive and has many limitations like queries with UDFs arent supported, doesnt support DDL queries, doesnt support COPY from parquet or ORC. 

### 3. schema evolution breaking pipelines

if theres schema changes in the data, it can break things in many areas: kafka, hudi, redshift. 

mitigation:
- kafka schema registry can mitigate some of the risk, but it needs to be configured properly.
- utilize hudi schema evolution
- reading hudi table directly to redshift table could cause schema issues if hudi has some schema evolution enabled, to mitigate there should be some transformation logic with explicit
  schema handling that reduces this risk (casting, explicit column names)

tradeoffs: 
- if not configurd correctly (e.g. too strict), kafka schema registry could create data loss by rejecting data we don't actually want to reject
- schema validation has a performance impact
- increased complexity of pipelines


