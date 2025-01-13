#!/bin/bash

docker exec spark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.apache.hudi:hudi-spark3.3-bundle_2.12:0.13.1 apps/prepare_for_redshift_incremental.py
