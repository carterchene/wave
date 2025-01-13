"""
File Name: stream_simulator.py
Description:
Python script that simulates a data stream of sample data at an average rate of
1 record per second.
The script uses the random module for generating random data, the time module
for simulating the rate of ~1 record per second,
and the datetime module for timestamps. It also simulates out-of-order
timestamps for some records.
"""
import random
import time
from datetime import datetime, timedelta


"""
    Description of changes made by Carter Chene:

    The original script just prints the records to console, i modify it slightly just so that
    the records get sent via kafkaproducer. this is more similiar to how it would work in an actual
    production environment. 

    Everything surrounded in hashtags are lines i added. 

"""

################# CARTER CHENE ADDITION
from kafka import KafkaProducer
import json
#################



def generate_transaction():
    """Generate a single transaction record."""
    transaction_id = random.randint(1000000, 9999999) # Updated range
    customer_id = random.randint(1, 100)
    amount = round(random.uniform(10.0, 500.0), 2)
    status = random.choice(["SUCCESS", "PENDING", "FAILED"])
    timestamp = datetime.now()
    # Introduce out-of-order timestamps for some records
    if random.random() < 0.2: # 20% chance of being late
        timestamp -= timedelta(seconds=random.randint(1, 60)) # Late by up to 60 seconds

    return {
    "transaction_id": transaction_id,
    "customer_id": customer_id,
    "amount": amount,
    "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
    "status": status,
    }

# def stream_simulator():
################# CARTER CHENE ADDITION
def stream_simulator(producer: KafkaProducer, topic: str) -> None: 
#################
    """Simulate a stream of transaction data."""
    try:
        while True:
            record = generate_transaction()
            print(record) # Simulate streaming by printing the record
            ################# CARTER CHENE ADDITION
            future = producer.send(topic, value=record)
            # Add callback to verify message was sent
            future.get(timeout=10)
            print("Record successfully sent to Kafka")
            time.sleep(random.uniform(0.1, 1.9))
            #################

            time.sleep(random.uniform(0.1, 1.9)) # Updated randomness in sleep
    except KeyboardInterrupt:
        print("Stream simulation stopped.")
        producer.close()

if __name__ == "__main__":

    ################# CARTER CHENE ADDITION
    producer = KafkaProducer(
        bootstrap_servers="localhost:9093",
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        request_timeout_ms = 30000,
        security_protocol = 'PLAINTEXT'
    )

    topic = 'transactions'
    print(f'Producing messages to topic {topic}...')

    stream_simulator(producer,topic)
    #################

