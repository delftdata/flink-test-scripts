import threading
import time
import uuid
from datetime import datetime
import json
from random import randint, random, seed
#import numpy as np
import sys
from confluent_kafka.cimpl import Producer, KafkaError
import random 

current_milli_time = lambda: int(round(time.time() * 1000))

def exec_producer(duration_s, resolution, throughput, kafka_loc, input_topic):
    """Performs resolution writes per second for duration_s seconds on input_topic topic of kafka_bootstrap cluster, achieving throughput throughput
    The default partitioner follows these rules:
            If a producer specifies a partition number in the message record, use it.
            If the message doesn’t hard code a partition number, but it provides a key, choose a partition based on a hash value of the key.
            If no partition number or key is present, pick a partition in a round-robin fashion.

    So, by not specifying any key or partitioning, round robin will be used!"""


    producer = Producer({'bootstrap.servers': kafka_loc})

    MS_PER_UPDATE = 1000 / resolution

    start_time = current_milli_time()
    last_time = start_time
    current_time = start_time

    lag = 0.0

    messages_produced_per_interval = throughput / resolution
    remainder = throughput % resolution

    word_file = "/home/ubuntu/flink-test-scripts/cracklib-small"
    words = open(word_file).read().splitlines()

    produces_this_second = 0
    count = 0
    while current_time < start_time + duration_s * 1000:
        current_time = current_milli_time()
        elapsed = current_time - last_time
        last_time = current_time
        lag += elapsed
        while lag >= MS_PER_UPDATE:
            for i in range(messages_produced_per_interval):
                producer.produce(topic, value=random.choice(words))
                count+=1

            if produces_this_second == resolution-1: # Throughput may not be divisible by resolution
                for i in range(remainder):
                    producer.produce(topic, value=random.choice(words))
                    count+=1

            produces_this_second = (produces_this_second + 1 ) % resolution
            producer.flush()
            lag -= MS_PER_UPDATE

    print("Done! Produced {} messages in {} seconds. Throughput: {}".format(count, duration_s, count/duration_s))


def main():
    if len(sys.argv) != 6:
        print("Error! Usage: python3 {} duration_in_seconds resolution throughput kafka_bootstrap input_topic")
        exit(1)
    exec_producer(int(sys.argv[1]),int(sys.argv[2]), sys.argv[3],sys.argv[4], sys.argv[5])

if __name__ == "__main__":
    main()

