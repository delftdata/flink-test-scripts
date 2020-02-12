import threading
import time
import uuid
from datetime import datetime
import json
from random import randint, random, seed
#import numpy as np
import sys
from confluent_kafka.cimpl import Producer, KafkaError
from confluent_kafka.cimpl import Consumer
import statistics

current_milli_time = lambda: int(round(time.time() * 1000))


def exec_benchmark(duration_s, fps, kafka_loc, output_topic, silent):
    """Measures throughput at the output Kafka topic,
    by checking the growth in all partitions"""


    c = Consumer({
        'bootstrap.servers': kafka_loc,
        'group.id': 'benchmark-' + str(uuid.uuid4()),
        'auto.offset.reset': 'latest',
        'max.poll.interval.ms': 86400000
    })

    # === Get topic partitions

    topic_partitions = None

    def store_topic_partition(consumer, partitions):
        nonlocal topic_partitions
        topic_partitions = partitions

    c.subscribe([output_topic], on_assign=store_topic_partition)
    while topic_partitions is None:
        c.consume(timeout=0.5)

    #Loop read partitions



    throughput_measured = []
    throughput_measured_per_partition = {}
    last_values = {}
    for p in topic_partitions:
        low, high = c.get_watermark_offsets(p)
        throughput_measured_per_partition[p.partition] = []
        last_values[p.partition] = high
        if silent != "silent":
            print("Starting value for partition {}: {}".format(p.partition, high))

    MS_PER_UPDATE = 1000 / fps

    start_time = current_milli_time()
    last_time = start_time
    current_time = start_time
    last_write_time = current_time

    lag = 0.0

    while current_time < start_time + duration_s * 1000:
        current_time = current_milli_time()
        elapsed = current_time - last_time
        last_time = current_time
        lag += elapsed
        while lag >= MS_PER_UPDATE:
            #calc new val
            total_new = 0
            curr_time_for_print = current_milli_time()
            for p in topic_partitions:
                low, high = c.get_watermark_offsets(p)
                delta = high - last_values[p.partition]
                total_new += delta
                throughput_measured_per_partition[p.partition].append((delta / ((curr_time_for_print - last_write_time)/1000), curr_time_for_print))
                last_values[p.partition] = high
            throughput_measured.append((total_new / ((curr_time_for_print - last_write_time)/1000), curr_time_for_print))
            last_write_time = curr_time_for_print

            lag -= MS_PER_UPDATE


    if silent != "silent":
        print(str(throughput_measured))
        print("\n")
        for p in topic_partitions:
            print(str(p.partition) + ": " + str(throughput_measured_per_partition[p.partition]))

    print(int(statistics.mean([x[0] for x in throughput_measured if x[0] > 0.0])))

def main():
    if len(sys.argv) != 6:
        print("Error! Usage: python3 {} duration_in_seconds readings_per_second kafka_bootstrap output_topic")
        exit(1)
    exec_benchmark(int(sys.argv[1]),int(sys.argv[2]), sys.argv[3],sys.argv[4], sys.argv[5])

if __name__ == "__main__":
    main()

