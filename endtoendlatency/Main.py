import argparse
import time
import uuid
import json

from confluent_kafka.cimpl import Consumer, TopicPartition, OFFSET_END

current_milli_time = lambda: int(round(time.time() * 1000))


def sample_output_topic(args):
    num_partitions = int(args["num_partitions"])
    mps = int(args["measures_per_second"])
    experiment_duration = int(args["duration"])
    topic = args["output_topic"]
    resolution = int(args["resolution"])
    transactional = args["transactional"]
    partition_table = {}

    consumers = create_consumers(args, num_partitions, partition_table)

    ms_per_update = 1000 / mps

    start_time = current_milli_time()
    last_time = start_time
    current_time = start_time

    lag = 0.0

    while current_time < start_time + experiment_duration * 1000:
        current_time = current_milli_time()
        elapsed = current_time - last_time
        last_time = current_time
        lag += elapsed
        while lag >= ms_per_update:
            if current_time >= start_time + experiment_duration * 1000:
                break
            step(consumers, partition_table, resolution, topic, transactional)
            # time.sleep()
            lag -= ms_per_update

    for i in range(num_partitions):
        array = partition_table[i]
        sorted_array = sorted(array, key=lambda triplet: triplet[0])
        partition_table[i] = sorted_array
        consumers[i].close()
    return partition_table


def step(consumers, partition_table, resolution, topic, transactional):
    if transactional:  # consume rest of data
        time_now = current_milli_time()
        for partition, c in enumerate(consumers):
            while True:
                msg = poll_next_message(c, partition, resolution, topic,
                                        transactional)
                if msg is None or msg.error():
                    if msg is not None:
                        print("error: " + str(msg.error()))
                    break
                process_message(msg, partition, partition_table, time_now)
    else:
        # Get one message from each consumer
        for partition, c in enumerate(consumers):
            msg = poll_next_message(c, partition, resolution, topic, transactional)
            if msg is None or msg.error():
                if msg is not None:
                    print("error:" + str(msg.error()))
                continue
            process_message(msg, partition, partition_table)


def poll_next_message(c, partition, resolution, topic, transactional):
    msg = None
    try:
        offset = get_next_offset(c, partition, resolution, topic, transactional)
        c.seek(TopicPartition(topic, partition, offset))
        msg = c.poll(timeout=0.05)
    except Exception as e:
        print(e)
    return msg


def get_next_offset(c, partition, resolution, topic, transactional):
    if transactional:
        return c.position([TopicPartition(topic, partition)])[0].offset + resolution
    else:
        return OFFSET_END


def process_message(msg, partition, partition_table, visibility_ts=0):
    dict = json.loads(msg.value())
    print("timestamp: " + dict["inputTS"])
    print("timestamp[2:]: " + dict["inputTS"][2:])
    input_ts = int(dict["inputTS"][2:])
    output_ts = int(msg.timestamp()[1])
    if visibility_ts == 0:
        partition_table[partition].append((input_ts, output_ts, visibility_ts, output_ts - input_ts))
    else:
        partition_table[partition].append((input_ts, output_ts, visibility_ts, visibility_ts - input_ts))


def create_consumers(args, num_partitions, partition_table):
    consumers = []
    transactional = args["transactional"]
    for i in range(num_partitions):
        partition_table[i] = []
        oc = Consumer({
            'bootstrap.servers': args["kafka"],
            'group.id': str(uuid.uuid4()),
            'auto.offset.reset': 'latest',
            'api.version.request': True,
            'isolation.level': ('read_committed' if transactional else 'read_uncommitted'),
            'max.poll.interval.ms': 86400000
        })
        oc.assign([TopicPartition(args["output_topic"], i)])
        oc.poll(0.5)
        consumers.append(oc)
    return consumers


def get_latency(args):
    num_partitions = int(args["num_partitions"])

    partition_table = sample_output_topic(args)
    print_header(num_partitions)
    print_table(num_partitions, partition_table)


def print_table(num_partitions, partition_table):
    num_readings = min(len(partition) for partition in partition_table.values())
    for i in range(num_readings):
        part0 = partition_table[0]
        row = "{}\t{}\t{}\t{}".format(part0[i][0], part0[i][1], part0[i][2], part0[i][3])

        for part in range(1, num_partitions):
            parti = partition_table[part]
            row += "\t{}\t{}\t{}\t{}".format(parti[i][0], parti[i][1], parti[i][2], parti[i][3])

        print(row)


def print_header(num_partitions):
    header = "INPUT-0\tOUTPUT-0\tVISIBLE-0\tLATENCY-0"
    for i in range(1, num_partitions):
        header += "\tINPUT-{}\tOUTPUT-{}\tVISIBLE-{}\tLATENCY-{}".format(i, i, i, i)
    print(header)


def parse_args():
    parser = argparse.ArgumentParser(description='SFaaS CLI')
    parser.add_argument("-k", "--kafka", help="The location of kafka. A comma separated list of ip:port pairs.")
    parser.add_argument("-o", "--output-topic", help="The output topic")
    parser.add_argument("-d", "--duration", help="Duration of experiment", default=120)
    parser.add_argument("-mps", "--measures-per-second",
                        help="The number of measurements to perform every second",
                        default=2)
    parser.add_argument("-r", "--resolution", help="The number of records to skip between each consume", default=10000)
    parser.add_argument("-p", "--num-partitions", help="The number of partitions", default=1)
    parser.add_argument("-t", "--transactional", help="Whether the consumer is transactional")
    args = parser.parse_args()
    args = vars(args)
    args["transactional"] = str2bool(args["transactional"])
    return args


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def main():
    args = parse_args()
    get_latency(args)


if __name__ == "__main__":
    main()
