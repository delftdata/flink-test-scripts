import argparse
import uuid

from confluent_kafka.cimpl import Consumer, TopicPartition


def consume_input_topic(consumer, join_table, resolution):
    print("input")
    num_partitions = len(consumer.assignment())
    iteration = 1
    done = False
    while not done:
        try:
            newoffset=int(iteration*int(resolution)/num_partitions)
            for tp in consumer.assignment():
                if newoffset >= consumer.get_watermark_offsets(tp)[1]:
                    done = True
                    continue
                consumer.seek(TopicPartition(topic=tp.topic,partition=tp.partition, offset=newoffset))
        except Exception as e:
            print(e)
            done = True
            continue
        msgs = consumer.consume(timeout=5, num_messages=1)
        if len(msgs) == 0:
            done = True
            continue
        for msg in msgs:
            print(msg.value())
            join_table[msg.value()] = int(msg.timestamp()[1])
        iteration+=1

def consume_output_topic(consumer, join_table):
    print("output")
    while True:
        msgs = consumer.consume(timeout=5)
        print("consume")
        if len(msgs) == 0:
            break
        for msg in msgs:
            key = msg.value()
            if key in join_table:
                input_ts = join_table[key]
                if isinstance(input_ts, int):
                    output_ts = int(msg.timestamp()[1])
                    join_table[key] = (input_ts, output_ts, output_ts - input_ts)
    #Filter out any unjoined keys. It should not happen, but still
    print("done consuming, filtering")
    return dict(filter(lambda kv: not isinstance(kv[1], int), join_table.items()))

def join(args):
    join_table = {}

    ic = Consumer({
        'bootstrap.servers': args["kafka"],
        'group.id': str(uuid.uuid4()),
        'auto.offset.reset': 'earliest',
        'api.version.request': True,
        'max.poll.interval.ms': 60000
    })

    ic.subscribe([args["input_topic"]])
    ic.consume(timeout=20, num_messages=1)
    consume_input_topic(ic, join_table, args["resolution"])



    oc = Consumer({
        'bootstrap.servers': args["kafka"],
        'group.id': str(uuid.uuid4()),
        'auto.offset.reset': 'earliest',
        'api.version.request': True,
        'max.poll.interval.ms': 60000
    })

    oc.subscribe([args["output_topic"]])
    oc.consume(timeout=20, num_messages=1)
    consume_output_topic(oc, join_table)

    print("sorting")
    sortedTimes = sorted(join_table.values(), key=lambda pair: pair[0])
    for triplet in sortedTimes:
        print("{}\t{}\t{}".format(triplet[0], triplet[1], triplet[2]))

def parse_args():
    parser = argparse.ArgumentParser(description='SFaaS CLI')
    parser.add_argument("-k", "--kafka", help="The location of kafka. A comma separated list of ip:port pairs.")
    parser.add_argument("-i", "--input-topic", help="The input topic")
    parser.add_argument("-o", "--output-topic", help="The output topic")
    parser.add_argument("-r", "--resolution", help="The number of messages to skip between samples + 1. Completes the sentence: 1 in every X records.", default=1000)
    parser.add_argument("-p", "--num-partitions", help="The number of partitions", default=1)
    args = parser.parse_args()
    args = vars(args)
    return args

def main():
    args = parse_args()
    join(args)

if __name__ == "__main__":
    main()
