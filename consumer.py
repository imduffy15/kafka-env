#!/usr/bin/env python
import logging
import threading
import time
import sys

from confluent_kafka import Consumer as KafkaConsumer
from confluent_kafka import KafkaError, KafkaException

topics = ['a', 'b', 'c', 'd']


class Consumer(threading.Thread):
    daemon = True

    def run(self):
        consumer = KafkaConsumer({'group.id': 'example-consumer',
                                  'bootstrap.servers': 'kafka:9092',
                                  'auto.offset.reset': 'earliest',
                                  'plugin.library.paths': 'monitoring-interceptor'})
        consumer.subscribe(topics)

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                # Error or event
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    # Error
                    raise KafkaException(msg.error())
            else:
                # Proper message
                sys.stderr.write('%% %s [%d] at offset %d with value %s:\n' %
                                 (msg.topic(), msg.partition(), msg.offset(),
                                  str(msg.value())))
                time.sleep(0.01)


def main():
    threads = [Consumer()]

    for t in threads:
        t.start()

    t.join()


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO)
    main()
