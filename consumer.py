#!/usr/bin/env python
import logging
import threading
import time

from kafka import KafkaConsumer

topics = ['a', 'b', 'c', 'd']


class Consumer(threading.Thread):
    daemon = True

    def run(self):
        consumer = KafkaConsumer(group_id='example-consumer',
                                 bootstrap_servers='kafka:9092',
                                 auto_offset_reset='earliest')
        consumer.subscribe(topics)

        for message in consumer:
            print(message)
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
