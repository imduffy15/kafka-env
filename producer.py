#!/usr/bin/env python
import logging
import threading
import time
from random import randint
from random import uniform

from kafka import KafkaProducer

topics = {'a': 0.2, 'b': 0.1, 'c': 0.5, 'd': 0.3}


class Producer(threading.Thread):
    daemon = True

    def __init__(self, topic, sleep):
        threading.Thread.__init__(self)
        self.sleep = sleep
        self.topic = topic

    def run(self):
        producer = KafkaProducer(bootstrap_servers='kafka:9092')

        while True:
            producer.send(self.topic, str.encode(str(randint(0, 9))))
            time.sleep(uniform(0.0, self.sleep))


def main():
    threads = []

    for (topic, sleep) in topics.items():
        threads.append(Producer(topic, sleep))

    for t in threads:
        t.start()

    t.join()


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO)
    main()
