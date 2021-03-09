import random
import sys
import time

import pika

queues_list = ["queue1", "queue2", "queue3"]
# queues_list = ["queue2"]

for queue in queues_list:
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))

    channel = connection.channel()
    channel.queue_declare(queue, durable=True)

    for i in range(500):
        message = 'Hello World..'
        # channel.basic_publish(exchange='', routing_key='task_queue', body=message,
        #                       properties=pika.BasicProperties(delivery_mode=2))

        channel.basic_publish(exchange='', routing_key=queue, body=message,
                              properties=pika.BasicProperties(delivery_mode=2))
        # time.sleep(random.randint(0, 3))
        print(" [x] Sent %r" % (message,))
    print('Done')
    connection.close()

