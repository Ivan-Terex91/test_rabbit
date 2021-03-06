import random

import pika
import time

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare('queue3', durable=True)


def callback(ch, method, properties, body):
    print(" [x] Received %r" % (body,))
    time.sleep(random.randint(1, 4))
    print(" [x] Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)
    # print(channel.consumer_tags)


channel.basic_qos(prefetch_count=1)

# channel.basic_consume(on_message_callback=callback, queue='task_queue')
channel.basic_consume(on_message_callback=callback, queue='queue3')

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
# print(channel.consumer_tags)
