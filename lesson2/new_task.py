import sys
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare('queue3', durable=True)

for i in range(300):
    message = ' '.join(sys.argv[1:]) or 'Hello World..'
    # channel.basic_publish(exchange='', routing_key='task_queue', body=message,
    #                       properties=pika.BasicProperties(delivery_mode=2))

    channel.basic_publish(exchange='', routing_key='queue3', body=message,
                          properties=pika.BasicProperties(delivery_mode=2))

    print(" [x] Sent %r" % (message,))
print('Done')
connection.close()
