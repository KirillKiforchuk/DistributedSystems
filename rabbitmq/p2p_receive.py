import sys
import time

import pika


def simple_callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    time.sleep(body.count(b'.'))
    print(" [x] Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def modify_callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    ch.queue_declare(queue='listen')
    time.sleep(body.count(b'.'))
    message = body + b' was modified'
    ch.basic_publish(exchange='',
                     routing_key='listen',
                     body=message)
    print(" [x] Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def receive(ch, callback):
    channel.queue_declare(queue='hello', durable=True, arguments={"x-max-length": 5, "x-overflow": "drop-head"})
    ch.basic_consume(callback,
                     queue='hello')
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

flag = sys.argv[1]

if flag == 'receive':
    receive(channel, simple_callback)

if flag == 'receive_n_modify':
    receive(channel, modify_callback)







