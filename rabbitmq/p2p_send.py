import sys

import pika


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    connection.close()


def send(ch):
    ch.queue_declare(queue='hello', durable=True, arguments={"x-max-length": 5, "x-overflow": "drop-head"})
    message = ' '.join(sys.argv[2:]) or "Hello World!"
    ch.basic_publish(exchange='',
                     routing_key='hello',
                     body=message,
                     properties=pika.BasicProperties(
                         delivery_mode=2,
                         expiration="100000",
                     ))
    print(" [x] Sent %r" % message)
    connection.close()


def wait_response(ch):
    ch.queue_declare(queue='listen')
    ch.basic_consume(callback,
                          queue='listen',
                          no_ack=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    ch.start_consuming()


connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

flag = sys.argv[1]

if flag == 'send':
    send(channel)

if flag == 'send_n_wait':
    send(channel)
    wait_response(channel)








