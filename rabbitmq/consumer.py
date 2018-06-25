import time

import pika


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)
    time.sleep(body.count(b'.'))
    print(" [x] Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)


connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.exchange_declare(exchange='my_exchange', exchange_type='direct')
channel.queue_declare(queue='my_queue', arguments={"x-max-length": 5,
                                                   "x-overflow": "drop-head",
                                                   "x-dead-letter-exchange": "my_exchange"})
channel.basic_consume(callback,
                      queue='my_queue')
print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
