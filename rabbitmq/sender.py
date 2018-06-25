import sys

import pika


connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.exchange_declare(exchange='my_exchange', exchange_type='direct')
channel.exchange_declare(exchange='dead_letter', exchange_type='direct')
channel.queue_declare(queue='my_queue', arguments={"x-max-length": 5,
                                                   "x-overflow": "drop-head",
                                                   "x-dead-letter-exchange": "dead_letter"})
message = ' '.join(sys.argv[1:]) or "Hello World!"
channel.basic_publish(exchange='my_exchange',
                      routing_key='',
                      body=message)
print(" [x] Sent %r" % message)
connection.close()