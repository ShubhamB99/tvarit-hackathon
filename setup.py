from confluent_kafka import Consumer


c = Consumer({
    'bootstrap.servers': 'ec2-99-79-7-20.ca-central-1.compute.amazonaws.com:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['packetbeats-topic'])

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: {}'.format(msg.value().decode('utf-8')))

c.close()



# import threading
# import logging
# import time
# from json import loads
# from kafka import KafkaConsumer, KafkaProducer

# # from kafka import KafkaConsumer
# # from pymongo import MongoClient
# # from json import loads

# consumer = KafkaConsumer(
#     'packetbeats-topic',
#      bootstrap_servers=['ec2-99-79-7-20.ca-central-1.compute.amazonaws.com:9092'],
#      auto_offset_reset='earliest',
#      enable_auto_commit=True,
#      group_id='my-group',
#      value_deserializer=lambda x: loads(x.decode('utf-8')))

# # client = MongoClient('localhost:27017')
# # collection = client.numtest.numtest

# for message in consumer:
#     message = message.value
#     # collection.insert_one(message)
#     print(message, '\n')
#     # print('{} added to {}'.format(message, collection))
