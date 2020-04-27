from confluent_kafka import Consumer
from datetime import datetime
from elasticsearch import Elasticsearch
import time
import json
import sys

es = Elasticsearch([{'host': '54.187.19.224', 'port': 9200}])

# To consume messages
c = Consumer({
    'bootstrap.servers': 'ec2-99-79-7-20.ca-central-1.compute.amazonaws.com:9092',
    'group.id': 'mygroup',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
})

c.subscribe(['packetbeats-topic'])
while True:
    esid += 1
    if esid % 1000 == 0:
        print(esid)

    msg={}
    message = c.poll(timeout=1)
    schema = json.loads(message.value)
    msg["index"] = "team-b"
    schema = json.dumps(schema, indent=1)
    msg["schema"] = schema
    print(schema)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue
    if not 'index' in msg:
        print("you must specify the index name in the json wrapper")
        sys.exit(-1)

    index = msg['index']
    try:
        es.index(index=index, id=esid, body=msg['schema'])
    except:
        continue

c.close()
