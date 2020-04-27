from kafka import KafkaConsumer
from datetime import datetime
from elasticsearch import Elasticsearch
import time
import json

es = Elasticsearch([{'host': '54.187.19.224', 'port': 9200}])

# To consume messages
consumer = KafkaConsumer('packetbeats-topic', group_id="teamb",
                          auto_commit_enable=True,
                          auto_commit_interval_ms=30 * 1000,
                          auto_offset_reset='smallest',
                          bootstrap_servers=['ec2-99-79-7-20.ca-central-1.compute.amazonaws.com:9092'])
esid = 0

for message in consumer:
    print("next")
    esid += 1
    if esid % 1000 == 0:
      print(esid)

    msg={}
    schema = json.loads(message.value)
    # for (k,v) in schema.items():
    #     if (k=='@timestamp')
    #         ts = v
    # msg['timestamp'] = ts
    msg["index"] = "team-b"
    msg["schema"] = schema

    if not 'index' in msg:
      print("you must specify the index name in the json wrapper")
      sys.exit(-1)

    index = msg['index']

    try:
        es.index(index=index, doc_type= msg['doc_type'], id=esid, body=msg['body'])
    except:
        continue

    