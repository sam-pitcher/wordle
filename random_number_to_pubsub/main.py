import logging
import json
from flask import abort
from google.cloud import pubsub
from random import random
import time

logger = logging.getLogger(__name__)

def index(var=None, var2=None):
  try:
    ts = time.time()
    random_number = round(random() * 1000)
    data = {
      "time": ts,
      "random_number": random_number
    }
    publisher = pubsub.PublisherClient()
    future = publisher.publish(
      f"projects/lookerplus/topics/wordle_topic",
      json.dumps(data).encode('utf-8'))
    
    print(future.result(timeout=60))
    logger.info('Data sent: %s', data)
    return '', 200

  except:
    abort(400)