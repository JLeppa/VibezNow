#!/usr/bin/env python
import sys
import simplejson
import requests
import time
from requests_oauthlib import OAuth1
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer

with open("twitter_secrets.json.nogit") as fh:
    secrets = simplejson.loads(fh.read())

auth = OAuth1(
    secrets["api_key"],
    secrets["api_secret"],
    secrets["access_token"],
    secrets["access_token_secret"]
)

US_BOUNDING_BOX = "-125.00,24.94, -66.93,49.59"
stream = requests.post('https://stream.twitter.com/1.1/statuses/filter.json',
                       auth=auth, stream=True,
                       data={"locations" : US_BOUNDING_BOX})

class Producer(object):

    def __init__(self, addr):
        self.client = SimpleClient(addr)
        self.producer = KeyedProducer(self.client)

    def produce_msgs(self, src_partition):
        scenario = 2 # continuous (default) ; limited testing (scenario == 2)
        msg_cnt = 0
        for tweet in stream.iter_lines():
            if not tweet:
                continue
            else:
                try:
                    self.producer.send_messages('twitter_test', src_partition, tweet)
                    msg_cnt += 1
                except ChunkedEncodingError:
                    continue
            if scenario == 2:
                if msg_cnt % 100 == 0:
                    print msg_cnt
                #time.sleep(0.05)
                #if msg_cnt >= 200000:
                #    break

if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    partition_key = str(args[2])
    prod = Producer(ip_addr)
    prod.produce_msgs(partition_key)

