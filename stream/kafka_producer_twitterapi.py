#!/usr/bin/env python
import sys
import simplejson
import requests
from requests_oauthlib import OAuth1
from itertools import islice
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
def tweet_generator():
    stream = requests.post('https://stream.twitter.com/1.1/statuses/filter.json',
                           auth=auth,
                           stream=True,
                           data={"locations" : US_BOUNDING_BOX})

    for line in stream.iter_lines():
        if not line:
            continue
        tweet = simplejson.loads(line)
        if 'text' in tweet:
            yield tweet['text']

# Test print of 20 messages to make sure I get tweets from Twitter Api
#for tweet in islice(tweet_generator(), 20):
#    print tweet

class Producer(object):

    def __init__(self, addr):
        self.client = SimpleClient(addr)
        self.producer = KeyedProducer(self.client)

    def produce_msgs(self, source_partition):
        msg_cnt = 0
        for tweet in islice(tweet_generator(), 2):
        #for tweet in tweet_generator():
            #print tweet
            str_fmt = "{};{}"
            message_info = str_fmt.format(source_partition,
                                          tweet)
            print message_info
            self.producer.send_messages('twitter_test', source_partition, message_info)
            msg_cnt += 1
           # print msg_cnt
            


if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    partition_key = str(args[2])
    prod = Producer(ip_addr)
    prod.produce_msgs(partition_key)

