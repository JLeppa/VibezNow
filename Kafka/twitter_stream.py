#!/usr/bin/env python
import sys
import simplejson
import requests
import time
from requests_oauthlib import OAuth1
#from kafka.client import SimpleClient
#from kafka.producer import KeyedProducer
from kafka import KafkaProducer

def main():
    """ Connect to twitter stream and send messages to kafka topics with one topic for all messages
    and six separate topics for messages containing given word """

    # Validate inline arguments
    if len(sys.argv) != 1 and len(sys.argv) != 3:
        print "Usage:"
        print "For real-time stream: ./twitter_stream"
        print "For reading from file: ./twitter_stream file_name messages_per_second"
        quit()

    # Set up Kafka producer
    ipfile = open('ip_addresses.txt', 'r')
    ips = ipfile.read()[:-1]
    ipfile.close()
    ips = ips.split(', ')
    
    producer_all = KafkaProducer(bootstrap_servers=ips)
    producer_topic = KafkaProducer(bootstrap_servers=ips)
    topic_words = {'food':'twitter_word1', 
                   'love':'twitter_word2',
                   'relax':'twitter_word3',
                   'shop':'twitter_word4',
                   'sport':'twitter_word5', 
                   'travel':'twitter_word6'}

    # Real-time stream from twitter api:
    if len(sys.argv) == 1:
        # Read authetication needed for Twitter api
        with open("twitter_secrets.json.nogit") as fh:
            secrets = simplejson.loads(fh.read())
        auth = OAuth1(
            secrets["api_key"],
            secrets["api_secret"],
            secrets["access_token"],
            secrets["access_token_secret"]
        )

        # Connect to twitter stream limited to US
        US_BOUNDING_BOX = "-125.00,24.94, -66.93,49.59"
        stream = requests.post('https://stream.twitter.com/1.1/statuses/filter.json',
                               auth=auth, stream=True,
                               data={"locations" : US_BOUNDING_BOX})
        
        # Read messages from twitter api and produce them to topics
        msg_cnt = 0
        for tweet in stream.iter_lines():
            if not tweet:
                continue
            else:
                # Produce all messages to Kafka topic "twitter_stream"
                producer_all.send('twitter_stream', tweet)
                # Produce messages that include word from the topic list to separate Kafka topics
                json_tweet = simplejson.loads(tweet)
                if 'text' in json_tweet:
                    message = json_tweet['text'].encode("utf-8","replace")
                    for word in topic_words:
                        if word in message:
                            print message
                            print topic_words[word]
                            producer_topic.send(topic_words[word], tweet)
                msg_cnt += 1
                if msg_cnt % 100 == 0:
                    print msg_cnt
    
    # Read tweets from a file and produce them
    if len(sys.argv) == 3:
        for x in range(30):
            with open(sys.argv[1]) as fh:
                msg_cnt = 0
                for line in fh:
                #print line
                    tweet = line
                    # Produce all messages to Kafka topic "twitter_stream"
                    #producer_all.send('twitter_stream', tweet)
                # Produce messages that include word from the topic list to separate Kafka topics
                #json_tweet = simplejson.loads(tweet)
                #if 'text' in json_tweet:
                #    message = json_tweet['text'].encode("utf-8","replace")
                #    for word in topic_words:
                #        if word in message:
                #            #print message
                #            #print topic_words[word]
                #            producer_topic.send(topic_words[word], tweet)
                #msg_cnt += 1
                #if msg_cnt % 100 == 0:
                    #print msg_cnt
                #wait = raw_input("PRESS ENTER TO CONTINUE.")
                #quit()
            #print msg_cnt

if __name__ == "__main__":
    main()

"""
class Producer(object):

    def __init__(self, addr):
        self.client = SimpleClient(addr)
        self.producer = KeyedProducer(self.client)

    def produce_msgs(self, src_partition):
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
"""
