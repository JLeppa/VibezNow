#!/usr/bin/env python
import sys
import simplejson
import requests
import time
from requests_oauthlib import OAuth1
from kafka import KafkaProducer

def main():
    """ Connect to twitter stream and send all messages from US to kafka topic  """

    # Validate inline arguments
    if len(sys.argv) != 1 and len(sys.argv) != 4:
        print "Usage:"
        print "For real-time stream: ./twitter_stream"
        print "For reading from file: ./twitter_stream file_name num_file_loops wait_time_between_messages"
        quit()

    # Set up Kafka producer
    ipfile = open('ip_addresses.txt', 'r')
    ips = ipfile.read()[:-1]
    ipfile.close()
    ips = ips.split(', ')
    
    producer = KafkaProducer(bootstrap_servers=ips)

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
                # Produce all messages to Kafka topic "twitter_stream_new"
                producer.send('twitter_stream_new', tweet)
                msg_cnt += 1
                if msg_cnt % 100 == 0:
                    print msg_cnt
                #wait = raw_input("PRESS ENTER TO CONTINUE.")
    
    # Read tweets from a file and produce them
    if len(sys.argv) == 4:
        # Set how many times file is looped over
        for x in range(int(sys.argv[2])):
            with open(sys.argv[1]) as fh:
                msg_cnt = 0
                for line in fh:
                    tweet = line
                    # Produce all messages to Kafka topic "twitter_stream_new"
                    producer.send('twitter_stream_new', tweet)
                    msg_cnt += 1
                    if msg_cnt % 1000 == 0:
                        print msg_cnt
                    #wait = raw_input("PRESS ENTER TO CONTINUE.")
                    time.sleep(float(sys.argv[3]))

if __name__ == "__main__":
    main()
