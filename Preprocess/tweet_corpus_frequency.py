import simplejson as json
import re
import redis
from stemming.porter2 import stem

""" Calculate how many times each word of interest appears in a batch of
10000 tweets (corpus frequency) and count the number of batches (corpus
count) in a set of approximately million tweets. """

with open("redis_pw.json.nogit") as fh:
        redis_info = json.loads(fh.read())

# Open connection to Redis data base
red = redis.StrictRedis(host=redis_info["host"], port=redis_info["port"], db=0,
                        password=redis_info["password"])

# Get dictionary to connect words into their indeces in the bow vector
word_index = red.hgetall('word_indeces_key')

# Get the non-ordered set of words
word_set = red.get('word_set_key')
word_set = eval(word_set)

# Make list of 4681 zeros to count corpus frequency
corpus_count = 0
corpus_frequency = []
for a in range(len(word_set)):
    corpus_frequency.append(0)

# Keep track of words already in current batch
batch_word_set = set()

# Go through tweets and calculate appearance frequency
with open("/mnt/data_store/tweets4.txt") as fh:
    msg_cnt = 0
    for line in fh:
        msg_cnt += 1
        tweet = json.loads(line)
        if 'text' in tweet:
            text = tweet['text']
            text = text.encode("utf-8","replace")
            text = re.sub(r'http\S+', "", text)
            text = re.sub(r'[^a-zA-Z\'\-\s]', "", text)
            text = text.split(" ")
            for item in text:
                word = item.lower()
                word = stem(word)
                if word in word_set and word not in batch_word_set:
                    batch_word_set.add(word)
        if msg_cnt % 10000 == 0:
            # Increase corpus frequency for each word in batch 
            for word in batch_word_set:
                index = word_index[word]
                corpus_frequency[int(index)] += 1
            corpus_count += 1
            # Reset word batch
            batch_word_set = set()
            print msg_cnt, corpus_count #Track progress
            #wait = raw_input("PRESS ENTER TO CONTINUE.")

# Write the final count and frequency into Redis database
red.set('corpus_count_key', corpus_count)
red.set('corpus_frequency_key', corpus_frequency)
