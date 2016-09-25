# Import modules:
from __future__ import print_function

import sys
import re
import redis
#import json
import simplejson as json
#import riak
#import pyspark_riak
from stemming.porter2 import stem
from math import log

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.mllib.linalg import SparseVector 


# Define auxiliary functions
def sparse_norm(sparse_vector):
    vector_list = sparse_vector.collect()
    sparse_vector = vector_list[0]
    vector_norm = sparse_vector.norm(2)
    return sc.parallelize([vector_norm])

def tuples_into_sparse(ind_rdd):
    # Input is RDD with (word_index, ntf_idf) tuples, output is a sparse vector of length 4681
    ind_rdd = ind_rdd.sortByKey()
    tuple_list = ind_rdd.collect()
    #print(tuple_list)
    indeces = []
    values = []
    for item in tuple_list:
        indeces.append(item[0])
        values.append(item[1])
    #print(indeces)
    #print(values)
    tweet_vector = SparseVector(4681, indeces, values)
    #print(tweet_vector)
    return sc.parallelize([tweet_vector])

def normalize_tf(tf_rdd):
    # Input is an RDD containing list of tuples in (term, term_freq) format
    # Output is an RDD containing the same list as (term, a+(1-a)*term_freq/max(term_freq))
    
    nf = 0.4 # Normalization factor used when calculating normalized term frequency
    # Search for the max term frequency, max_tf, from all term frequencies, tf
    freq = tf_rdd.map(lambda x: x[1])
    max_tf = freq.max()
    #max_tf = max_tf.collect()[0]
    #max_tf = tf.reduce(lambda x, y: max(x, y))
    ntf_rdd = tf_rdd.map(lambda x: (x[0], nf + (1-nf)*x[1]/max_tf))
    return ntf_rdd
    #ntf_idf_rdd = ntf_rdd.map(lambda x: (x[0], x[1]*log(query_count/(1+int(query_freq[x[0]])))))
    #return ntf_idf_rdd

def raw_stream_to_words(input_stream):
    # Tweet in json string as a second object of input tuple
    raw_stream = input_stream.map(lambda x: json.loads(x[1]))

    # Filter out messages with no text field
    raw_stream = raw_stream.filter(lambda x: 'text' in x)

    # Pick only "text" field and remove non-ascii characters
    tweet = raw_stream.map(lambda x: x['text'].encode("utf-8","replace"))
    
    # Remove links
    tweet = tweet.map(lambda x: re.sub(r'http\S+', "", x))

    # Split hashtags at camel case (OMITTED)

    # Remove characters other than letters, ' and -
    tweet = tweet.map(lambda x: re.sub(r'[^a-zA-Z\'\-\s]', "", x))

    # Split the lines into words
    tweet = tweet.flatMap(lambda x: x.split(" "))

    # Remove empty strings
    tweet = tweet.filter(lambda x: len(x) > 0)

    # Lower case the words
    tweet = tweet.map(lambda x: x.lower())

    # Stem the words
    tweet = tweet.map(lambda x: stem(x))

    # Return the words
    return tweet


if __name__ == "__main__":
    
    if len(sys.argv) != 3:
        print("Usage: kafka_spark_stream.py <zk> <topic>")
        exit(-1)

    # Open strict connection to redis data store
    red = redis.StrictRedis(host='172.31.0.231', port=6379, db=0,
                            password='tamaonsalasanaredikselle')
    # Get keys to the lyrics vectors as a list
    lyrics_keys_small = red.get('get_keys_small')
    # Get track info in a dictionary, track_id is key, tuple of artist and song as value
    track_info = red.hgetall("track_info_key")
    # Get the list of 4681 words in the order of ntf-idf vectors of lyrics
    lyric_words = red.get('words_key')
    # Get dictionary to connect words into their indeces in the bow vector
    word_index = red.hgetall('word_indeces_key')
    # Get the non.ordered set of words
    word_set = red.get('word_set_key')
    word_set = eval(word_set)
    # Get vector of how many times each word has been present in previous queries
    query_freq = red.hgetall('hist_freq_key')
    # Get the total number of previous queries
    query_count = red.get('hist_count_key')
    query_count = int(query_count)
    # Get the lyrics of 237642 songs as sparse bag of words
    #  key=track_id, value=(indeces to words, ntf-idf of words, norm of lyrics vector)
    lyrics_sparse = red.hgetall('ntf_idf_lyrics_key')

    # Set the Spark context (connection to spark cluster, make RDDs)
    sc = SparkContext(appName="KafkaTwitterStream")
    
    # Set Spark streaming context (connection to spark cluster, make Dstreams)
    batch_duration = 20  # Batch duration (s)
    ssc = StreamingContext(sc, batch_duration)

    # Test putting some data into Riak
    riak_test = 0
    if riak_test:
        # Patch SparkContext instance to enable Riak APIs
        pyspark_riak.riak_context(sc)

        # Test putting some data into Riak
        test_data = [{"key1":{"t_key":"t_val1"}}, {"key2":{"t_key":"t_val2"}}]
        test_rdd = sc.parallelize(test_data, 1)
        test_rdd.saveToRiak("test-python-bucket", "default")
        # Test reading from the bucket
        rdd = sc.riakBucket("test-python-bucket", "default").queryAll()
        test_data = rdd.collect()
        values = map(lambda x: x[1], test_data)
        for e in values:
            print(e)

    # Set the Kafka topic
    #zkQuorum, topic = sys.argv[1:]  # hostname and Kafka topic
    zkQuorum = "localhost::2181"
    topic = "twitter_test"

    # List the Kafka Brokers
    kafkaBrokers = {"metadata.broker.list": "ec2-52-27-232-130.us-west-2.compute.amazonaws.com:9092, ec2-54-70-124-67.us-west-2.compute.amazonaws.com:9092, ec2-54-70-110-215.us-west-2.compute.amazonaws.com:9092, ec2-54-70-81-69.us-west-2.compute.amazonaws.com:9092"}

    # Create input stream that pulls messages from Kafka Brokers
    # kvs is a DStream object
    kvs = KafkaUtils.createDirectStream(ssc, [topic], kafkaBrokers)
    #kvs.pprint()
    words = raw_stream_to_words(kvs)
    words.pprint()

    # Filter words not in the set of words used for bag-of-words
    words = words.filter(lambda x: x in word_set)
    words.pprint()

    # Count the number of words in the batch of tweets
    pairs = words.map(lambda word: (word, 1))
    word_count = pairs.reduceByKey(lambda x, y: x + y)
    word_count.pprint()

    # Search for the max term frequency, max_tf, from all term frequencies, tf
    tf = word_count.map(lambda x: x[1])
    max_tf = tf.reduce(lambda x, y: max(x, y))
    tf.pprint()
    max_tf.pprint()

    # Update historical query frequency and count
    query_count += 1
    # query_freq OMITTED

    # Normalize the word counts
    ntf = word_count.transform(normalize_tf)
    ntf.pprint()
    

    # Pick up 10 words with highest ntf-idf values
    # OMITTED FOR NOW

    # Transform the (word, weight) tuples into a sparse vector
    ntf_ind = ntf.map(lambda x: (int(word_index[x[0]]), x[1]))
    ntf_ind.pprint()
    tweet_vector = ntf_ind.transform(tuples_into_sparse)
    tweet_vector.pprint()

    # Calculate the norm of the tweet_vector
    tweet_norm = tweet_vector.transform(sparse_norm)
    tweet_norm.pprint()
    

    ssc.start()
    ssc.awaitTermination()
