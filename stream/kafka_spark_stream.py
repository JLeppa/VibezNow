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

def cosine_similarity_old(sparse_vec):
    # Input is sparse vector of tweet batch in form (4681, ([indeces], [tf]))
    #   In format (ind, ntf-idf)
    # Lyrics dictionary is in format track_id: ([indeces], [tf-idf], norm_of_vector)
    lyrics = lyrics_broadcast.value
    tracks = track_ids_broadcast.value
    top_tracks = []
    for track in tracks:
        aux_dic = {}
        aux_set = set()
        lyric_vector = lyrics[track]
        lyric_ind = lyric_vector[0]
        lyric_val = lyric_vector[1]
        lyric_norm = lyric_vector[2]
        for item in lyric_ind:
            aux_dic[lyric_ind] = lyric_val
            aux_set.add(lyric_ind)
        tweet_vec = sparse_vec.filter(lambda x: x[0] in aux_set)
        if tweet_vec.isEmpty():
            top_tracks.append((track, 0))
            print((track, 0))
        else:
            dot_prod = tweet_vec.map(lambda x: x[1]*aux_dic[x[0]])
            dot_prod = dot_prod.reduce(lambda x, y: x+y)
            similarity = dot_prod.collect()/lyric_norm
            print((track, similarity))
            top_tracks.append((track, similarity))
        
    return sc.serialize(top_tracks)

def sparse_norm(sparse_vector):
    vector_list = sparse_vector.collect()
    sparse_vector = vector_list[0]
    vector_norm = sparse_vector.norm(2)
    return sc.parallelize([vector_norm])

def tuples_into_sparse(ind_rdd):
    # Input is RDD with (word_index, ntf_idf) tuples, output is a sparse vector of length 4681
    ind_rdd_sort = ind_rdd.sortByKey() # Sort tuples based on the word index for sparse vector format
    ind_rdd_t = ind_rdd_sort.map(lambda x: (4681, x))
    sparse_vec = ind_rdd_t.combineByKey(lambda value: ([value[0]], [value[1]]),
                                        lambda x, value: (x[0]+[value[0]], x[1]+[value[1]]),
                                        lambda x, y: (x[0]+y[0], x[1]+y[1]))
    return sparse_vec

def tuples_into_sparse_old(ind_rdd):
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

def cosine_similarity(rdd):
    # The format of input rdd:
    # (1, ((4681, [tweet_indeces], [tweet_tf]), (track_id, track_norm, 4681, [track_indeces], [track_tf_idf])))
    #tweet_norm = rdd.map(lambda x: SparseVector(x[1][0][0], x[1][0][1], x[1][0][2]).norm(2)) #Norm of tweet
    cos_sim = rdd.map(lambda x: (x[1][1][0], 
                                 SparseVector(x[1][0][0], x[1][0][1], x[1][0][2])\
                                 .dot(SparseVector(x[1][1][2], x[1][1][3], x[1][1][4]))/\
                                 SparseVector(x[1][0][0], x[1][0][1], x[1][0][2]).norm(2)/x[1][1][1]))
    return cos_sim

def take_top(rdd):
    # input format (track_id, cos_similarity)
    #rdd = rdd.map(lambda x: (x[1], x[0]))
    cos_values = rdd.map(lambda x: x[1])
    n = 5
    top_n_val = cos_values.takeOrdered(n, key=lambda x: -x)
    top_n = rdd.filter(lambda x: x[1] in top_n_val)
    return top_n
    

if __name__ == "__main__":
    
    if len(sys.argv) != 3:
        print("Usage: kafka_spark_stream.py <zk> <topic>")
        exit(-1)

    # Open strict connection to redis data store
    red = redis.StrictRedis(host='172.31.0.231', port=6379, db=0,
                            password='tamaonsalasanaredikselle')
    # Get keys to the lyrics vectors as a list
    lyrics_keys_small = red.get('get_keys_small')
    lyrics_keys_small = eval(lyrics_keys_small)
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
    lyrics_vec_dict = red.hgetall('ntf_idf_lyrics_key')
    lyrics_sparse = []
    line_limit = 100
    counter = 0
    for key in lyrics_vec_dict:
        # track_id: ([indeces], [tf-idf], vec_norm)
        aux_tuple = eval(lyrics_vec_dict[key])
        # (track_id, vec_norm, 4681, [indeces], [tf-idf]))
        lyrics_tuple = (key, aux_tuple[2], 4681, aux_tuple[0], aux_tuple[1])
        lyrics_sparse.append(lyrics_tuple)
        counter += 1
        if counter >= line_limit:
            break
    

    # Set the Spark context (connection to spark cluster, make RDDs)
    sc = SparkContext(appName="KafkaTwitterStream")
    
    # Set Spark streaming context (connection to spark cluster, make Dstreams)
    batch_duration = 10  # Batch duration (s)
    ssc = StreamingContext(sc, batch_duration)

    # Broadcast lyrics to nodes
    #lyrics_broadcast = sc.broadcast(lyrics_sparse)
    lyrics_rdd = sc.parallelize(lyrics_sparse)
    track_ids_broadcast = sc.broadcast(lyrics_keys_small)

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
#    words.pprint()

    # Filter words not in the set of words used for bag-of-words
    words = words.filter(lambda x: x in word_set)
#    words.pprint()

    # Count the number of words in the batch of tweets
    pairs = words.map(lambda word: (word, 1))
    word_count = pairs.reduceByKey(lambda x, y: x + y)
#    word_count.pprint()

    # Search for the max term frequency, max_tf, from all term frequencies, tf
    tf = word_count.map(lambda x: x[1])
    max_tf = tf.reduce(lambda x, y: max(x, y))
#    tf.pprint()
#    max_tf.pprint()

    # Update historical query frequency and count
    query_count += 1
    # query_freq OMITTED

    # Normalize the word counts
    ntf = word_count.transform(normalize_tf)
#    ntf.pprint()
    

    # Pick up 10 words with highest ntf-idf values
    # OMITTED FOR NOW

    # Transform the (word, weight) tuples into a sparse vector
    ntf_ind = ntf.map(lambda x: (int(word_index[x[0]]), x[1]))
#    ntf_ind.pprint()
    tweet_vector = ntf_ind.transform(tuples_into_sparse)
    tweet_vector = tweet_vector.map(lambda x: (x[0], x[1][0], x[1][1]))
#    tweet_vector.pprint()

    # Join the tweet vector dstream with the lyrics rdd
    # (1, ((4681, [tweet_indeces], [tweet_tf]), (track_id, track_norm, 4681, [track_indeces], [track_tf_idf])))
    tweet_and_lyrics = tweet_vector.transform(lambda rdd: rdd.map(lambda x: (1,x)).join(lyrics_rdd.map(lambda x: (1,x))))
#    tweet_and_lyrics.pprint()

    # Calculate the cosine similarity and return n top matches
    cos_similarity = tweet_and_lyrics.transform(cosine_similarity)
    cos_similarity.pprint()

    # Take the top n of the matches
    cos_sim_top = cos_similarity.transform(take_top)
    #n = 10 # How many best matches are chosen and returned
    #cos_sim_top = cos_similarity.takeOrdered(n, key = lambda x: x[1])
    #cos_sim_top = cos_similarity.map(lambda x: (x[1], x[0]))
    #cos_sim_top = cos_sim_top.sortByKey(False)
    cos_sim_top.pprint()
    print(cos_sim_top)

    # Calculate cosine similarity against lyrics and return top 10
    #top_tracks = ntf_ind.transform(cosine_similarity)
    #top_tracks.pprint()

    # Calculate the norm of the tweet_vector
    #tweet_norm = tweet_vector.transform(sparse_norm)
    #tweet_norm.pprint()
    

    ssc.start()
    ssc.awaitTermination()
