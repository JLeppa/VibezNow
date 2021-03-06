# Import modules:
from __future__ import print_function

import sys
import re
import redis
import simplejson as json
from stemming.porter2 import stem
from math import log
from operator import itemgetter

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.mllib.linalg import SparseVector


def raw_stream_to_text(input_stream):
    """ Map incoming raw kafka stream into text fields of tweets. """

    # Tweet in json string as a second object of input tuple
    raw_stream = input_stream.map(lambda x: json.loads(x[1]))

    # Filter out messages with no text field
    raw_stream = raw_stream.filter(lambda x: 'text' in x)

    # Pick only "text" field and remove non-ascii characters
    tweet_stream = raw_stream.map(lambda x: x['text'].encode("utf-8","replace"))

    return tweet_stream

def messages_to_words(tweets):
    """ Map incoming stream of messages into collection of words. """

    # Remove links
    tweets = tweets.map(lambda x: re.sub(r'http\S+', "", x))

    # Remove characters other than letters, ' and -
    tweets = tweets.map(lambda x: re.sub(r'[^a-zA-Z\'\-\s]', "", x))

    # Split the lines into words
    words = tweets.flatMap(lambda x: x.split(" "))

    # Remove empty strings
    words = words.filter(lambda x: len(x) > 0)

    # Lower case the words
    words = words.map(lambda x: x.lower())

    # Stem the words
    words = words.map(lambda x: stem(x))

    # Filter words not in the set of words used for bag-of-words
    words = words.filter(lambda x: x in word_set_bc.value)

    # Count the number of words
    words = words.map(lambda word: (word, 1))
    word_count = words.reduceByKey(lambda x, y: x + y)

    # Return the words and their counts
    return word_count

def word_count_to_ntf(tf_rdd):
    """ Input is an RDD containing list of tuples in (term, term_freq) format
    Output is an RDD containing the same list as (term, a+(1-a)*term_freq/max(term_freq))"""

    nf = 0.4 # Normalization factor used when calculating normalized term frequency
    #query_idfs = query_idf_bc.value (OMITTED)

    # Search for the max term frequency, max_tf, from all term frequencies, tf
    freq = tf_rdd.map(lambda x: x[1])
    max_tf = freq.max()

    # Calculate the ntf
    ntf_rdd = tf_rdd.map(lambda x: (x[0], nf + (1-nf)*x[1]/max_tf))
    return ntf_rdd
    
    # Calculate the ntf-idf (OMITTED)
    #ntf_idf_rdd = ntf_rdd.map(lambda x: (x[0], x[1]*query_idfs[x[0]]))
    #return ntf_idf_rdd

def take_top_words(rdd):
    """ Take top n words with largest weights.
    Input format: (word, word_tf) ; Output format: (unstemmed word, word_tf). """
    n = 10
    weight_values = rdd.map(lambda x: x[1])
    top_n_val = weight_values.takeOrdered(n, key=lambda x: -x)
    top_n = rdd.filter(lambda x: x[1] in top_n_val)
    top_words = top_n.map(lambda x: (unstem_bc.value[x[0]], x[1]))
    return top_words

def process_ntf_to_suggestions(ntf):
    # Map the (word, weight) tuples into (word_index, weight) tuples
    word_index = word_index_bc.value
    ntf_ind = ntf.map(lambda x: (int(word_index[x[0]]), x[1]))

    # Convert tuples into sparse vector
    tweet_sparse_norm = ntf_ind.transform(tuples_to_sparse)

    # Calculate the cosine similarity, return (track_id, similarity) tuples
    #  and get top ten suggestions
    #lyrics_similarity = tweet_sparse_norm.flatMap(lambda x: loop_lyrics_ordered(x))
    lyrics_similarity = tweet_sparse_norm.flatMap(lambda x: loop_lyrics(x))
    lyrics_similarity = lyrics_similarity.transform(take_top)

    # Get the lyric info of the top n songs ((artist, song), similarity)
    track_info = track_info_bc.value
    top_songs = lyrics_similarity.map(lambda x: (track_info[x[0]], x[1]))
    return top_songs

def tuples_to_sparse(ind_rdd):
    """ Transform input RDD with (word_index, ntf_idf) tuples into sparse vector of length 4681. """

    ind_rdd = ind_rdd.sortByKey() # Sort tuples based on the word index for sparse vector format
    ind_rdd_t = ind_rdd.map(lambda x: (4681, x))
    
    # Sparse vector, (4681, [word_ind], [word_weight]), into mllib.SparseVector
    sparse_vec = ind_rdd_t.combineByKey(lambda value: ([value[0]], [value[1]]),
                                        simple_value_merger,
                                        sorting_merger_combiner)
    tweet_vector = sparse_vec.map(lambda x: SparseVector(x[0], x[1][0], x[1][1]))
    tweet_vector_norm = tweet_vector.map(lambda x: (x, x.norm(2)))

    return tweet_vector_norm

def simple_value_merger(lists_in, val):
    """ Input is a tuple of lists and tuple of values that are added to the lists. """
    lists_in[0].append(val[0])
    lists_in[1].append(val[1])
    return lists_in

def sorting_value_merger(lists_in, val):
    """ Input is a tuple of lists and tuple of values that are added to the lists maintaining the order
    sorted based on the values of the first list. """
    if lists_in[0][-1] < val[0]:
        lists_in[0].append(val[0])
        lists_in[1].append(val[1])
    else:
        for x in range(len(lists_in[0])):
            if lists_in[0][x] > val[0]:
                lists_in[0].insert(x, val[0])
                lists_in[1].insert(x, val[1])
    return lists_in

def sorting_merger_combiner(list1, list2):
    """ Inputs are tuples of lists with sorted indeces in the first list. """
    if list1[0][0] < list2[1][0]:
        list_both = list1 + list2
    else:
        list_both = list2 + list1
    return list_both

def loop_lyrics(tweet_vector_norm):
    """ Loop through the lyrics to calculate cosine similarity against each of them.
    Return tuples (track_id, cosine_similarity_value). """
    lyrics = lyrics_bc.value
    top_list = []
    for track in lyrics:
        lyrics_vec_norm = lyrics[track]
        cosine_sim = (track, tweet_vector_norm[0].dot(lyrics_vec_norm[0])/lyrics_vec_norm[1]/tweet_vector_norm[1])
        top_list.append(cosine_sim)
    return top_list

def loop_lyrics_ordered(tweet_vector_norm):
    """ Loop through the lyrics to calculate cosine similarity against each of them.
    Return tuples (track_id, cosine_similarity_value) of top n similarity. """
    lyrics = lyrics_bc.value
    n = 10
    top_list = []
    counter = 0
    for track in lyrics:
        counter += 1
        lyrics_vec_norm = lyrics[track]
        cosine_sim = (track, tweet_vector_norm[0].dot(lyrics_vec_norm[0])/lyrics_vec_norm[1]/tweet_vector_norm[1])
        if counter <= n:
            top_list.append(cosine_sim)
            if counter == n:
                top_list = sorted(top_list, key=lambda tup: tup[1], reverse=True)
                cs_min = top_list[-1][1]
        else:
            if cosine_sim[1] > cs_min:
                top_list.pop
                top_list.append(cosine_sim)
                top_list = sorted(top_list, key=lambda tup: tup[1], reverse=True)
                cs_min = top_list[-1][1]
    return top_list

def take_top(rdd):
    # input format (track_id, cos_similarity)
    cos_values = rdd.map(lambda x: x[1])
    n = 10
    top_n_val = cos_values.takeOrdered(n, key=lambda x: -x)
    top_n = rdd.filter(lambda x: x[1] in top_n_val)
    return top_n

def songs_to_redis(rdd):
    top_to_redis = rdd.collect()
    if top_to_redis:
        red.set('top_songs_key', top_to_redis)

def songs_to_redis_w1(rdd):
    top_to_redis = rdd.collect()
    if top_to_redis:
        red.set('top_songs_w1_key', top_to_redis)

def songs_to_redis_w2(rdd):
    top_to_redis = rdd.collect()
    if top_to_redis:
        red.set('top_songs_w2_key', top_to_redis)

def songs_to_redis_w3(rdd):
    top_to_redis = rdd.collect()
    if top_to_redis:
        red.set('top_songs_w3_key', top_to_redis)

def songs_to_redis_w4(rdd):
    top_to_redis = rdd.collect()
    if top_to_redis:
        red.set('top_songs_w4_key', top_to_redis)

def words_to_redis(rdd):
    top_to_redis = rdd.collect()
    red.set('top_words_key', top_to_redis)

def words_to_redis_w1(rdd):
    top_to_redis = rdd.collect()
    red.set('top_words_w1_key', top_to_redis)

def words_to_redis_w2(rdd):
    top_to_redis = rdd.collect()
    red.set('top_words_w2_key', top_to_redis)

def words_to_redis_w3(rdd):
    top_to_redis = rdd.collect()
    red.set('top_words_w3_key', top_to_redis)

def words_to_redis_w4(rdd):
    top_to_redis = rdd.collect()
    red.set('top_words_w4_key', top_to_redis)


if __name__ == "__main__":
    """ Get streams of twitter messages from Kafka topics,
    map them into sparse bag-of-words vectors, apply ntf-idf normalization,
    calculate cosinine similarity against all lyrics in data set,
    write words with highest weights and suggested songs into Redis db."""

    # Get the password and connection info for Redis into redis_info
    with open("redis_pw.json.nogit") as fh:
        redis_info = json.loads(fh.read())

    # Open connection to Redis data base
    red = redis.StrictRedis(host=redis_info["host"], port=redis_info["port"], db=0,
                            password=redis_info["password"])

    # Get track info in a dictionary, track_id is key, tuple of artist and song as value
    track_info = red.hgetall("track_info_key")

    # Get dictionary with stemmed word as key and unstemmed as value
    unstem = red.hgetall('unstem_key')

    # Get dictionary to connect words into their indeces in the bow vector
    word_index = red.hgetall('word_indeces_key')

    # Get the non-ordered set of words
    word_set = red.get('word_set_key')
    word_set = eval(word_set)

    # Get the list of 4681 words in the order of ntf-idf vectors of lyrics (OMITTED)
    #lyric_words = red.get('words_key')
    #lyric_words = eval(lyric_words)

    # Get vector of how many times each word has been present in previous queries (OMITTED)
    #query_freq = red.get('corpus_frequency_key')
    #query_freq = eval(query_freq)

    # Get the total number of previous queries (OMITTED)
    #query_count = red.get('corpus_count_key')
    #query_count = float(query_count)

    # Calculate query idf values and put them in dictionary using word as key (OMITTED)
    #query_idf = {}
    #idf_index = 0
    #for value in query_freq:
    #    idf_value = log(1+query_count/(1+value))
    #    idf_word = lyric_words[idf_index]
    #    query_idf[idf_word] = max(idf_value, 0)
    #    idf_index += 1

    # Get the lyrics of 237642 songs as sparse bag of words
    #  key=track_id, value=(indeces to words, ntf-idf of words, norm of lyrics vector)
    lyrics_vec_dict = red.hgetall('ntf_idf_lyrics_key')
    lyrics_dict = {}
    line_limit = 237642
    counter = 0
    for key in lyrics_vec_dict:
        # track_id: ([indeces], [tf-idf], vec_norm)
        aux_tuple = eval(lyrics_vec_dict[key])
        lyrics_vec = SparseVector(4681, aux_tuple[0], aux_tuple[1])
        lyrics_dict[key] = (lyrics_vec, aux_tuple[2])
        counter += 1
        if counter >= line_limit:
            break

    # Set the Spark context (connection to spark cluster, make RDDs)
    sc = SparkContext(appName="TwitterStreaming")
    
    # Set Spark streaming context (connection to spark cluster, make Dstreams)
    batch_duration = 240  # Batch duration (s)
    ssc = StreamingContext(sc, batch_duration)

    # Broadcast word_set, unstemming dictionary, lyrics and query_idf to nodes
    word_set_bc = sc.broadcast(word_set)
    unstem_bc = sc.broadcast(unstem)
    lyrics_bc = sc.broadcast(lyrics_dict)
    word_index_bc = sc.broadcast(word_index)
    track_info_bc = sc.broadcast(track_info)
    #query_idf_bc = sc.broadcast(query_idf) (OMITTED)

    # Set the Kafka topic
    topic = "twitter_stream_new"

    # List the Kafka Brokers
    broker_file = open('kafka_brokers.txt', 'r')
    kafka_brokers = broker_file.read()[:-1]
    broker_file.close()
    kafkaBrokers = {"metadata.broker.list": kafka_brokers}

    # Create input stream that pull messages from Kafka Brokers (DStream object)
    tweets_raw = KafkaUtils.createDirectStream(ssc, [topic], kafkaBrokers)

    # Extract tweet text fields from the raw stream
    tweets = raw_stream_to_text(tweets_raw)

    # Filter from stream messages that contain theme word
    theme_words = ['food', 'love', 'shop', 'sport']
    tweets_w1 = tweets.filter(lambda x: theme_words[0] in x)
    tweets_w2 = tweets.filter(lambda x: theme_words[1] in x)
    tweets_w3 = tweets.filter(lambda x: theme_words[2] in x)
    tweets_w4 = tweets.filter(lambda x: theme_words[3] in x)

    # Map tweet text fields to collections of words and their counts
    word_count = messages_to_words(tweets)
    word_count_w1 = messages_to_words(tweets_w1)
    word_count_w2 = messages_to_words(tweets_w2)
    word_count_w3 = messages_to_words(tweets_w3)
    word_count_w4 = messages_to_words(tweets_w4)

    # Calculate normalized term frequency, ntf
    ntf = word_count.transform(word_count_to_ntf)
    ntf_w1 = word_count_w1.transform(word_count_to_ntf)
    ntf_w2 = word_count_w2.transform(word_count_to_ntf)
    ntf_w3 = word_count_w3.transform(word_count_to_ntf)
    ntf_w4 = word_count_w4.transform(word_count_to_ntf)

    # Pick up n words with highest ntf-idf values (word, tf)
    top_words = ntf.transform(take_top_words)
    top_words_w1 = ntf_w1.transform(take_top_words)
    top_words_w2 = ntf_w2.transform(take_top_words)
    top_words_w3 = ntf_w3.transform(take_top_words)
    top_words_w4 = ntf_w4.transform(take_top_words)

    # Write top words into Redis
    top_words.foreachRDD(words_to_redis)
    top_words_w1.foreachRDD(words_to_redis_w1)
    top_words_w2.foreachRDD(words_to_redis_w2)
    top_words_w3.foreachRDD(words_to_redis_w3)
    top_words_w4.foreachRDD(words_to_redis_w4)

    # Process ntf into top n suggestions (artist, song title)
    top_songs = process_ntf_to_suggestions(ntf)
    top_songs_w1 = process_ntf_to_suggestions(ntf_w1)
    top_songs_w2 = process_ntf_to_suggestions(ntf_w2)
    top_songs_w3 = process_ntf_to_suggestions(ntf_w3)
    top_songs_w4 = process_ntf_to_suggestions(ntf_w4)

    # Write top songs into redis
    top_songs.foreachRDD(songs_to_redis)
    top_songs_w1.foreachRDD(songs_to_redis_w1)
    top_songs_w2.foreachRDD(songs_to_redis_w2)
    top_songs_w3.foreachRDD(songs_to_redis_w3)
    top_songs_w4.foreachRDD(songs_to_redis_w4)

    ssc.start()
    ssc.awaitTermination()
