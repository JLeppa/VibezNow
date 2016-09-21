import sys
import redis
#import riak
#import pyspark_riak

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    
    if len(sys.argv) != 3:
        print "Usage: kafka_spark_stream.py <zk> <topic>"
        exit(-1)

    # Open strict connection to redis data store
    red = redis.StrictRedis(host='172.31.0.231', port=6379, db=0,
                            password='tamaonsalasanaredikselle')

    # Set the Spark streaming context
    batch_duration = 1  # Batch duration (s)
    sc = SparkContext(appName="KafkaTwitterStream")
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
            print e

    # Set the Kafka topic
    #zkQuorum, topic = sys.argv[1:]  # hostname and Kafka topic
    zkQuorum = "localhost::2181"
    topic = "twitter_test"

    # List the Kafka Brokers
    kafkaBrokers = {"metadata.broker.list": "ec2-52-27-232-130.us-west-2.compute.amazonaws.com:9092, ec2-54-70-124-67.us-west-2.compute.amazonaws.com:9092, ec2-54-70-110-215.us-west-2.compute.amazonaws.com:9092, ec2-54-70-81-69.us-west-2.compute.amazonaws.com:9092"}

    # Create input stream that pulls messages from Kafka Brokers
    # kvs is a DStream object
    kvs = KafkaUtils.createDirectStream(ssc, [topic], kafkaBrokers)
    
    # Processing part:
    def process(rdd):
        print rdd.collect()
        tweet_rdd = rdd.collect()
        red.set('tweet_key', tweet_rdd) # Put tweet into redis
        red.get('tweet_key')

    kvs.foreachRDD(process)
        
    """
    lines = kvs.map(lambda x: x[1])
    print type(lines)
    counts = lines.flatMap(lambda line: line.split(" ")) \
            .map(lambda word: (word, 1)) \
            .reduceByKey(lambda a, b: a+b)
    print type(counts)
    counts.pprint()
    """

    red.set('tweet_key', 'tweet') # Put tweet into redis
    red.get('tweet_key')

    ssc.start()
    ssc.awaitTermination()
