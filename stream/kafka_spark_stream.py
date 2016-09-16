import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    
    if len(sys.argv) != 3:
           print "Usage: kafka_spark_stream.py <zk> <topic>"
           exit(-1)

    batch_duration = 1  # One second batch
    #spark_context = SparkContext(appName="KafkaTwitterStream")
    sc = SparkContext(appName="KafkaTwitterStream")
    #stream_context = StreamingContext(spark_context, batch_duration)
    ssc = StreamingContext(sc, 30)

    #zkQuorum, topic = sys.argv[1:]  # hostname and Kafka topic
    zkQuorum = "localhost::2181"
    topic = "twitter_test"

    # List the Kafka Brokers
    kafkaBrokers = {"metadata.broker.list": "ec2-52-27-232-130.us-west-2.compute.amazonaws.com:9092, ec2-54-70-124-67.us-west-2.compute.amazonaws.com:9092, ec2-54-70-110-215.us-west-2.compute.amazonaws.com:9092, ec2-54-70-81-69.us-west-2.compute.amazonaws.com:9092"}

    # Create input stream that pulls messages from Kafka Brokers
    kvs = KafkaUtils.createDirectStream(ssc, [topic], kafkaBrokers)
    #input_stream = KafkaUtils.createDirectStream(stream_context, [topic],
    #                                {"metadata.broker.list": brokers})
    
    lines = kvs.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split(" ")) \
            .map(lambda word: (word, 1)) \
            .reduceByKey(lambda a, b: a+b)
    print "start"
    counts.pprint()
    print "end"

    ssc.start()
    ssc.awaitTermination()
    #stream_context.start()
    #stream_context.awaitTermination()
