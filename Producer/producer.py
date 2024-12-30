import json
import time
from confluent_kafka import Producer
from logger import Logger

PORT = 9092
KAFKA_PRODUCER_TOPIC = "raw-tweets"

class KafkaProducer:
    def __init__(self, tweet_list: list) -> None:
        self.__tweet_list = tweet_list

    def produce(self):
        producer_conf = {"bootstrap.servers": f"localhost:{PORT}"}
        producer = Producer(producer_conf)
        for tweet in self.__tweet_list:
            producer.produce(KAFKA_PRODUCER_TOPIC, value=json.dumps(tweet))
        producer.flush()

i = 1
tweets = []
kafka_producer: KafkaProducer | None = None
logger = Logger("errors.log")

with open("tweets.json", "r", encoding="utf-8") as json_file:
    for line in json_file:
        tweet_data = json.loads(line)
        try:
            tweets.append(tweet_data)
        except KeyError as e:
            logger.log(f"Key error: {e} in tweet {tweet_data}")
        
        if i % 150 == 0:
            kafka_producer = KafkaProducer(tweets)
            kafka_producer.produce()
            tweets = []
            time.sleep(3)
        i += 1

if tweets:
    if kafka_producer:
        kafka_producer.produce()
