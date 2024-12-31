import time
import sys
import json
from datetime import datetime
from confluent_kafka import Consumer
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

def delete_indices(es_client, index_names):
    for index_name in index_names:
        try:
            if es_client.indices.exists(index=index_name):
                es_client.indices.delete(index=index_name)
                print(f"Index '{index_name}' deleted.")
            else:
                print(f"Index '{index_name}' does not exist.")
        except Exception as e:
            print(f"Error deleting index '{index_name}': {e}")

def create_index(es_client, index_name):
    mapping = {
        "mappings": {
            "properties": {
                "text": {
                    "type": "text",
                    "analyzer": "autocomplete", 
                    "search_analyzer": "standard" 
                },
                "created_at": {
                    "type": "date"
                },
                "location": {
                    "type": "geo_point"
                },
                "hashtags": {
                    "type": "text",
                    "analyzer": "autocomplete",
                    "search_analyzer": "standard",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "sentiment": {
                    "type": "nested",
                    "properties": {
                        "polarity": {
                            "type": "float"
                        },
                        "subjectivity": {
                            "type": "float"
                        }
                    }
                }
            }
        },
        "settings": {
            "analysis": {
                "analyzer": {
                    "autocomplete": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": ["lowercase", "edge_ngram"]
                    }
                },
                "filter": {
                    "edge_ngram": {
                        "type": "edge_ngram",
                        "min_gram": 3,
                        "max_gram": 20
                    }
                }
            }
        }
    }


    try:
        if not es_client.indices.exists(index=index_name):
            es_client.indices.create(index=index_name, body=mapping)
            print(f"Index '{index_name}' created.")
        else:
            print(f"Index '{index_name}' already exists.")
    except Exception as e:
        print(f"Error creating index: {e}")


def prepare_bulk_data(tweets, index_name):
    actions = []
    for tweet in tweets:
        if "created_at" in tweet:
            tweet["created_at"] = datetime.strptime(tweet["created_at"], '%Y-%m-%dT%H:%M:%S.%f%z')

        action = {"_op_type": "index", "_index": index_name, "_source": tweet}
        actions.append(action)
    return actions


def index_tweets_bulk(es_client, tweets, index_name):
    try:
        for i in range(0, len(tweets), 50):
            bulk_data = prepare_bulk_data(tweets[i : i + 50], index_name)
            success, failed = bulk(es_client, bulk_data, chunk_size=50)
            print(f"Successfully indexed {success} documents, failed {failed}.")
            time.sleep(2)
    except Exception as e:
        print(f"Error indexing tweets: {e}")


def kafka_to_elasticsearch(es_client):
    consumer_conf = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "streaming-group",
        "auto.offset.reset": "earliest",
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe(["sentiment-tweets"])

    index_name = "streaming"
    create_index(es_client, index_name)

    tweet_batch = []

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            tweet_data = json.loads(msg.value().decode("utf-8"))
            if isinstance(tweet_data, dict):
                tweet_data = [tweet_data]

            tweet_batch.extend(tweet_data) 
            print(f"Accumulated {len(tweet_batch)} tweets.")

            if len(tweet_batch) >= 50:  
                index_tweets_bulk(es_client, tweet_batch[:50], index_name)
                tweet_batch = tweet_batch[50:]  

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        if tweet_batch:
            index_tweets_bulk(es_client, tweet_batch, index_name)


if __name__ == "__main__":
    es_client = Elasticsearch(["https://localhost:9200"], http_auth=("elastic", "vOH*1sz*3-sf3gDC+aBNR"), verify_certs=False)   
    if len(sys.argv) > 1:
        print(sys.argv)
        args = sys.argv[1]
        if args == "clean":
            indices_to_delete = ["streaming"]
            delete_indices(es_client, indices_to_delete)
    else:
        kafka_to_elasticsearch()