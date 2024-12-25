import json
from elasticsearch import Elasticsearch

def create_index(es_client, index_name):
    mapping = {
        "mappings": {
            "properties": {
                "text": {"type": "text", "analyzer": "standard"},
                "created_at": {"type": "geo_point"},
                "created_at": {"type": "date"},
                "hashtags": {
                    "type": "text",
                    "analyzer": "autocomplete",
                    "search_analyzer": "standard"
                },
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

def index_tweet(es_client, tweet, index_name):
    try:
        es_client.index(index=index_name, body=tweet)
        print("Tweet indexed successfully.")
    except Exception as e:
        print(f"Error indexing tweet: {e}")

def main():
    es_client = Elasticsearch(
        ["https://localhost:9200"],
        http_auth=("elastic", "vOH*1sz*3-sf3gDC+aBN"),
        verify_certs=False
    )

    index_name = "tweets-index"
    create_index(es_client, index_name)

    ## mock data 
    tweets = [
        {
            "text": "Just setting up my Elasticsearch instance!",
            "location": {"lat": 40.7128, "lon": -74.0060},
            "created_at": "2024-12-25T12:00:00",
            "hashtags": ["Elasticsearch", "Setup"],
        },
        {
            "text": "Learning how to store tweets in Elasticsearch!",
            "location": {"lat": 34.0522, "lon": -118.2437},
            "created_at": "2024-12-25T12:05:00",
            "hashtags": ["Elasticsearch", "Learning"],
        },
        {
            "text": "Elasticsearch is amazing for search and analytics!",
            "location": {"lat": 51.5074, "lon": -0.1278},
            "created_at": "2024-12-25T12:10:00",
            "hashtags": ["Elasticsearch", "Analytics"],

        }
    ]

    ## should take form kafka and bulk tweets 
    for tweet in tweets:
        index_tweet(es_client, tweet, index_name)

if __name__ == "__main__":
    main()