import nltk
import string
import json
import pandas as pd
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import FunctionTransformer, Pipeline
from textblob import TextBlob
from confluent_kafka import Consumer, Producer
from textblob import TextBlob
from textblob.blob import cached_property

nltk.download('punkt_tab')
nltk.download('punkt')
nltk.download('stopwords')

tfidf_vectorizer = TfidfVectorizer()

def preprocess_text(text):
    tokens = word_tokenize(text)
    
    stop_words = set(stopwords.words('english'))
    filtered_tokens = [token for token in tokens if token not in stop_words and token not in string.punctuation]
    
    stemmer = PorterStemmer()
    stemmed_tokens = [stemmer.stem(token) for token in filtered_tokens]
    
    return ' '.join(stemmed_tokens)

class Analyzer:
    def __init__(self, text) -> None:
        self.text = text

    def sentiment(self) -> dict[str, cached_property]:
        """Perform sentiment analysis using TextBlob."""
        analysis: TextBlob = TextBlob(self.text)

        return {
            "polarity": analysis.polarity,
            "subjectivity": analysis.subjectivity
        }

def sentiment_analysis_consumer():
    consumer_conf: dict[str, str] = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "sentiment-analysis-group",
        "auto.offset.reset": "earliest"
    }
    consumer: Consumer = Consumer(consumer_conf)
    consumer.subscribe(["transform-tweets"])

    producer_conf: dict[str, str] = {"bootstrap.servers": "localhost:9092"}
    producer: Producer = Producer(producer_conf)

    pipeline = Pipeline([
        ('preprocessor', FunctionTransformer(lambda texts: [preprocess_text(text) for text in texts])),
        ('tfidf', tfidf_vectorizer),
    ])

    try:
        sample_data = ["sample text"]
        pipeline.fit(sample_data)

        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            tweet_data = json.loads(msg.value().decode("utf-8"))
            print(tweet_data)

            if isinstance(tweet_data, list):
                tweet_df = pd.DataFrame(tweet_data)
            else:
                tweet_df = pd.DataFrame([tweet_data])

            enriched_tweets = []
            for _, row in tweet_df.iterrows():
                text = row["text"]

                preprocessed_text = preprocess_text(text)
                pipeline.transform([preprocessed_text])

                analyzer = Analyzer(text)
                sentiment = analyzer.sentiment()

                enriched_row = row.to_dict()
                enriched_row.update({"sentiment": sentiment})
                enriched_tweets.append(enriched_row)

            producer.produce("sentiment-tweets", value=json.dumps(enriched_tweets))
            producer.flush()

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    sentiment_analysis_consumer()
