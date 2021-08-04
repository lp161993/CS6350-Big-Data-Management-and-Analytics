from kafka import KafkaConsumer
import json
from elasticsearch import Elasticsearch
from textblob import TextBlob
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
#nltk.download("vader_lexicon")
es = Elasticsearch()

def main():
    '''
    main function initiates a kafka consumer, initialize the tweetdata database.
    Consumer consumes tweets from producer, extracts features, cleanses the tweet text,
    calculates sentiments and loads the data into elasticsearch
    '''
    # set-up a Kafka consumer
    consumer = KafkaConsumer("twitter")
    sia = SentimentIntensityAnalyzer()
    for msg in consumer:
        dict_data = json.loads(msg.value)
        tweet = TextBlob(dict_data["text"])
        hashtag = ""
        if(dict_data["text"].lower().find("#coronavirus") != -1):
        	hashtag = "#coronavirus"
        if(dict_data["text"].lower().find("#vaccine") != -1):
        	hashtag = "#vaccine"
        print(tweet)
        print("\n")
        sentiment_score = sia.polarity_scores(dict_data['text'])['compound']
        #print(sentiment_score)
        # add text and sentiment info to elasticsearch
        es.index(index="tweet",
                  doc_type="test-type",
                  body={"author": dict_data["user"]["screen_name"],
                        "date": dict_data["created_at"],
                        "message": dict_data["text"],
                        "sentiment": sentiment_score,
                        "hashtag": hashtag})
        print('\n')

if __name__ == "__main__":
    main()