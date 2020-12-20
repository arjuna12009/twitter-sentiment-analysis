
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import json
import twitter_config
import pykafka
from afinn import Afinn
import sys

class TweetListener(StreamListener):
        def __init__(self):
                self.client = pykafka.KafkaClient("localhost:9092")
                self.producer = self.client.topics[bytes('twitter','ascii')].get_producer()

        def on_data(self, data):
                try:
                        json_data = json.loads(data)

                        send_data = '{}'
                        json_send_data = json.loads(send_data)
                        json_send_data['text'] = json_data['text']
                        json_send_data['senti_val']=afinn.score(json_data['text'])

                        print(json_send_data['text'], " >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ", json_send_data['senti_val'])

                        self.producer.produce(bytes(json.dumps(json_send_data),'ascii'))
                        return True
                except KeyError:
                        return True

        def on_error(self, status):
                print(status)
                return True



consumer_key = "uTIlaM3x7ampCtwvXctMrBhoD"
consumer_secret = "1GVVXQCfAfSHSozSNKTmegJbiPc38D6GFdbbtARI3pX1dKce09"
access_token = "1135589356045946880-kKBaW3oLrRw3uNyLg7bSrGyiNG2Z6Q"
access_secret = "w00hub1FYKj0ajLZOwn91fhkfvEOxd0Ikj0ve72bNow4I"
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

# create AFINN object for sentiment analysis
afinn = Afinn()

twitter_stream = Stream(auth, TweetListener())
twitter_stream.filter(languages=['en'], track=["trump"])

