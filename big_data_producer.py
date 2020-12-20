import json
import sys
import os
import tweepy
import boto3
# OAuth authentication credentials
consumer_key = 'QHvTETppk2QrZNB0AQGnpapdd'
consumer_secret = 'Y6Gs12OhpQBeJU6qn5MIP9Elqmn2dXWEmrKtpglejIBXBcrQlo'
access_token = '1139245487226273792-CvF2nSxvPyyqqY4qJth1Dnk8pMHq48'
access_secret = 'NXL0hh7KKVPiB0vbkRgqmomQrEm028OuuXLnCzF0wbFtx'

# Consumer key authentication
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)

# Access key authentication
auth.set_access_token(access_token, access_secret)

# Set up the API
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

DeliveryStreamName = 'Twitter-stream'

client = boto3.client('firehose')

class MyStreamListener(tweepy.StreamListener):
    """
    Receive tweets from the stream.
    """
    def __init__(self, api, num_tweets, file_name):
        self.api = api
        self.tweets_count = 0
        self.num_tweets = num_tweets
        self.file_name = file_name

    def on_status(self, status):
        tweet = status._json
        print(json.dumps(tweet['text']) + '\n')
        data=json.dumps(tweet['text'])
        #with open(self.file_name,'a',encoding='utf-8') as file:
            #file.write(data+'\n')
        client.put_record(DeliveryStreamName='Twitter-stream',Record={'Data': data})
        self.tweets_count += 1
        sys.stdout.write(
            "\rProgress: {}/{} tweets downloaded".format(
                self.tweets_count, self.num_tweets
            )
        )
        sys.stdout.flush()
        if self.tweets_count < self.num_tweets:
            return True
        else:
            return False

    def on_error(self, status):
        print(status.text)
# # Number tweets to download
num_tweets = 1000

# # File path to store the tweets
file_name = 'tweets_final_big_data.txt'

# # Create a stream listener object
listener = MyStreamListener(api, num_tweets, file_name)

# # Create a Stream object
stream = tweepy.Stream(auth, listener)

# # Start collecting tweets on keywords from the stream
# # It might take a while to downloaded if the number of tweets is large
stream.filter(track=['Trump', 'Biden'], languages=['en'])
# Load the tweets from .txt file into a dataframe
