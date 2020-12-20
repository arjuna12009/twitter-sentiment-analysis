import base64
import json
import boto3


def lambda_handler(event, context):
    for record in event['records']:

        dict_data = base64.b64decode(record['data']).decode('utf-8').strip()
        comprehend = boto3.client(service_name='comprehend', region_name='us-east-1')
        sentiment_all = comprehend.detect_sentiment(Text=dict_data, LanguageCode='en')
        sentiment = sentiment_all['Sentiment']
        data_record = {
                    'message': dict_data,
                    'sentiment': sentiment,
                }
        print(data_record)
