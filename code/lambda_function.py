import os
from elasticsearch import Elasticsearch
import json
import sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration

ELASTIC_ENDPOINT = os.environ.get('ELASTIC_ENDPOINT')
ELASTIC_PORT = os.environ.get('ELASTIC_PORT')
ELASTIC_STRING = f'{ELASTIC_ENDPOINT}:{ELASTIC_PORT}'
ELASTIC_KEY = os.environ.get('ELASTIC_KEY')

DICTS_TO_KEYS = [
    'hypnogram', "heart_rate_samples", "timeOffsetSleepSpo2", "timeOffsetSleepRespiration", "timeOffsetSpo2Values", "timeOffsetHeartRateSamples"
]

sentry_sdk.init(
    dsn=os.getenv('SENTRY_DSN'),
    send_default_pii=True,
    environment=os.getenv('ENV'),
    integrations=[AwsLambdaIntegration()]
)

def convert_time_series_to_list(time_series):
    return [{"time": time, "value": value} for time, value in time_series.items()]

def payload_with_replaced_dict_keys(payload):
    for key in DICTS_TO_KEYS:
        if key in payload:
            payload[key] = convert_time_series_to_list(payload[key])
    return payload

def __send_to_elastic_payload(index_name, doc):
    try:
        es_client = Elasticsearch(ELASTIC_STRING, api_key=ELASTIC_KEY)
        doc_payload = doc.get('payload', {})
        doc['payload'] = payload_with_replaced_dict_keys(doc_payload)
        response = es_client.index(index=index_name, document=doc)
    except Exception as e:
        sentry_sdk.capture_exception(e)
        print(f'Error indexing document in {index_name} index: {str(e)}')
        response = {}
    return response

def __send_to_elastic(index_name, doc):
    try:
        es_client = Elasticsearch(ELASTIC_STRING, api_key=ELASTIC_KEY)
        response = es_client.index(index=index_name, document=doc)
    except Exception as e:
        sentry_sdk.capture_exception(e)
        print(f'Error indexing document in {index_name} index: {str(e)}')
        response = {}
    return response

def __send_doc_splitted(index_name:str = None, doc:dict = None):
    if not index_name:
        raise ValueError('Index name is required')
    if not doc:
        return {}
    if 'payload' in doc:
        if isinstance(doc['payload'], list):
            for payload in doc['payload']:
                doc_to_send = doc.copy()
                doc_to_send['payload'] = payload
                __send_to_elastic_payload(index_name, doc_to_send)
        elif isinstance(doc['payload'], dict):
            __send_to_elastic_payload(index_name, doc)
    else:
        __send_to_elastic(index_name, doc)
    return doc

def lambda_handler(event, context):
    try:
        es_client = Elasticsearch(ELASTIC_STRING, api_key=ELASTIC_KEY) 
        # API key should have cluster monitor rights
        info = es_client.info()

        # Process each record in the SQS event
        for record in event['Records']:
            message = record['body']
            message_json = json.loads(message)
            print(f'Message received: {message_json}')
            doc = message_json.get('doc', {})
            docs = message_json.get('docs', [])
            index_name = message_json.get('index_name', {})
            if len(docs) > 0:
                for doc in docs:
                    doc = __send_doc_splitted(index_name, doc)
            else:
                doc = __send_doc_splitted(index_name, doc)
            
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'All documents indexed successfully'})
        }

    except Exception as e:
        sentry_sdk.capture_exception(e)
        return {
            'statusCode': 500,
            'body': json.dumps({'message': f'Error indexing documents: {str(e)}'})
        }