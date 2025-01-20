import os
from elasticsearch import Elasticsearch
import json

ELASTIC_ENDPOINT = os.environ.get('ELASTIC_ENDPOINT')
ELASTIC_PORT = os.environ.get('ELASTIC_PORT')
ELASTIC_STRING = f'{ELASTIC_ENDPOINT}:{ELASTIC_PORT}'
ELASTIC_KEY = os.environ.get('ELASTIC_KEY')

def __send_to_elastic(index_name, doc):
    es_client = Elasticsearch(ELASTIC_STRING, api_key=ELASTIC_KEY) 
    response = es_client.index(index=index_name, document=doc)
    print(f'Indexing document in {index_name} index: {type(doc)}', doc)
    return response

def __send_doc_splitted(index_name:str = None, doc:dict = None):
    if not index_name:
        raise ValueError('Index name is required')
    if not doc:
        raise ValueError('Document is required')
    if 'payload' in doc:
        if isinstance(doc['payload'], list):
            for payload in doc['payload']:
                doc_to_send = doc.copy()
                doc_to_send['payload'] = payload
                __send_to_elastic(index_name, doc_to_send)
        else:
            __send_to_elastic(index_name, doc)
    else:
        __send_to_elastic(index_name, doc)
    return doc


def lambda_handler(event, context):
    try:
        # Connect to Elasticsearch
        print('ELASTIC_STRING', ELASTIC_STRING)
        print('ELASTIC_KEY', ELASTIC_KEY)
        es_client = Elasticsearch(ELASTIC_STRING, api_key=ELASTIC_KEY) 
        # API key should have cluster monitor rights
        info = es_client.info()

        # Process each record in the SQS event
        for record in event['Records']:
            message = record['body']
            message_json = json.loads(message)
            doc = message_json.get('doc', {})
            index_name = message_json.get('index', {})
            doc = __send_doc_splitted(index_name, doc)

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'All documents indexed successfully'})
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'message': f'Error indexing documents: {str(e)}'})
        }