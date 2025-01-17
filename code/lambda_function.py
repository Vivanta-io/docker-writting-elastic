from elasticsearch import Elasticsearch
import json

def lambda_handler(event, context):
    """
    Lambda function to index multiple documents in Elasticsearch from SQS.

    Args:
        event: SQS event containing multiple records.
        context: Lambda context object.

    Returns:
        dict: Response with status code and message.
    """

    try:
        # Connect to Elasticsearch
        es_client = Elasticsearch(hosts=[{'host': 'your-elasticsearch-endpoint', 'port': 9200}]) 

        # Process each record in the SQS event
        for record in event['Records']:
            message = record['body']
            message_json = json.loads(message)
            index_name = message_json.get('index', 'default_index')  # Get index from message
            doc = message_json.get('doc', {})  # Get document from message

            # Index document in Elasticsearch
            response = es_client.index(index=index_name, document=doc)

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'All documents indexed successfully'})
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'message': f'Error indexing documents: {str(e)}'})
        }