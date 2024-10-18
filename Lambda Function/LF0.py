import json
import boto3

def lambda_handler(event, context):
    client = boto3.client('lex-runtime', region_name='us-east-1')
    temp = json.loads(event['body'])
    response = client.post_text(
        botName= 'DiningConciergechatbot',
        botAlias= 'App',
        userId= 'user1',
        inputText= temp['messages'][0]['unstructured']['text']
    )
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type'  # Allowed headers
        },
        'body': json.dumps({
            'messages':[ 
                {
                    'type': "unstructured", 
                    'unstructured': {'text': response['message']} 
                } 
            ] 
        })
    }