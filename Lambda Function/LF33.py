import boto3
import json
import random
from elasticsearch import Elasticsearch

# Initialize clients
sqs = boto3.client('sqs')
#sqs = boto3.resource("sqs", region_name="us-east-1")
dynamodb = boto3.resource('dynamodb')
ses = boto3.client('ses')
elastic_search = Elasticsearch(
    hosts=[{'host': 'search-restaurants-evpmex3vepjedojnz653pu27vy.us-east-1.es.amazonaws.com', 'port': 443, 'scheme': 'https'}],
    http_auth=('adrian', 'Adrian@12345'),
    verify_certs=True,
    headers={"Content-Type": "application/json"}
)

# SQS and DynamoDB setup
queue_url = "https://sqs.us-east-1.amazonaws.com/503561431144/ChatbotDining"
table = dynamodb.Table('yelp-restaurants')

def lambda_handler(event, context):
    # Step 1: Pull a message from SQS
    response = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=15)
    messages = response.get('Messages', [])
    print("Message Body:", messages)
    if not messages:
        print("No messages in the queue")
        print("Full SQS response for debugging:", response)
        return

    message = messages[0]
    receipt_handle = message['ReceiptHandle']
    #message_body = json.loads(message['Body'])
    cuisine = message.get('MessageAttributes', {}).get('CuisineType', {}).get('StringValue')
    email = message.get('MessageAttributes', {}).get('Email', {}).get('StringValue')
    # cuisine = message_body.get('CuisineType')
    # email = message_body.get('Email')

    # Step 2: Get a random restaurant recommendation from Elasticsearch based on cuisine
    es_response = elastic_search.search(index='restaurants', body={
        "query": {"match": {"Cuisine": cuisine}},
        "size": 100
    })
    
    if not es_response['hits']['hits']:
        print(f"No recommendations found for cuisine: {cuisine}")
        return

    restaurant_ids = [hit['_source']['business_id'] for hit in es_response['hits']['hits']]
    chosen_id = random.choice(restaurant_ids)

    # Step 3: Fetch full details from DynamoDB
    restaurant_data = table.get_item(Key={'business_id': chosen_id}).get('Item', {})
    name = restaurant_data.get('name', 'Unknown')
    address = restaurant_data.get('display_address', 'Unknown')
    rating = restaurant_data.get('rating', 'N/A')
    review_count = restaurant_data.get('review_count', 'N/A')

    # Step 4: Format and send the email using SES
    email_subject = f"Your {cuisine} Restaurant Recommendation"
    email_body = (f"Hello! Here’s a {cuisine} restaurant recommendation for you:\n\n"
                  f"{name}\n{address}\nRating: {rating} ({review_count} reviews)\n\n"
                  f"Enjoy your meal!")
    
    try:
        ses.send_email(
            Source='ad7628@nyu.edu',
            Destination={'ToAddresses': [email]},
            Message={
                'Subject': {'Data': email_subject},
                'Body': {'Text': {'Data': email_body}}
            }
        )
        print("Email sent successfully!")
        
        # Delete the message from SQS after processing
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        
    except Exception as e:
        print(f"Failed to send email: {e}")
