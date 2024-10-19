import boto3
import json
import random
from elasticsearch import Elasticsearch
from datetime import datetime

# Initialize clients
sqs = boto3.client('sqs')
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
restaurants_table = dynamodb.Table('yelp-restaurants')
user_state_table = dynamodb.Table('UserSearchHistory')  # Table to store user's last search state

def get_previous_state(email):
    """Fetch the user's previous search state from DynamoDB."""
    try:
        response = user_state_table.get_item(Key={'Email': email})
        return response.get('Item', None)
    except Exception as e:
        print(f"Error fetching user state: {e}")
        return None

def save_user_state(email, cuisine, restaurant_data):
    """Save the user's current search state to DynamoDB."""
    try:
        user_state_table.put_item(
            Item={
                'Email': email,
                'LastCuisine': cuisine,
                'LastRestaurant': restaurant_data.get('name', 'Unknown'),
                'LastTimestamp': datetime.now().isoformat()
            }
        )
        print("User state saved successfully.")
    except Exception as e:
        print(f"Error saving user state: {e}")

def lambda_handler(event, context):
    # Step 1: Pull a message from SQS
    response = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=15)
    messages = response.get('Messages', [])
    print("Message Body:", messages)
    if not messages:
        print("No messages in the queue")
        return

    message = messages[0]
    receipt_handle = message['ReceiptHandle']
    cuisine = message.get('MessageAttributes', {}).get('CuisineType', {}).get('StringValue')
    email = message.get('MessageAttributes', {}).get('Email', {}).get('StringValue')

    # Step 2: Check for previous state from DynamoDB
    previous_state = get_previous_state(email)
    
    # Step 3: Get a random restaurant recommendation from Elasticsearch based on cuisine
    es_response = elastic_search.search(index='restaurants', body={
        "query": {"match": {"Cuisine": cuisine}},
        "size": 100
    })
    
    if not es_response['hits']['hits']:
        print(f"No recommendations found for cuisine: {cuisine}")
        return

    restaurant_ids = [hit['_source']['business_id'] for hit in es_response['hits']['hits']]
    chosen_id = random.choice(restaurant_ids)

    # Step 4: Fetch full details from DynamoDB
    restaurant_data = restaurants_table.get_item(Key={'business_id': chosen_id}).get('Item', {})
    name = restaurant_data.get('name', 'Unknown')
    address = restaurant_data.get('display_address', 'Unknown')
    rating = restaurant_data.get('rating', 'N/A')
    review_count = restaurant_data.get('review_count', 'N/A')

    # Step 5: Format email content
    if previous_state:
        previous_cuisine = previous_state.get('LastCuisine', 'Unknown')
        previous_restaurant = previous_state.get('LastRestaurant', 'Unknown')
        previous_timestamp = previous_state.get('LastTimestamp', 'Unknown')
        previous_info = (f"Based on your previous search for {previous_cuisine} cuisine, your last restaurant recommendation was:\n"
                         f"{previous_restaurant} \n\n")
    else:
        previous_info = "No previous search found.\n\n"
    
    email_subject = f"Your {cuisine} Restaurant Recommendation"
    email_body = (f"Hello! Here’s a {cuisine} restaurant recommendation for you:\n\n"
                  f"{name}\n{address}\nRating: {rating} ({review_count} reviews)\n\n"
                  f"{previous_info}"
                  f"Enjoy your meal!")

    try:
        # Step 6: Send the email using SES
        ses.send_email(
            Source='ad7628@nyu.edu',
            Destination={'ToAddresses': [email]},
            Message={
                'Subject': {'Data': email_subject},
                'Body': {'Text': {'Data': email_body}}
            }
        )
        print("Email sent successfully!")
        
        # Save current state to DynamoDB
        save_user_state(email, cuisine, restaurant_data)
        
        # Step 7: Delete the message from SQS after processing
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        
    except Exception as e:
        print(f"Failed to send email: {e}")

