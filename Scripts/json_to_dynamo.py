import boto3
import json
from decimal import Decimal

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')  # Replace with your region
table = dynamodb.Table('yelp-restaurants')

with open('restaurants.json') as file:
    data = json.load(file, parse_float=Decimal)  # Convert floats to Decimal

# Function to ensure all numeric values are Decimal types
def convert_to_dynamodb_format(item):
    return {
        'business_id': item['business_id'],
        'name': item['name'],
        'cuisine': item['cuisine'],  # DynamoDB can handle lists of strings
        'display_address': item['display_address'],
        'review_count': Decimal(str(item['review_count'])),  # Convert to Decimal
        'rating': Decimal(str(item['rating'])),  # Convert to Decimal
        'zip_code': item['zip_code']
    }

# Insert each item into DynamoDB
for item in data:
    dynamodb_item = convert_to_dynamodb_format(item)
    table.put_item(Item=dynamodb_item)

print("Data inserted successfully.")