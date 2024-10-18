import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('UserSearchHistory')

def lambda_handler(event, context):
    for record in event['Records']:
        # Extract message attributes
        user_id = record['messageAttributes']['Email']['stringValue']
        location = record['messageAttributes']['Location']['stringValue']
        category = record['messageAttributes']['CuisineType']['stringValue']

        # Store search data in DynamoDB
        table.put_item(
            Item={
                'UserId': user_id,
                'Location': location,
                'Category': category
            }
        )
    
    return {"statusCode": 200, "body": "Search state saved successfully"}
