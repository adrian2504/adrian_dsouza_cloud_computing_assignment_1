import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('UserSearchHistory')

def lambda_handler(event, context):
    user_id = event['UserId']

    # Retrieve the last search state
    response = table.get_item(Key={'UserId': user_id})
    if 'Item' in response:
        location = response['Item']['Location']
        category = response['Item']['Category']

        # Generate a recommendation based on the last search (placeholder example)
        recommendation = f"Based on your last search in {location} for {category}, we recommend..."

        return {"statusCode": 200, "body": recommendation}
    else:
        return {"statusCode": 404, "body": "No previous search found"}
