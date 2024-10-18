import json
import requests
from requests.auth import HTTPBasicAuth

# Replace with your OpenSearch endpoint
ELASTICSEARCH_ENDPOINT = 'https://search-restaurants-evpmex3vepjedojnz653pu27vy.us-east-1.es.amazonaws.com/restaurants/_doc'
HEADERS = {"Content-Type": "application/json"}

# Basic Auth Credentials
USERNAME = 'adrian'
PASSWORD = 'Adrian@12345'

# Load data from JSON file
with open("restaurants.json", "r") as file:
    restaurant_data = json.load(file)

# Function to upload filtered data
def upload_data(entry):
    # Extract only RestaurantID and Cuisine
    data = {
        "RestaurantID": entry["business_id"],
        #"Cuisine": entry["cuisine"]  # Assuming cuisine is a list of cuisines
        "Cuisine":  entry["cuisine"][0] if entry["cuisine"] else None
    }
    # Use PUT to ensure idempotency and specific document IDs
    url = f"{ELASTICSEARCH_ENDPOINT}/{data['RestaurantID']}"
    
    try:
        # Use Basic Authentication instead of AWS4Auth
        response = requests.put(url, auth=HTTPBasicAuth(USERNAME, PASSWORD), headers=HEADERS, json=data)
        if response.status_code in [200, 201]:
            print(f"Successfully uploaded: {data['RestaurantID']}")
        else:
            print(f"Failed to upload {data['RestaurantID']}. Status Code: {response.status_code}")
            print("Response:", response.text)
    except Exception as e:
        print(f"An error occurred: {e}")

# Iterate and upload each restaurant entry with filtered fields
for entry in restaurant_data:
    upload_data(entry)
