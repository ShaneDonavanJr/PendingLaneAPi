import json
from pymongo import MongoClient

""" 

To make a package for the lamda function
pip install pymongo -t ./package 

"""

def lambda_handler(event, context):
    MONGO_URL = "mongodb+srv://shane-compass:Gi98VaSEbG3KG3lW@circle-alldata.ok8naze.mongodb.net/?retryWrites=true&w=majority"

    # Check if the event body is already a dictionary, or if it needs to be parsed
    # if isinstance(event['body'], str):
    #     body = json.loads(event['body'])  # Parse the body if it's a string
    # else:
    #     body = event['body']  # Use it directly if it's already a dictionary

    body = event

    # Connect to MongoDB
    client = MongoClient(MONGO_URL)
    db = client["CarrierSalesQuotes"]
    collection = db["LaneInfo"]

    # Example coordinates
    pickup_point = [body['originLongitude'], body['originLatitude']]  # [longitude, latitude]
    delivery_point = [body['destinationLongitude'], body['destinationLatitude']]  # [longitude, latitude]
    distance_miles = 50

    # Convert miles to radians for $geoWithin
    earth_radius_miles = 3963.2
    distance_radians = distance_miles / earth_radius_miles

    # Use $geoWithin for both locations (doesn't use geoNear)
    results = collection.find({
        "pickup.location": {
            "$geoWithin": {
                "$centerSphere": [pickup_point, distance_radians]
            }
        },
        "delivery.location": {
            "$geoWithin": {
                "$centerSphere": [delivery_point, distance_radians]
            }
        },
        # "active": True,
        # "carriers.active": True
    },
    {
      "_id": 0,
      "customerId": 1,
      "truckType": 1,
      "carriers.carrier_name": 1,
      "carriers.carrier_mc": 1,
      "carriers.contact_name": 1,
      "carriers.contact_email": 1,
      "carriers.contact_phone": 1,
      "carriers.carrier_comments": 1
    }
    )

    # Method 2: Convert cursor to list (careful with large result sets)
    results_list = list(results)
    print(results_list)


    def transform_data(input_data, dedicated_customer):
        transformed_data = {"data": []}
        
        for entry in input_data:
            carriers = entry['carriers']
            dedicated = True if int(entry['customerId']) == dedicated_customer else False
            for carrier in carriers:
                carrier_info = {
                    "carrierName": carrier['carrier_name'],
                    "dotNumber": "",  # Assuming dot number is not provided, leave it blank or adjust accordingly
                    "mcNumber": carrier['carrier_mc'],
                    "numberOfTrucks": 0,  # Assuming number of trucks is not provided, leave as 0
                    "equipmentType": entry['truckType'],
                    "dedicated": dedicated,  # Assuming "dedicated" is false unless stated otherwise
                    "comments": carrier['carrier_comments'],
                    "contacts": [{
                        "name": carrier['contact_name'],
                        "phone": carrier['contact_phone'].strip(),
                        "email": carrier['contact_email'].strip()
                    }]
                }
                transformed_data["data"].append(carrier_info)
        
        return transformed_data

    formatted_data = transform_data(results_list, body['customerId'])
    print(formatted_data)
    
    # Return results
    return {
        'statusCode': 200,
        'body': json.dumps(formatted_data, default=str)
    }

# Example test event and context for local testing
if __name__ == "__main__":
    event = {}  # Replace with actual event if needed
    context = None  # Replace with actual context if needed
    result = lambda_handler(event, context)
    print(result)