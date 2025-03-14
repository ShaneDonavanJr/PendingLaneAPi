from pymongo import MongoClient
import pandas as pd
import json

MONGO_URL = "mongodb+srv://shane-compass:Gi98VaSEbG3KG3lW@circle-alldata.ok8naze.mongodb.net/?retryWrites=true&w=majority"

# Connect to MongoDB
client = MongoClient(MONGO_URL)
db = client["CarrierSalesQuotes"]
collection = db["LaneInfo"]

# Example coordinates
pickup_point = [-90.373859, 38.4441963]  # [longitude, latitude]
delivery_point = [-83.9737935, 35.7895271]  # [longitude, latitude]
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
  "comments": 1, 
  "carriers.carrier_name": 1,
  "carriers.carrier_mc": 1,
  "carriers.contact_name": 1,
  "carriers.contact_email": 1,
  "carriers.contact_phone": 1
}
)

# Method 2: Convert cursor to list (careful with large result sets)
results_list = list(results)
print(results_list)


def transform_data(input_data):
    transformed_data = {"data": []}
    
    for entry in input_data:
        carriers = entry['carriers']
        dedicated = True if entry['customerId'] == '6451' else False
        for carrier in carriers:
            carrier_info = {
                "carrierName": carrier['carrier_name'],
                "dotNumber": "",  # Assuming dot number is not provided, leave it blank or adjust accordingly
                "mcNumber": carrier['carrier_mc'],
                "numberOfTrucks": 0,  # Assuming number of trucks is not provided, leave as 0
                "equipmentType": entry['truckType'],
                "dedicated": dedicated,  # Assuming "dedicated" is false unless stated otherwise
                "comments": entry['comments'],
                "contacts": [{
                    "name": carrier['contact_name'],
                    "phone": carrier['contact_phone'].strip(),
                    "email": carrier['contact_email'].strip()
                }]
            }
            transformed_data["data"].append(carrier_info)
    
    return transformed_data

formatted_data = transform_data(results_list)
print(formatted_data)

pd.DataFrame(formatted_data).to_json("output.json")

