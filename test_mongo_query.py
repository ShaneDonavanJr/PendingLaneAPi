import json
import datetime
from pymongo import MongoClient

def lambda_handler(event, context):
    # MongoDB connection
    myclient = MongoClient("mongodb+srv://vince-circleconnect-dev:7rtd6EBamOJgXoUd@circle-alldata.ok8naze.mongodb.net/LoadDetail?retryWrites=true&w=majority")
    mydb = myclient["LoadDetail"]
    mycol = mydb["extensionsummaries"]
    
    # Date range
    today = datetime.datetime.today().replace(microsecond=0)
    yesterday = today - datetime.timedelta(days=465)
    
    # Romoved 8108 and 1437
    
    # Extensions array
    extensions = [
      "1492", "8754", "8489", "1536", "8006", "8031", "1341", "8132", "1205",
      "8109", "8130", "1378", "8030", "1386", "8230", "1226", "8652", "8112",
      "8231", "8424", "8421", "8012", "8011", "8156", "8017", "8456", "8089", 
      "8200","8218", "8165", "8027", "1537", "1187", "1038", "1666", "8145", 
      "8206", "1452","8090", "8740", "8246", "8426", "1600","1603","1897",
      "1422","1846","1425","1424","1195","1854", "8078", "8076", "8320"
    ]
    
    #  Glen's Team
    # "1600","1603","1897","1422","1846","1425","1424","1195","1854"








    # MongoDB query
    query = { 
        "extension": { "$in": extensions},
        "date": {"$gte": yesterday}
    }
    projection = {
        "_id": 0, 
        "extension": 1,
        "date": 1,
        "externalInboundAnswered": 1, 
        "externalOutboundAnswered": 1, 
        "totalTalkTime": 1
    }
    
    # Execute query
    results = list(mycol.find(query, projection))
    
    # # Close MongoDB connection
    myclient.close()
    
    # Return results
    return {
        'statusCode': 200,
        'body': json.dumps(results, default=str)
    }