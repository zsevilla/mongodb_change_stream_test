#!/usr/bin/env python3

from timeit import default_timer as timer
from pymongo import MongoClient
import settings
import certifi

####
# Start script
####
start = timer()
print("============================")
print("       RESET SCRIPT         ")
print("============================")


####
# Main start function
####
def main():
    mongo_client = MongoClient(MONGODB_ATLAS_URL, tlsCAFile=certifi.where())
    db = mongo_client[DATABASE]
    device_collection = db[COLLECTION]

    # Drop collection
    print("Dropping collection: " + COLLECTION)
    device_collection.drop()

    # Sample record to create collection
    device = {
        "deviceName": "Test Device",
        "deviceId": "123"
    }

    # insert sample record to create collection
    print("Create collection: " + COLLECTION)
    result = device_collection.insert_one(device)

    print("Sample record id: " + str(result.inserted_id))


####
# Constants loaded from .env file
####
MONGODB_ATLAS_URL = settings.MONGODB_ATLAS_URL
DATABASE = settings.DATABASE
COLLECTION = settings.COLLECTION

####
# Main
####
if __name__ == '__main__':
    main()

####
# Indicate end of script
####
end = timer()
print('====================================================')
print('Total Time Elapsed (in seconds): ' + str(end - start))
print('====================================================')
