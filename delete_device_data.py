#!/usr/bin/env python3

import time
from timeit import default_timer as timer
from pymongo import MongoClient
import settings
import certifi

####
# Start script
####
start = timer()
print("=====================================")
print("  Delete Device Data                 ")
print("=====================================")


####
# Main start function
####
def main():
    mongo_client = MongoClient(MONGODB_ATLAS_URL, tlsCAFile=certifi.where())
    db = mongo_client[DATABASE]
    device_collection = db[COLLECTION]

    find_query = {
        "status": "inactive"
    }

    for document in device_collection.find(find_query):
        print("\nFound device with inactive status.")

        device_name = document['deviceName']
        device_id = document['deviceId']

        filter_query = {"_id": document['_id']}

        deleted_results = device_collection.delete_one(filter_query)
        print("Successfully deleted device with inactive status.")
        print("Device name: " + device_name + " " + device_id)
        print("Number of documents deleted: " + str(deleted_results.deleted_count))

        time.sleep(0.5)


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
