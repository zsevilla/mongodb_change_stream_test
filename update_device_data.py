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
print("=============================")
print("  Update Device Data         ")
print("Status: Maintenance to Active")
print("============================")


####
# Main start function
####
def main():
    mongo_client = MongoClient(MONGODB_ATLAS_URL, tlsCAFile=certifi.where())
    db = mongo_client[DATABASE]
    device_collection = db[COLLECTION]

    find_query = {
        "status": "maintenance"
    }

    for document in device_collection.find(find_query):
        print("\nFound Device with Status Maintenance.")

        filter_query = {"_id": document['_id']}

        update_query = {
            "$set": {"status": "active"}
        }

        update_results = device_collection.update_one(filter_query, update_query)
        print("Successful update with matched: " + str(update_results.matched_count) + " and modified count: " +
              str(update_results.modified_count))
        print("Updated status to active: " + str(document['deviceName']) + " " + str(document['deviceId']))

        time.sleep(1)


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
