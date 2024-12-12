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
print("     Resume Example          ")
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
        "status": "maintenance",
        "resumeCounter": {"$exists": False}
    }

    counter = 0
    for document in device_collection.find(find_query):
        print("\nFound Device with status maintenance.")

        # increment counter
        counter = counter + 1

        filter_query = {
                        "_id": document['_id']
                        }

        update_query = {
            "$set": {"status": "active",
                "resumeCounter": counter}
        }

        update_results = device_collection.update_one(filter_query, update_query)
        print("Successful update with matched: " + str(update_results.matched_count) + " and modified count: " +
              str(update_results.modified_count))
        print("Incremented resumeCounter to customer acct with _id: " + str(document['_id']))
        print("Counter value: " + str(counter))

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
