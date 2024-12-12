#!/usr/bin/env python3

import asyncio
import time
import settings
import sys
import os
from pymongo import MongoClient
from threading import Thread
import certifi


####
# Start script
####
print("==========================================")
print("    Change Stream Listener                ")
print("All events ")
print("==========================================")


####
# Main start function
# Start each individual thread for each event
# Sleep momentarily after starting each thread
####
def main():
    print('Starting Change Stream Listener.\n')

    mongo_client = MongoClient(MONGODB_ATLAS_URL, tlsCAFile=certifi.where())
    db = mongo_client[DATABASE]
    device_collection = db[COLLECTION]

    try:
        for document in device_collection.watch():
            print(document)
    except KeyboardInterrupt:
        keyboard_shutdown()

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

