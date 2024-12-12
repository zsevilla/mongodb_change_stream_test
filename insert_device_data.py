#!/usr/bin/env python3

import time
from timeit import default_timer as timer
from pymongo import MongoClient
import settings
import datetime
import random
from faker import Faker
import certifi


# Initialize Faker
fake = Faker()

start = timer()
print("============================")
print("  Insert Device  Data       ")
print("============================")

####
# Main start function
####
def main():
    mongo_client = MongoClient(MONGODB_ATLAS_URL, tlsCAFile=certifi.where())
    db = mongo_client[DATABASE]
    device_collection = db[COLLECTION]

    for idx in range(int(NUM_RECORDS_TO_GENERATE)):
        device = {
            "deviceId": fake.uuid4(),
            "deviceName": fake.word(),
            "deviceType": random.choice(["sensor", "actuator", "controller"]),
            "configuration": {
                "ipAddress": fake.ipv4(),
                "macAddress": fake.mac_address(),
                "firmwareVersion": f"{random.randint(1, 5)}.{random.randint(0, 9)}.{random.randint(0, 9)}",
                "lastUpdated": datetime.datetime.strftime(fake.date_time_this_year(), '%Y-%m-%d %H:%M:%S')
            },
            "status": random.choice(["active", "inactive", "maintenance"]),
            "lastCommunication": datetime.datetime.strftime(fake.date_time_this_month(), '%Y-%m-%d %H:%M:%S')
        }
        device_collection.insert_one(device)
        record_num = ++idx
        print("Inserted record: " + str(record_num))
        time.sleep(0.5)

####
# Constants loaded from .env file
####
MONGODB_ATLAS_URL = settings.MONGODB_ATLAS_URL
NUM_RECORDS_TO_GENERATE = int(settings.NUM_RECORDS_TO_GENERATE)
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
