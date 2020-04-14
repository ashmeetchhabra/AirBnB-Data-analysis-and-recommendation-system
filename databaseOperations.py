import pymongo
from pprint import pprint

connection = pymongo.MongoClient('localhost', 27017)
database = connection['Airbnb']
collection = database['listing']
print("Database connected")

# Basic Query 1
neighbourhood_group = input("Which neighbourhood_group you want near your house: ")

# Retrieve from database
retrieve_data = collection.find({"neighbourhood_group": neighbourhood_group})

for document in retrieve_data:
    pprint(document)
