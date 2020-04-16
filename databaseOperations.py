import pymongo
from pprint import pprint
from flask import Flask, render_template, request, redirect, url_for


app = Flask(__name__)

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


@app.route("/")
def display():
    data_test = collection.find({"host_name": "Ben"})
    return render_template('index.html', t="Testing", data=list(data_test), h="Airbnb  Data")


if __name__ == "__main__":
    app.run()
