import pymongo
from pprint import pprint
# from flask import Flask, render_template
#
# app = Flask(__name__)

connection = pymongo.MongoClient('localhost', 27017)
database = connection['Airbnb']
collection = database['listing']
print("Database connected")

# # Basic Query 1
# neighbourhood_group = input("Which neighbourhood_group you want near your house: ")
#
# # Retrieve from database
# retrieve_data = collection.find({"neighbourhood_group": neighbourhood_group})
#
# for document in retrieve_data:
#     pprint(document)

# Basic Query 3
# listing_id = input("Enter the listing id to which you want to provide reviews.")
# review = int(input("Give the reviews (out of 5) to the listing: "))
# if review < 0 or review > 5:
#     print("Invalid Review.")
# else:
#     given_query = {"id": listing_id}
#     num_of_reviews = collection.find({"id": listing_id}, {"number_of_reviews": 1})
#     num_of_reviews_1 = list(num_of_reviews)[0]["number_of_reviews"]
#     print("currently num_of_reviews are:", int(num_of_reviews_1))
#     review_per_month = (num_of_reviews_1 + 1) / 120
#     update_query = {
#         "$set": {"reviews": review, "number_of_reviews": int(num_of_reviews_1) + 1,
#                  "reviews_per_month": review_per_month},
#         "$currentDate": {"last_review": True}
#     }
#     print("Correct Review Given")
#     collection.update_one(given_query, update_query)



# Basic query 2


hostname = input("Enter name of host to find listings of:  ")
query = {"host_name":hostname}
result = collection.find(query,{"_id":0})
for results in result:
    print(results)


# Basic query 4

hostname = input("Enter name of host to find listings of:  ")
query = {"host_name":hostname}
result = collection.find(query,{"_id":0})
for results in result:
    print(results)
sorter = int(input("For high to low input -1; for low to high input 1:  "))
if sorter == 1 or sorter == -1:
    if sorter == -1:
        result2 = collection.find(query,{"_id":0}).sort("price", -1)
        for result in result2:
            print(result)

    else:
        result2 = collection.find(query, {"_id": 0}).sort("price", 1)
        for result in result2:
            print(result)


else:
    print("Invalid entry. press 1 or -1")


# Sophisticated Query 2

# Sophisticated Query 4



# @app.route("/")
# def display():
#     data_test = collection.find({"host_name": "Ben"})
#     return render_template('index.html', t="Testing", data=list(data_test), h="Airbnb Data")
#
#
# if __name__ == "__main__":
#     app.run()
