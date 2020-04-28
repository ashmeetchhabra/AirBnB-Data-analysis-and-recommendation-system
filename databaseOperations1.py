import pymongo
from pprint import pprint
# from flask import Flask, render_template
from math import cos, asin, sqrt, pi
import datetime as dt
from dateutil.parser import parse

# app = Flask(__name__)

connection = pymongo.MongoClient('localhost', 27017)
database = connection['Airbnb3']
collection = database['listing']
print("Database connected")


# Basic Query 1
def getListingByNeighbourhood(neighbourhood_group):
    retrieve_data = collection.find({"neighbourhood_group": neighbourhood_group})
    for document in retrieve_data:
        pprint(document)


# Basic Query 3
def updateReviewsOfListing(listing_id):
    review = float(input("Give the rating (out of 5) to the listing: "))
    text_review = input("Give The Review of the listing")
    if review < 0 or review > 5:
        print("Invalid Review.")
    else:
        # TODO: Call the function from hrishikesh's file. give input as review and text_review
        given_query = {"id": listing_id}
        num_of_reviews = collection.find({"id": listing_id}, {"number_of_reviews": 1})
        review_from_db_cursor = collection.find({"id": listing_id}, {"review_val": 1})
        review_from_db = float(list(review_from_db_cursor)[0]["review_val"])
        print("The review is:", review_from_db)
        review_avg = (review_from_db + review) / 2
        num_of_reviews_1 = int(list(num_of_reviews)[0]["number_of_reviews"])
        print("Review to be updated is :", review_avg)
        review_per_month = (num_of_reviews_1 + 1) / 120
        update_query = {
            "$set": {"review_val": review_avg, "number_of_reviews": int(num_of_reviews_1) + 1,
                     "reviews_per_month": review_per_month},
            "$currentDate": {"last_review": True}
        }
        collection.update_one(given_query, update_query)


# Sophisticated Query 2
def getNearestAttractionListing():
    print("Listing of house upon nearest attractions")
    """
    near Taro Shushi: address: Taro Sushi
    244 Flatbush Ave
    Brooklyn, NY 11217
    lat:40.67932
    lon:-73.973503
    """
    posh_area_latitude = 40.67932
    posh_area_longitude = -73.973503
    p = pi / 180
    test_lat = collection.find({}, {"latitude": 1})
    test_lon = collection.find({}, {"longitude": 1})
    list_lat = list(test_lat)
    list_lon = list(test_lon)
    dict_of_dist = {}
    for document_lat in list_lat:
        for document_lon in list_lon:
            # If condition for matching _id for both latitude and longitude
            if document_lat['_id'] == document_lon['_id']:
                try:
                    lat_converted = float(document_lat['latitude'])
                    lon_converted = float(document_lon['longitude'])
                    b = 0.5 - cos((posh_area_latitude - lat_converted) * p) / 2 + cos(
                        lat_converted * p) * cos(
                        posh_area_latitude * p) * (1 - cos((posh_area_longitude - lon_converted) * p)) / 2
                    distance = 12742 * asin(sqrt(b))
                    dict_of_dist[(document_lat['_id'])] = distance

                except Exception as err:
                    pass
    # To find min of the value in dict
    itemMinValue = min(dict_of_dist.items(), key=lambda x: x[1])
    print("Minimum of the distance of listing is:: ", itemMinValue[1], "Km")
    listing_shortest_distance = collection.find({"_id": itemMinValue[0]})
    print("The listing with minimum distance is::", list(listing_shortest_distance))


# Sophisticated Query 4
def lowerPricePrediction():
    print("Lower Price Prediction based on present data")
    all_data = collection.find({})
    list_all_data = list(all_data)
    for each_listing in list_all_data:
        try:
            count = 0
            # First Condition: If nighbourhood group is not Brooklyn
            if (each_listing['neighbourhood']) != "Brooklyn":
                count = count + 1

            # Second Condition: If last review is given more than 300 days
            d = each_listing['last_review']
            dt_db = parse(d)
            dtime = dt.datetime.now()
            difference = dtime.date() - dt_db.date()
            if difference.days > 300:
                count = count + 1

            # Third Condition: If the room is shared
            room_type = each_listing['room_type']
            if room_type == "Shared room":
                count = count + 1

            # Forth Condition: If the number of reviews given is less than or equal to 1
            num_of_reviews = int(each_listing['number_of_reviews'])
            if num_of_reviews <= 1:
                count = count + 1

            # Fifth Condition:
            review_val = float(each_listing['review_val'])
            number_of_reviews = float(each_listing['number_of_reviews'])
            review = review_val / number_of_reviews
            if review < 3:
                count = count + 1

            if 4 <= count <= 5:
                ori_price = float(each_listing['price'])
                print("before updation: ", each_listing)
                print("Original Price:", ori_price)
                new_price = ori_price - ((ori_price * 10) / 100)
                print("Degraded Price by 10%:", new_price)

                given_query = {"id": each_listing['id']}
                update_query = {
                    "$set": {"price": new_price}
                }
                print("New Price Updated")
                collection.update_one(given_query, update_query)
                data_retrieved_priceUpdated = collection.find({"id": each_listing['id']})
                print("After updation in db", list(data_retrieved_priceUpdated))
        except Exception as err:
            pass


flag = True
while flag:
    print("Choose an option:")
    print("1. Search availability by neighbourhood group")
    print("2. Contribute towards the reviews and ratings")
    print("3. Show listings based upon nearest attractions")
    print("4. Price prediction of rooms based on present data")
    print("5. Quit")
    i = int(input("Enter Your Choice:"))
    if i == 1:
        neighbourhood_group = input("Which neighbourhood_group you want near your house: ")
        getListingByNeighbourhood(neighbourhood_group)
    elif i == 2:
        listing_id = input("Enter the listing id to which you want to provide reviews.")
        updateReviewsOfListing(listing_id)
    elif i == 3:
        getNearestAttractionListing()
    elif i == 4:
        lowerPricePrediction()
    elif i == 5:
        flag = False
    else:
        print("Wrong Entry")

"""
@app.route("/")
def display():
    data_test = collection.find({"host_name": "Ben"})
    return render_template('index.html', t="Testing", data=list(data_test), h="Airbnb Data")


if __name__ == "__main__":
    app.run()
"""
