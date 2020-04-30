import pymongo
from pprint import pprint
from math import cos, asin, sqrt, pi
import datetime as dt
from dateutil.parser import parse

connection = pymongo.MongoClient('localhost', 27017)
database = connection['Airbnb4']
collection = database['listing']
print("Database connected")


# Basic Query 1
def getListingByNeighbourhood(neighbourhood_group):
    retrieve_data = collection.find({"neighbourhood_group": neighbourhood_group})
    list_retrieve_data = list(retrieve_data)
    if not list_retrieve_data:
        print("No neighbourhood group with", neighbourhood_group, " present")
    else:
        for document in list_retrieve_data:
            pprint(document)


# Basic Query 3
def updateReviewsOfListing(listing_id):
    try:
        review = float(input("Give the rating (out of 5) to the listing: "))
        text_review = input("Give The Review of the listing")
        if review < 0 or review > 5:
            print("Invalid Review.")
        else:
            sophisticated_query1(listing_id, review, text_review)
            print("Execution till here successful")

    except Exception as err:
        print("Wrong Record",err)


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
    listing_shortest_distance = collection.find({"_id": itemMinValue[0]})
    print("The listing with minimum distance from Posh Area is::", list(listing_shortest_distance))
    print("This listing is very near to the Posh area, Flatbush Ave. "
          "It has many restaurants like: Taro Shushi, Alta Calidad, Chuko, Olmsted, Ki Shushi and many more, ")
    print("which are very near and in walking distance. It has Atlantic Train terminal, Best Buy and a "
          "super market such as Walmart nearby ")
    print("Minimum of the distance of listing from the Flatbush Ave is:: ", itemMinValue[1], "Km")


# Sophisticated Query 4
def lowerPricePrediction():
    print("Lower Price Prediction based on present data")
    all_data = collection.find({})
    list_all_data = list(all_data)
    for each_listing in list_all_data:
        try:
            count = 0
            # First Condition: If neighbourhood group is not Brooklyn
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
                print("After updating in db", list(data_retrieved_priceUpdated))
        except Exception as err:
            pass


# Basic query 2
def basic_query2(host_name):
    try:
        query = {"host_name": host_name}
        result = collection.find(query, {"_id": 0})
        list_result = list(result)
        if not list_result:
            print("No listing with ", host_name, " as host name")
        else:
            for results in list_result:
                print(results)

    except Exception as e:
        print("Exception " + str(e))


# Basic query 4
def basic_query4(nhood_group):
    try:
        query = {"neighbourhood_group": nhood_group}
        result = collection.find(query, {"_id": 0})
        for results in result:
            print(results)
        sorter = int(input("For high to low input -1; for low to high input 1:  "))
        if sorter == 1 or sorter == -1:
            if sorter == -1:
                result2 = collection.find(query, {"_id": 0}).sort("price", -1)
                for result in result2:
                    print(result)

            else:
                result2 = collection.find(query, {"_id": 0}).sort("price", 1)
                for result in result2:
                    print(result)

        else:
            print("Invalid entry. press 1 or -1")

    except Exception as e:
        print("No Record Found " + str(e))


# Sophisticated Query 1
def sophisticated_query1(listing_id, review, text_review):
    given_query = {"id": listing_id}
    num_of_reviews = collection.find({"id": listing_id}, {"number_of_reviews": 1})
    display_cursor = collection.find({"id": listing_id}, {"neighbourhood_group": 1})
    review_from_db_cursor = collection.find({"id": listing_id}, {"review_val": 1})
    review_from_db = float(list(review_from_db_cursor)[0]["review_val"])
    rating_cursor = collection.find({"id": listing_id}, {"rating": 1})
    rating_db = float(list(rating_cursor)[0]["rating"])
    query1 = str(list(display_cursor)[0]["neighbourhood_group"])
    print("Old Rating was :", rating_db)
    num_of_reviews_1 = int(list(num_of_reviews)[0]["number_of_reviews"])
    review_per_month = str((num_of_reviews_1 + 1) / 30)
    review_avg = ((review_from_db + review) / num_of_reviews_1)
    print("The new Rating of the listing is :", review_avg)
    update_query = {
        "$set": {"rating": str(review_avg), "review_val": str(review_from_db + review),
                 "number_of_reviews": int(num_of_reviews_1) + 1,
                 "reviews_per_month": review_per_month, "text_review": text_review},
        "$currentDate": {"last_review": True}
    }
    collection.update_one(given_query, update_query)
    print("Listings for that Neighbourhood group according to updated reviews is :")
    query2 = {"neighbourhood_group": query1}
    display_query = collection.find(query2,
                                    {"id": 1, "name": 1, "host_name": 1, "neighbourhood_group": 1, "neighbourhood": 1,
                                     "rating": 1, "text_review": 1, "_id": 0}).sort("rating", -1)
    for x in display_query:
        print(x)


# Sophisticated Query 3
def sophisticated_query3(listing_id):
    print("The result is: ")
    query = {"id": listing_id}
    result = collection.find_one(query, {"_id": 0})
    print(result)
    nhood_cursor = collection.find(query, {"neighbourhood_group": 1})
    nhood = str(list(nhood_cursor)[0]["neighbourhood_group"])
    price_cursor = collection.find(query, {"price": 1})
    pr = str(list(price_cursor)[0]["price"])
    rating_cursor = collection.find(query, {"rating": 1})
    rat = str(list(rating_cursor)[0]["rating"])
    print("Additional Recommendations with lesser price and better reviews: ")
    fil1 = {"price": {"$lt": pr}, "neighbourhood_group": nhood, "rating": {"$gt": rat}}
    price_range_cursor = collection.find(fil1, {"_id": 0})
    for y in price_range_cursor:
        print(y)


flag = True
while flag:
    print("/************************************************************************************/")
    print("Choose an option:  ")
    print("1. Search availability by neighbourhood group")
    print("2. View Listings of same host at different locations")
    print("3. Contribute towards the reviews and ratings")
    print("4. Sort houses based upon price")
    print("5. Degrade listing of house based on reviews")
    print("6. Show listings based upon nearest attractions")
    print("7. Show additional recommendations subjective to price and better reviews")
    print("8. Price prediction of rooms based on present data")
    print("9. Quit")
    i = input("Enter Your Choice:  ")
    if i == '1':
        neighbourhood_group = input("Which neighbourhood_group you want near your house:  ")
        getListingByNeighbourhood(neighbourhood_group)
    elif i == '2':
        host_name = input("Enter the name of the host you want to search listings of:  ")
        basic_query2(host_name)
    elif i == '3':
        listing_id = input("Enter the listing id to which you want to provide reviews.")
        listing = collection.find({"id": listing_id})
        # print("List_Listing::",list(listing))
        list_listing = list(listing)
        if not list_listing:
            print("Listing not Found")
        else:
            updateReviewsOfListing(listing_id)
    elif i == '4':
        nhood_group = input("Enter the name of Neighbourhood group you are looking prices for:  ")
        nhood_group_db = collection.find({"neighbourhood_group": nhood_group})
        nhood_group_list=list(nhood_group_db)
        if not nhood_group_list:
            print("Neighbourhood_group is not found")
        else:
            basic_query4(nhood_group)
    elif i == '5':
        listing_id = input("Enter the listing id to which you want to provide reviews.")
        listing = collection.find({"id": listing_id})
        # print("List_Listing::",list(listing))
        list_listing = list(listing)
        if not list_listing:
            print("Listing not Found")
        else:
            updateReviewsOfListing(listing_id)
    elif i == '6':
        getNearestAttractionListing()
    elif i == '7':
        listing_id = input("Enter the listing id you are looking better options for:  ")
        listing = collection.find({"id": listing_id})
        # print("List_Listing::",list(listing))
        list_listing = list(listing)
        if not list_listing:
            print("Listing not Found")
        else:
            sophisticated_query3(listing_id)
    elif i == '8':
        lowerPricePrediction()
    elif i == '9':
        print("Thank you!")
        flag = False
    else:
        print("Wrong Entry")
