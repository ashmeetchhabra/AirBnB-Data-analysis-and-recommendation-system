import pymongo

connection = pymongo.MongoClient('localhost', 27017)
database = connection['AirbnbFinal']
collection = database['listing']
print("Database connected")


# Basic query 2

def basic_query2(host_name):
    try:
        query = {"host_name": host_name}
        result = collection.find(query, {"_id": 0})
        for results in result:
            print(results)

    except Exception as e:
        print("Exception " + str(e))


# Basic query 4

def basic_query4(host_name):
    try:
        query = {"host_name": host_name}
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


sophisticated_query3("13859196")
