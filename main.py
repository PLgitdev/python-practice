# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
import random
import sqlite3
from datetime import timedelta
import asyncio
import User
import db_functions
import pymongo
import redis


def main():
    # NOSQL NON RELATIONAL MONGODB
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["example_database"]
    users = db["customers"]
    items = db["items"]
    # users_data = [{"firstname": "Bob", "lastname": "Adams"},
    #              {"firstname": "Amy", "lastname": "Smith"},
    #              {"firstname": "Rob", "lastname": "Bennet"}, ]
    # items_data = [{"title": "USB", "price": 10.2},
    #              {"title": "Mouse", "price": 12.23},
    #              {"title": "Monitor", "price": 199.99}, ]
    # users.insert_many(users_data)
    # items.insert_many(items_data)
    # update a user with a purchased item
    # bob = users.update_many(
    #    {"firstname": "Bob"},
    #    {
    #        "$set":
    #            {
    #                "purchase": [
    #                    {
    #                        "title": "USB",
    #                        "price": 10.2,
    #                        "currency": "EUR",
    #                        "notes": "Customer wants delivered via FedEx",
    #                        "original_item_id": 1
    #                    },
    #                    {
    #                        "title": "Mouse",
    #                        "price": 12.23,
    #                        "currency": "EUR",
    #                        "notes": "Customer wants delivered via UPS",
    #                        "original_item_id": 2
    #                    }
    #                ]
    #            }
    #    }
    # )
    # SQL RELATIONAL DB
    con = sqlite3.connect('practice.db')
    print(con.total_changes)
    # create user and update two databases with async function
    user = User.User("user", "P", "L")
    v: tuple = (random.randint(1, 10000), str(user.username),
                str(user.first_name), str(user.last_name), "world")
    # create an event loop
    loop = asyncio.new_event_loop()
    # Create task to run in event loop
    task = loop.create_task(db_functions.insert_user(c=con, user=v, db=db))
    # Run task (a insert in both databases)
    loop.run_until_complete(task)
    # how to do a quick mongo query
    users.create_index([("name", pymongo.DESCENDING)])
    users = users.find().sort("name", pymongo.ASCENDING)
    # purchase = item.get("purchase")
    # for item in users:
    #    print(item.get("purchase"))
    print(users.distinct("firstname"))
    # Inner join with between 3 tables
    print(db_functions.select_users_with_purchases(con))
    print(db_functions.select_all_users_purchases(con))
    print(db_functions.avg_purchase_price_items(con))
    print(db_functions.sum_of_item_purchases(con))
    print(db_functions.count_of_item_purchases(con))
    print(db_functions.select_ball_purchased_price(con))

    # how to make a triangle of r stars
    def pyfunc(r):
        for x in range(r):
            print(' ' * (r - x - 1) + '*' * (2 * x + 1))

    pyfunc(9)
    # Is this a palindrome
    a = input("enter sequence")
    b = a[::-1]
    if a == b:
        print("palindrome")
    else:
        print("Not a Palindrome")

    # Create a list of n size of even numbers
    # (modular arithmetic with n)
    start = 1
    n = 11
    a = []
    for i in range(start, n * 2 + 1):
        if i % 2 == 0:
            a.append(i)
    print(a)
    # cursor = con.cursor() HOW TO GET A CURSOR OBJECT
    # cursor.execute(
    # in memory cache db redis
    r = redis.Redis()

    # This function checks if we have the id in the cache mem database (redis) then returns the requ
    def get_name(request, con, **kwargs):
        cursor = con.cursor
        try:
            request_id = request.get('id')
            assert request_id
            if request_id in r:
                return r.get(id)  # assume that we have an {id: name} store
            else:
                # SQL query or Mongo
                # Get data from the main DB here, assume we already did it
                # use django exceptions to make sure bob query works
                name = 'Bob'  # bob is from the main db

                # Set the value in the cache database, with an expiration time
                r.setex(id, timedelta(minutes=60), value=name)
                return name
        except AssertionError as error:
            print(error)
            print("The request does not contain an id")
            pass
        except ConnectionAbortedError:
            print("The connection has been aborted")


main()
# RAW SQL BELOW IF YOU NEED TO RMBR
# con.cursor().execute('''CREATE TABLE IF NOT EXISTS user(id integer PRIMARY KEY,
#  username varchar , firstname varchar, lastname varchar, password varchar )''')

# '''CREATE TABLE IF NOT EXISTS purchase(
#            ordernumber integer PRIMARY KEY,
#            customerid integer,
#            itemid integer,
#            price decimal)'''
# '''INSERT INTO   user(id, username, password)
#       VALUES   (1, 'Bob', 'Adams'),
#                (2, 'Amy', 'Smith'),
#                (3, 'Rob', 'Bennet')'''
# '''INSERT INTO   Item(id, title, price)
#       VALUES   (1, 'bat', '6'),
#                (2, 'ball', '3'),
#                (3, 'truck', '9')'''
# '''Insert into purchase(customerid, itemid, price)
# VALUES (1, 1, 10.2),
#                       (1, 2, 12.23),
#                       (1, 3, 199.99),
#                       (2, 3, 180.00),
#                       (3, 2, 11.23);'''
# get average price
# '''SELECT itemid, AVG(price) FROM purchase GROUP by itemid '''
# get title with average price
# '''SELECT item.title, AVG(purchase.price) FROM purchase as purchase
# INNER JOIN Item as item on (item.id = purchase.itemid) GROUP BY purchase.itemid'''
# print(cursor.fetchall())
# )
# con.commit() REMEMBER THIS you need to commit
