import sqlite3
import pymongo


async def insert_user(c: sqlite3, user: tuple, db: pymongo.MongoClient):
    cursor = c.cursor()
    sql = '''INSERT INTO  user(id, username, firstname, lastname, password) 
    VALUES (?,?,?,?,?)'''
    cursor.execute(sql, user)
    c.commit()
    return await mongo_trigger(db, user)


async def mongo_trigger(db: pymongo.MongoClient, user: tuple):
    users = db['customers']
    user_data: dict = {
        "firstname": str(user[2]),
        "lastname": str(user[3])
    }
    users.insert_one(user_data)


def select_users_with_purchases(c: sqlite3) -> list:
    cursor = c.cursor()
    sql = '''Select u.username, p.ordernumber, i.title FROM user u 
        INNER JOIN purchase p on p.customerid = u.id
        INNER JOIN Item i on i.id = p.itemid'''
    cursor.execute(sql)
    return cursor.fetchall()


# right join is users joined with all purchases
# full outer join would be all of the users with all of the purchases


def select_all_users_purchases(c: sqlite3) -> list:
    cursor = c.cursor()
    sql = '''SELECT u.username, p.ordernumber, i.title  FROM user AS u 
    LEFT JOIN purchase p on p.customerid = u.id
    LEFT JOIN Item i on i.id = p.itemid'''
    cursor.execute(sql)
    # could potentially use a callback function to insert this data into a new table or process this data in a
    # certain way
    return cursor.fetchall()


def avg_purchase_price_items(c: sqlite3) -> list:
    cursor = c.cursor()
    sql = '''SELECT i.title, AVG(p.price) FROM purchase p
        INNER JOIN Item i on i.id = p.itemid GROUP BY p.itemid'''
    cursor.execute(sql)
    return cursor.fetchall()


def sum_of_item_purchases(c: sqlite3) -> list:
    cursor = c.cursor()
    sql = '''SELECT i.title, SUM(p.price) FROM purchase p
        INNER JOIN Item i on i.id = p.itemid GROUP BY i.title'''
    cursor.execute(sql)
    return cursor.fetchall()


def select_all_high_priced_items(c: sqlite3) -> list:
    cursor = c.cursor()
    sql = '''SELECT i.title, i.price FROM Item i
        WHERE i.id IN (SELECT i.id FROM i WHERE i.price > 5) '''
    cursor.execute(sql)
    return cursor.fetchall()


def select_ball_purchased_price(c: sqlite3) -> list:
    cursor = c.cursor()
    sql = '''SELECT Item.price FROM Item WHERE Item.id IN 
    (SELECT purchase.itemid FROM purchase WHERE purchase.itemid = 2)'''
    cursor.execute(sql)
    return cursor.fetchall()


def count_of_item_purchases(c: sqlite3) -> list:
    cursor = c.cursor()
    sql = '''SELECT i.title, COUNT(p.ordernumber) FROM purchase p
        INNER JOIN Item i on i.id = p.itemid GROUP BY i.title'''
    cursor.execute(sql)
    return cursor.fetchall()
