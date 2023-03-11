import pymongo
from message import Message


def connect():
    conn_str = "mongodb+srv://shuifanzz:shuifanzz@cluster0.hksonbt.mongodb.net/?retryWrites=true&w=majority"
    client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
    try:
        print("Connect to Server")
    except Exception:
        print("Unable to connect to the server.")
    db = client["p1"]
    seats_collection = db["seat_map"]
    status_collection = db["status"]
    return (seats_collection, status_collection)


def bookSeat(seats_collection, status_collection, seats):
    occupied = 0
    result = query(seats_collection, seats)
    for data in result:
        if not data["occupied"]:
            occupied += 1

    updateStatus(status_collection, occupied)
    for seat in seats:
        update(seats_collection, seat, True)
    


def cancelSeat(seats_collection, status_collection, seats):
    occupied = 0
    result = query(seats_collection, seats)
    for data in result:
        if data["occupied"]:
            occupied -= 1
    updateStatus(status_collection, occupied)
    for seat in seats:
        update(seats_collection, seat, False)


def update(seats_collection, seat, status):
    query = {"seat_number" : seat}
    update = {"$set": {"occupied": status}}
    seats_collection.update_one(query, update)


def query(seats_collection, seats):
    res = []
    for seat in seats:
        query = {"seat_number" : seat}
        projection = seats_collection.find_one(query)
        res.append(projection)
    return res


def updateStatus(status_collection, occupied):
    document = status_collection.find_one()
    occupied_seat = int(document["occupied_seat"]) + occupied
    print(occupied_seat)
    update = {"$set": {"occupied_seat": str(occupied_seat)}}
    status_collection.update_one(document, update)



"""
operations = {0 : bookSeat, 1 : cancelSeat}
M = Message(0, [1,2,3,4])
seats, status = connect()
f = operations[M.msg_id]
f(seats, status, M.seats)


M = Message(1, [1,2,4])
f = operations[M.msg_id]
f(seats, status, M.seats)
"""

