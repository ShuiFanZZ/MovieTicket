import pymongo
from kafka import KafkaConsumer, KafkaProducer
import random
import argparse
import json

request = {
        "id": i,
        "movieId": 1,
        "numberTickets": seat_number,
        "seat": [],
        "mode": "seat_request"
    }

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
        else:
            return False

    updateStatus(status_collection, occupied)
    for seat in seats:
        update(seats_collection, seat, True)
    return True
    

def cancelSeat(seats_collection, status_collection, seats):
    occupied = 0
    result = query(seats_collection, seats)
    for data in result:
        if data["occupied"]:
            occupied -= 1
    updateStatus(status_collection, occupied)
    for seat in seats:
        update(seats_collection, seat, False)
    return True


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


def getAvailability(seats_collection):
    """
    document = status_collection.find_one()
    occupied = int(document["occupied_seat"])
    total = int(document["total_seat"])
    return total - occupied
    """
    seats = [i for i in range(0, 20)]
    result = query(seats_collection, seats)
    number_of_available_seats = 0
    available_seats = []
    for data in result:
        if not data["occupied"]:
            number_of_available_seats += 1
            available_seats.append(data["seat_number"])
    return available_seats, number_of_available_seats


def updateStatus(status_collection, occupied):
    document = status_collection.find_one()
    occupied_seat = int(document["occupied_seat"]) + occupied
    #print(occupied_seat)
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

def listener(args):
    #create a consumer and producer
    consumer = KafkaConsumer(args.topic1, group_id=args.group, bootstrap_servers=[args.server + ':' + args.port])
    producer = KafkaProducer(bootstrap_servers=[args.server + ':' + args.port])

    #subscribe to the topic1
    consumer.subscribe([args.topic1])

    #connect to mongodb
    seats, status = connect()

    #listen to the topic1
    while True:
        for msg in consumer:
            reply = {"available_seats" : [], "status" : "success"}
            msg = msg.value.decode('utf-8')
            msg_type = msg["mode"]

            #return the number of available seats
            if msg_type == "seat_request":
                available_seats, number_of_available_seats = getAvailability(seats)
                reply["available_seats"] = available_seats
                if number_of_available_seats < msg["numberTickets"]:
                    reply["status"] = "not_enough_seats"
                else:
                    reply["status"] = "success"
            
            #cancel the booking
            elif msg_type == "seat_cancel":
                if cancelSeat(seats, status, msg["seat"]):
                    reply["status"] = "success"
                else:
                    reply["status"] = "fail"
                available_seats, number_of_available_seats = getAvailability(seats)
                reply["available_seats"] = available_seats

            #book the seats if seats are available
            elif msg_type == "seat_confirm":
                if bookSeat(seats, status, msg["seat"]):
                    reply["status"] = "success"
                else:
                    reply["status"] = "fail"
                available_seats, number_of_available_seats = getAvailability(seats)
                reply["available_seats"] = available_seats

            producer.send(args.topic2, json.dumps(reply).encode('utf-8'))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('topic1', help='Kafka topic1')
    parser.add_argument('topic2', help='Kafka topic2')
    parser.add_argument('group', help='Kafka consumer group')
    parser.add_argument('server', help='Kafka server')
    parser.add_argument('port', help='Kafka port')
    args = parser.parse_args()







    

