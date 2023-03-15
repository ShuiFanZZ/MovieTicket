import pymongo
from kafka import KafkaConsumer, KafkaProducer
import random
import argparse
import json
import threading


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
    n = len(seats)
    filter_list = [None] * 2 * len(seats)
    update = {}
    seat_str = ""
    for i in range(0, n):
        seat_str = "occupied_" + str(seats[i])
        filter_list[i] = {seat_str : {"$exists": True}}
        filter_list[i + n] = {seat_str : False}
        update[seat_str] = True

    filter_query = {"$and" : filter_list}
    update_query = {"$set" : update}
    result = seats_collection.update_many({"$and": [filter_query]}, update_query)
    if result.modified_count == 0:
        return False
    return True


def cancelSeat(seats_collection, status_collection, seats):
    n = len(seats)
    filter_list = [None] * 2 * len(seats)
    update = {}
    seat_str = ""
    for i in range(0, n):
        seat_str = "occupied_" + str(seats[i])
        filter_list[i] = {seat_str : {"$exists": True}}
        filter_list[i + n] = {seat_str : True}
        update[seat_str] = False

    filter_query = {"$and" : filter_list}
    update_query = {"$set" : update}
    result = seats_collection.update_many({"$and": [filter_query]}, update_query)
    if result.modified_count == 0:
        return False
    updateStatus(status_collection, -n)
    return True


def getAvailability(seats_collection):
    """
    get the number and list of available seats
    """
    result = seats_collection.find()
    number_of_available_seats = 0
    available_seats = []
    seat_idx = 0
    for seat in result[0].keys():
        if (seat[0] == "_"):
            continue
        seat_idx = int(seat[9:])
        if not result[0][seat]:
            number_of_available_seats += 1
            available_seats.append(seat_idx)
    return available_seats, number_of_available_seats
    

def updateStatus(status_collection, occupied):
    """
    update the number of occupied seats
    """
    filter_query = {}
    update_query = {"$inc": {"occupied_seat": occupied}}
    status_collection.update_one(filter_query, update_query)


def listener(args, seats, status):
    #create a consumer and producer
    consumer = KafkaConsumer(args.topic1, group_id=args.group, bootstrap_servers=[args.server + ':' + args.port])
    producer = KafkaProducer(bootstrap_servers=[args.server + ':' + args.port])

    #subscribe to the topic1
    consumer.assign([TopicPartition(args.topic1, i)])

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
                    updateStatus(status, msg["numberTickets"])
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

            producer.send(args.topic2, json.dumps(reply).encode('utf-8'), partition=msg["id"])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('topic1', help='Kafka topic1')
    parser.add_argument('topic2', help='Kafka topic2')
    parser.add_argument('group', help='Kafka consumer group')
    parser.add_argument('server', help='Kafka server')
    parser.add_argument('port', help='Kafka port')
    args = parser.parse_args()

    seats_collection, status_collection = connect()
    
    #create a thread to listen to the topic
    for i in range(0, 3):
        t = threading.Thread(target=listener, args=(args, seats_collection, status_collection))
        t.start()





"""
filter_query = {
    "$and": [
        {"occupied_1": {"$exists": True}},
        {"occupied_2": {"$exists": True}},
        {"occupied_3": {"$exists": True}},
        {"occupied_1": False},
        {"occupied_2": False},
        {"occupied_3": False},
    ]
}

# Define the update query
update_query = {"$set": {"occupied_1": True, "occupied_2": False, "occupied_3": True}}

# Check if all the fields match the filter query and update them
result = seats_collection.update_many({"$and": [filter_query]}, update_query)

if result.modified_count > 0:
    print("All fields have been updated.")
else:
    print("No fields were updated.")
"""
seats_collection, status_collection = connect()
res = bookSeat(seats_collection, status_collection, [1, 2, 3])
res = cancelSeat(seats_collection, status_collection, [1, 2, 3])
print(res)
