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
    """
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
    """
    updateStatus(status_collection, -seats)
    return True


def getTicketsLeft(status_collection, numberTickets):
    """
    get the number of available seats
    """
    status_result = status_collection.find()
    number_of_available_seats = status_result[0]["total_seat"] - status_result[0]["occupied_seat"]
    return number_of_available_seats


def getAvailability(seats_collection):
    """
    get the number and list of available seats
    """
    available_seats = []
    seat_result = seats_collection.find()
    seat_idx = 0
    for seat in seat_result[0].keys():
        if (seat[0] == "_"):
            continue
        seat_idx = int(seat[9:])
        if not seat_result[0][seat]:
            available_seats.append(seat_idx)
    return available_seats
    

def updateStatus(status_collection, occupied):
    """
    update the number of occupied seats
    """
    filter_query = {"occupied_seat": {"$lte": 20 - occupied}}
    update_query = {"$inc": {"occupied_seat": occupied}}
    res = status_collection.update_one(filter_query, update_query)
    return res.modified_count
    #print(res.modified_count)


def listener(topic2, seats, status, producer, msg):


    #listen to the topic1
    msg = msg.value
    reply = msg
    print("Before reply: ", reply)
    msg_type = msg["mode"]

    #return the number of available seats
    if msg_type == "seat_request":
        flag = updateStatus(status, msg["numberTickets"])
        if flag == 0:
            reply["status"] = "fail"
        else:
            reply["available_seats"] = getAvailability(seats)
            reply["status"] = "success"
    
    #cancel the booking
    elif msg_type == "seat_cancel":
        if cancelSeat(seats, status, msg["numberTickets"]):
            reply["status"] = "success"
        else:
            reply["status"] = "fail"

    #book the seats if seats are available
    elif msg_type == "seat_confirm":
        if bookSeat(seats, status, msg["seat"]):
            reply["status"] = "success"
        else:
            reply["status"] = "fail"
            reply["available_seats"] = getAvailability(seats)

    print("After reply: ", reply)
    print("")
    producer.send(topic2, reply, partition=msg["id"])



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--topic1', help='Kafka topic1', default="request")
    parser.add_argument('--topic2', help='Kafka topic2', default="response")
    parser.add_argument('--server', help='Kafka server', default="")
    args = parser.parse_args()
    
    #connect to the database
    seats, status = connect()

    #create a consumer and producer
    producer = KafkaProducer(bootstrap_servers="169.234.248.230:9092", value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    # Create consumer
    consumer = KafkaConsumer(
        "request",
        bootstrap_servers="169.234.248.230:9092", 
        value_deserializer=lambda m: json.loads(m.decode('utf-8')), 
        auto_offset_reset='earliest',
        max_poll_records=1
    )

    while True:
        for message in consumer:
            t = threading.Thread(target=listener, args=(args.topic2, seats, status, producer, message))
            t.start()

    """
    seats_collection, status_collection = connect()
    
    #create a thread to listen to the topic
    for i in range(0, 3):
        t = threading.Thread(target=listener, args=(args, seats_collection, status_collection))
        t.start()
    """




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

"""
seats_collection, status_collection = connect()
res = bookSeat(seats_collection, status_collection, [1, 2, 3])
res = cancelSeat(seats_collection, status_collection, [1, 2, 3])
print(res)
"""

"""
def receiveMsg(consumer):
    for message in consumer:
        message = message.value
        print(f"Received message: {message}")
        return message

consumer = KafkaConsumer(
    "request",
    bootstrap_servers="169.234.248.230:9092", 
    value_deserializer=lambda m: json.loads(m.decode('utf-8')), 
    auto_offset_reset='earliest',
    max_poll_records=1
)
consumer.assign([TopicPartition(args.consumer_topic, i)])
receiveMsg(consumer)
"""

"""
#producer = KafkaProducer(bootstrap_servers=args.bootstrap_server, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
producer = KafkaProducer(bootstrap_servers="169.234.248.230:9092", value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Create consumer
consumer = KafkaConsumer(
    "request",
    bootstrap_servers="169.234.248.230:9092", 
    value_deserializer=lambda m: json.loads(m.decode('utf-8')), 
    auto_offset_reset='earliest',
    max_poll_records=1
)
"""
main()


