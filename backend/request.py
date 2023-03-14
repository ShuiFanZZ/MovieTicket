"""

"""

from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import TopicPartition
import random
import argparse
import json
import multiprocess
from functools import partial
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
CANCEL_PROB = 0.1

random.seed(1)

def receive_response(consumer):
    for message in consumer:
        message = message.value
        #print(f"Received message: {message}")
        return message


def ticket_booking(i, args):
    # Create producer
    producer = KafkaProducer(bootstrap_servers=args.bootstrap_server, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    # Create consumer
    consumer = KafkaConsumer(bootstrap_servers=args.bootstrap_server, value_deserializer=lambda m: json.loads(m.decode('utf-8')), auto_offset_reset='earliest')
    
    # Assign consumer to a partition for each user
    consumer.assign([TopicPartition(args.consumer_topic, i)])
    
    # Randomly generate a request
    seat_number = random.randint(1, 3)
    request = {
        "id": i,
        "movieId": 1,
        "numberTickets": seat_number,
        "seat": [],
        "mode": "seat_request"
    }

    request_number = 1
    # Send request to Kafka
    producer.send(args.producer_topic, request, partition=i)

    message = receive_response(consumer)
    assert message["id"] == i, "Wrong user id"

    # If not enough seats, abort the booking
    if message["status"] == "fail":
        print(f"Not enough seats for user {i}")
        return i, [], request_number
     
    index = 0
    
    while True:
        # Randomly select seats
        request["seat"] = random.sample(message["available_seats"], seat_number)
        request["mode"] = "seat_confirm"

        # Send request to Kafka
        producer.send(args.producer_topic, request, partition=i)

        request_number += 1

        message = receive_response(consumer)
        assert message["id"] == i, "Wrong user id"
        
        if message["status"] == "success":
            print(f"Booking success for user {i} with seats {request['seat']}")
            return i, request['seat'], request_number

        # Randomly cancel a request
        if random.random() < index * CANCEL_PROB: 
            request["mode"] = "seat_cancel"

            # Send request to Kafka
            producer.send(args.producer_topic, request, partition=i)

            request_number += 1

            message = receive_response(consumer)
            assert message["id"] == i, "Wrong user id"

            if message["status"] == "success":
                print(f"Cancelled booking for user {i}")
                return i, [], request_number

        index += 1

    return 

def main(args):
    

    # Randomly generate 20 requests and send to Kafka through multiple threads
    workers = multiprocess.cpu_count()
    seat_booking = {}
    total_requests = 0

    t1 = datetime.now()
    
    with ProcessPoolExecutor(max_workers=workers) as executor:

        for i, seat, request_number in executor.map(partial(ticket_booking, args=args), list(range(20))):
            if len(seat) > 0:
                seat_booking[i] = seat
            total_requests += request_number
    
    time_delta = datetime.now() - t1
    print(f"Time taken: {time_delta.total_second} seconds")
    print(f"Total requests: {total_requests}")
    print(f"Average requests per user: {total_requests / time_delta.total_second}")
    
    for user, seats in seat_booking.items():
        print(f"User {user} booked seats {seats}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-server", help="Kafka bootstrap server", default="localhost:9092")
    parser.add_argument("--consumer_topic", help="Kafka consumer topic", default="reponse")
    parser.add_argument("--producer_topic", help="Kafka producer topic", default="request")
    args = parser.parse_args()

    main(args)