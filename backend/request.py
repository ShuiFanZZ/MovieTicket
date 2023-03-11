"""

"""

from kafka import KafkaConsumer, KafkaProducer
import random
import argparse

CANCEL_PROB = 0.1


def receive_response(consumer):
    while True:
        message = consumer.poll()
        message = message.value.decode("utf-8")
        print(f"Received message: {message}")
        break
    return message


def ticket_booking(producer, consumer, i):
    # Randomly generate a request
    seat_number = random.randint(1, 3)
    request = {
        "id": i,
        "movieId": 1,
        "numberTickets": seat_number,
        "seat": [],
        "mode": "seat_request"
    }

    # Send request to Kafka
    producer.send(args.producer_topic, str(request).encode("utf-8"))

    message = receive_response(consumer)
    
    # If not enough seats, abort the booking
    if message["status"] == "not_enough_seats":
        print(f"Not enough seats for user {i}")
        return
    
    while True:
        # Randomly select seats
        request["seat"] = random.sample(message["available_seats"], seat_number)
        request["mode"] = "seat_confirm"

        # Send request to Kafka
        producer.send(args.producer_topic, str(request).encode("utf-8"))

        message = receive_response(consumer)
        
        if message["status"] == "success":
            print(f"Booking success for user {i} with seats {request['seat']}")
            return

        # Randomly cancel a request
        if random.random() < CANCEL_PROB:
            request["mode"] = "seat_cancel"

        # Send request to Kafka
        producer.send(args.producer_topic, str(request).encode("utf-8"))

        message = receive_response(consumer)

        if message["status"] == "success":
            print(f"Cancelled booking for user {i}")
            return

def main(args):
    # Create producer
    producer = KafkaProducer(bootstrap_servers=args.bootstrap_server)

    # Create consumer
    consumer = KafkaConsumer(
        args.consumer_topic,
        bootstrap_servers=args.bootstrap_server,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="my-group",
    )

    # Randomly generate 100 requests and send to Kafka through multiple threads
    for i in range(5):
        ticket_booking(producer, consumer, i)


    

    for message in consumer:
        message = message.value.decode("utf-8")
        print(f"Received message: {message}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-server", help="Kafka bootstrap server", default="localhost:9092")
    parser.add_argument("--consumer_topic", help="Kafka consumer topic", default="reponse")
    parser.add_argument("--producer_topic", help="Kafka producer topic", default="request")
    args = parser.parse_args()

    main(args)