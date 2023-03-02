from confluent_kafka import Consumer, KafkaError
from multiprocessing import Process
import pymongo
import argparse

def create_database():
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    db = client['mydatabase']

    return db

def initiate_database(db):
    # Create collection
    seat_db = db['seats']
    movie_db = db['movies']

    # Insert
    seat_db.insert_many(
        [
            {'seat': 'A', 'status': 0}, 
            {'seat': 'B', 'status': 0}, 
            {'seat': 'C', 'status': 0}, 
            {'seat': 'D', 'status': 0}, 
            {'seat': 'E', 'status': 0}, 
            {'seat': 'F', 'status': 0}, 
            {'seat': 'G', 'status': 0}, 
            {'seat': 'H', 'status': 0}, 
            {'seat': 'I', 'status': 0}, 
        ]
    )
    movie_db.inssert_one({"movie": 1, "seatLeft": 9})
    
    return db

def book_movie(db, message):
    movie_db = db['movies']
    seat_db = db['seats']
    if len(message['seat']) > movie_db.find({'movie': message['movie']})['seatLeft']:
        return 'Not enough seat'

    # TODO book seat



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap-server', help='Kafka bootstrap server', default='localhost:9092')
    parser.add_argument('--topic', help='Kafka topic', default='movie')
    args = parser.parse_args()

    # Create database
    db = create_database()

    db = initiate_database(db)

    # Create consumer
    consumer = Consumer({
        'bootstrap.servers': args.bootstrap_server,
        'group.id': 'mygroup',
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe([args.topic])

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        print('Received message: {}'.format(msg.value().decode('utf-8')))

        book_movie(msg.value().decode('utf-8'))