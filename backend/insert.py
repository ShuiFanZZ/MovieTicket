import pymongo

conn_str = "mongodb+srv://shuifanzz:shuifanzz@cluster0.hksonbt.mongodb.net/?retryWrites=true&w=majority"
client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
try:
    print(client.server_info())
except Exception:
    print("Unable to connect to the server.")

db = client["p1"]
seats_collection = db["seat_map"]

seats_collection.create_index("seat_number")

# Create a compound index on the "seat_number" field
seats_collection.create_index("seat_number")

# Create a list of new seats to insert into the collection
new_seats = []
for i in range(0, 20):
    new_seat = {
        "seat_number": i,
        "occupied": False
    }
    new_seats.append(new_seat)
result = seats_collection.insert_many(new_seats)






