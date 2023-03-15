import pymongo

conn_str = "mongodb+srv://shuifanzz:shuifanzz@cluster0.hksonbt.mongodb.net/?retryWrites=true&w=majority"
client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
try:
    print("Good")
except Exception:
    print("Unable to connect to the server.")

db = client["p1"]
seats_collection = db["seat_map"]

# Create a list of new seats to insert into the collection
new_seats = []
document = {}
for i in range(0, 20):
    key = "occupied_" + str(i) 
    document[key] = False

new_seats.append(document)
print(new_seats)
result = seats_collection.insert_many(new_seats)






