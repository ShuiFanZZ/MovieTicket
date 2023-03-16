import pymongo

conn_str = "mongodb+srv://shuifanzz:shuifanzz@cluster0.hksonbt.mongodb.net/?retryWrites=true&w=majority"
client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
try:
    print("Good")
except Exception:
    print("Unable to connect to the server.")

db = client["p1"]
seats_collection = db["seat_map"]
status_collection = db["status"]

#update all fields to False
seats_collection.update_many({}, {"$set": {"occupied_0": False, "occupied_1": False, "occupied_2": False, "occupied_3": False, "occupied_4": False, "occupied_5": False, "occupied_6": False, "occupied_7": False, "occupied_8": False, "occupied_9": False, "occupied_10": False, "occupied_11": False, "occupied_12": False, "occupied_13": False, "occupied_14": False, "occupied_15": False, "occupied_16": False, "occupied_17": False, "occupied_18": False, "occupied_19": False}})
status_collection.update_many({}, {"$set": {"total": 20, "occupied": 0}})