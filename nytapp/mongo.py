import os

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

MONGODB_CONN_STRING =  os.environ.get('MONGODB_CONN_STRING')


db = MongoClient(MONGODB_CONN_STRING)


for db_info in db.list_database_names():
   print(db_info)


# db.init.insert_one(
#     {
#         "item": "canvas",
#         "qty": 100,
#         "tags": ["cotton"],
#         "size": {"h": 28, "w": 35.5, "uom": "cm"},
#     }
# )


# collection_name = db["user_1_items"]

# item_1 = {
#   "_id" : "U1IT00001",
#   "item_name" : "Blender",
#   "max_discount" : "10%",
#   "batch_number" : "RR450020FRG",
#   "price" : 340,
#   "category" : "kitchen appliance"
# }

# item_2 = {
#   "_id" : "U1IT00002",
#   "item_name" : "Egg",
#   "category" : "food",
#   "quantity" : 12,
#   "price" : 36,
#   "item_description" : "brown country eggs"
# }
# collection_name.insert_many([item_1,item_2])
