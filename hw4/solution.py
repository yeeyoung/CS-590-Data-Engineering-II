#HOMEWORK 4
import sqlite3
import csv
import pymongo
from pymongo import MongoClient
import pprint
#con = sqlite3.connect("air.db")
#cur = con.cursor()

#cur.execute(".headers on")
#cur.execute(".mode csv")
#cur.execute(".output airlines.csv")
#cur.execute("SELECT * FROM airlines;")
#cur.execute(".output airports.csv")
#cur.execute("SELECT * FROM airports;")
#cur.execute(".output cancellations.csv")
#cur.execute("SELECT * FROM cancellations;")
#cur.execute(".output flights.csv")
#cur.execute("SELECT * FROM flights;")
# mongoimport --db air --collection airlines --type csv --headerline --file ./airlines.csv
# mongoimport --db air --collection airports --type csv --headerline --file ./airports.csv
# mongoimport --db air --collection cancellations --type csv --headerline --file ./cancellations.csv
# mongoimport --db air --collection flights --type csv --headerline --file ./flights.csv
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["air"]
outputOrder = [("DAY_OF_WEEK", pymongo.DESCENDING), ("ARR_DELAY", pymongo.ASCENDING), ("TAIL_NUM", pymongo.DESCENDING), ("AIR_TIME", pymongo.ASCENDING)]
# print list of database
#dblist = client.list_database_names()
#print(dblist)
#print list of collections
#collist = db.list_collection_names()
#print(collist)


print("QUESTION 1")
#TODO
collection = db.flights
cursor = collection.find({"$and": [
    {"OP_UNIQUE_CARRIER": "MQ"},
    {"DAY_OF_MONTH": 1}
    ]},{'_id':False}).sort(outputOrder)
cursor.limit(5)
for doc in cursor:
    print(doc)

print("QUESTION 2")
#TODO
cursor2 = collection.find({"$and": [
    {"OP_UNIQUE_CARRIER": "MQ"},
    {"MONTH": 5},
    {"DAY_OF_MONTH": 5}
    ]},{'_id':False}).sort(outputOrder)
cursor2.limit(15)
for doc in cursor2:
    print(doc)

#print("QUESTION 3")
#TODO
result3 = collection.insert_one({"YEAR":2020, "MONTH":9, "DAY_OF_MONTH":1, "DAY_OF_WEEK":7, "No schema":"Yes"})
#print("QUESTION 4")
#TODO
result4 = collection.insert_many([{"YEAR":2020,"MONTH":10,"DAY_OF_MONTH":1,"DAY_OF_WEEK":3},
    {"YEAR":2020,"MONTH":10,"DAY_OF_MONTH":2,"DAY_OF_WEEK":4}])
print("QUESTION 5")
#TODO
cursor5 = collection.find({"YEAR":2020},{'_id':False}).sort(outputOrder)
for doc in cursor5:
    print(doc)
#print("QUESTION 6")
#TODO
collection.update({"YEAR":2020},{"$set":{"OP_UNIQUE_CARRIER":"CS"}},multi=True)

print("QUESTION 7")
#TODO
cursor7 = collection.find({"OP_UNIQUE_CARRIER":"CS"},{'_id':False}).sort(outputOrder)
for doc in cursor7:
    print(doc)
#print("QUESTION 8")
#TODO
collection.delete_many({"OP_UNIQUE_CARRIER":"CS"})

print("QUESTION 9")
#TODO
cursor9 = collection.find({"OP_UNIQUE_CARRIER":"CS"},{'_id':False}).sort(outputOrder)
for doc in cursor9:
    print(doc)

#print("QUESTION 10")
#TODO

temp=collection.aggregate([
    {
        "$lookup":
        {
            "from": "airlines",
            "localField": "OP_UNIQUE_CARRIER",
            "foreignField": "OP_UNIQUE_CARRIER",
            "as": "airline"
            }
        },
    {
        "$out": "flights"
        }
    ])


#collection.aggregate([
#    {
#        "$lookup":
#        {
#            "from": "airlines",
#            "let": {"op_unique_carrier": "$OP_UNIQUE_CARRIER"},
#            "pipeline": [
#                {"$match":
#                    {"$expr":
#                        {
#                            "$eq": ["OP_UNIQUE_CARRIER", "$$op_unique_carrier"]
#                            }
#                        }
#                    },
#                {"$project": {"_id":1,"OP_UNIQUE_CARRIER":1,"FULL_OP_UNIQUE_CARRIER":1}}
#                ],
#            "as": "airline"
#            }
#        },
#    {
#        "$group":{"_id":"$_id", "airline":{"$push": "$airline"}}
#        },
#    {
#        "$out": "collection"
#        }
#    ])


print("QUESTION 11")

#TODO
cursor11 = db.flights.find({},{'_id':False}).sort(outputOrder)
cursor11.limit(10)
for doc in cursor11:
    print(doc)
#print("QUESTION111")
#for doc in temp:
#    print(doc)
#cursor12 = test.find({},{'_id':False}).sort(outputOrder)
#cursor12.limit(10)
#for doc in cursor12:
#    print(doc)

#print("QUESTION 12")
#TODO
temp1 = db.flights.aggregate([
    {
        "$lookup":{
            "from": "airports",
            "localField": "ORIGIN_AIRPORT_ID",
            "foreignField": "AIRPORT_ID",
            "as": "origin"
            }
        },
    {
        "$out": "flights"
        }
    ])
temp2 = db.flights.aggregate([
    {
        "$lookup":{
            "from": "airports",
            "localField": "DEST_AIRPORT_ID",
            "foreignField": "AIRPORT_ID",
            "as": "dest"
            }
        },
    {
        "$out": "flights"
        }
    ])

print("QUESTION 13")
#TODO
cursor13 = db.flights.find({},{'_id':False}).sort(outputOrder).limit(10)
for doc in cursor13:
    print(doc)
