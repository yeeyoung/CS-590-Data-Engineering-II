#!/Users/yeeyoung/anaconda/bin/python
import sqlite3
import csv

# Open connection to the SQLite database file
con = sqlite3.connect("air.db")
# Create a cursor that will return an instance of the database for data set traversal
cur = con.cursor()
# Read the csv file and begin parsing it
with open('flights.csv', 'r') as flights_table:
    dr = csv.DictReader(flights_table, delimiter=',')
    to_db = [(i['YEAR'], i['MONTH'], i['DAY_OF_MONTH'], i["DAY_OF_WEEK"], i["OP_UNIQUE_CARRIER"], i["TAIL_NUM"], i["OP_CARRIER_FL_NUM"], i["ORIGIN_AIRPORT_ID"],
        i["DEST_AIRPORT_ID"], i["CRS_DEP_TIME"], i["DEP_TIME"], i["DEP_DELAY"], i["CRS_ARR_TIME"], i["ARR_TIME"], i["ARR_DELAY"], i["CANCELLED"], i["CANCELLATION_CODE"], 
        i["CRS_ELAPSED_TIME"], i["ACTUAL_ELAPSED_TIME"], i["AIR_TIME"], i["DISTANCE"], i["CARRIER_DELAY"], i["WEATHER_DELAY"], i["NAS_DELAY"], i["SECURITY_DELAY"], 
        i["LATE_AIRCRAFT_DELAY"]) for i in dr] 

print(to_db[0]) 
print(to_db[1])
cur.execute("DROP TABLE IF EXISTS flights;")
# Create tables in the SQLite database
cur.execute("CREATE TABLE flights (YEAR number, MONTH number, DAY_OF_MONTH number, DAY_OF_WEEK number, OP_UNIQUE_CARRIER number, TAIL_NUM number, OP_CARRIER_FL_NUM number, ORIGIN_AIRPORT_ID number, DEST_AIRPORT_ID number, CRS_DEP_TIME number, DEP_TIME number, DEP_DELAY number, CRS_ARR_TIME number, ARR_TIME number, ARR_DELAY number, CANCELLED number, CANCELLATION_CODE number, CRS_ELAPSED_TIME number, ACTUAL_ELAPSED_TIME number, AIR_TIME number, DISTANCE number, CARRIER_DELAY number, WEATHER_DELAY number, NAS_DELAY number, SECURITY_DELAY number, LATE_AIRCRAFT_DELAY number);")
# Insert all parameter sequences to the table
cur.executemany("INSERT INTO flights VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);", to_db)
# Select Year from Table flights

with open('airlines.csv', 'r') as airlines_table:
    dr = csv.DictReader(airlines_table, delimiter=',')
    to_db1 = [(i['OP_UNIQUE_CARRIER'], i['FULL_OP_UNIQUE_CARRIER']) for i in dr]
print(to_db1[0])
print(to_db1[1])

cur.execute("DROP TABLE IF EXISTS airlines;")
cur.execute("CREATE TABLE airlines (OP_UNIQUE_CARRIER text, FULL_OP_UNIQUE_CARRIER text);")
cur.executemany("INSERT INTO airlines VALUES(?,?);", to_db1)

with open('airports.csv', 'r') as airports_table:
    dr = csv.DictReader(airports_table, delimiter=',')
    to_db2 = [(i['AIRPORT_ID'], i['FULL_AIRPORT_ID']) for i in dr]
print(to_db2[0])
print(to_db2[1])

cur.execute("DROP TABLE IF EXISTS airports;")
cur.execute("CREATE TABLE airports (AIRPORT_ID text, FULL_AIRPORT_ID text);")
cur.executemany("INSERT INTO airports VALUES(?,?);", to_db2)

with open('cancellations.csv', 'r') as cancellations_table:
    dr = csv.DictReader(cancellations_table, delimiter=',')
    to_db3 = [(i['CODE'], i['CODE_DESCRIPTION']) for i in dr]
print(to_db3[0])
print(to_db3[1])

cur.execute("DROP TABLE IF EXISTS cancellations;")
cur.execute("CREATE TABLE cancellations (CODE text, CODE_DESCRIPTION text);")
cur.executemany("INSERT INTO cancellations VALUES(?,?);", to_db3)

con.commit()
