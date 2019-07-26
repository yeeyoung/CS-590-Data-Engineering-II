import sqlite3
import csv

# Insert the following records into the table flights
# a. (2020,1,1,6,'','','','','','','','','','','','','','','','','','','','','','','','')
# b. (2020,1,2,7,'','','','','','','','','','','','','','','','','','','','','','','','')
# c. (2020,1,2,7,'TP','','','','','','','','','','','','','','','','','','','','','','','')

con = sqlite3.connect("air.db")
cur = con.cursor()
#'''
#cur.execute("INSERT INTO flights VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);",(2020,1,1,6,'','','','','','','','','','','','','','','','','','','','','',''))
#cur.execute("INSERT INTO flights VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);",(2020,1,2,7,'','','','','','','','','','','','','','','','','','','','','',''))
#cur.execute("INSERT INTO flights VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);",(2020,1,2,7,'TP','','','','','','','','','','','','','','','','','','','','',''))
#con.commit()
#results = cur.execute("SELECT * FROM flights WHERE YEAR='2020';").fetchall()
#print("QUESTION 1")
#for row in results:
#    print(row)
#cur.execute("INSERT INTO flights VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);",(2020,1,1,6,'','','','','','','','','','','','','','','','','','','','','',''))
#cur.execute("INSERT INTO flights VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);",(2020,1,2,7,'','','','','','','','','','','','','','','','','','','','','',''))
#con.commit()
#results = cur.execute("SELECT * FROM flights WHERE YEAR='2020';").fetchall()
#for row in results:
#    print(row)

cur.execute("DELETE FROM flights WHERE YEAR='2020';")
con.commit()
cur.execute("INSERT INTO flights VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);",(2020,1,1,6,'','','','','','','','','','','','','','','','','','','','','',''))
cur.execute("INSERT INTO flights VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);",(2020,1,1,6,'','','','','','','','','','','','','','','','','','','','','',''))
cur.execute("INSERT INTO flights VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);",(2020,1,2,7,'','','','','','','','','','','','','','','','','','','','','',''))
cur.execute("INSERT INTO flights VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);",(2020,1,1,6,'','','','','','','','','','','','','','','','','','','','','',''))
cur.execute("INSERT INTO flights VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);",(2020,1,2,7,'','','','','','','','','','','','','','','','','','','','','',''))
cur.execute("INSERT INTO flights VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);",(2020,1,2,7,'TP','','','','','','','','','','','','','','','','','','','','',''))
con.commit()
results = cur.execute("SELECT * FROM flights WHERE YEAR='2020';").fetchall() 
print("QUESTION 1")
for row in results:
    print(row)

cur.execute("DELETE FROM flights WHERE YEAR='2020';")
con.commit()
cur.execute("INSERT INTO flights VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);",(2020,1,1,6,'','','','','','','','','','','','','','','','','','','','','',''))
cur.execute("INSERT INTO flights VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);",(2020,1,2,7,'','','','','','','','','','','','','','','','','','','','','',''))
cur.execute("INSERT INTO flights VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);",(2020,1,2,7,'TP','','','','','','','','','','','','','','','','','','','','',''))
con.commit()
# Update the OP_UNIQUE_CARRIER of the table flights to be 'CS' for YEAR=2020. The print all flights where YEAR = 2020 (make sure commit() is used)
cur.execute("UPDATE flights SET OP_UNIQUE_CARRIER='CS' WHERE YEAR='2020';")
con.commit()
results = cur.execute("SELECT * FROM flights WHERE YEAR='2020';").fetchall()
print("QUESTION 2")
for row in results:
    print(row)

# Delete the flight that has the OP_UNIQUE_CARRIER of 'CS'. Then print all flights where YEAR=2020
cur.execute("DELETE FROM flights WHERE OP_UNIQUE_CARRIER='CS';")
con.commit()
results = cur.execute("SELECT * FROM flights WHERE YEAR='2020';").fetchall()
print("QUESTION 3")
for row in results:
    print(row)

# Select the OP_CARRIER_FL_NUM, ORIGIN_AIRPORT_ID, airports.FULL_AIRPORT_ID, and DISTANCE of flights that have trips with more than 4000 miles 
# tables = flights+airports, columns to join = ORIGIN_AIRPORT_ID, AIRPORT_ID, columns to match=DISTANCE. Use an INNER JOIN. To only get the unique names, use the 
# keyword DISTINCT (i.e., SELECT DISTINCT name.)
results = cur.execute("SELECT DISTINCT flights.OP_CARRIER_FL_NUM, flights.ORIGIN_AIRPORT_ID, airports.FULL_AIRPORT_ID, flights.DISTANCE FROM (flights INNER JOIN airports ON flights.ORIGIN_AIRPORT_ID=airports.AIRPORT_ID) WHERE flights.DISTANCE>4000 ORDER BY flights.ORIGIN_AIRPORT_ID ASC;").fetchall()
print("QUESTION 4")
for row in results:
    print(row)

# Select all DISTINCT airport names (FULL_AIRPORT_ID) from the database where the airline "Delta Air Lines Inc." has as an Origin Airport (ORIGIN_AIRPORT_ID).
results = cur.execute("SELECT DISTINCT airports.FULL_AIRPORT_ID FROM (airports INNER JOIN flights ON airports.AIRPORT_ID=flights.ORIGIN_AIRPORT_ID) WHERE flights.OP_UNIQUE_CARRIER='DL';").fetchall()
print("QUESTION 5")
for row in results:
    print(row)

# Select all DISTINCT carrier names (FULL_OP_UNIQUE_CARRIER) from the database, where flights in originating from "New York, NY:LaGuardia" were cancelled due to "Weather".
results = cur.execute("SELECT DISTINCT airlines.FULL_OP_UNIQUE_CARRIER FROM (((airlines INNER JOIN flights ON airlines.OP_UNIQUE_CARRIER=flights.OP_UNIQUE_CARRIER) INNER JOIN airports ON flights.ORIGIN_AIRPORT_ID=airports.AIRPORT_ID) INNER JOIN cancellations ON flights.CANCELLATION_CODE=cancellations.CODE) WHERE airports.FULL_AIRPORT_ID='New York, NY: LaGuardia' AND cancellations.CODE_DESCRIPTION='Weather';").fetchall()
print("QUESTION 6")
for row in results:
    print(row)

# Drop the airlines table from the database. Print the names of all the remaining tables in the database
cur.execute("DROP TABLE IF EXISTS airlines;")
results = cur.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
print("QUESTION 7")
for row in results:
    print(row)


