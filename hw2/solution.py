#!/Users/yeeyoung/anaconda/bin/python
import sqlite3
import csv

# Open connection to the SQLite database file
con = sqlite3.connect("air.db")
# Create a cursor that will return an instance of the database for data retrival
cur = con.cursor()
# 1. Select all from the cancellations table
results = cur.execute("SELECT * FROM cancellations;").fetchall()
for row in results:
    print(row)
# 2. Select the first 10 rows of FULL_OP_UNIQUE_CARRIER column from the airlines table
results = cur.execute("SELECT FULL_OP_UNIQUE_CARRIER FROM airlines LIMIT 10;").fetchall()
for row in results:
    print(row)
# 3. Select the first 5 rows from the flights table where the OP_UNIQUE_CARRIER = AA
results = cur.execute("SELECT * FROM flights WHERE OP_UNIQUE_CARRIER='AA' LIMIT 5;").fetchall()
for row in results:
    print(row)
# 4. Select the first 5 rows from the flights table where the OP_UNIQUE_CARRIER = AA, MONTH = 3
results = cur.execute("SELECT * FROM flights WHERE OP_UNIQUE_CARRIER='AA' AND MONTH=3 LIMIT 5;").fetchall()
for row in results:
    print(row)
# 5. Select the row from the airlines table where the FULL_OP_UNIQUE_CARRIER = Titan Airways
results = cur.execute("SELECT * FROM airlines WHERE FULL_OP_UNIQUE_CARRIER='Titan Airways';").fetchall()
for row in results:
    print(row)
# 6. Select the longest flights to each unique destination and order it in decreasing order of distance. Select the 
# ORIGINAL_AIRPORT_ID, DEST_AIRPORT_ID, and MAX DISTANCE
# Update July 17: After ordering by decreasing distance, order by increasing origin id, and then by increasing destination id
results = cur.execute("SELECT ORIGIN_AIRPORT_ID, DEST_AIRPORT_ID, MAX(DISTANCE) FROM flights GROUP BY DEST_AIRPORT_ID ORDER BY MAX(DISTANCE) DESC, ORIGIN_AIRPORT_ID ASC, DEST_AIRPORT_ID ASC;")
for row in results:
    print(row)
'''
results = cur.execute("SELECT ORIGIN_AIRPORT_ID, DEST_AIRPORT_ID, MAX(DISTANCE) FROM flights GROUP BY DEST_AIRPORT_ID ORDER BY ORIGIN_AIRPORT_ID ASC;")
for row in results:
    print(row)
results = cur.execute("SELECT ORIGIN_AIRPORT_ID, DEST_AIRPORT_ID, MAX(DISTANCE) FROM flights GROUP BY DEST_AIRPORT_ID ORDER BY DEST_AIRPORT_ID ASC;")
for row in results:
    print(row)
'''
# 7. Select the destinations that have less than 10 arrivals. Select the DEST_AIRPORT_ID (Hint You will need to group the destinations.)
results = cur.execute("SELECT DEST_AIRPORT_ID FROM flights GROUP BY DEST_AIRPORT_ID HAVING COUNT(DEST_AIRPORT_ID)<10;")
for row in results:
    print(row)
# 8. Select all of the distinct DAY_OF_MONTH and order them in ascending order.
results = cur.execute("SELECT DISTINCT DAY_OF_MONTH FROM flights ORDER BY DAY_OF_MONTH ASC;")
for row in results:
    print(row)
