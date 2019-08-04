# after delete the old air db, run chmod +x <fileName> and ./setupdb.sh in shell
mongoimport --db air --collection airlines --type csv --headerline --file ./airlines.csv
mongoimport --db air --collection airports --type csv --headerline --file ./airports.csv
mongoimport --db air --collection cancellations --type csv --headerline --file ./cancellations.csv
mongoimport --db air --collection flights --type csv --headerline --file ./flights.csv
