mongoexport --db air --collection airlines --pretty --out airlines.json
mongoexport --db air --collection airports --pretty --out airports.json
mongoexport --db air --collection cancellations --pretty --out cancellations.json
# flights is so large, it is not pushed to github
mongoexport --db air --collection flights --pretty --out flights.json
