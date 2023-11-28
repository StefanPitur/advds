import pymysql
import csv

host = "database-ads-sap86.cgrre17yxw11.eu-west-2.rds.amazonaws.com"
port = 3306
user = "admin"
password = "SJ}4v6S7]jNk^v~"
database = "property_prices"

conn = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
cur = conn.cursor()

longitudeMin, longitudeMax, latitudeMin, latitudeMax, dateMin, dateMax = -2.3, -2.2, 52.3, 52.4, '2019-01-01', '2019-12-31'

print(longitudeMin, longitudeMax, latitudeMin, latitudeMax, dateMin, dateMax)
sql_query = f"""
    SELECT pp.db_id, pp.price, pp.date_of_transfer, pp.postcode, pp.property_type, pp.new_build_flag, pp.tenure_type, pp.locality, pp.town_city, pp.district, pp.county, pd.country, pd.latitude, pd.longitude
    FROM (
        SELECT postcode, country, latitude, longitude
        FROM postcode_data
        WHERE (longitude BETWEEN {str(longitudeMin)} AND {str(longitudeMax)}) AND (latitude BETWEEN {str(latitudeMin)} AND {str(latitudeMax)})
    ) pd
    INNER JOIN (
        SELECT db_id, price, date_of_transfer, postcode, property_type, new_build_flag, tenure_type, locality, town_city, district, county
        FROM pp_data
        WHERE date_of_transfer >= '{dateMin}' AND date_of_transfer <= '{dateMax}'
    ) pp
    ON pp.postcode = pd.postcode;
"""

cur.execute(sql_query)

rows = cur.fetchall()

csv_filename = "query_result.csv"

with open(csv_filename, 'w', newline='') as f:
    csv_writer = csv.writer(f)
    csv_writer.writerow([i[0] for i in cur.description])
    csv_writer.writerows(rows)

cur.close()
conn.close()