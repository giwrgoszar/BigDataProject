import math
from pyspark.sql import SparkSession, Row
import csv
import time

HDFS_path_for_csv_2010_2019 = "hdfs://master:9000/bigdata/Crime_Data_from_2010_to_2019.csv"
HDFS_path_for_csv_2020_today = "hdfs://master:9000/bigdata/Crime_Data_from_2020_to_Present.csv"
HDFS_police_station_path = "hdfs://master:9000/bigdata/LAPD_Police_Stations.csv"

def parse_csv_row(row):
    return list(csv.reader([row]))[0]

def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371.0
    return c * r

spark = SparkSession.builder \
    .appName("Broadcast Query 4 with RDD") \
    .getOrCreate()
spark.catalog.clearCache()
sc = spark.sparkContext
sc.setLogLevel("WARN")

start_time = time.time()

data_2010_2019 = sc.textFile(HDFS_path_for_csv_2010_2019).map(parse_csv_row)
data_2020_present = sc.textFile(HDFS_path_for_csv_2020_today).map(parse_csv_row)\

combined_la_crime_data = data_2010_2019.union(data_2020_present) \
    .filter(lambda x: (x[26] !='0' and x[26]!= 'LAT') and (x[27] !='0' and x[27]!= 'LON') and x[16].startswith("1"))\
    .map(lambda x: (int(x[4]),x[5],x[26],x[27]))

data_police_station = sc.textFile(HDFS_police_station_path) \
    .map(parse_csv_row)\
    .filter(lambda x: x[0]!='X')\
    .map(lambda x: (x[1],x[0], int(x[5])))\
    .keyBy(lambda x: x[2]) \
    .collectAsMap()

broadcast_police_stations = sc.broadcast(data_police_station)

def join_datasets(row):
    area, area_name, lat, lon = row
    lat_police_station = broadcast_police_stations.value[area][0]
    lon_police_station = broadcast_police_stations.value[area][1]
    return area, area_name, lat, lon, lat_police_station, lon_police_station

join_datasets = combined_la_crime_data.map(join_datasets)

result = join_datasets.map(lambda x: (x[1], haversine(float(x[2]), float(x[3]), float(x[4]), float(x[5]))))\
                      .mapValues(lambda distance: (distance, 1)) \
                      .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
                      .mapValues(lambda x: (x[0] / x[1], x[1]))\
                      .sortBy(lambda x: x[1][1], ascending=False)\
                      .map(lambda x: Row(Division=x[0], AverageDistance=x[1][0], TotalIncidents=x[1][1]))
df = spark.createDataFrame(result)
df.show(21)
end_time = time.time()
Total_time = end_time - start_time
print(f"Total time for Query 4 with RDD API and Broadcast join is: {Total_time} seconds")
spark.stop()


