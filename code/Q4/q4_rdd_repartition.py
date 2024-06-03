from pyspark.sql import SparkSession, Row
import csv
import time
import math

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
    .appName("Repartition Query 4 with RDD") \
    .getOrCreate()
spark.catalog.clearCache()
sc = spark.sparkContext
sc.setLogLevel("WARN")
start_time = time.time()

data_2010_2019 = sc.textFile(HDFS_path_for_csv_2010_2019).map(parse_csv_row)

data_2020_present = sc.textFile(HDFS_path_for_csv_2020_today).map(parse_csv_row)

la_crime_data = data_2010_2019.union(data_2020_present) \
    .filter(lambda x: (x[26] !='0' and x[26]!= 'LAT') and (x[27] !='0' and x[27]!= 'LON') and x[16].startswith("1"))\
    .map(lambda x: (int(x[4]),x[5],x[26],x[27]))

data_police_station = spark.sparkContext.textFile(HDFS_police_station_path) \
    .map(parse_csv_row)\
    .filter(lambda x: x[0]!='X')\
    .map(lambda x: (x[1],x[0], int(x[5])))

la_crime_data_tagged = la_crime_data.map(lambda x: (x[0],("la_crime",x[1],x[2],x[3])))

data_police_station_tagged = data_police_station.map(lambda x: (x[2],("la_police_station",x[0],x[1])))

combine_tagged = la_crime_data_tagged.union(data_police_station_tagged).groupByKey()


def produce_pairs_for_join(x):
    area_id = x[0]
    rec_list = x[1]
    la_crime_list = []
    police_station_list = []
    for rec in rec_list:
        if rec[0] == "la_crime":
            la_crime_list.append(rec[1:])
        elif rec[0] == "la_police_station":
            police_station_list.append(rec[1:])
    result_join = []
    for la_crime in la_crime_list:
        for police_station in police_station_list:
            result_join.append((area_id,) + la_crime + police_station)
    return result_join

join_datasets = combine_tagged.flatMap(produce_pairs_for_join)

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

