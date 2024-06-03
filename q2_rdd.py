from pyspark.sql import SparkSession
import time
import csv
spark = SparkSession.builder \
    .appName("Query 2 with RDD API") \
    .getOrCreate()
    
sc = spark.sparkContext
sc.setLogLevel("WARN")

start_time = time.time()    
HDFS_path_for_csv_2010_2019 = "hdfs://master:9000/bigdata/Crime_Data_from_2010_to_2019.csv"
HDFS_path_for_csv_2020_today = "hdfs://master:9000/bigdata/Crime_Data_from_2020_to_Present.csv"

def parse_csv_row(row):
    return list(csv.reader([row]))[0]

def classify_time(record):
    time_occ = int(record)  # The TIME OCC column is the fourth column
    # Classify time segments
    if time_occ >= 500 and time_occ < 1200:
        return "Morning (05:00 - 11:59)"
    elif time_occ >= 1200 and time_occ < 1700:
        return "Afternoon (12:00 - 16:59)"
    elif time_occ >= 1700 and time_occ < 2100:
        return "Evening (17:00 - 20:59)"
    else:
        return "Night (21:00 - 04:59)"    
data_2010_2019 = sc.textFile(HDFS_path_for_csv_2010_2019)
data_2020_today = sc.textFile(HDFS_path_for_csv_2020_today)

result = data_2010_2019.union(data_2020_today)\
                        .map(parse_csv_row)\
                        .filter(lambda x: (x[15] == "STREET") and (x[3].strip() != '')) \
                        .map(lambda x: (classify_time(x[3].strip()), 1)) \
                        .reduceByKey(lambda x, y: x + y) \
                        .sortBy(lambda x: x[1], ascending=False)
print("************************************")                                                
print(result.collect())
end_time = time.time()
Total_time = end_time - start_time
print(f"Total time for Query 2 with RDD API:{Total_time}")
print("************************************")  
