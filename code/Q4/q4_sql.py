from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, avg, count
from pyspark.sql.types import FloatType
import time
import math

def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371.0
    return c * r

# Create a SparkSession
spark = SparkSession.builder.appName("Query 4 with SQL API").getOrCreate()
spark.catalog.clearCache()

# Define file paths
HDFS_path_for_csv_2010_2019 = "hdfs://master:9000/bigdata/Crime_Data_from_2010_to_2019.csv"
HDFS_path_for_csv_2020_today = "hdfs://master:9000/bigdata/Crime_Data_from_2020_to_Present.csv"
HDFS_police_station_path = "hdfs://master:9000/bigdata/LAPD_Police_Stations.csv"

start_time = time.time()

# Read CSV files into DataFrames
crime_2010_2019_df = spark.read.csv(HDFS_path_for_csv_2010_2019, header=True)
crime_2020_present_df = spark.read.csv(HDFS_path_for_csv_2020_today, header=True)
police_station_df = spark.read.csv(HDFS_police_station_path, header=True)
crime_df = crime_2010_2019_df.union(crime_2020_present_df)

crime_df = crime_df.filter((col('LAT') != '0') &
                           (col('LON') != '0') &
                           col('Weapon Used Cd').startswith('1')).select('AREA ', 'AREA NAME', 'LAT', 'LON')

police_station_df = police_station_df.select('PREC', 'X', 'Y')

joined_df = crime_df.join( police_station_df, col('AREA ').cast('int') == col('PREC').cast('int'),'inner')

haversine_udf = udf(haversine, FloatType())

joined_df = joined_df.withColumn('Distance', haversine_udf(col('LAT').cast(FloatType()),
                                                           col('LON').cast(FloatType()),
                                                           col('Y').cast(FloatType()),
                                                           col('X').cast(FloatType())))
result_df = joined_df.groupBy('AREA NAME').agg(
     avg('Distance').alias('AverageDistance'),
     count('*').alias('TotalIncidents'))

result_df = result_df.withColumnRenamed('AREA NAME', 'Division')
result_df = result_df.orderBy(col('TotalIncidents').desc())

result_df.show(21)
end_time = time.time()
Total_time = end_time - start_time
print(f"Total time for Query 4 with SQL API : {Total_time} seconds")





