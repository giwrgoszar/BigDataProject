from pyspark.sql import SparkSession
import time

# Create SparkSession
spark = SparkSession.builder \
    .appName("Query 2 with RDD API") \
    .getOrCreate()

# Function to classify time segments
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

# Define HDFS paths for the CSV files
HDFS_path_for_csv_2010_2019 = "path_to_your_csv_file_2010_2019"
HDFS_path_for_csv_2020_today = "path_to_your_csv_file_2020_today"

start_time = time.time()

# Process data from 2010-2019 CSV file
result_2010_2019 = spark.sparkContext.textFile(HDFS_path_for_csv_2010_2019) \
    .map(lambda x: x.split(",")) \
    .filter(lambda x: (x[15] == "STREET") and (x[3] != "")) \
    .map(lambda x: (classify_time(x[3]), 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .sortBy(lambda x: x[1], ascending=False)

# Process data from 2020-present CSV file
result_2020_present = spark.sparkContext.textFile(HDFS_path_for_csv_2020_today) \
    .map(lambda x: x.split(",")) \
    .filter(lambda x: (x[15] == "STREET") and (x[3] != "")) \
    .map(lambda x: (classify_time(x[3]), 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .sortBy(lambda x: x[1], ascending=False)

# Combine results from both periods
combined_result = result_2010_2019.union(result_2020_present) \
    .reduceByKey(lambda x, y: x + y) \
    .sortBy(lambda x: x[1], ascending=False)

end_time = time.time()
elapsed = end_time - start_time

# Collect and print the combined result
print(combined_result.collect())
print(f"Time elapsed: {elapsed} seconds")

# Stop SparkSession
spark.stop()
