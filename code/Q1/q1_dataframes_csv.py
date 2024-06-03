from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, year, month, count, row_number
from pyspark.sql.window import Window
import time

# Create SparkSession and start timer
spark = SparkSession.builder.appName("Query 1 with Dataframes API & .csv files").getOrCreate()
sc = spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext.setLogLevel("ERROR")
start_time = time.time()

# Define HDFS paths for the CSV files and read as Dataframes
HDFS_path_for_csv_2010_2019 = "hdfs://master:9000/bigdata/Crime_Data_from_2010_to_2019.csv"
HDFS_path_for_csv_2020_today = "hdfs://master:9000/bigdata/Crime_Data_from_2020_to_Present.csv"
crime_df_2010_2019 = spark.read.csv(HDFS_path_for_csv_2010_2019, header=True, inferSchema=True)
crime_df_2020_today = spark.read.csv(HDFS_path_for_csv_2020_today, header=True, inferSchema=True)

# Combine the DataFrames
crime_df = crime_df_2010_2019.union(crime_df_2020_today)

# Extract year and month from the DATE OCC column into new columns
crime_df = crime_df.withColumn('Date', to_date(crime_df['DATE OCC'], 'MM/dd/yyyy hh:mm:ss a'))
crime_df = crime_df.withColumn("Year", year(crime_df['Date'])).withColumn("Month", month(crime_df['Date']))

# Group by year and month to get the count of crimes
crime_total = crime_df.groupBy("Year", "Month").agg(count("*").alias("Crime_Total"))

# Define window specification for each year to rank months by crime count
window_spec = Window.partitionBy("Year").orderBy(crime_total["Crime_Total"].desc())

# Add a ranking column for each year based on the crime count
crime_total = crime_total.withColumn("Ranking", row_number().over(window_spec))

# Filter for the top 3 months for each year
result = crime_total.filter("Ranking <= 3")

# Print results
result.show(result.count(), truncate=False)

# Calculate time and stop session
end_time = time.time()
Total_time = end_time - start_time
print("Answer for Query 1 with\nDataframes API & .csv files:\n")
print(f"Total time: {Total_time:.2f} sec")
spark.stop()