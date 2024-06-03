from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder.appName("CSV to Parquet").getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

# HDFS directory containing the CSV files
input_path = "hdfs://master:9000/bigdata/"

# HDFS directory to store the Parquet files
output_path = "hdfs://master:9000/parquet/"

# Read CSV files from the input directory
crime_data_10_19_df = spark.read.option("header", "true").option("inferSchema","true"). \
      csv("hdfs://master:9000/bigdata/Crime_Data_from_2010_to_2019.csv")
crime_data_20_present_df = spark.read.option("header", "true").option("inferSchema","true"). \
      csv("hdfs://master:9000/bigdata/Crime_Data_from_2020_to_Present.csv")

# Write the DataFrame as Parquet files
crime_data_10_19_df.write.parquet("hdfs://master:9000/bigdata/Crime_Data_from_2010_to_2019.parquet")
crime_data_20_present_df.write.parquet("hdfs://master:9000/bigdata/Crime_Data_from_2020_to_Present.parquet")

# Stop the SparkSession
spark.stop()

