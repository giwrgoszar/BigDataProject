from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, year, col, count, desc, first, regexp_replace, udf
from pyspark.sql.types import StringType
import time

# Create SparkSession and start timer
spark = SparkSession.builder.appName("Query 3 with Auto optimizer join").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")
sc.setLogLevel("ERROR")
start_time = time.time()

# Define HDFS paths for the files and read as Dataframes
crime_2010_2019 = "hdfs://master:9000/bigdata/Crime_Data_from_2010_to_2019.parquet"
LA_income_2015 = "hdfs://master:9000/bigdata/LA_income_2015.csv"
revgeocoding = "hdfs://master:9000/bigdata/revgecoding.csv"

#Read crime .parquet dataset filtering out records with null or Uknown (X) values in the Vict Descent column,
#Convert the DATE OCC column to date format, extract the year, and filter for the year 2015
crime_df_2010_2019 = spark.read.parquet(crime_2010_2019, header=True, inferSchema=True) \
                               .filter((col("Vict Descent").isNotNull())) \
                               .filter((col("Vict Descent") != "X")) \
                               .withColumn("Date", to_date(col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a")) \
                               .filter(year(col("Date")) == 2015)

# Read the LA income data CSV file
income_df = spark.read.csv(LA_income_2015, header=True, inferSchema=True)

# Read the reverse geocoding CSV file and select only the first matching zip code for each set of coordinates
revgeocoding_df = spark.read.csv(revgeocoding, header=True, inferSchema=True) \
                            .groupBy("LAT", "LON").agg(first("ZIPcode").alias("ZIPcode"))

# Join the crime data with the reverse geocoding data based on latitude and longitude
crime_with_zip_df = crime_df_2010_2019.join(revgeocoding_df, (crime_df_2010_2019["LAT"] == revgeocoding_df["LAT"]) \
                                       & (crime_df_2010_2019["LON"] == revgeocoding_df["LON"]), "left")
#Join the result with the LA income data based on the zip code, removing the $ and ","" and converting to int 
#(e.g. $55,362 --> 55326)
crime_with_zip_income_df = crime_with_zip_df.join(income_df, crime_with_zip_df["ZIPcode"] == income_df["Zip Code"], "inner") \
                                            .withColumn("Median Income", regexp_replace(col("Estimated Median Income"), "[^\d]", "") \
                                            .cast("int"))

# Group the data by zip code and median income, calculate the total number of crimes  for each zip code
zip_code_stats = crime_with_zip_income_df.groupBy("Zip Code", "Community", "Median Income").agg(count("*").alias("Total Crimes"))

# Extract the top 3 and bottom 3 areas based on income and set in descending order
top_3_IncomeAreas = zip_code_stats.orderBy(desc("Median Income")).limit(3).collect()
bottom_3_IncomeAreas = zip_code_stats.orderBy("Median Income").limit(3).collect()

# Helper function to calculate victim descent for a given area
def calculate_victim_descent(area, rank, order): 
    # Dictionary for mapping victim descent codes to their full descriptions
    descent_mapping = {
        "A": "Other Asian", "B": "Black", "C": "Chinese", "D": "Cambodian", "F": "Filipino", "G": "Guamanian", "H": "Hispanic/Latin/Mexican",
        "I": "American Indian/Alaskan Native", "J": "Japanese", "K": "Korean", "L": "Laotian", "O": "Other", "P": "Pacific Islander", "S": "Samoan",
        "U": "Hawaiian", "V": "Vietnamese", "W": "White", "X": "Unknown", "Z": "Asian Indian"}

    zip_code, community, median_income = area["Zip Code"], area["Community"], area["Median Income"]

    # Define the caption for the printout
    order_desc = "highest" if order == "top" else "lowest"; rank_letter = "st" if rank == 1 else ("nd" if rank == 2 else "d")
    print(f"2015 Crime victims' descent for the {rank}{rank_letter} {order_desc} income area\n"
          f"{community}\nZip Code: {zip_code},  Median Income: ${median_income}")

    # Filter data for the given area
    area_data = crime_with_zip_income_df.filter(col("Zip Code") == zip_code)
    # Group by victim descent, calculate the total number of victims for each group and sort by descending order
    victim_descent_stats = area_data.groupBy("Vict Descent").agg(count("*").alias("Total Victims")).orderBy(desc("Total Victims"))
    # Define a UDF to map victim descent codes to descriptions
    map_descent_udf = udf(lambda code: f"{descent_mapping.get(code, 'Unknown')} ({code})", StringType())
    # Replace victim descent codes with descriptions and print results
    victim_descent_stats = victim_descent_stats.withColumn("Vict Descent", map_descent_udf("Vict Descent")).show(truncate=False) 
 
# Print victim descent for top 3 and bottom 3 income areas
[calculate_victim_descent(area, i, "top") for i, area in enumerate(top_3_IncomeAreas, 1)]
[calculate_victim_descent(area, i, "low") for i, area in enumerate(bottom_3_IncomeAreas, 1)]

# Calculate time and stop session
end_time = time.time()
Total_time = end_time - start_time
print("Answer for Query 3 with Auto Optimizer join:\n")
print(f"Total time: {Total_time:.2f} sec")
spark.stop()
