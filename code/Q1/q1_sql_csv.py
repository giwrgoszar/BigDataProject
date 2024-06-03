from pyspark.sql import SparkSession
import time

# Create SparkSession and start timer
spark = SparkSession.builder.appName("Query 1 with SQL API & .csv files").getOrCreate()
sc = spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext.setLogLevel("ERROR")
start_time = time.time()

HDFS_path_for_2010_2019 = "hdfs://master:9000/bigdata/Crime_Data_from_2010_to_2019.csv"
HDFS_path_for_2020_today = "hdfs://master:9000/bigdata/Crime_Data_from_2020_to_Present.csv"

## Load CSV data and create temporary views
for year, file_path in [("2010_2019", HDFS_path_for_2010_2019), ("2020_today", HDFS_path_for_2020_today)]:
    spark.sql(f"""
        CREATE OR REPLACE TEMPORARY VIEW crime_data_{year}
        USING csv
        OPTIONS (
            path "{file_path}",
            header "true",
            inferSchema "true"
        )
    """)

# Union of crime data
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW unioned_crime_data AS
    SELECT * FROM crime_data_2010_2019
    UNION ALL
    SELECT * FROM crime_data_2020_today
""")

# Extract year and month, calculate crime count, and rank
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW ranked_crime_total AS
    SELECT 
        YEAR(to_date(`DATE OCC`, 'MM/dd/yyyy hh:mm:ss a')) AS Year,
        MONTH(to_date(`DATE OCC`, 'MM/dd/yyyy hh:mm:ss a')) AS Month,
        COUNT(*) AS Crime_Total
    FROM 
        unioned_crime_data
    GROUP BY
        Year, Month
""")

# Add a ranking column for each year based on the crime count
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW ranked_crime_total_with_rank AS
    SELECT 
        Year, Month, Crime_Total,
        ROW_NUMBER() OVER (PARTITION BY Year ORDER BY Crime_Total DESC) AS Ranking
    FROM 
        ranked_crime_total
""")

# Filter for the top 3 months for each year
result = spark.sql("""
    SELECT Year, Month, Crime_Total, Ranking
    FROM ranked_crime_total_with_rank
    WHERE Ranking <= 3
""")

# Print results
result.show(result.count(), truncate=False)

# Calculate time and stop session
end_time = time.time()
Total_time = end_time - start_time
print("Answer for Query 1 with\nSQL API & .csv files:\n")
print(f"Total time: {Total_time:.2f} sec")
spark.stop()