import time
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Query 2 with SQL API").getOrCreate()
spark.catalog.clearCache()
HDFS_path_for_csv_2010_2019 = "hdfs://master:9000/bigdata/Crime_Data_from_2010_to_2019.csv"
HDFS_path_for_csv_2020_today = "hdfs://master:9000/bigdata/Crime_Data_from_2020_to_Present.csv"
start_time = time.time()
data_2010_2019 = spark.read.format("csv").options(header='true', inferSchema='true').load(HDFS_path_for_csv_2010_2019)
data_2020_today = spark.read.format("csv").options(header='true', inferSchema='true').load(HDFS_path_for_csv_2020_today)

# Register temporary tables
data_2010_2019.createOrReplaceTempView("data_2010_2019")
data_2020_today.createOrReplaceTempView("data_2020_today")

query = """
    SELECT
       CASE
            WHEN `TIME OCC` >= 500 AND `TIME OCC` < 1200 THEN 'Morning (05:00 - 11:59)'
            WHEN `TIME OCC` >= 1200 AND `TIME OCC` < 1700 THEN 'Afternoon (12:00 - 16:59)'
            WHEN `TIME OCC` >= 1700 AND `TIME OCC` < 2100 THEN 'Evening (17:00 - 20:59)'
            ELSE 'Night (21:00 - 04:59)'
        END AS Time_Segment,
        COUNT(*) AS Crime_Count
    FROM (
        SELECT
           `TIME OCC`
        FROM
            data_2010_2019
        WHERE
            `PREMIS DESC` = 'STREET' AND `TIME OCC` IS NOT NULL
        UNION ALL
        SELECT
            `TIME OCC`
        FROM
            data_2020_today
        WHERE
            `PREMIS DESC` = 'STREET' AND `TIME OCC` IS NOT NULL
    ) combined_data
    GROUP BY
        Time_Segment
    ORDER BY
        Crime_Count DESC
"""
result = spark.sql(query)
print("***************************************************\n")
print(" Answer for Query 2 is:\n}")
result.show()
end_time = time.time()
Total_time = end_time - start_time
print(f"Total time to execute Query with SQL API: {Total_time} seconds")
spark.stop()



