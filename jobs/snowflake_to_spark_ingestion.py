import json
import sys
from pyspark.sql import SparkSession
from snowflake_utility import db_snowflake, find_s3_bucket

def data_ingestion(input_table_name, output_table_name):
    try:
        # Fetch Snowflake connection details
        bucket_name = find_s3_bucket()
        sfOptions = db_snowflake()
        sfOptions['sfDatabase'] = 'DEMO'
        sfOptions['sfSchema'] = 'DEMO_SCHEMA'
    
        # Initialize Spark Session
        spark = SparkSession.builder \
            .appName("Snowflake Ingestion") \
            .getOrCreate()
    
        # Read data from Snowflake
        print(f"Reading data from table: {input_table_name}")
        df = spark.read.format("net.snowflake.spark.snowflake").options(**sfOptions).option(
            "dbtable", input_table_name).load()
    
        # Write data to Snowflake
        print(f"Writing data to table: {output_table_name}")
        df.write.format("net.snowflake.spark.snowflake").options(**sfOptions).option(
            "dbtable", output_table_name).mode("overwrite").save()
    
        print("Data ingestion completed successfully.")

    except Exception as e:
        print("Job Failed with " + str(e))
        sys.exit(1)

if __name__ == "__main__":
    input_table_name = sys.argv[1]
    output_table_name = sys.argv[2]
    data_ingestion(input_table_name, output_table_name)