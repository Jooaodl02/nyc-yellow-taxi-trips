import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when, expr
from datetime import datetime, timezone


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


LAG_MONTHS = 2  # mesma defasagem m-2 do Lambda

def get_target_year_month(reference=None):
    ref = reference or datetime.now(timezone.utc)
    month = ref.month - LAG_MONTHS
    year = ref.year
    if month <= 0:
        month += 12
        year -= 1
    return str(year), f"{month:02d}"

year, month = get_target_year_month()   # em julho/2026 -> ("2026", "05")

df = spark.read.table("nyctaxi_bronze.nyc_taxi_yellowbronze")

df = df.filter((col("year") == year) & (col("month") == month))

# Select and transform fields, decoding codes and computing duration
df = df.select(
    # Map numeric VendorID to vendor names
    when(col("VendorID") == 1, "Creative Mobile Technologies, LLC")
      .when(col("VendorID") == 2, "Curb Mobility, LLC")
      .when(col("VendorID") == 6, "Myle Technologies Inc")
      .when(col("VendorID") == 7, "Helix")
      .otherwise("Unknown")
      .alias("vendor"),
    
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    # Calculate trip duration in minutes
    expr("timestampdiff(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime)").alias("trip_duration"),
    "passenger_count",
    "trip_distance",

    # Decode rate codes into readable rate types
    when(col("RatecodeID") == 1, "Standard Rate")
      .when(col("RatecodeID") == 2, "JFK")
      .when(col("RatecodeID") == 3, "Newark")
      .when(col("RatecodeID") == 4, "Nassau or Westchester")
      .when(col("RatecodeID") == 5, "Negotiated Fare")
      .when(col("RatecodeID") == 6, "Group Ride")
      .otherwise("Unknown")
      .alias("rate_type"),
    
    "store_and_fwd_flag",
    # alias columns for consistent naming convention
    col("PULocationID").alias("pu_location_id"),
    col("DOLocationID").alias("do_location_id"),
    
    # Decode payment types
    when(col("payment_type") == 0, "Flex Fare trip")
      .when(col("payment_type") == 1, "Credit card")
      .when(col("payment_type") == 2, "Cash")
      .when(col("payment_type") == 3, "No charge")
      .when(col("payment_type") == 4, "Dispute")
      .when(col("payment_type") == 6, "Voided trip")
      .otherwise("Unknown")
      .alias("payment_type"),
    
    "fare_amount",
    "extra",
    "mta_tax",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    # alias columns for consistent naming convention
    col("Airport_fee").alias("airport_fee"),
    "cbd_congestion_fee",
    "month",
    "year"
)

df = df.na.drop(subset=["vendor", "rate_type", "payment_type"])

df.writeTo("glue_catalog.nyctaxi_silver.nyc_taxi_yellow_silver_tratada").overwritePartitions()
job.commit()