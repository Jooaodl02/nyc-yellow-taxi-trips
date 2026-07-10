import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import sum, count, avg, max, min, round, col
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

df = spark.read.table("glue_catalog.nyctaxi_silver.nyc_taxi_yellow_silver_enriquecida")

df = df.filter((col("year") == year) & (col("month") == month))

df_gold = df.groupBy(
    col("tpep_pickup_datetime").cast("date").alias("pickup_date"),
    col("month").alias("month"),
    col("year").alias("year")
).agg(
    count("*").alias("total_trips"),
    avg("passenger_count").alias("avg_passengers_per_trip"),
    avg("trip_distance").alias("avg_distance_per_trip"),
    avg("fare_amount").alias("avg_fare_per_trip"),
    max("fare_amount").alias("max_fare"),
    min("fare_amount").alias("min_fare"),
    round(sum("total_amount"), 2).alias("total_revenue"),
)

df_gold.writeTo("glue_catalog.nyctaxi_gold.daily_trip_yellow_summary").overwritePartitions()
job.commit()