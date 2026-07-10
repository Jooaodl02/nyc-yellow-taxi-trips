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

df_zones_do = spark.read.table("nyctaxi_silver.taxi_zone_lookup")
df_zones_pu = spark.read.table("nyctaxi_silver.taxi_zone_lookup")
df_trips = spark.read.table("glue_catalog.nyctaxi_silver.nyc_taxi_yellow_silver_tratada")

df_trips = df_trips.filter((col("year") == year) & (col("month") == month))

# Join trips with pickup zone details (borough and zone name)
df_join_1 = df_trips.join(
                df_zones_pu, 
                df_trips.pu_location_id == df_zones_pu.locationid,
                "left"
                ).select(
                    df_trips.vendor,
                    df_trips.tpep_pickup_datetime,
                    df_trips.tpep_dropoff_datetime,
                    df_trips.trip_duration,
                    df_trips.passenger_count,
                    df_trips.trip_distance,
                    df_trips.rate_type,
                    df_trips.store_and_fwd_flag,
                    df_zones_pu.borough.alias("pu_borough"),   # pickup borough
                    df_zones_pu.zone.alias("pu_zone"),         # pickup zone
                    df_trips.do_location_id,                # dropoff location ID for next join
                    df_trips.payment_type,
                    df_trips.fare_amount,
                    df_trips.extra,
                    df_trips.mta_tax,
                    df_trips.tolls_amount,
                    df_trips.improvement_surcharge,
                    df_trips.total_amount,
                    df_trips.congestion_surcharge,
                    df_trips.airport_fee,  
                    df_trips.cbd_congestion_fee,
                    df_trips.month,
                    df_trips.year
                )
                
df_join_final = df_join_1.join(
                                df_zones_do, 
                                df_join_1.do_location_id == df_zones_do.locationid,
                                "left"
                                ).select(
                                            df_join_1.vendor,
                                            df_join_1.tpep_pickup_datetime,
                                            df_join_1.tpep_dropoff_datetime,
                                            df_join_1.trip_duration,
                                            df_join_1.passenger_count,
                                            df_join_1.trip_distance,
                                            df_join_1.rate_type,
                                            df_join_1.store_and_fwd_flag,
                                            df_join_1.pu_borough,
                                            df_zones_do.borough.alias("do_borough"), # dropoff borough
                                            df_join_1.pu_zone,
                                            df_zones_do.zone.alias("do_zone"),       # dropoff zone
                                            df_join_1.payment_type,
                                            df_join_1.fare_amount,
                                            df_join_1.extra,
                                            df_join_1.mta_tax,
                                            df_join_1.tolls_amount,
                                            df_join_1.improvement_surcharge,
                                            df_join_1.total_amount,
                                            df_join_1.congestion_surcharge,
                                            df_join_1.airport_fee,  
                                            df_join_1.cbd_congestion_fee,
                                            df_join_1.month,
                                            df_join_1.year
                                )

df_join_final.writeTo("glue_catalog.nyctaxi_silver.nyc_taxi_yellow_silver_enriquecida").overwritePartitions()