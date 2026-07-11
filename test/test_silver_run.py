import importlib.util
import sys
import types
from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "modules"))

# O módulo awsglue só existe dentro do runtime do Glue.
# Para importar o script localmente, criamos stubs mínimos dele.
for module_name in ["awsglue", "awsglue.transforms", "awsglue.utils", "awsglue.context", "awsglue.job"]:
    sys.modules.setdefault(module_name, types.ModuleType(module_name))
sys.modules["awsglue.utils"].getResolvedOptions = lambda argv, options: {}
sys.modules["awsglue.context"].GlueContext = type("GlueContext", (), {})
sys.modules["awsglue.job"].Job = type("Job", (), {})

# O nome do arquivo tem hífen (não é um identificador Python válido),
# então o import precisa ser feito manualmente via caminho.
spec = importlib.util.spec_from_file_location(
    "nyc_taxi_silver_run",
    PROJECT_ROOT / "transformers" / "silver" / "nyc-taxi-silver-run.py",
)
silver_run = importlib.util.module_from_spec(spec)
spec.loader.exec_module(silver_run)


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[1]").appName("test-silver-run").getOrCreate()
    yield spark
    spark.stop()


def test_transform_silver_decodifica_vendor_e_filtra_periodo(spark):
    schema = StructType([
        StructField("VendorID", IntegerType()),
        StructField("tpep_pickup_datetime", TimestampType()),
        StructField("tpep_dropoff_datetime", TimestampType()),
        StructField("passenger_count", IntegerType()),
        StructField("trip_distance", DoubleType()),
        StructField("RatecodeID", IntegerType()),
        StructField("store_and_fwd_flag", StringType()),
        StructField("PULocationID", IntegerType()),
        StructField("DOLocationID", IntegerType()),
        StructField("payment_type", IntegerType()),
        StructField("fare_amount", DoubleType()),
        StructField("extra", DoubleType()),
        StructField("mta_tax", DoubleType()),
        StructField("tolls_amount", DoubleType()),
        StructField("improvement_surcharge", DoubleType()),
        StructField("total_amount", DoubleType()),
        StructField("congestion_surcharge", DoubleType()),
        StructField("Airport_fee", DoubleType()),
        StructField("cbd_congestion_fee", DoubleType()),
        StructField("month", StringType()),
        StructField("year", StringType()),
    ])

    input_df = spark.createDataFrame(
        [
            # dentro do período alvo (2026-05): deve ser mantida e decodificada
            (1, datetime(2026, 5, 1, 8, 0), datetime(2026, 5, 1, 8, 15), 2, 3.5, 1, "N",
             100, 200, 1, 12.0, 0.5, 0.5, 0.0, 1.0, 14.0, 2.5, 0.0, 0.0, "05", "2026"),
            # fora do período alvo: deve ser filtrada
            (2, datetime(2026, 4, 1, 8, 0), datetime(2026, 4, 1, 8, 15), 1, 1.0, 1, "N",
             100, 200, 2, 10.0, 0.0, 0.5, 0.0, 1.0, 11.5, 0.0, 0.0, 0.0, "04", "2026"),
        ],
        schema,
    )

    resultado = silver_run.transform_silver(input_df, year="2026", month="05").collect()

    assert len(resultado) == 1
    assert resultado[0]["vendor"] == "Creative Mobile Technologies, LLC"
    assert resultado[0]["trip_duration"] == 15
