import urllib.request
import urllib.error
import boto3
from datetime import datetime, timezone

s3 = boto3.client("s3")
BUCKET = "bucket-yellow-taxi"
LAG_MONTHS = 2  # defasagem fixa: processa sempre m-2

def get_target_date(reference=None):
    """Retorna 'YYYY-MM' correspondente a m-LAG_MONTHS."""
    ref = reference or datetime.now(timezone.utc)
    month = ref.month - LAG_MONTHS
    year = ref.year
    if month <= 0:      # janeiro/fevereiro caem no ano anterior
        month += 12
        year -= 1
    return f"{year}-{month:02d}"

def lambda_handler(event, context):
    # override manual para backfill/teste: {"date": "2025-11"}
    date = (event or {}).get("date") or get_target_date()
    year, month = date.split("-")

    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{date}.parquet"
    key = f"bronze/year={year}/month={month}/yellow_tripdata_{date}.parquet"

    print(f"Iniciando download: {date}")
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            s3.upload_fileobj(response, BUCKET, key)
        print(f"OK: {date} -> {key}")
        return {"date": date, "status": "ok", "key": key}
    except urllib.error.HTTPError as e:
        print(f"HTTP error {e.code}: {date}")
        return {"date": date, "status": f"http_error_{e.code}"}
    except Exception as e:
        print(f"ERRO inesperado em {date}: {type(e).__name__}: {e}")
        return {"date": date, "status": f"error_{type(e).__name__}"}