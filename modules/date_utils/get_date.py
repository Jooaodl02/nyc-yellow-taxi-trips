from datetime import datetime, timezone

LAG_MONTHS = 2  # mesma defasagem m-2 usada pelos jobs de silver, silver-enriquecida e gold


def get_target_year_month(reference=None, lag_months=LAG_MONTHS):
    ref = reference or datetime.now(timezone.utc)
    month = ref.month - lag_months
    year = ref.year
    if month <= 0:
        month += 12
        year -= 1
    return str(year), f"{month:02d}"
