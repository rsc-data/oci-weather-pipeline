from datetime import datetime, timedelta
import os

import oracledb
import requests
from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator

CITIES = {
    "New York": (40.71, -74.01),
    "Chicago": (41.88, -87.63),
    "Houston": (29.76, -95.37),
    "Phoenix": (33.45, -112.07),
    "Seattle": (47.61, -122.33),
}
DBT = "/opt/airflow/dbt_venv/bin/dbt"
DBT_DIR = "/opt/airflow/dbt"


@dag(
    schedule="0 7 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["portfolio"],
)
def weather_pipeline():

    @task
    def extract_load() -> int:
        rows = []
        for city, (lat, lon) in CITIES.items():
            resp = requests.get(
                "https://api.open-meteo.com/v1/forecast",
                params={
                    "latitude": lat,
                    "longitude": lon,
                    "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
                    "past_days": 7,
                    "forecast_days": 1,
                    "timezone": "auto",
                },
                timeout=30,
            )
            resp.raise_for_status()
            d = resp.json()["daily"]
            for i, day in enumerate(d["time"]):
                rows.append((
                    city,
                    datetime.strptime(day, "%Y-%m-%d"),
                    d["temperature_2m_max"][i],
                    d["temperature_2m_min"][i],
                    d["precipitation_sum"][i],
                ))

        with oracledb.connect(
            user=os.environ["ADW_USER"],
            password=os.environ["ADW_PASSWORD"],
            dsn=os.environ["ADW_DSN"],
        ) as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO raw_weather_daily "
                    "(city, obs_date, temp_max_c, temp_min_c, precip_mm) "
                    "VALUES (:1, :2, :3, :4, :5)",
                    rows,
                )
            conn.commit()
        return len(rows)

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"ORA_PYTHON_DRIVER_TYPE=thin {DBT} build "
                     f"--project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
    )

    extract_load() >> dbt_build


weather_pipeline()
