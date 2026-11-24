import os

import oracledb
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Weather pipeline", layout="wide")
st.title("Daily weather pipeline")
st.caption("Open-Meteo → Airflow → Oracle Autonomous Data Warehouse → dbt → Streamlit. Refreshed daily at 7 a.m. ET.")


@st.cache_data(ttl=3600)
def load(sql: str) -> pd.DataFrame:
    with oracledb.connect(
        user=os.environ["ADW_USER"],
        password=os.environ["ADW_PASSWORD"],
        dsn=os.environ["ADW_DSN"],
    ) as conn, conn.cursor() as cur:
        cur.execute(sql)
        cols = [c[0].lower() for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


daily = load("select city, obs_date, temp_max_f, temp_min_f, precip_in "
             "from pipeline.stg_weather_daily order by obs_date")
monthly = load("select * from pipeline.fct_city_weather_monthly "
               "order by month_start desc, city")

city = st.selectbox("City", sorted(daily["city"].unique()))
d = daily[daily["city"] == city].set_index("obs_date")

left, right = st.columns(2)
left.subheader("Daily high and low (°F)")
left.line_chart(d[["temp_max_f", "temp_min_f"]])
right.subheader("Daily precipitation (in)")
right.bar_chart(d["precip_in"])

st.subheader("Monthly summary (dbt mart)")
st.dataframe(monthly, use_container_width=True)
