select
    city,
    trunc(obs_date, 'MM')                            as month_start,
    count(*)                                         as days_observed,
    round(avg(temp_max_c), 1)                        as avg_high_c,
    round(avg(temp_min_c), 1)                        as avg_low_c,
    round(sum(precip_mm), 1)                         as total_precip_mm,
    sum(case when precip_mm >= 1 then 1 else 0 end)  as rainy_days
from {{ ref('stg_weather_daily') }}
group by city, trunc(obs_date, 'MM')
