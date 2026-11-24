select
    city,
    trunc(obs_date, 'MM')                               as month_start,
    count(*)                                            as days_observed,
    round(avg(temp_max_f), 1)                           as avg_high_f,
    round(avg(temp_min_f), 1)                           as avg_low_f,
    round(sum(precip_in), 2)                            as total_precip_in,
    sum(case when precip_in >= 0.04 then 1 else 0 end)  as rainy_days
from {{ ref('stg_weather_daily') }}
group by city, trunc(obs_date, 'MM')
