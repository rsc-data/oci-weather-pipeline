with ranked as (
    select
        city,
        obs_date,
        temp_max_c,
        temp_min_c,
        precip_mm,
        loaded_at,
        row_number() over (
            partition by city, obs_date
            order by loaded_at desc
        ) as rn
    from {{ source('raw', 'raw_weather_daily') }}
    where obs_date < trunc(loaded_at)
)
select city, obs_date, temp_max_c, temp_min_c, precip_mm, loaded_at
from ranked
where rn = 1
