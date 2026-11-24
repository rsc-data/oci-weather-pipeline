with ranked as (
    select
        city,
        obs_date,
        temp_max_f,
        temp_min_f,
        precip_in,
        loaded_at,
        row_number() over (
            partition by city, obs_date
            order by loaded_at desc
        ) as rn
    from {{ source('raw', 'raw_weather_daily') }}
    where obs_date < trunc(loaded_at)
)
select city, obs_date, temp_max_f, temp_min_f, precip_in, loaded_at
from ranked
where rn = 1
