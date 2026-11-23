select city, obs_date, count(*) as n
from {{ ref('stg_weather_daily') }}
group by city, obs_date
having count(*) > 1
