-- models/util/time_spine.sql

with spine as (
    select
        dateadd(day, seq4(), date('2013-01-01')) as date_day
    from table(generator(rowcount => 5000)) -- ~13 years of daily records
)

select
    date_day
from spine
where date_day <= current_date
