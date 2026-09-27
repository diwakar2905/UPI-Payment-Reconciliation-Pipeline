with spine as (
    select generate_series(
        '2024-01-01'::date,
        '2030-12-31'::date,
        interval '1 day'
    )::date as date_day
)

select
    date_day,
    extract(year from date_day)::int as year,
    extract(month from date_day)::int as month,
    extract(day from date_day)::int as day_of_month,
    extract(isodow from date_day)::int as day_of_week,
    to_char(date_day, 'Day') as day_name,
    to_char(date_day, 'Month') as month_name,
    extract(quarter from date_day)::int as quarter,
    (extract(isodow from date_day) in (6, 7)) as is_weekend
from spine
