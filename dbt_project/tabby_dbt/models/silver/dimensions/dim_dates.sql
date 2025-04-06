{{
    config(
        materialized='table',
        tags=['common', 'silver']
    )
}}

-- Create a date dimension table with a range of dates
with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast(current_date() + interval '3 years' as date)"
    ) }}
),

expanded as (
    select
        date_day as date_key,
        date_day as calendar_date,
        extract('year' from date_day) as year,
        extract('month' from date_day) as month,
        extract('day' from date_day) as day_of_month,
        extract('dayofweek' from date_day) as day_of_week,
        extract('dayofyear' from date_day) as day_of_year,
        case
            when extract('month' from date_day) = 1 then 'January'
            when extract('month' from date_day) = 2 then 'February'
            when extract('month' from date_day) = 3 then 'March'
            when extract('month' from date_day) = 4 then 'April'
            when extract('month' from date_day) = 5 then 'May'
            when extract('month' from date_day) = 6 then 'June'
            when extract('month' from date_day) = 7 then 'July'
            when extract('month' from date_day) = 8 then 'August'
            when extract('month' from date_day) = 9 then 'September'
            when extract('month' from date_day) = 10 then 'October'
            when extract('month' from date_day) = 11 then 'November'
            when extract('month' from date_day) = 12 then 'December'
        end as month_name,
        case
            when extract('dayofweek' from date_day) = 0 then 'Sunday'
            when extract('dayofweek' from date_day) = 1 then 'Monday'
            when extract('dayofweek' from date_day) = 2 then 'Tuesday'
            when extract('dayofweek' from date_day) = 3 then 'Wednesday'
            when extract('dayofweek' from date_day) = 4 then 'Thursday'
            when extract('dayofweek' from date_day) = 5 then 'Friday'
            when extract('dayofweek' from date_day) = 6 then 'Saturday'
        end as day_name,
        case
            when extract('dayofweek' from date_day) in (0, 6) then true
            else false
        end as is_weekend,
        -- Quarter
        concat('Q', extract('quarter' from date_day)) as quarter,
        -- Year-Month
        concat(extract('year' from date_day), '-', lpad(extract('month' from date_day)::varchar, 2, '0')) as year_month,
        -- Current date flag
        case
            when date_day = current_date() then true
            else false
        end as is_current_day,
        -- Other date flags
        case when date_day = date_trunc('month', date_day) then true else false end as is_first_day_of_month,
        case
            when date_day = (date_trunc('month', date_day) + interval '1 month' - interval '1 day')
            then true else false
        end as is_last_day_of_month,
        
        -- Metadata
        current_timestamp as dbt_updated_at
    from date_spine
)

select * from expanded