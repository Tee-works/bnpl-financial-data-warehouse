{{
    config(
        materialized='table',
        tags=['transactions', 'gold']
    )
}}

with fact_transactions as (
    select * from {{ ref('fact_transactions') }}
),

dim_customers as (
    select * from {{ ref('dim_customers') }}
),

dim_merchants as (
    select * from {{ ref('dim_merchants') }}
),

dim_dates as (
    select * from {{ ref('dim_dates') }}
),

-- Daily transaction aggregation
daily_transactions as (
    select
        transaction_date_key,
        count(distinct transaction_sk) as transaction_count,
        count(distinct customer_sk) as customer_count,
        count(distinct merchant_sk) as merchant_count,
        sum(amount) as total_amount,
        avg(amount) as avg_amount,
        min(amount) as min_amount,
        max(amount) as max_amount,
        array_agg(distinct currency) as currencies
    from fact_transactions
    group by 1
),

-- Daily transaction aggregation by merchant
daily_merchant_transactions as (
    select
        transaction_date_key,
        merchant_sk,
        count(distinct transaction_sk) as transaction_count,
        count(distinct customer_sk) as customer_count,
        sum(amount) as total_amount,
        avg(amount) as avg_amount
    from fact_transactions
    group by 1, 2
),

-- Daily transaction aggregation by status
daily_status_transactions as (
    select
        transaction_date_key,
        status,
        count(distinct transaction_sk) as transaction_count,
        sum(amount) as total_amount
    from fact_transactions
    group by 1, 2
),

-- Final transaction analytics model
final as (
    select
        -- Date attributes
        dt.transaction_date_key,
        d.calendar_date as transaction_date,
        d.year as transaction_year,
        d.month as transaction_month,
        d.month_name as transaction_month_name,
        d.day_of_month as transaction_day,
        d.day_name as transaction_day_name,
        d.is_weekend,
        d.quarter,
        d.year_month,
        
        -- Daily metrics
        dt.transaction_count,
        dt.customer_count,
        dt.merchant_count,
        dt.total_amount,
        dt.avg_amount,
        dt.min_amount,
        dt.max_amount,
        dt.currencies,
        
        -- Status metrics
        sum(case when dst.status = 'completed' then dst.transaction_count else 0 end) as completed_count,
        sum(case when dst.status = 'completed' then dst.total_amount else 0 end) as completed_amount,
        sum(case when dst.status = 'pending' then dst.transaction_count else 0 end) as pending_count,
        sum(case when dst.status = 'pending' then dst.total_amount else 0 end) as pending_amount,
        sum(case when dst.status = 'failed' then dst.transaction_count else 0 end) as failed_count,
        sum(case when dst.status = 'failed' then dst.total_amount else 0 end) as failed_amount,
        sum(case when dst.status = 'cancelled' then dst.transaction_count else 0 end) as cancelled_count,
        sum(case when dst.status = 'cancelled' then dst.total_amount else 0 end) as cancelled_amount,
        sum(case when dst.status = 'refunded' then dst.transaction_count else 0 end) as refunded_count,
        sum(case when dst.status = 'refunded' then dst.total_amount else 0 end) as refunded_amount,
        
        -- Top merchants (limited to top 5 for simplicity)
        array(
            select json_object(
                'merchant_sk', dmt.merchant_sk,
                'merchant_name', m.merchant_name,
                'transaction_count', dmt.transaction_count,
                'total_amount', dmt.total_amount
            )
            from daily_merchant_transactions dmt
            left join dim_merchants m on dmt.merchant_sk = m.merchant_sk
            where dmt.transaction_date_key = dt.transaction_date_key
            order by dmt.total_amount desc
            limit 5
        ) as top_merchants,
        
        -- Add metadata
        current_timestamp as dbt_updated_at
    from daily_transactions dt
    left join dim_dates d on dt.transaction_date_key = d.date_key
    left join daily_status_transactions dst on dt.transaction_date_key = dst.transaction_date_key
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 
             11, 12, 13, 14, 15, 16, 17, 18
)

select * from final