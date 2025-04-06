{{
    config(
        materialized='table',
        tags=['finance', 'gold']
    )
}}

with fact_payment_plans as (
    select * from {{ ref('fact_payment_plans') }}
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

-- Daily payment plan aggregation
daily_payment_plans as (
    select
        plan_date_key,
        count(distinct plan_sk) as plan_count,
        count(distinct customer_sk) as customer_count,
        count(distinct merchant_sk) as merchant_count,
        sum(total_amount) as total_amount,
        avg(total_amount) as avg_plan_amount,
        avg(installment_count) as avg_installment_count,
        count(distinct case when status = 'active' then plan_sk end) as active_plans,
        count(distinct case when status = 'completed' then plan_sk end) as completed_plans,
        count(distinct case when status = 'defaulted' then plan_sk end) as defaulted_plans,
        sum(payment_completion_rate * total_amount) / sum(total_amount) as weighted_completion_rate
    from fact_payment_plans
    group by 1
),

-- Payment plan aggregation by merchant
merchant_payment_plans as (
    select
        merchant_sk,
        plan_date_key,
        count(distinct plan_sk) as plan_count,
        count(distinct customer_sk) as customer_count,
        sum(total_amount) as total_amount,
        avg(total_amount) as avg_plan_amount,
        count(distinct case when status = 'defaulted' then plan_sk end) as defaulted_plans,
        count(distinct case when status = 'defaulted' then plan_sk end) / nullif(count(distinct plan_sk), 0) as default_rate
    from fact_payment_plans
    group by 1, 2
),

-- Final payment analytics model
final as (
    select
        -- Date attributes
        dp.plan_date_key,
        d.calendar_date as plan_date,
        d.year as plan_year,
        d.month as plan_month,
        d.month_name as plan_month_name,
        d.quarter as plan_quarter,
        d.year_month as plan_year_month,
        
        -- Daily metrics
        dp.plan_count,
        dp.customer_count,
        dp.merchant_count,
        dp.total_amount,
        dp.avg_plan_amount,
        dp.avg_installment_count,
        dp.active_plans,
        dp.completed_plans,
        dp.defaulted_plans,
        dp.weighted_completion_rate,
        
        -- Default rate
        dp.defaulted_plans / nullif(dp.plan_count, 0) as default_rate,
        
        -- Top merchants by default rate (limited to top 5)
        array(
            select json_object(
                'merchant_sk', mp.merchant_sk,
                'merchant_name', m.merchant_name,
                'plan_count', mp.plan_count,
                'defaulted_plans', mp.defaulted_plans,
                'default_rate', mp.default_rate
            )
            from merchant_payment_plans mp
            left join dim_merchants m on mp.merchant_sk = m.merchant_sk
            where mp.plan_date_key = dp.plan_date_key
                and mp.plan_count >= 5  -- Only include merchants with sufficient volume
            order by mp.default_rate desc
            limit 5
        ) as top_defaulting_merchants,
        
        -- Top merchants by volume (limited to top 5)
        array(
            select json_object(
                'merchant_sk', mp.merchant_sk,
                'merchant_name', m.merchant_name,
                'plan_count', mp.plan_count,
                'total_amount', mp.total_amount
            )
            from merchant_payment_plans mp
            left join dim_merchants m on mp.merchant_sk = m.merchant_sk
            where mp.plan_date_key = dp.plan_date_key
            order by mp.total_amount desc
            limit 5
        ) as top_merchants_by_volume,
        
        -- Add metadata
        current_timestamp as dbt_updated_at
    from daily_payment_plans dp
    left join dim_dates d on dp.plan_date_key = d.date_key
)

select * from final