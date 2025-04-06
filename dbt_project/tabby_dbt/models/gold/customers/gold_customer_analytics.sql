{{
    config(
        materialized='table',
        tags=['customers', 'gold']
    )
}}

with dim_customers as (
    select * from {{ ref('dim_customers') }}
),

fact_transactions as (
    select * from {{ ref('fact_transactions') }}
),

fact_payment_plans as (
    select * from {{ ref('fact_payment_plans') }}
),

fact_customer_events as (
    select * from {{ ref('fact_customer_events') }}
),

-- Calculate customer purchase metrics
customer_purchase_metrics as (
    select
        customer_sk,
        count(distinct transaction_sk) as total_transactions,
        sum(amount) as total_spent,
        avg(amount) as avg_transaction_amount,
        min(transaction_date) as first_transaction_date,
        max(transaction_date) as last_transaction_date,
        count(distinct merchant_sk) as unique_merchants_count,
        array_agg(distinct currency) as currencies_used
    from fact_transactions
    where status = 'completed'
    group by 1
),

-- Calculate payment plan metrics
customer_payment_plan_metrics as (
    select
        customer_sk,
        count(distinct plan_sk) as total_payment_plans,
        sum(case when status = 'active' then 1 else 0 end) as active_payment_plans,
        sum(case when status = 'completed' then 1 else 0 end) as completed_payment_plans,
        sum(case when status = 'defaulted' then 1 else 0 end) as defaulted_payment_plans,
        avg(payment_completion_rate) as avg_payment_completion_rate
    from fact_payment_plans
    group by 1
),

-- Calculate engagement metrics
customer_engagement_metrics as (
    select
        customer_sk,
        count(distinct event_sk) as total_events,
        count(distinct session_id) as total_sessions,
        count(distinct event_date_key) as active_days,
        count(distinct case when event_type = 'app_open' then event_sk end) as app_opens,
        count(distinct case when event_type = 'product_view' then event_sk end) as product_views,
        count(distinct case when event_type = 'search' then event_sk end) as searches,
        count(distinct case when event_type = 'add_to_cart' then event_sk end) as add_to_carts,
        count(distinct case when event_type = 'checkout' then event_sk end) as checkouts,
        count(distinct case when event_type = 'purchase' then event_sk end) as purchases
    from fact_customer_events
    group by 1
),

-- Final customer analytics model
final as (
    select
        -- Customer identifiers and attributes
        c.customer_sk,
        c.customer_id,
        c.full_name,
        c.email,
        c.phone_number,
        c.country,
        c.city,
        c.registration_date,
        c.last_login_date,
        c.status,
        
        -- Transaction metrics
        coalesce(pm.total_transactions, 0) as total_transactions,
        coalesce(pm.total_spent, 0) as total_spent,
        pm.avg_transaction_amount,
        pm.first_transaction_date,
        pm.last_transaction_date,
        coalesce(pm.unique_merchants_count, 0) as unique_merchants_count,
        pm.currencies_used,
        
        -- Days since metrics
        datediff('day', c.registration_date, current_date()) as days_since_registration,
        datediff('day', coalesce(pm.last_transaction_date, c.registration_date), current_date()) as days_since_last_transaction,
        
        -- Payment plan metrics
        coalesce(pp.total_payment_plans, 0) as total_payment_plans,
        coalesce(pp.active_payment_plans, 0) as active_payment_plans,
        coalesce(pp.completed_payment_plans, 0) as completed_payment_plans,
        coalesce(pp.defaulted_payment_plans, 0) as defaulted_payment_plans,
        pp.avg_payment_completion_rate,
        
        -- Engagement metrics
        coalesce(em.total_events, 0) as total_events,
        coalesce(em.total_sessions, 0) as total_sessions,
        coalesce(em.active_days, 0) as active_days,
        coalesce(em.app_opens, 0) as app_opens,
        coalesce(em.product_views, 0) as product_views,
        coalesce(em.searches, 0) as searches,
        coalesce(em.add_to_carts, 0) as add_to_carts,
        coalesce(em.checkouts, 0) as checkouts,
        coalesce(em.purchases, 0) as purchase_events,
        
        -- Customer segments
        case
            when coalesce(pm.total_spent, 0) = 0 then 'No Purchases'
            when coalesce(pm.total_spent, 0) >= 5000 then 'High Value'
            when coalesce(pm.total_spent, 0) >= 1000 then 'Medium Value'
            else 'Low Value'
        end as value_segment,
        
        case
            when coalesce(pm.last_transaction_date, c.registration_date) >= current_date() - interval '30 days' then 'Active'
            when coalesce(pm.last_transaction_date, c.registration_date) >= current_date() - interval '90 days' then 'Recent'
            when coalesce(pm.last_transaction_date, c.registration_date) >= current_date() - interval '180 days' then 'Lapsed'
            else 'Inactive'
        end as recency_segment,
        
        case
            when coalesce(pp.defaulted_payment_plans, 0) > 0 then 'Defaulted'
            when coalesce(pp.completed_payment_plans, 0) > 0 then 'Good Standing'
            when coalesce(pp.active_payment_plans, 0) > 0 then 'In Progress'
            else 'No Plans'
        end as payment_plan_segment,
        
        -- Add metadata
        current_date() as snapshot_date,
        current_timestamp as dbt_updated_at
    from dim_customers c
    left join customer_purchase_metrics pm on c.customer_sk = pm.customer_sk
    left join customer_payment_plan_metrics pp on c.customer_sk = pp.customer_sk
    left join customer_engagement_metrics em on c.customer_sk = em.customer_sk
)

select * from final