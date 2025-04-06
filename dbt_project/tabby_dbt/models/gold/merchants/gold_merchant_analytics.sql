{{
    config(
        materialized='table',
        tags=['merchants', 'gold']
    )
}}

with dim_merchants as (
    select * from {{ ref('dim_merchants') }}
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

-- Calculate merchant sales metrics
merchant_sales_metrics as (
    select
        merchant_sk,
        count(distinct transaction_sk) as total_transactions,
        count(distinct customer_sk) as unique_customers,
        sum(amount) as total_sales_amount,
        avg(amount) as avg_transaction_amount,
        min(transaction_date) as first_transaction_date,
        max(transaction_date) as last_transaction_date,
        array_agg(distinct currency) as currencies_used
    from fact_transactions
    where status = 'completed'
    group by 1
),

-- Calculate payment plan metrics
merchant_payment_plan_metrics as (
    select
        merchant_sk,
        count(distinct plan_sk) as total_payment_plans,
        sum(case when status = 'active' then 1 else 0 end) as active_payment_plans,
        sum(case when status = 'completed' then 1 else 0 end) as completed_payment_plans,
        sum(case when status = 'defaulted' then 1 else 0 end) as defaulted_payment_plans,
        avg(payment_completion_rate) as avg_payment_completion_rate,
        sum(defaulted_installments) as total_defaulted_installments
    from fact_payment_plans
    group by 1
),

-- Calculate customer engagement metrics
merchant_engagement_metrics as (
    select
        merchant_sk,
        count(distinct event_sk) as total_events,
        count(distinct customer_sk) as engaged_customers,
        count(distinct case when event_type = 'product_view' then event_sk end) as product_views,
        count(distinct case when event_type = 'add_to_cart' then event_sk end) as add_to_carts,
        count(distinct case when event_type = 'checkout' then event_sk end) as checkouts,
        count(distinct case when event_type = 'purchase' then event_sk end) as purchases
    from fact_customer_events
    where merchant_sk is not null
    group by 1
),

-- Calculate recent metrics (last 30 days)
merchant_recent_metrics as (
    select
        merchant_sk,
        count(distinct transaction_sk) as transactions_last_30d,
        count(distinct customer_sk) as customers_last_30d,
        sum(amount) as sales_amount_last_30d
    from fact_transactions
    where 
        status = 'completed'
        and transaction_date >= current_date() - interval '30 days'
    group by 1
),

-- Final merchant analytics model
final as (
    select
        -- Merchant identifiers and attributes
        m.merchant_sk,
        m.merchant_id,
        m.merchant_name,
        m.category,
        m.country,
        m.integration_type,
        m.onboarding_date,
        m.status,
        m.days_since_onboarding,
        
        -- Sales metrics
        coalesce(sm.total_transactions, 0) as total_transactions,
        coalesce(sm.unique_customers, 0) as unique_customers,
        coalesce(sm.total_sales_amount, 0) as total_sales_amount,
        sm.avg_transaction_amount,
        sm.first_transaction_date,
        sm.last_transaction_date,
        sm.currencies_used,
        
        -- Days since metrics
        datediff('day', coalesce(sm.last_transaction_date, m.onboarding_date), current_date()) as days_since_last_transaction,
        
        -- Payment plan metrics
        coalesce(pm.total_payment_plans, 0) as total_payment_plans,
        coalesce(pm.active_payment_plans, 0) as active_payment_plans,
        coalesce(pm.completed_payment_plans, 0) as completed_payment_plans,
        coalesce(pm.defaulted_payment_plans, 0) as defaulted_payment_plans,
        coalesce(pm.total_defaulted_installments, 0) as total_defaulted_installments,
        pm.avg_payment_completion_rate,
        
        -- Default rate
        case
            when coalesce(pm.total_payment_plans, 0) > 0 
                then coalesce(pm.defaulted_payment_plans, 0) / coalesce(pm.total_payment_plans, 0)
            else 0
        end as default_rate,
        
        -- Engagement metrics
        coalesce(em.total_events, 0) as total_events,
        coalesce(em.engaged_customers, 0) as engaged_customers,
        coalesce(em.product_views, 0) as product_views,
        coalesce(em.add_to_carts, 0) as add_to_carts,
        coalesce(em.checkouts, 0) as checkouts,
        coalesce(em.purchases, 0) as purchase_events,
        
        -- Conversion metrics
        case
            when coalesce(em.product_views, 0) > 0 
                then coalesce(em.purchases, 0) / coalesce(em.product_views, 0)
            else 0
        end as view_to_purchase_rate,
        
        -- Recent metrics
        coalesce(rm.transactions_last_30d, 0) as transactions_last_30d,
        coalesce(rm.customers_last_30d, 0) as customers_last_30d,
        coalesce(rm.sales_amount_last_30d, 0) as sales_amount_last_30d,
        
        -- Customer value
        case
            when coalesce(sm.unique_customers, 0) > 0 
                then coalesce(sm.total_sales_amount, 0) / coalesce(sm.unique_customers, 0)
            else 0
        end as avg_customer_value,
        
        -- Merchant segments
        case
            when coalesce(sm.total_sales_amount, 0) = 0 then 'No Sales'
            when coalesce(sm.total_sales_amount, 0) >= 50000 then 'High Volume'
            when coalesce(sm.total_sales_amount, 0) >= 10000 then 'Medium Volume'
            else 'Low Volume'
        end as volume_segment,
        
        case
            when coalesce(sm.last_transaction_date, m.onboarding_date) >= current_date() - interval '7 days' then 'Very Active'
            when coalesce(sm.last_transaction_date, m.onboarding_date) >= current_date() - interval '30 days' then 'Active'
            when coalesce(sm.last_transaction_date, m.onboarding_date) >= current_date() - interval '90 days' then 'Moderately Active'
            else 'Inactive'
        end as activity_segment,
        
        -- Add metadata
        current_date() as snapshot_date,
        current_timestamp as dbt_updated_at
    from dim_merchants m
    left join merchant_sales_metrics sm on m.merchant_sk = sm.merchant_sk
    left join merchant_payment_plan_metrics pm on m.merchant_sk = pm.merchant_sk
    left join merchant_engagement_metrics em on m.merchant_sk = em.merchant_sk
    left join merchant_recent_metrics rm on m.merchant_sk = rm.merchant_sk
)

select * from final