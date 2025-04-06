{{
    config(
        materialized='table',
        tags=['transactions', 'finance', 'silver']
    )
}}

with stg_payment_plans as (
    select * from {{ ref('stg_payment_plans') }}
),

stg_installments as (
    select * from {{ ref('stg_installments') }}
),

dim_customers as (
    select * from {{ ref('dim_customers') }}
),

dim_merchants as (
    select * from {{ ref('dim_merchants') }}
),

fact_transactions as (
    select * from {{ ref('fact_transactions') }}
),

payment_plans_with_sk as (
    select
        p.plan_id,
        t.transaction_sk,
        c.customer_sk,
        m.merchant_sk,
        p.plan_date,
        cast(p.plan_date as date) as plan_date_key,
        p.total_amount,
        p.installment_count,
        p.first_installment_amount,
        p.status,
        p._etl_extracted_at
    from stg_payment_plans p
    left join dim_customers c on p.customer_id = c.customer_id
    left join dim_merchants m on p.merchant_id = m.merchant_id
    left join fact_transactions t on p.transaction_id = t.transaction_id
),

-- Calculate metrics from installments
payment_plan_metrics as (
    select
        i.plan_id,
        count(distinct i.installment_id) as total_installments,
        count(distinct case when i.status = 'paid' or i.status = 'paid_late' then i.installment_id end) as paid_installments,
        count(distinct case when i.status = 'defaulted' then i.installment_id end) as defaulted_installments,
        sum(case when i.status = 'paid' or i.status = 'paid_late' then i.amount else 0 end) as total_paid_amount,
        avg(case when i.status = 'paid_late' and i.due_date is not null and i.paid_date is not null
                then datediff('day', i.due_date, i.paid_date)
                else null end) as avg_days_late
    from stg_installments i
    group by 1
),

final as (
    select
        -- Generate a surrogate key
        {{ dbt_utils.generate_surrogate_key(['p.plan_id']) }} as plan_sk,
        
        -- Natural and foreign keys
        p.plan_id,
        p.transaction_sk,
        p.customer_sk,
        p.merchant_sk,
        p.plan_date_key,
        
        -- Plan details
        p.plan_date,
        p.total_amount,
        p.installment_count,
        p.first_installment_amount,
        p.status,
        
        -- Metrics from installments
        coalesce(m.total_installments, 0) as total_installments,
        coalesce(m.paid_installments, 0) as paid_installments,
        coalesce(m.defaulted_installments, 0) as defaulted_installments,
        coalesce(m.total_paid_amount, 0) as total_paid_amount,
        m.avg_days_late,
        
        -- Calculated metrics
        case
            when p.total_amount > 0 then coalesce(m.total_paid_amount, 0) / p.total_amount
            else null
        end as payment_completion_rate,
        
        -- Add metadata
        p._etl_extracted_at as source_extracted_at,
        current_timestamp as dbt_updated_at
    from payment_plans_with_sk p
    left join payment_plan_metrics m on p.plan_id = m.plan_id
)

select * from final