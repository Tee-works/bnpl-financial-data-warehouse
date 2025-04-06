{{
    config(
        materialized='table',
        tags=['transactions', 'silver']
    )
}}

with stg_transactions as (
    select * from {{ ref('stg_transactions') }}
),

dim_customers as (
    select * from {{ ref('dim_customers') }}
),

dim_merchants as (
    select * from {{ ref('dim_merchants') }}
),

transactions_with_sk as (
    select
        t.transaction_id,
        c.customer_sk,
        m.merchant_sk,
        t.transaction_date,
        cast(t.transaction_date as date) as transaction_date_key,
 -- Date key for dim_dates
        t.amount,
        t.currency,
        t.payment_method,
        t.status,
        t._etl_extracted_at
    from stg_transactions t
    left join dim_customers c on t.customer_id = c.customer_id
    left join dim_merchants m on t.merchant_id = m.merchant_id
),

final as (
    select
        -- Generate a surrogate key
        {{ dbt_utils.generate_surrogate_key(['transaction_id']) }} as transaction_sk,
        
        -- Natural and foreign keys
        transaction_id,
        customer_sk,
        merchant_sk,
        transaction_date_key,
        
        -- Transaction details
        transaction_date,
        amount,
        currency,
        payment_method,
        status,
        
        -- Add metadata
        _etl_extracted_at as source_extracted_at,
        current_timestamp as dbt_updated_at
    from transactions_with_sk
)

select * from final