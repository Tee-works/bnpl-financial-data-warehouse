{{
    config(
        materialized='table',
        tags=['customers', 'silver']
    )
}}

with stg_customers as (
    select * from {{ ref('stg_customers') }}
),

final as (
    select
        -- Generate a surrogate key
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_sk,
        
        -- Source columns
        customer_id,
        email,
        first_name,
        last_name,
        phone_number,
        country,
        city,
        registration_date,
        last_login_date,
        status,
        
        -- Add calculated columns
        concat(first_name, ' ', last_name) as full_name,
        
        -- Add metadata
        _etl_extracted_at as source_extracted_at,
        current_timestamp as dbt_updated_at
    from stg_customers
)

select * from final