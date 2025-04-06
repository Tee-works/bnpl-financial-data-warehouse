with source as (
    select * from {{ source('bronze', 'transactions') }}
),

renamed as (
    select
        transaction_id,
        customer_id,
        merchant_id,
        transaction_date,
        amount,
        currency,
        payment_method,
        status,
        _etl_extracted_at
    from source
)

select * from renamed