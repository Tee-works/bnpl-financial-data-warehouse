with source as (
    select * from {{ source('bronze', 'installments') }}
),

renamed as (
    select
        installment_id,
        plan_id,
        installment_number,
        amount,
        due_date,
        paid_date,
        status,
        _etl_extracted_at
    from source
)

select * from renamed