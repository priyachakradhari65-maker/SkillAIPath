-- dbt Snapshot: Customer SCD Type 2 Tracking
-- Captures historical changes to customer records

{% snapshot customer_snapshot %}

{{
  config(
    target_schema='silver',
    unique_key='CustomerID',
    strategy='timestamp',
    updated_at='UpdatedAt',
    invalidate_hard_deletes=True
  )
}}

SELECT
  CustomerID,
  CustomerName,
  Email,
  Phone,
  City,
  State,
  Country,
  PostalCode,
  IsActive,
  UpdatedAt
FROM {{ source('bronze', 'customers') }}

{% endsnapshot %}
