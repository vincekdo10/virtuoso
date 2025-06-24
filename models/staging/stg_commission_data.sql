{{ config(
    materialized='dynamic_table',
    on_configuration_change='apply',
    target_lag='downstream',
    snowflake_warehouse='DEMO_VINCENT_WH'
) }}

with source_data as (
    select
        commissionid,
        scbookingid,
        datasourceid
    from {{ source('commission', 'commission_materialized_withuuid') }}
),

transformed as (
    select
        commissionid,
        scbookingid,
        datasourceid,
        -- Add computed columns for staging
        lower(trim(commissionid)) as commission_id_clean,
        case 
            when datasourceid is not null then true 
            else false 
        end as has_data_source
    from source_data
)

select * from transformed 