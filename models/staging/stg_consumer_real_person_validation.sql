{{ config(
    materialized='incremental',
    unique_key='consumerid',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    snowflake_warehouse='WH_PIPE_COMMISSION'
) }}

with source_data as (
    select
        consumerid,
        first_name,
        last_name,
        account_name
    from {{ source('consumer', 'stream_isrealperson') }}
    where metadata$action = 'INSERT'
    
    {% if is_incremental() %}
        and consumerid in (
            select distinct consumerid 
            from {{ source('consumer', 'stream_isrealperson') }}
            where metadata$action = 'INSERT'
        )
    {% endif %}
),

transformed as (
    select
        consumerid,
        first_name,
        last_name,
        account_name,
        -- Create fullname using the same logic as original procedure
        case 
            when nullif(
                cast(
                    coalesce(
                        iff(lower(first_name) = 'unknown', '', lower(first_name)), 
                        ''
                    ) as varchar
                ) || ' ' || 
                cast(
                    coalesce(
                        iff(lower(last_name) = 'unknown', '', lower(last_name)), 
                        ''
                    ) as varchar
                ), ' '
            ) is null 
            and nullif(account_name, '') is not null 
            then lower(nullif(account_name, ''))
            else nullif(
                cast(
                    coalesce(
                        iff(lower(first_name) = 'unknown', '', lower(first_name)), 
                        ''
                    ) as varchar
                ) || ' ' || 
                cast(
                    coalesce(
                        iff(lower(last_name) = 'unknown', '', lower(last_name)), 
                        ''
                    ) as varchar
                ), ' '
            )
        end as fullname,
        current_timestamp() as updated_timestamp,
        current_timestamp() as inserted_timestamp,
        'stream_isrealperson' as source_stream
    from source_data
)

select * from transformed 