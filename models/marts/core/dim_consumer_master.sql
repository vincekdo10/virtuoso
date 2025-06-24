{{ config(
    materialized='incremental',
    unique_key='consumerid',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    merge_exclude_columns=['inserted_timestamp'],
    pre_hook="{{ log('Starting consumer master merge process', info=true) }}",
    post_hook="{{ log('Completed consumer master merge process', info=true) }}"
) }}

with consumer_base as (
    select *
    from {{ ref('stg_consumer_base') }}
    {% if is_incremental() %}
        where processed_at > (select max(updated_timestamp) from {{ this }})
    {% endif %}
),

consumer_extension as (
    select *
    from {{ ref('stg_consumer_extension') }}
    {% if is_incremental() %}
        where processed_at > (select max(updated_timestamp) from {{ this }})
    {% endif %}
),

crm_consumer as (
    select 
        consumer_id as consumerid,
        crm_consumer_id,
        current_timestamp() as processed_at
    from {{ source('consumer', 'stream_crm_consumer') }}
    where metadata$action = 'INSERT'
    {% if is_incremental() %}
        and metadata$isupdate = false
    {% endif %}
),

consumer_isrealperson as (
    select 
        consumerid,
        isrealperson,
        current_timestamp() as processed_at
    from {{ source('consumer', 'stream_consumer_isrealperson') }}
    where metadata$action = 'INSERT'
    {% if is_incremental() %}
        and metadata$isupdate = false
    {% endif %}
),

seasonal_address as (
    select 
        consumerid,
        isseasonalflag,
        current_timestamp() as processed_at
    from {{ source('consumer', 'stream_consumer_seasonal_address_selector') }}
    where metadata$action = 'INSERT'
    {% if is_incremental() %}
        and metadata$isupdate = false
    {% endif %}
),

email_marketing as (
    select 
        consumerid,
        email_marketing_override,
        current_timestamp() as processed_at
    from {{ source('consumer', 'stream_consumer_email_marketing_override') }}
    where metadata$action = 'INSERT'
    {% if is_incremental() %}
        and metadata$isupdate = false
    {% endif %}
),

special_dates as (
    select 
        consumerid,
        anniversary,
        birthday,
        wedding,
        current_timestamp() as processed_at
    from {{ source('consumer', 'stream_special_dates') }}
    where metadata$action = 'INSERT'
    {% if is_incremental() %}
        and metadata$isupdate = false
    {% endif %}
),

additional_attributes as (
    select 
        consumerid,
        gender,
        citizenship,
        issmoker,
        customertraveltype,
        ismarketable,
        current_timestamp() as processed_at
    from {{ source('consumer', 'stream_additional_attributes') }}
    where metadata$action = 'INSERT'
    {% if is_incremental() %}
        and metadata$isupdate = false
    {% endif %}
),

append_attributes as (
    select *
    from {{ ref('stg_consumer_append_attributes') }}
    {% if is_incremental() %}
        where processed_at > (select max(updated_timestamp) from {{ this }})
    {% endif %}
),

travel_segment as (
    select 
        ts.consumerid,
        ts.tourgroups,
        ts.tourhostedtourssetdepartures,
        ts.tourcustomexperiencefit,
        ts.tourchartersincentive,
        ts.tourspecialtycruise,
        ts.tourvacationpackages,
        ts.tourtravelservices,
        ts.touractive,
        ts.tourspend12months,
        ts.tourspend12months0to10k,
        ts.tourspend12months10kto25k,
        ts.tourspend12months25kto50k,
        ts.tourspend12months50kplus,
        ts.tourspend12months0,
        ts.tourhistory1tour,
        ts.tourhistory2to5tour,
        ts.tourhistory6plustours,
        ts.tourhistory0tour,
        ts.tourpending,
        ts.tourhistory,
        current_timestamp() as processed_at
    from {{ source('consumer', 'stream_travel_segment') }} ts
    inner join {{ source('consumer', 'consumer') }} c 
        on ts.consumerid = c.consumerid
    where ts.metadata$action = 'INSERT'
    {% if is_incremental() %}
        and ts.metadata$isupdate = false
    {% endif %}
),

hotel_segment as (
    select 
        hts.consumerid,
        hts.hotelexperiencebeach,
        hts.hotelexperienceecotourism,
        hts.hotelexperiencelandmarks,
        hts.hotelexperiencecitylife,
        hts.hotelexperiencegolf,
        hts.hotelexperienceski,
        hts.hotelexperienceseclusion,
        hts.hotelexperiencelocalimmersion,
        hts.hotelexperiencewellness,
        hts.hotelexperienceadventure,
        hts.hotelroomstyleeclectic,
        hts.hotelroomstyleindigenous,
        hts.hotelroomstyleclassic,
        hts.hotelroomstylecontemporary,
        hts.hotelvibecasual,
        hts.hotelvibesophisticated,
        hts.hotelvibezen,
        hts.hotelvibehip,
        hts.hotelpending,
        hts.hotelspend12months,
        hts.hotelhistory,
        current_timestamp() as processed_at
    from {{ source('consumer', 'stream_hotel_travel_segment') }} hts
    inner join {{ source('consumer', 'consumer') }} c 
        on hts.consumerid = c.consumerid
    where hts.metadata$action = 'INSERT'
    {% if is_incremental() %}
        and hts.metadata$isupdate = false
    {% endif %}
),

profile_segment as (
    select 
        p.consumer_id as consumerid,
        p.clientsegment,
        current_timestamp() as processed_at
    from {{ source('consumer', 'stream_profile_segment') }} p
    inner join {{ source('consumer', 'consumer') }} c 
        on p.consumer_id = c.consumerid
    where p.metadata$action = 'INSERT'
    {% if is_incremental() %}
        and p.metadata$isupdate = false
    {% endif %}
),

profile_data as (
    select 
        consumerid,
        totaltripbudget_3000_8000,
        totaltripbudget_8000_15000,
        totaltripbudget_upto3000,
        totaltripbudget_15000plus,
        beddingtype_tempurpedic,
        beddingtype_tempurpedicclone,
        cartype_full_size,
        classofservice_businessclass,
        classofservice_firstclass,
        clienttype_client,
        clienttype_referral,
        clienttype_vip,
        destinations_alaska,
        destinations_antarctica,
        destinations_arctic,
        destinations_australia,
        destinations_bermuda,
        destinations_centralamerica,
        destinations_continentalus,
        destinations_costarica,
        destinations_easterneurope,
        destinations_europe,
        destinations_france,
        destinations_germany,
        destinations_hawaii,
        destinations_hongkong,
        destinations_newzealand,
        destinations_polarregions,
        destinations_spain,
        destinations_turkey,
        destinations_us_northwest,
        destinations_us_west_ca,
        destinations_worldgrandvoyages,
        interests_adventure,
        interests_climbing,
        interests_snowsports,
        lifecycle_retired,
        -- Add many more profile fields based on the stored procedure
        marketing_virtuosolife,
        marketing_virtuosolifeenespanol,
        marketing_virtuosolifeaustralianewzealand,
        current_timestamp() as processed_at
    from {{ source('consumer', 'stream_profile') }}
    where metadata$action = 'INSERT'
    {% if is_incremental() %}
        and metadata$isupdate = false
    {% endif %}
),

address_normalized as (
    select 
        consumerid,
        normalized_primary_address_1,
        normalized_primary_address_2,
        normalized_primary_apt_suite,
        normalized_primary_zip,
        normalized_primary_country,
        normalized_primary_state,
        normalized_primary_city,
        normalized_primary_region,
        normalized_secondary_address_1,
        normalized_secondary_address_2,
        normalized_secondary_apt_suite,
        normalized_secondary_zip,
        normalized_secondary_country,
        normalized_secondary_state,
        normalized_secondary_city,
        normalized_secondary_region,
        primary_geocode_id,
        secondary_geocode_id,
        current_timestamp() as processed_at
    from {{ source('consumer', 'stream_address_normalized') }}
    where metadata$action = 'INSERT'
    {% if is_incremental() %}
        and metadata$isupdate = false
    {% endif %}
),

email_validation as (
    select 
        consumerid,
        is_primary_email_valid,
        verified_primary_email,
        current_timestamp() as processed_at
    from {{ source('consumer', 'stream_email_validation') }}
    where metadata$action = 'INSERT'
    {% if is_incremental() %}
        and metadata$isupdate = false
    {% endif %}
),

agency_advisor as (
    select 
        consumerid,
        agencyid,
        agencybranchid,
        primary_advisor_id,
        marketing_advisor_id,
        marketing_agency_branch_id,
        is_agency_active,
        current_timestamp() as processed_at
    from {{ source('consumer', 'stream_agency_advisor_map') }}
    where metadata$action = 'INSERT'
    {% if is_incremental() %}
        and metadata$isupdate = false
    {% endif %}
),

consumer_excluded as (
    select 
        consumerid,
        coalesce(is_excluded, false) as is_excluded,
        exclude_reason,
        current_timestamp() as processed_at
    from {{ source('consumer', 'stream_consumer_isexcluded') }}
    where metadata$action = 'INSERT'
    {% if is_incremental() %}
        and metadata$isupdate = false
    {% endif %}
),

consumer_totalspend as (
    select 
        t.consumerid,
        t.totalspend12months,
        current_timestamp() as processed_at
    from {{ source('consumer', 'stream_consumer_totalspend') }} t
    inner join {{ source('consumer', 'consumer') }} c 
        on t.consumerid = c.consumerid
    where t.metadata$action = 'INSERT'
    {% if is_incremental() %}
        and t.metadata$isupdate = false
    {% endif %}
),

consumer_soft_deleted as (
    select 
        consumerid,
        is_soft_deleted,
        current_timestamp() as processed_at
    from {{ source('consumer', 'stream_consumer_soft_deleted') }}
    where metadata$action = 'INSERT'
    {% if is_incremental() %}
        and metadata$isupdate = false
    {% endif %}
),

-- Combine all data sources
all_consumer_updates as (
    select 
        consumerid,
        'consumer_base' as source_stream,
        processed_at
    from consumer_base
    
    union all
    
    select 
        consumerid,
        'consumer_extension' as source_stream,
        processed_at
    from consumer_extension
    
    union all
    
    select 
        consumerid,
        'crm_consumer' as source_stream,
        processed_at
    from crm_consumer
    
    -- Add all other streams...
),

-- Get the most recent update per consumer
latest_updates as (
    select 
        consumerid,
        max(processed_at) as latest_processed_at
    from all_consumer_updates
    group by consumerid
),

-- Build the final consumer master record
final_consumer_master as (
    select 
        coalesce(
            cb.consumerid,
            ce.consumerid, 
            cc.consumerid,
            cir.consumerid,
            sa.consumerid,
            em.consumerid,
            sd.consumerid,
            aa.consumerid,
            apa.consumerid,
            ts.consumerid,
            hs.consumerid,
            ps.consumerid,
            pd.consumerid,
            an.consumerid,
            ev.consumerid,
            aga.consumerid,
            ce2.consumerid,
            cts.consumerid,
            csd.consumerid
        ) as consumerid,
        
        -- Consumer base fields
        cb.sourcesystemtimestamp,
        cb.is_active,
        cb.first_name,
        cb.last_name,
        cb.membernumber,
        cb.branch_member_number,
        cb.prefix,
        cb.suffix,
        cb.salutation,
        cb.account_name,
        cb.address_line_1 as raw_address_line_1,
        cb.address_line_2 as raw_address_line_2,
        cb.aptsuite as raw_aptsuite,
        cb.city as raw_city,
        cb.state as raw_state,
        cb.zip as raw_zip,
        cb.country as raw_country,
        cb.secondary_address_line_1 as raw_secondary_address_line_1,
        cb.secondary_address_line_2 as raw_secondary_address_line_2,
        cb.secondary_aptsuite as raw_secondary_aptsuite,
        cb.secondary_city as raw_secondary_city,
        cb.secondary_state as raw_secondary_state,
        cb.secondary_zip as raw_secondary_zip,
        cb.secondary_country as raw_secondary_country,
        cb.secondary_address_from_month,
        cb.secondary_address_to_month,
        cb.primary_email,
        cb.phone_number,
        cb.business_phone,
        cb.cell_phone,
        cb.account_id,
        cb.preferred_language,
        cb.fax_phone,
        cb.sourcesystem,
        cb.primary_agent_id,
        cb.data_source_id,
        cb.create_date,
        cb.isdeleted,
        cb.deleted_timestamp,
        
        -- Extension fields
        ce.interface_id,
        ce.agent_interface_id,
        ce.extended_data,
        
        -- CRM fields
        cc.crm_consumer_id,
        
        -- Real person validation
        cir.isrealperson,
        
        -- Seasonal address
        sa.isseasonalflag,
        
        -- Email marketing
        em.email_marketing_override,
        
        -- Special dates
        sd.anniversary,
        sd.birthday,
        sd.wedding,
        
        -- Additional attributes
        aa.gender,
        aa.citizenship,
        aa.issmoker,
        aa.customertraveltype,
        aa.ismarketable,
        
        -- Append attributes (sample - would include all 200+ fields)
        apa.age,
        apa.incomerange,
        apa.mosaic,
        apa.mosaic_group,
        
        -- Travel segment
        ts.tourgroups,
        ts.touractive,
        ts.tourspend12months,
        ts.tourhistory,
        
        -- Hotel segment
        hs.hotelexperiencebeach,
        hs.hotelspend12months,
        hs.hotelhistory,
        
        -- Profile segment
        ps.clientsegment,
        
        -- Profile data (sample)
        pd.destinations_hawaii,
        pd.interests_adventure,
        pd.lifecycle_retired,
        
        -- Normalized addresses
        an.normalized_primary_address_1,
        an.normalized_primary_city,
        an.normalized_primary_state,
        an.normalized_primary_zip,
        an.primary_geocode_id,
        
        -- Email validation
        ev.is_primary_email_valid,
        ev.verified_primary_email,
        
        -- Agency advisor
        aga.agencyid,
        aga.agencybranchid,
        aga.primary_advisor_id as primary_advisor_id_agency,
        aga.marketing_advisor_id,
        aga.is_agency_active,
        
        -- Consumer exclusion
        ce2.is_excluded,
        ce2.exclude_reason,
        
        -- Total spend
        cts.totalspend12months,
        
        -- Soft deleted
        csd.is_soft_deleted,
        
        -- Audit fields
        case 
            when cb.consumerid is not null then current_timestamp()
            else null
        end as inserted_timestamp,
        current_timestamp() as updated_timestamp,
        
        -- Source tracking
        array_construct_compact(
            case when cb.consumerid is not null then 'consumer_base' end,
            case when ce.consumerid is not null then 'consumer_extension' end,
            case when cc.consumerid is not null then 'crm_consumer' end,
            case when cir.consumerid is not null then 'consumer_isrealperson' end,
            case when sa.consumerid is not null then 'seasonal_address' end,
            case when em.consumerid is not null then 'email_marketing' end,
            case when sd.consumerid is not null then 'special_dates' end,
            case when aa.consumerid is not null then 'additional_attributes' end,
            case when apa.consumerid is not null then 'append_attributes' end,
            case when ts.consumerid is not null then 'travel_segment' end,
            case when hs.consumerid is not null then 'hotel_segment' end,
            case when ps.consumerid is not null then 'profile_segment' end,
            case when pd.consumerid is not null then 'profile_data' end,
            case when an.consumerid is not null then 'address_normalized' end,
            case when ev.consumerid is not null then 'email_validation' end,
            case when aga.consumerid is not null then 'agency_advisor' end,
            case when ce2.consumerid is not null then 'consumer_excluded' end,
            case when cts.consumerid is not null then 'consumer_totalspend' end,
            case when csd.consumerid is not null then 'consumer_soft_deleted' end
        ) as source_streams_updated
        
    from consumer_base cb
    full outer join consumer_extension ce on cb.consumerid = ce.consumerid
    full outer join crm_consumer cc on coalesce(cb.consumerid, ce.consumerid) = cc.consumerid
    full outer join consumer_isrealperson cir on coalesce(cb.consumerid, ce.consumerid, cc.consumerid) = cir.consumerid
    full outer join seasonal_address sa on coalesce(cb.consumerid, ce.consumerid, cc.consumerid, cir.consumerid) = sa.consumerid
    full outer join email_marketing em on coalesce(cb.consumerid, ce.consumerid, cc.consumerid, cir.consumerid, sa.consumerid) = em.consumerid
    full outer join special_dates sd on coalesce(cb.consumerid, ce.consumerid, cc.consumerid, cir.consumerid, sa.consumerid, em.consumerid) = sd.consumerid
    full outer join additional_attributes aa on coalesce(cb.consumerid, ce.consumerid, cc.consumerid, cir.consumerid, sa.consumerid, em.consumerid, sd.consumerid) = aa.consumerid
    full outer join append_attributes apa on coalesce(cb.consumerid, ce.consumerid, cc.consumerid, cir.consumerid, sa.consumerid, em.consumerid, sd.consumerid, aa.consumerid) = apa.consumerid
    full outer join travel_segment ts on coalesce(cb.consumerid, ce.consumerid, cc.consumerid, cir.consumerid, sa.consumerid, em.consumerid, sd.consumerid, aa.consumerid, apa.consumerid) = ts.consumerid
    full outer join hotel_segment hs on coalesce(cb.consumerid, ce.consumerid, cc.consumerid, cir.consumerid, sa.consumerid, em.consumerid, sd.consumerid, aa.consumerid, apa.consumerid, ts.consumerid) = hs.consumerid
    full outer join profile_segment ps on coalesce(cb.consumerid, ce.consumerid, cc.consumerid, cir.consumerid, sa.consumerid, em.consumerid, sd.consumerid, aa.consumerid, apa.consumerid, ts.consumerid, hs.consumerid) = ps.consumerid
    full outer join profile_data pd on coalesce(cb.consumerid, ce.consumerid, cc.consumerid, cir.consumerid, sa.consumerid, em.consumerid, sd.consumerid, aa.consumerid, apa.consumerid, ts.consumerid, hs.consumerid, ps.consumerid) = pd.consumerid
    full outer join address_normalized an on coalesce(cb.consumerid, ce.consumerid, cc.consumerid, cir.consumerid, sa.consumerid, em.consumerid, sd.consumerid, aa.consumerid, apa.consumerid, ts.consumerid, hs.consumerid, ps.consumerid, pd.consumerid) = an.consumerid
    full outer join email_validation ev on coalesce(cb.consumerid, ce.consumerid, cc.consumerid, cir.consumerid, sa.consumerid, em.consumerid, sd.consumerid, aa.consumerid, apa.consumerid, ts.consumerid, hs.consumerid, ps.consumerid, pd.consumerid, an.consumerid) = ev.consumerid
    full outer join agency_advisor aga on coalesce(cb.consumerid, ce.consumerid, cc.consumerid, cir.consumerid, sa.consumerid, em.consumerid, sd.consumerid, aa.consumerid, apa.consumerid, ts.consumerid, hs.consumerid, ps.consumerid, pd.consumerid, an.consumerid, ev.consumerid) = aga.consumerid
    full outer join consumer_excluded ce2 on coalesce(cb.consumerid, ce.consumerid, cc.consumerid, cir.consumerid, sa.consumerid, em.consumerid, sd.consumerid, aa.consumerid, apa.consumerid, ts.consumerid, hs.consumerid, ps.consumerid, pd.consumerid, an.consumerid, ev.consumerid, aga.consumerid) = ce2.consumerid
    full outer join consumer_totalspend cts on coalesce(cb.consumerid, ce.consumerid, cc.consumerid, cir.consumerid, sa.consumerid, em.consumerid, sd.consumerid, aa.consumerid, apa.consumerid, ts.consumerid, hs.consumerid, ps.consumerid, pd.consumerid, an.consumerid, ev.consumerid, aga.consumerid, ce2.consumerid) = cts.consumerid
    full outer join consumer_soft_deleted csd on coalesce(cb.consumerid, ce.consumerid, cc.consumerid, cir.consumerid, sa.consumerid, em.consumerid, sd.consumerid, aa.consumerid, apa.consumerid, ts.consumerid, hs.consumerid, ps.consumerid, pd.consumerid, an.consumerid, ev.consumerid, aga.consumerid, ce2.consumerid, cts.consumerid) = csd.consumerid
    
    where coalesce(
        cb.consumerid,
        ce.consumerid, 
        cc.consumerid,
        cir.consumerid,
        sa.consumerid,
        em.consumerid,
        sd.consumerid,
        aa.consumerid,
        apa.consumerid,
        ts.consumerid,
        hs.consumerid,
        ps.consumerid,
        pd.consumerid,
        an.consumerid,
        ev.consumerid,
        aga.consumerid,
        ce2.consumerid,
        cts.consumerid,
        csd.consumerid
    ) is not null
)

select * from final_consumer_master 