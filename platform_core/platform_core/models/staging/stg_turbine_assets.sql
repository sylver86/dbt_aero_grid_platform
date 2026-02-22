with source as (

    select * from {{ source('external_data_raw', 'raw_turbine_assets') }}

),

renamed as (

    select
        trim(turbine_id) as turbine_id,
        model,
        location,
        cast(installation_date as date) as installation_date,
        case when capacity_kw = 0 then null else capacity_kw end as capacity_kw

    from source

),

deduplicated as (

    select distinct
    {{ dbt_utils.generate_surrogate_key(['turbine_id']) }} as turbine_key,                          /* Chiave primaria */
    *
    from renamed

)

select * from deduplicated
