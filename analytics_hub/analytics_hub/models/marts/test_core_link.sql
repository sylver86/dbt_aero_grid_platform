{{ config(materialized='view') }}

select * from {{ ref('platform_core', 'stg_turbine_assets') }}
limit 10
