

{{
    config(
        materialized = 'table'
    )
}}

with days as (

    {{

            dbt_utils.date_spine(
                datepart = "day",
                start_date = "cast('2020-01-01' as date)",
                end_date = "cast('2030-12-31' as date)"
            )

    }}

),

enriched_dates as (

    select
        cast(date_day as date) as date_day,

        -- Estrazione di Anno, Mese e Trimestre
        extract(year from date_day) as date_year,
        extract(month from date_day) as date_month,
        extract(quarter from date_day) as date_quarter,

        cast(extract(year from date_day) as string) || '-' || lpad(cast(extract(month from date_day) as string), 2, '0') as year_month

    from days
)

select * from enriched_dates
