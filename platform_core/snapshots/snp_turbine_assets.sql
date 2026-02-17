{#
    Se una turbina viene potenziata o se la sua posizione viene corretta
    nel DB ci registriamo i cambiamenti (SCD Type 2).
#}

{% snapshot turbine_assets %}

{{
    config(
        unique_key='turbine_id',
        strategy='check',
        check_cols=['capacity_kw', 'location', 'model']
    )
}}

select * from (
    select *,
           row_number() over (partition by turbine_id order by installation_date desc) as rn
    from {{ source('external_data_raw', 'raw_turbine_assets') }}
) where rn = 1

{% endsnapshot %}
