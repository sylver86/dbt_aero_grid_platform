{% snapshot turbine_assets %}

/* Definisco lo snapshot verificando cosa cambia nei 3 campi indicati usando la chiave 'turbine_key' */

{{
    config(
        unique_key='turbine_id',
        strategy='check',
        check_cols=['capacity_kw', 'location', 'model']
    )
}}

with source_data as (
    select * from {{ source('external_data_raw', 'raw_turbine_assets') }}
),

deduplicated as (
                                                                                                    -- Usiamo una window function per numerare le righe con lo stesso ID
                                                                                                    -- Ordiniamo per una data o un ID se presente, altrimenti prendiamo una riga arbitraria
    select *,
        row_number() over (
            partition by turbine_id
            order by installation_date desc
        ) as row_num
    from source_data
)

-- Selezioniamo solo la riga numero 1 per ogni turbine_id in questo modo evitiamo duplicazioni nello snapshot
select * except(row_num)
from deduplicated
where row_num = 1

{% endsnapshot %}
