
/* Test che verifica  la coerenza tra velocità del vento e produzione elettrica.
   Regola: Una turbina non può produrre energia (power_output_kw > 0) se il vento
           è sotto la soglia di "cut-in" (es. meno di 2 m/s).
           Se succede, c'è un errore nel sensore o nei dati.
*/

Select
a.turbine_id,
a.wind_speed_ms,
a.power_output_kw,
b.capacity_kw
from {{ ref('stg_turbine_telemetry') }} a
left join
{{ref('stg_turbine_assets')}} b
on a.turbine_id = b.turbine_id
Where (wind_speed_ms < 2.0 and power_output_kw > 0)
or a.power_output_kw > b.capacity_kw
