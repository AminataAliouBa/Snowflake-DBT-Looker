{{
  config(
    materialized='table',
    tags=['gold', 'dimension', 'client'],
    unique_key='client_key'
  )
}}

SELECT DISTINCT
    id_client, -- natural key
    nom_client,
    segment,
    ville,
    region,
    pays,
    zone_geographique,
    -- Audit
    CURRENT_TIMESTAMP() AS date_creation
FROM {{ ref('silver_ventes') }}
