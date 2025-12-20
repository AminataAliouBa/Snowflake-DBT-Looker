{{
  config(
    materialized='table',
    tags=['gold', 'dimension', 'commande'],
    unique_key='produit_key'
  )
}}


SELECT DISTINCT
    id_commande,
    date_commande,
    date_expedition,
    mode_expedition,
    delai_expedition_jours,
    -- Audit
    CURRENT_TIMESTAMP() AS date_creation
FROM {{ ref('silver_ventes') }}