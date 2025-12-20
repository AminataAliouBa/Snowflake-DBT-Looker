{{
  config(
    materialized='table',
    tags=['gold', 'dimension', 'produit'],
    unique_key='produit_key'
  )
}}


SELECT DISTINCT
    id_produit,
    nom_produit,
    categorie,
    sous_categorie,
    -- Audit
    CURRENT_TIMESTAMP() AS date_creation
FROM {{ ref('silver_ventes') }}