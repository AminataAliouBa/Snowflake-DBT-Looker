{{
  config(
    materialized='table',
    tags=['silver', 'cleaning']
  )
}}

WITH source AS (
    SELECT * FROM {{ ref('bronze_objectifs_ventes') }}
),

cleaned AS (
    SELECT
        -- Dimensions
        TRIM(categorie) AS categorie,
        UPPER(TRIM(segment)) AS segment,
        date_commande,
        
        -- Objectif
        objectif_vente,
        
        -- Flags de qualité
        CASE 
            WHEN objectif_vente IS NULL OR objectif_vente <= 0 THEN TRUE 
            ELSE FALSE 
        END AS flag_objectif_invalide,
        
        -- Audit
        _loaded_at,
        _source_file,
        _file_row_number,
        CURRENT_TIMESTAMP() AS _transformed_at
        
    FROM source
    
    -- Filtres de qualité
    WHERE 1=1
        AND date_commande IS NOT NULL
        AND categorie IS NOT NULL
        AND segment IS NOT NULL
        AND objectif_vente > 0
)

SELECT * FROM cleaned