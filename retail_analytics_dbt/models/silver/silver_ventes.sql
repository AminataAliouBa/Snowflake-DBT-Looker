{{
  config(
    materialized='table',
    tags=['silver', 'cleaning']
  )
}}

WITH source AS (
    SELECT * FROM {{ ref('bronze_hypermarche') }}
),

cleaned AS (
    SELECT
        -- Identifiants
        UPPER(TRIM(id_ligne)) AS id_ligne,
        UPPER(TRIM(id_commande)) AS id_commande,
        
        -- Dates
        date_commande,
        date_expedition,
        DATEDIFF(day, date_commande, date_expedition) AS delai_expedition_jours,
        
        -- Expédition
        TRIM(mode_expedition) AS mode_expedition,
        
        -- Client
        UPPER(TRIM(id_client)) AS id_client,
        TRIM(nom_client) AS nom_client,
        UPPER(TRIM(segment)) AS segment,
        
        -- Géographie
        TRIM(ville) AS ville,
        TRIM(region) AS region,
        TRIM(pays) AS pays,
        TRIM(zone_geographique) AS zone_geographique,
        
        -- Produit
        UPPER(TRIM(id_produit)) AS id_produit,
        TRIM(categorie) AS categorie,
        TRIM(sous_categorie) AS sous_categorie,
        TRIM(nom_produit) AS nom_produit,
        
        -- Métriques financières
        montant_ventes,
        quantite,
        remise,
        profit,
        
        -- Métriques calculées
        CASE 
            WHEN quantite > 0 THEN ROUND(montant_ventes / quantite, 2)
            ELSE 0
        END AS prix_unitaire,
        
        montant_ventes - profit AS cout_total,
        
        CASE 
            WHEN montant_ventes > 0 THEN ROUND((profit / montant_ventes) * 100, 2)
            ELSE 0
        END AS marge_pct,
        
        -- Montant sans remise (reconstruit)
        CASE 
            WHEN remise > 0 THEN ROUND(montant_ventes / (1 - remise), 2)
            ELSE montant_ventes
        END AS montant_avant_remise,
        
        -- Extraction temporelle
        YEAR(date_commande) AS annee,
        MONTH(date_commande) AS mois,
        QUARTER(date_commande) AS trimestre,
        DAYOFWEEK(date_commande) AS jour_semaine,
        
        -- Flags de qualité
        CASE 
            WHEN profit < 0 THEN TRUE 
            ELSE FALSE 
        END AS flag_perte,
        
        CASE 
            WHEN remise >= 0.5 THEN TRUE 
            ELSE FALSE 
        END AS flag_forte_remise,
        
        CASE 
            WHEN date_expedition < date_commande THEN TRUE
            WHEN DATEDIFF(day, date_commande, date_expedition) > 30 THEN TRUE
            ELSE FALSE 
        END AS flag_delai_anormal,
        
        -- Audit
        _loaded_at,
        _source_file,
        _file_row_number,
        CURRENT_TIMESTAMP() AS _transformed_at
        
    FROM source
    
    -- Filtres de qualité
    WHERE 1=1
        AND date_commande IS NOT NULL
        AND id_commande IS NOT NULL
        AND id_client IS NOT NULL
        AND id_produit IS NOT NULL
        AND montant_ventes IS NOT NULL
        AND quantite IS NOT NULL
        AND quantite > 0
)

SELECT * FROM cleaned