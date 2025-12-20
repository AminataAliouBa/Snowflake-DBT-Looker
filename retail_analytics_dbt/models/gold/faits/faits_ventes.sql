{{
  config(
    materialized='table',
    tags=['gold', 'fact', 'ventes'],
    unique_key='vente_key'
  )
}}

WITH ventes AS (
    SELECT * FROM {{ ref('silver_ventes') }}
),

objectifs AS (
    SELECT * FROM {{ ref('silver_objectifs') }}
),

dim_date AS (
    SELECT * FROM {{ ref('dim_date') }}
),

fact_table AS (
    SELECT
        -- Surrogate key pour la ligne de vente
        MD5(v.id_ligne) AS vente_key,
        
        -- Foreign Keys vers les dimensions
        d.date_id AS date_key,
        v.id_client,
        v.id_produit,
        v.id_commande,
        
        -- Mesures additives (peuvent être sommées)
        v.montant_ventes,
        v.quantite,
        v.profit,
        v.cout_total,
        v.montant_avant_remise,
        v.montant_avant_remise - v.montant_ventes AS montant_remise,
        
        -- Mesures semi-additives (moyennes)
        v.prix_unitaire,
        v.remise,
        v.marge_pct,
        
        -- Objectifs (mesures de comparaison) - agrégés par mois/catégorie/segment
        o.objectif_vente,
        
        -- KPIs calculés
        CASE 
            WHEN o.objectif_vente IS NOT NULL AND o.objectif_vente > 0
            THEN ROUND((v.montant_ventes / o.objectif_vente) * 100, 2)
            ELSE NULL 
        END AS taux_atteinte_objectif_pct,
        
        CASE 
            WHEN o.objectif_vente IS NOT NULL
            THEN v.montant_ventes - o.objectif_vente
            ELSE NULL 
        END AS ecart_objectif,
        
        -- Flags de qualité et business
        v.flag_perte,
        v.flag_forte_remise,
        v.flag_delai_anormal,
        
        CASE 
            WHEN o.objectif_vente IS NULL THEN TRUE 
            ELSE FALSE 
        END AS flag_sans_objectif,
        
        CASE 
            WHEN v.remise > 0 THEN TRUE 
            ELSE FALSE 
        END AS flag_avec_remise,
        
        -- Métadonnées d'audit
        v._loaded_at AS date_chargement_source,
        v._transformed_at AS date_transformation,
        CURRENT_TIMESTAMP() AS date_creation_fact
        
    FROM ventes v
    
    -- Jointures aux dimensions (LEFT JOIN pour ne pas perdre de données)
    LEFT JOIN dim_date d 
        ON v.date_commande = d.date_day
    
    -- Jointure aux objectifs (LEFT JOIN car pas d'objectifs pour toutes les lignes)
    -- Agrégation au niveau mois/catégorie/segment
    LEFT JOIN objectifs o 
        ON  o.date_commande = v.date_commande
        AND v.categorie = o.categorie
        AND v.segment = o.segment
    
    -- Filtres de qualité finale
    WHERE 1=1
        AND d.date_id IS NOT NULL  -- S'assurer que la date existe
        AND v.id_client IS NOT NULL  -- S'assurer que le client existe
        AND v.id_produit IS NOT NULL  -- S'assurer que le produit existe
)

SELECT distinct * FROM fact_table