{{
  config(
    materialized='view',
    tags=['analytics', 'kpi', 'produits']
  )
}}

WITH fact AS (
    SELECT * FROM {{ ref('faits_ventes') }}
),

dim_produit AS (
    SELECT * FROM {{ ref('dim_produit') }}
),

dim_date AS (
    SELECT * FROM {{ ref('dim_date') }}
),

top_produits AS (
    SELECT
        -- Informations produit
        p.id_produit,
        p.nom_produit,
        p.categorie,
        p.sous_categorie,
        
        -- Métriques globales
        SUM(f.montant_ventes) AS ca_total,
        SUM(f.profit) AS profit_total,
        SUM(f.quantite) AS quantite_totale,
        COUNT(DISTINCT f.id_commande) AS nb_commandes,
        COUNT(DISTINCT f.id_client) AS nb_clients,
        
        -- Métriques moyennes
        AVG(f.prix_unitaire) AS prix_moyen,
        AVG(f.remise) AS remise_moyenne,
        ROUND(AVG(f.marge_pct), 2) AS marge_moyenne_pct,
        
        -- Métriques calculées
        ROUND(SUM(f.profit) / NULLIF(SUM(f.montant_ventes), 0) * 100, 2) AS marge_realisee_pct,
        ROUND(SUM(f.montant_ventes) / COUNT(DISTINCT f.id_commande), 2) AS ca_moyen_par_commande,
        
        -- Ranking
        ROW_NUMBER() OVER (ORDER BY SUM(f.montant_ventes) DESC) AS rang_ca,
        ROW_NUMBER() OVER (ORDER BY SUM(f.profit) DESC) AS rang_profit,
        ROW_NUMBER() OVER (ORDER BY SUM(f.quantite) DESC) AS rang_quantite,
        
        -- Part de marché
        ROUND(
            (SUM(f.montant_ventes) / SUM(SUM(f.montant_ventes)) OVER ()) * 100, 
            2
        ) AS part_ca_pct,
        
        -- Performance récente (30 derniers jours)
        SUM(CASE WHEN d.date_day >= DATEADD(day, -30, CURRENT_DATE()) 
            THEN f.montant_ventes ELSE 0 END) AS ca_30j,
        
        SUM(CASE WHEN d.date_day >= DATEADD(day, -30, CURRENT_DATE()) 
            THEN f.profit ELSE 0 END) AS profit_30j,
        
        -- Période d'activité
        MIN(d.date_day) AS premiere_vente,
        MAX(d.date_day) AS derniere_vente,
        DATEDIFF(day, MIN(d.date_day), MAX(d.date_day)) AS nb_jours_actif
        
    FROM fact f
    JOIN dim_produit p ON f.id_produit = p.id_produit
    JOIN dim_date d ON f.date_key = d.date_id
    
    GROUP BY 
        p.id_produit,
        p.nom_produit,
        p.categorie,
        p.sous_categorie
)

SELECT * FROM top_produits
ORDER BY ca_total DESC