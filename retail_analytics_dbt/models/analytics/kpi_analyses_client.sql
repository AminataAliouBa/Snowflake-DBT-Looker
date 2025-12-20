{{
  config(
    materialized='view',
    tags=['analytics', 'kpi', 'clients']
  )
}}

WITH fact AS (
    SELECT * FROM {{ ref('faits_ventes') }}
),

dim_client AS (
    SELECT * FROM {{ ref('dim_client') }}
),

dim_date AS (
    SELECT * FROM {{ ref('dim_date') }}
),
dim_commande AS (
    SELECT * FROM {{ ref('dim_commande') }}
),

-- Étape 1 : CA total par client
ca_client AS (
    SELECT
        c.id_client,
        SUM(f.montant_ventes) AS montant_total
    FROM fact f
    JOIN dim_client c ON f.id_client = c.id_client
    GROUP BY c.id_client
),

-- Étape 2 : calcul des percentiles
seuils AS (
    SELECT
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY montant_total) AS p75,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY montant_total) AS p50,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY montant_total) AS p25
    FROM ca_client
),

-- Étape 3 : Agrégations RFM et détails client
analyse_clients_pre AS (
    SELECT
        c.id_client,
        c.nom_client,
        c.segment,
        c.ville,
        c.region,
        c.pays,
        c.zone_geographique,
        
        -- RFM
        MAX(d.date_day) AS date_derniere_commande,
        COUNT(DISTINCT f.id_commande) AS frequence_commandes,
        SUM(f.montant_ventes) AS montant_total,

        -- Détails ventes
        SUM(f.profit) AS profit_total,
        SUM(f.quantite) AS quantite_totale,
        COUNT(DISTINCT f.id_produit) AS nb_produits_distincts,

        -- Moyennes
        ROUND(AVG(f.montant_ventes), 2) AS panier_moyen,
        ROUND(AVG(f.prix_unitaire), 2) AS prix_unitaire_moyen,
        ROUND(AVG(f.remise), 4) AS remise_moyenne,

        -- Préférences
        MODE(co.mode_expedition) AS mode_expedition_prefere,

        -- Cycle de vie
        MIN(d.date_day) AS premiere_commande,
        MAX(d.date_day) AS derniere_commande,
        DATEDIFF(day, MIN(d.date_day), MAX(d.date_day)) AS duree_vie_client_jours,

        -- Statut client
        CASE
            WHEN DATEDIFF(day, MAX(d.date_day), CURRENT_DATE()) <= 30 THEN 'Actif'
            WHEN DATEDIFF(day, MAX(d.date_day), CURRENT_DATE()) <= 90 THEN 'À risque'
            WHEN DATEDIFF(day, MAX(d.date_day), CURRENT_DATE()) <= 180 THEN 'Dormant'
            ELSE 'Perdu'
        END AS statut_client,

        -- Percentiles pour classification
        s.p75,
        s.p50,
        s.p25

    FROM fact f
    JOIN dim_client c ON f.id_client = c.id_client
    JOIN dim_commande co ON f.id_commande = co.id_commande
    JOIN dim_date d ON f.date_key = d.date_id
    CROSS JOIN seuils s
    GROUP BY
        c.id_client,
        c.nom_client,
        c.segment,
        c.ville,
        c.region,
        c.pays,
        c.zone_geographique,
        s.p75, s.p50, s.p25
),

-- Étape 4 : Classification finale
analyse_clients AS (
    SELECT
        *,
        CASE
            WHEN montant_total >= p75 THEN 'VIP'
            WHEN montant_total >= p50 THEN 'Important'
            WHEN montant_total >= p25 THEN 'Standard'
            ELSE 'Occasionnel'
        END AS categorie_valeur
    FROM analyse_clients_pre
)

SELECT *
FROM analyse_clients
ORDER BY montant_total DESC
