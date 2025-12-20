{{
  config(
    materialized='view',
    tags=['analytics', 'kpi', 'dashboard']
  )
}}

WITH fact AS (
    SELECT * FROM {{ ref('faits_ventes') }}
),

dim_date AS (
    SELECT * FROM {{ ref('dim_date') }}
),

dim_client AS (
    SELECT * FROM {{ ref('dim_client') }}
),

dim_produit AS (
    SELECT * FROM {{ ref('dim_produit') }}
),
dim_commande AS (
    SELECT * FROM {{ ref('dim_commande') }}
),

performance_mensuelle AS (
    SELECT
        -- Dimensions temporelles
        d.annee,
        d.mois_numero,
        d.mois_nom,
        d.trimestre,
        d.annee_mois,
        
        -- Dimensions business
        c.segment,
        c.region,
        c.pays,
        c.zone_geographique,
        p.categorie,
        p.sous_categorie,
        
        -- Métriques de vente
        COUNT(DISTINCT co.id_commande) AS nb_commandes,
        COUNT(DISTINCT f.id_client) AS nb_clients,
        SUM(f.montant_ventes) AS ca_realise,
        SUM(f.profit) AS profit_total,
        SUM(f.quantite) AS quantite_totale,
        SUM(f.montant_remise) AS remise_totale,
        
        -- Métriques moyennes
        AVG(f.montant_ventes) AS panier_moyen,
        AVG(f.prix_unitaire) AS prix_unitaire_moyen,
        AVG(f.remise) AS remise_moyenne,
        AVG(f.marge_pct) AS marge_moyenne_pct,
        AVG(co.delai_expedition_jours) AS delai_moyen_jours,
        
        -- Objectifs
        MAX(f.objectif_vente) AS objectif_vente,
        
        -- Performance vs objectifs
        AVG(f.taux_atteinte_objectif_pct) AS taux_atteinte_moyen,
        SUM(f.ecart_objectif) AS ecart_total_objectif,
        
        -- Compteurs de flags
        SUM(CASE WHEN f.flag_perte THEN 1 ELSE 0 END) AS nb_lignes_perte,
        SUM(CASE WHEN f.flag_forte_remise THEN 1 ELSE 0 END) AS nb_fortes_remises,
        SUM(CASE WHEN f.flag_avec_remise THEN 1 ELSE 0 END) AS nb_avec_remise,
        
        -- Evolution (comparaison N vs N-1)
        LAG(SUM(f.montant_ventes)) OVER (
            PARTITION BY c.segment, p.categorie 
            ORDER BY d.annee, d.mois_numero
        ) AS ca_mois_precedent,
        
        -- Calcul de croissance
        CASE 
            WHEN LAG(SUM(f.montant_ventes)) OVER (
                PARTITION BY c.segment, p.categorie 
                ORDER BY d.annee, d.mois_numero
            ) > 0
            THEN ROUND(
                ((SUM(f.montant_ventes) - LAG(SUM(f.montant_ventes)) OVER (
                    PARTITION BY c.segment, p.categorie 
                    ORDER BY d.annee, d.mois_numero
                )) / LAG(SUM(f.montant_ventes)) OVER (
                    PARTITION BY c.segment, p.categorie 
                    ORDER BY d.annee, d.mois_numero
                )) * 100, 2
            )
            ELSE NULL
        END AS croissance_mensuelle_pct
        
    FROM fact f
    JOIN dim_date d ON f.date_key = d.date_id
    JOIN dim_client c ON f.id_client = c.id_client
    JOIN dim_commande co ON f.id_commande = co.id_commande
    JOIN dim_produit p ON f.id_produit = p.id_produit
    
    GROUP BY 
        d.annee,
        d.mois_numero,
        d.mois_nom,
        d.trimestre,
        d.annee_mois,
        c.segment,
        c.region,
        c.pays,
        c.zone_geographique,
        p.categorie,
        p.sous_categorie
)

SELECT * FROM performance_mensuelle
ORDER BY annee DESC, mois_numero DESC, ca_realise DESC