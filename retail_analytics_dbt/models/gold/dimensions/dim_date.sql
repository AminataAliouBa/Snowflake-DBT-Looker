{{
  config(
    materialized='table',
    tags=['gold', 'dimension', 'date']
  )
}}

WITH date_spine AS (
    -- Génération d'une série de dates
    SELECT 
        DATEADD(
            day, 
            SEQ4(), 
            TO_DATE('{{ var("start_date") }}')
        ) AS date_day
    FROM TABLE(GENERATOR(ROWCOUNT => 3653))  -- ~10 ans
    WHERE date_day <= TO_DATE('{{ var("end_date") }}')
),

date_dimension AS (
    SELECT
        -- Clé primaire
        date_day AS date_id,
        date_day,
        
        -- Attributs année
        YEAR(date_day) AS annee,
        YEAR(date_day) || '-' || LPAD(MONTH(date_day), 2, '0') AS annee_mois,
        YEAR(date_day) || '-Q' || QUARTER(date_day) AS annee_trimestre,
        
        -- Attributs mois
        MONTH(date_day) AS mois_numero,
        MONTHNAME(date_day) AS mois_nom,
        LPAD(MONTH(date_day), 2, '0') || '-' || MONTHNAME(date_day) AS mois_complet,
        
        -- Attributs trimestre
        QUARTER(date_day) AS trimestre,
        'T' || QUARTER(date_day) AS trimestre_label,
        
        -- Attributs semaine
        WEEKOFYEAR(date_day) AS semaine_annee,
        WEEK(date_day) AS semaine_numero,
        
        -- Attributs jour
        DAY(date_day) AS jour_mois,
        DAYOFYEAR(date_day) AS jour_annee,
        DAYOFWEEK(date_day) AS jour_semaine_numero,
        DAYNAME(date_day) AS jour_semaine_nom,
        
        -- Flags utiles
        CASE 
            WHEN DAYOFWEEK(date_day) IN (6, 7) THEN TRUE 
            ELSE FALSE 
        END AS est_weekend,
        
        CASE 
            WHEN DAYOFWEEK(date_day) IN (1, 2, 3, 4, 5) THEN TRUE 
            ELSE FALSE 
        END AS est_jour_ouvrable,
        
        CASE 
            WHEN DAY(date_day) = 1 THEN TRUE 
            ELSE FALSE 
        END AS est_premier_jour_mois,
        
        CASE 
            WHEN date_day = LAST_DAY(date_day) THEN TRUE 
            ELSE FALSE 
        END AS est_dernier_jour_mois,
        
        -- Dates relatives
        DATEADD(day, -1, date_day) AS date_jour_precedent,
        DATEADD(day, 1, date_day) AS date_jour_suivant,
        DATEADD(week, -1, date_day) AS date_semaine_precedente,
        DATEADD(month, -1, date_day) AS date_mois_precedent,
        DATEADD(year, -1, date_day) AS date_annee_precedente,
        
        -- Audit
        CURRENT_TIMESTAMP() AS date_creation
        
    FROM date_spine
)

SELECT * FROM date_dimension
ORDER BY date_day