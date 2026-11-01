-- Nettoyage et enrichissement en Spark SQL (vue source : raw_events)
-- Regles :
--  R1 : lignes sans date, type, produit, utilisateur ou session supprimees
--  R2 : seuls les types view / cart / remove_from_cart / purchase sont conserves
--  R3 : prix negatifs supprimes (prix absent tolere)
--  R4 : brand / category_code manquants remplaces par 'unknown'
--  R5 : colonnes derivees : event_date, event_hour, category_main
--  (R6 : suppression des doublons exacts, faite dans transform.py via dropDuplicates)
SELECT
    to_timestamp(regexp_replace(event_time, ' UTC$', '')) AS event_time,
    event_type,
    product_id,
    category_id,
    COALESCE(category_code, 'unknown') AS category_code,
    COALESCE(brand, 'unknown')         AS brand,
    price,
    user_id,
    user_session,
    to_date(to_timestamp(regexp_replace(event_time, ' UTC$', '')))     AS event_date,
    hour(to_timestamp(regexp_replace(event_time, ' UTC$', '')))        AS event_hour,
    split(COALESCE(category_code, 'unknown'), '[.]')[0]                AS category_main
FROM raw_events
WHERE event_time   IS NOT NULL
  AND event_type   IN ('view', 'cart', 'remove_from_cart', 'purchase')
  AND product_id   IS NOT NULL
  AND user_id      IS NOT NULL
  AND user_session IS NOT NULL
  AND (price IS NULL OR price >= 0)
