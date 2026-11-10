-- Nettoyage en Spark SQL (vue source : raw_events)
-- R1 : lignes avec user_id, product_id ou session_id manquant supprimées
-- R2 : timestamp invalide supprimé
-- R3 : seuls les 5 types d'action valides sont conservés
-- R4 : colonne dt (date de partition) dérivée du timestamp
-- (R5 : doublons exacts supprimés dans transform.py via dropDuplicates)
SELECT
    user_id,
    product_id,
    session_id,
    action_type,
    to_timestamp(`timestamp`)                            AS event_time,
    date_format(to_timestamp(`timestamp`), 'yyyy-MM-dd') AS dt
FROM raw_events
WHERE user_id     IS NOT NULL
  AND product_id  IS NOT NULL
  AND session_id  IS NOT NULL
  AND to_timestamp(`timestamp`) IS NOT NULL
  AND action_type IN ('page_view', 'click', 'add_to_cart', 'remove_from_cart', 'purchase')