-- Une ligne par session : durée, nombre d'événements, temps moyen entre actions, panier, achat
SELECT session_id,
       MIN(user_id)                                                       AS user_id,
       MIN(event_time)                                                    AS session_start,
       MAX(event_time)                                                    AS session_end,
       unix_timestamp(MAX(event_time)) - unix_timestamp(MIN(event_time))  AS duration_s,
       COUNT(*)                                                           AS nb_events,
       ROUND(AVG(secs_since_prev), 1)                                     AS avg_secs_between_actions,
       MAX(CASE WHEN action_type = 'add_to_cart' THEN 1 ELSE 0 END)       AS has_cart,
       MAX(CASE WHEN action_type = 'purchase'    THEN 1 ELSE 0 END)       AS has_purchase
FROM events_enriched
GROUP BY session_id