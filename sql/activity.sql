-- KPI 1 : activite temporelle - nombre d'evenements par jour et par type
SELECT event_date,
       COUNT(*)                                                  AS nb_events,
       SUM(CASE WHEN event_type = 'view'     THEN 1 ELSE 0 END) AS nb_views,
       SUM(CASE WHEN event_type = 'cart'     THEN 1 ELSE 0 END) AS nb_cart,
       SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS nb_purchase
FROM events
GROUP BY event_date
ORDER BY event_date
