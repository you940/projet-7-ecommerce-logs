-- KPI 2 : popularite - top 10 des produits les plus consultes (evenements 'view')
-- Mesure : nombre de vues ET nombre d'utilisateurs distincts
SELECT product_id, brand, category_code,
       COUNT(*)                AS nb_views,
       COUNT(DISTINCT user_id) AS nb_distinct_users
FROM events
WHERE event_type = 'view'
GROUP BY product_id, brand, category_code
ORDER BY nb_views DESC
LIMIT 10
