-- KPI 4 : parcours d'achat (entonnoir) au niveau des SESSIONS
-- Une ligne par session, avec 3 indicateurs : a-t-elle une vue / un ajout panier / un achat ?
-- Attention : un evenement 'purchase' n'est pas forcement une commande.
-- On mesure aussi les achats SANS ajout au panier dans la session (frequents dans ce dataset).
WITH s AS (
  SELECT user_session,
         MAX(CASE WHEN event_type = 'view'     THEN 1 ELSE 0 END) AS has_view,
         MAX(CASE WHEN event_type = 'cart'     THEN 1 ELSE 0 END) AS has_cart,
         MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS has_purchase
  FROM events
  GROUP BY user_session
)
SELECT
  SUM(has_view)                                                                   AS sessions_view,
  SUM(CASE WHEN has_view = 1 AND has_cart = 1 THEN 1 ELSE 0 END)                  AS sessions_view_cart,
  SUM(CASE WHEN has_view = 1 AND has_cart = 1 AND has_purchase = 1 THEN 1 ELSE 0 END) AS sessions_view_cart_purchase,
  SUM(has_purchase)                                                               AS sessions_purchase_total,
  SUM(CASE WHEN has_purchase = 1 AND has_cart = 0 THEN 1 ELSE 0 END)              AS purchase_without_cart,
  ROUND(100.0 * SUM(CASE WHEN has_view = 1 AND has_cart = 1 THEN 1 ELSE 0 END) / SUM(has_view), 2) AS pct_view_to_cart,
  ROUND(100.0 * SUM(CASE WHEN has_view = 1 AND has_cart = 1 AND has_purchase = 1 THEN 1 ELSE 0 END) / SUM(has_view), 2) AS pct_view_cart_to_purchase
FROM s