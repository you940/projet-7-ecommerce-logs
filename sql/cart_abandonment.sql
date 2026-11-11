-- Taux de conversion et taux d'abandon de panier, au niveau des sessions
-- Abandon = session avec au moins un ajout au panier mais sans achat
SELECT COUNT(*)           AS total_sessions,
       SUM(has_cart)      AS sessions_avec_panier,
       SUM(has_purchase)  AS sessions_avec_achat,
       SUM(CASE WHEN has_cart = 1 AND has_purchase = 0 THEN 1 ELSE 0 END) AS paniers_abandonnes,
       ROUND(100.0 * SUM(CASE WHEN has_cart = 1 AND has_purchase = 0 THEN 1 ELSE 0 END)
             / SUM(has_cart), 2)                    AS taux_abandon_pct,
       ROUND(100.0 * SUM(has_purchase) / COUNT(*), 2) AS taux_conversion_pct
FROM sessions