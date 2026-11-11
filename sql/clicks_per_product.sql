-- Clics, vues, ajouts panier et achats par produit
SELECT product_id,
       SUM(CASE WHEN action_type = 'click'       THEN 1 ELSE 0 END) AS nb_clics,
       SUM(CASE WHEN action_type = 'page_view'   THEN 1 ELSE 0 END) AS nb_vues,
       SUM(CASE WHEN action_type = 'add_to_cart' THEN 1 ELSE 0 END) AS nb_ajouts_panier,
       SUM(CASE WHEN action_type = 'purchase'    THEN 1 ELSE 0 END) AS nb_achats
FROM events
GROUP BY product_id
ORDER BY nb_clics DESC, product_id