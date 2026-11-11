-- Statistiques globales : durée des sessions et temps entre actions
SELECT COUNT(*)                                  AS nb_sessions,
       ROUND(AVG(duration_s), 1)                 AS duree_moyenne_session_s,
       ROUND(AVG(nb_events), 1)                  AS events_moyens_par_session,
       ROUND(AVG(avg_secs_between_actions), 1)   AS secs_moyennes_entre_actions
FROM sessions