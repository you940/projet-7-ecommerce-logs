-- KPI 3 : engagement - utilisateurs actifs et sessions par jour
-- Utilisateur actif = user_id distinct ; session = user_session distinct
SELECT event_date,
       COUNT(DISTINCT user_id)      AS active_users,
       COUNT(DISTINCT user_session) AS sessions,
       ROUND(COUNT(*) / COUNT(DISTINCT user_session), 2) AS events_per_session
FROM events
GROUP BY event_date
ORDER BY event_date
