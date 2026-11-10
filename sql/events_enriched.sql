-- Window Functions : rang de l'événement dans la session + durée depuis l'action précédente
SELECT session_id, user_id, product_id, action_type, event_time, dt,
       event_rank,
       prev_time,
       unix_timestamp(event_time) - unix_timestamp(prev_time) AS secs_since_prev
FROM (
    SELECT *,
           ROW_NUMBER() OVER w AS event_rank,
           LAG(event_time)  OVER w AS prev_time
    FROM events
    WINDOW w AS (PARTITION BY session_id ORDER BY event_time)
)