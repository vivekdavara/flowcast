-- Per one-minute tumbling window of event time and per event type: the number of events and the purchase revenue
-- in cents, over the relation `events`, counting each event_id once: the copy that came first by `input_order` (the
-- order records were received; for records read from Kafka, their offset, since a duplicate shares its original's key
-- and so its partition). DESIGN.md "Ground truth". Windows are [start, start + 60 s) with
-- start = floor(event_time / 60000) * 60000, Spark's window() alignment.
WITH unique_events AS (
    SELECT DISTINCT ON (event_id) *
    FROM events
    ORDER BY event_id, input_order
)
SELECT
    (event_time // 60000) * 60000 AS window_start,
    event_type,
    count(*) AS events,
    CAST(sum(CASE WHEN event_type = 'purchase' THEN price_cents * quantity ELSE 0 END) AS BIGINT) AS revenue_cents
FROM unique_events
GROUP BY ALL
ORDER BY window_start, event_type
