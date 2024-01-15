-- Insert the data into that table (`insert_average_web_events`)
INSERT INTO insert_average_web_events
SELECT window_timestamp as event_hour,
    host, referrer, COUNT(host) as num_hits from processed_events
