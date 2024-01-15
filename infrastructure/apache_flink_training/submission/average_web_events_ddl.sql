-- Define a table to store the average number of web events in a session by hostname (`average_web_events_ddl`)
CREATE TABLE average_web_events_ddl (
            event_hour TIMESTAMP(3),
            host VARCHAR,
            referrer VARCHAR,
            num_hits BIGINT
        ) 
