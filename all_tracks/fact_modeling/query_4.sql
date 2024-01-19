WITH
  today AS (
    SELECT
      *
    FROM
      anjumercian.web_users_cumulated
    WHERE
      DATE = DATE('2023-01-07')
  ),
  date_list_int AS (
    SELECT
      User_id,
browser_type,
      CAST(
        SUM(
          CASE
            WHEN CONTAINS(dates_active, sequence_date) THEN POW(2, 31 - DATE_DIFF('day', sequence_date, DATE))
            ELSE 0
          END
        ) AS BIGINT
      ) AS history_int
    FROM
      today
      CROSS JOIN UNNEST (SEQUENCE(DATE('2023-01-01'), DATE('2023-01-07'))) AS t (sequence_date)
    GROUP BY
      User_id, browser_type
  )
SELECT
  *,
  TO_BASE(history_int, 2) AS history_in_binary,
  TO_BASE(
    FROM_BASE('11111110000000000000000000000000', 2),
    2
  ) AS weekly_base,
  BIT_COUNT(history_int, 64) AS num_days_active,
  BIT_COUNT(
    BITWISE_AND(
      history_int,
      FROM_BASE('11111110000000000000000000000000', 2)
    ),
    64
  ) > 0 AS is_weekly_active,
  BIT_COUNT(
    BITWISE_AND(
      history_int,
      FROM_BASE('00000001111111000000000000000000', 2)
    ),
    64
  ) > 0 AS is_weekly_active_last_week,
  BIT_COUNT(
    BITWISE_AND(
      history_int,
      FROM_BASE('11100000000000000000000000000000', 2)
    ),
    64
  ) > 0 AS is_active_last_three_days
FROM
  date_list_int
