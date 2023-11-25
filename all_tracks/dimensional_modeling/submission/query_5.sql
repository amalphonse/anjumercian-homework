--query 5
--Incremental query for actors_scd. Merge into to update the previous year before procedding onto this year
INSERT INTO anjumercian.actors_history_scd
WITH
last_season_scd AS (
  SELECT *
  FROM anjumercian.actors_history_scd
  WHERE current_year = 2001 ),
current_season_scd AS (
  SELECT *
  FROM anjumercian.actors
  WHERE current_year = 2002),
combined AS (
  SELECT
    COALESCE(ls.actor, cs.actor) AS actor,
    COALESCE(ls.start_date, cs.current_year) as start_date,
    COALESCE(ls.end_date, cs.current_year) AS end_date,
    CASE
      WHEN ls.is_active <> cs.is_active THEN 1
      WHEN ls.is_active = cs.is_active THEN 0 END as did_change,
    ls.is_active as is_active_last_year,
    cs.is_active as is_active_this_year,
    2002 AS current_year
  FROM last_season_scd ls
  FULL OUTER JOIN current_season_scd cs
  ON ls.actor = cs.actor AND ls.end_date + 1 = cs.current_year),
changes AS (
  SELECT
    actor,
    current_year,
    CASE
      WHEN did_change = 0 THEN
        ARRAY[
            CAST(
              ROW(is_active_last_year, start_date, end_date + 1)
                AS ROW(is_active boolean, start_date integer, end_date integer))]
      WHEN did_change = 1 THEN
        ARRAY[
            CAST(
              ROW(is_active_last_year, start_date, end_date)
                AS ROW(is_active boolean, start_date integer, end_date integer)),
            CAST(
              ROW(is_active_this_year, current_year, current_year)
                AS ROW(is_active boolean, start_date integer, end_date integer))]
      WHEN did_change IS NULL THEN
        ARRAY[
          CAST(
            ROW(
              COALESCE(is_active_last_year, is_active_this_year),
              start_date,
              end_date )
              AS ROW(is_active boolean, start_date integer, end_date integer))]
      END as change_array FROM combined)
SELECT
  actor,
  arr.is_active,
  arr.start_date,
  arr.end_date,
  current_year
FROM changes
CROSS JOIN UNNEST(change_array) as arr
