#Query 4
insert into anjumercian.actors_history_scd
with
lagged as (
  select
         actor,
    is_active as is_active_this_year,
    lag(is_active, 1) over(
      partition by actor
      order by current_year) as is_active_last_year,
quality_class,
    current_year
  from anjumercian.actors
  where current_year <= 2021),
changed as (
  select
    *,
    case
      when is_active_this_year <> is_active_last_year then 1
      else 0
    end as is_changed
  from lagged),
changed_with_grps as (
  select
    *,
    sum(is_changed) over(
      partition by actor
      order by current_year
    ) as grp
  from changed)
select
  actor,
  max(is_active_this_year) as is_active,
quality_class,
  min(current_year) as start_date,
  max(current_year) as end_date,
  2021 as current_year
from changed_with_grps
group by actor, grp, quality_class
