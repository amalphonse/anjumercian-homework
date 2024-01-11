from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def query_1(output_table_name: str) -> str:
    return f"""
        
                WITH
            last_season AS (
                SELECT
                *
                FROM
                nba_players
                WHERE
                current_season = 1995
            ),
            this_season AS (
                SELECT
                *
                FROM
                nba_player_seasons
                WHERE
                season = 1996
            )
            SELECT
            COALESCE(ls.player_name, ts.player_name) AS player_name,
            COALESCE(ls.height, ts.height) AS height,
            COALESCE(ls.college, ts.college) AS college,
            COALESCE(ls.country, ts.country) AS country,
            COALESCE(ls.draft_year, ts.draft_year) AS draft_year,
            COALESCE(ls.draft_number, ts.draft_number) AS draft_number,
            NULL AS seasons,
            ts.season IS NOT NULL AS is_active,
            CASE
                WHEN ts.season IS NOT NULL THEN 0
                ELSE years_since_last_active + 1
            END AS years_since_last_active,
            COALESCE(ts.season, ls.current_season + 1) AS current_season
            FROM
            last_season ls
            FULL OUTER JOIN this_season ts ON ls.player_name = ts.player_name
    """

def job_1(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_1(output_table_name))

def main():
    output_table_name: str = "nba_players"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
