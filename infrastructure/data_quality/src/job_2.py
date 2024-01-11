from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2(output_table_name: str) -> str:
    return f"""
                    
                WITH
                lagged AS (
                    SELECT
                    player_name,
                    CASE
                        WHEN is_active THEN 1
                        ELSE 0
                    END AS is_active,
                    CASE
                        WHEN LAG(is_active, 1) OVER (
                        PARTITION BY
                            player_name
                        ORDER BY
                            current_season
                        ) THEN 1
                        ELSE 0
                    END AS is_active_last_season,
                    current_season
                    FROM
                    nba_players
                    WHERE
                    current_season <= 2001
                ),
                streaked AS (
                    SELECT
                    *,
                    SUM(
                        CASE
                        WHEN is_active <> is_active_last_season THEN 1
                        ELSE 0
                        END
                    ) OVER (
                        PARTITION BY
                        player_name
                        ORDER BY
                        current_season
                    ) AS streak_identifier
                    FROM
                    lagged
                )
                SELECT
                player_name,
                MAX(is_active) = 1 AS is_active,
                MIN(current_season) AS start_season,
                MAX(current_season) AS end_season,
                2001 AS current_season
                FROM
                streaked
                GROUP BY
                player_name,
                streak_identifier
    """

def job_2(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_2(output_table_name))

def main():
    output_table_name: str = "nba_players_scd"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
