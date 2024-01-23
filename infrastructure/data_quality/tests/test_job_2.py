from chispa.dataframe_comparer import *
from src.job_2 import job_2
from collections import namedtuple

PlayerSeason = namedtuple("PlayerSeason", "player_name is_active is_active_last_season current_season")
PlayerScd = namedtuple("PlayerScd", "player_name is_active start_date end_date current_season")


def test_scd_generation(spark):
    source_data = [
        PlayerSeason("Michael Jordan", 2001, 'Good'),
        PlayerSeason("Michael Jordan", 2002, 'Good'),
        
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_player_scd_transformation(spark, source_df)
    expected_data = [
        PlayerScd("Michael Jordan", 'Good', 2001, 2002),
        PlayerScd("Michael Jordan", 'Bad', 2003, 2003),
        PlayerScd("Someone Else", 'Bad', 2003, 2003)
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)