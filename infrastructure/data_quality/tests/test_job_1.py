from chispa.dataframe_comparer import *
from src.job_1 import job_1
from collections import namedtuple
nba_player_seasons = namedtuple("nba_players", "player_name height college country draft_year draft_number seasons is_active years_since_last_active current_season ")
nba_players = namedtuple("nba_players_scd", "player_name height college country draft_year draft_number seasons is_active years_since_last_active current_season ")


def test_scd_generation(spark):
    source_data = [
        nba_player_seasons("Michael Jordan", '6-6', 'North Carolina', 'USA','1984', '1','3', '[[1997,35,216,82,28.7,3.5,5.8],[1996,34,216,82,29.6,4.3,5.9]]', False, 3, 2000),
        nba_player_seasons("Michael Jordan",	"6-6",	"North Carolina",	"USA",	"1984"	'1',	'3',	'[[1997,35,216,82,28.7,3.5,5.8],[1996,34,216,82,29.6,4.3,5.9]]',True,	0),
        
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = job_1(spark, source_df)
    expected_data = [
        nba_players("Michael Jordan", '6-6', 'North Carolina', 'USA','1984', '1','3', '[[1997,35,216,82,28.7,3.5,5.8],[1996,34,216,82,29.6,4.3,5.9]]', False, 3, 2000),
        nba_players("Michael Jordan",	"6-6",	"North Carolina",	"USA",	"1984"	'1',	'3',	'[[1997,35,216,82,28.7,3.5,5.8],[1996,34,216,82,29.6,4.3,5.9]]',True,	0),
        
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)



    
