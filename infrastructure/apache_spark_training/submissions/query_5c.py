third_sort_df = bucketedValues.sortWithinPartitions(col("playlist_id"))

third_sort_df.count()
6885858
