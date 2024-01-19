first_sort_df = bucketedValues.sortWithinPartitions(col("m.match_id"))

first_sort_df.count()
6885858
