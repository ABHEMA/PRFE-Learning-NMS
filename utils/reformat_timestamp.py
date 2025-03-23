import pandas as pd

# 1. Read the two CSV files
df_system = pd.read_csv("system_stats-test1.csv")
df_interface = pd.read_csv("interface_stats-test1.csv")

# 2. Merge on the 'timestamp' column
df_merged = pd.merge(df_system, df_interface, on="timestamp", how="inner")

# 3. Sort in ascending order by 'timestamp'
df_merged = df_merged.sort_values("timestamp")

# 4. Build a "timestamp -> T index" dictionary
#    We start from the list of unique (sorted) timestamps.
unique_timestamps = df_merged["timestamp"].unique()
ts_to_index = {ts: i for i, ts in enumerate(unique_timestamps)}

# 5. Create a 'T' column mapped from the 'timestamp' column
df_merged["T"] = df_merged["timestamp"].map(ts_to_index)

# 6. Remove the original 'timestamp' column
df_merged.drop(columns=["timestamp"], inplace=True)

# 7. Save the final result
df_merged.to_csv("system_stats-interface_stats_formated_timestamp.csv", index=False)

print("Merging and transformation complete. The result is saved in 'system_stats-interface_stats_formated_timestamp.csv'.")
