import pandas as pd

# Paths to your two CSV files
csv_file_1 = 'interface_stats-test1.csv'
csv_file_2 = 'system_stats-test1.csv'

# Read both CSV files
df1 = pd.read_csv(csv_file_1)
df2 = pd.read_csv(csv_file_2)

# Merge on the 'timestamp' column using an inner join
merged_df = pd.merge(df1, df2, on='timestamp', how='inner')

# Display the first 5 rows of the merged DataFrame
print(merged_df.head())

# (Optional) Save the merged DataFrame to a new CSV file
merged_df.to_csv('system_stats-interface_stats.csv', index=False)
