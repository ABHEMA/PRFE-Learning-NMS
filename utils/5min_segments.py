import pandas as pd
import os

# Name of the input CSV file (the merged dataset)
input_csv = "merged_dataset.csv"

# Name of the output folder for the segments
output_folder = "5min_segments"
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Load the CSV and convert the 'timestamp' column to datetime
df = pd.read_csv(input_csv)
if "timestamp" not in df.columns:
    raise ValueError("The 'timestamp' column is missing from the input file.")

df["timestamp"] = pd.to_datetime(df["timestamp"])

# Sort the data in chronological order
df = df.sort_values("timestamp")

# Set 'timestamp' as the index to use resampling
df.set_index("timestamp", inplace=True)

# Resample the DataFrame into 5-minute time windows
# Each segment will cover a 5-minute interval
resampled = df.resample("5T")

# For each time window, save the segment as a separate CSV file
for window_start, group in resampled:
    if not group.empty:
        # Calculate the end time of the window (window_start + 5 minutes)
        window_end = window_start + pd.Timedelta(minutes=5)
        # Name the file based on the time window, e.g., 20230301_120000-20230301_120500.csv
        file_name = f"{window_start.strftime('%Y%m%d_%H%M%S')}-{window_end.strftime('%Y%m%d_%H%M%S')}.csv"
        file_path = os.path.join(output_folder, file_name)
        # Reset the index to include the timestamp column in the CSV
        group.reset_index().to_csv(file_path, index=False)
        print(f"Segment {file_name} saved with {len(group)} rows.")
