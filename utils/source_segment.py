import pandas as pd
import os

# Name of the input merged CSV file
input_csv = "merged_dataset.csv"

# Name of the output folder for files separated by source
output_folder = "by_source"
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Load the merged CSV
df = pd.read_csv(input_csv)

# Check that the "source" column exists in the DataFrame
if "source" not in df.columns:
    raise ValueError("The 'source' column does not exist in the input file.")

# For each unique value in the "source" column,
# filter the DataFrame and save it to a separate CSV file
for source_value in df["source"].unique():
    # Filter rows matching the given source
    df_source = df[df["source"] == source_value]
    
    # Clean the source value to create a valid file name
    safe_source = str(source_value).replace(" ", "_").replace("/", "_").replace("\\", "_")
    output_file = os.path.join(output_folder, f"{safe_source}.csv")
    
    # Save the filtered DataFrame to a CSV file
    df_source.to_csv(output_file, index=False)
    print(f"File saved for source '{source_value}': {output_file}")
