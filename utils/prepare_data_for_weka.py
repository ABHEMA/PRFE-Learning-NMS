import pandas as pd
import re

# Name of the input merged CSV file
input_csv = "1.0.0.4.csv"

# Load the merged CSV
df = pd.read_csv(input_csv)

# Check that the expected columns exist
expected_cols = ["timestamp", "source", "record_type", "metric", "value", "label", "message"]
for col in expected_cols:
    if col not in df.columns:
        raise ValueError(f"The column '{col}' is missing in the input file.")

# 1. Drop the "message" column as it contains unstructured text not usable directly for ML
df = df.drop(columns=["message"])

# 2. Convert the timestamp to datetime and sort chronologically
df["timestamp"] = pd.to_datetime(df["timestamp"])
df = df.sort_values("timestamp")

# 3. Create a new column "T": each unique timestamp is replaced by a numeric index
unique_ts = df["timestamp"].unique()
ts_to_index = {ts: i for i, ts in enumerate(unique_ts)}
df["T"] = df["timestamp"].map(ts_to_index)

# Optionally drop the "timestamp" column if no longer needed
df = df.drop(columns=["timestamp"])

# 4. Encode categorical columns into integer values
# For 'source': if the value is an IP address, assign it the value 0.
# For 'label': if the value is empty (or just spaces), assign 0.
cat_cols = ["source", "record_type", "metric", "label"]
mappings = []  # List to save the mapping details

# Regular expression to detect IPv4 addresses
ip_pattern = re.compile(r'^\d{1,3}(?:\.\d{1,3}){3}$')

for col in cat_cols:
    if col == "source":
        # For "source", assign 0 to any value that is an IP address
        mapping_dict = {}
        non_ip_counter = 1  # Counter for non-IP values
        for val in sorted(df[col].dropna().unique()):
            if ip_pattern.match(val):
                mapping_dict[val] = 0
            else:
                mapping_dict[val] = non_ip_counter
                non_ip_counter += 1
    elif col == "label":
        # For "label", assign 0 to empty or whitespace-only values
        mapping_dict = {}
        unique_vals = sorted(set(df[col].fillna("").astype(str)))
        mapping_dict[""] = 0  # Empty or missing value = 0
        counter = 1
        for val in unique_vals:
            if val.strip() != "":
                mapping_dict[val] = counter
                counter += 1
    else:
        # For other columns, simple mapping: 0, 1, 2, ...
        unique_vals = sorted(df[col].dropna().unique())
        mapping_dict = {val: i for i, val in enumerate(unique_vals)}
    
    # Replace text values with their numeric codes in the DataFrame
    df[col] = df[col].fillna("")  # Ensure no NaNs
    df[col] = df[col].map(mapping_dict)
    
    # Save the mapping for each value
    for val, code in mapping_dict.items():
        mappings.append({
            "field": col,
            "original_value": val,
            "enum_value": code
        })

# 5. Save the enum mappings to a CSV file
mapping_df = pd.DataFrame(mappings)
mapping_df.to_csv("enum_mapping.csv", index=False)
print("Enum mapping saved as 'enum_mapping.csv'.")

# 6. Save the transformed DataFrame (only numerical values) to CSV
output_csv = "1.0.0.4.csv"
df.to_csv(output_csv, index=False)
print("Transformed file saved as", output_csv)
