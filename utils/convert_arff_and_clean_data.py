import csv
import glob
import os

# Pattern for the four split CSV files (adjust the pattern as needed)
csv_files = glob.glob("merged_dataset_part*.csv")

# Loop over each CSV file found
for input_csv in csv_files:
    # Get base filename without extension for naming the ARFF file and relation name
    base_name = os.path.splitext(input_csv)[0]
    output_arff = base_name + ".arff"
    
    # Read the CSV file into a list of dictionaries
    with open(input_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames

    # Check that the expected fields exist
    expected_cols = ["timestamp", "source", "record_type", "metric", "value", "label", "message"]
    for col in expected_cols:
        if col not in fieldnames:
            raise ValueError(f"Column '{col}' is missing in the file {input_csv}.")

    # Replace "1.0.0.4" and "UNKNOWN" in the "source" column with "R1"
    for row in rows:
        if "source" in row:
            source_val = row["source"].strip()
            if source_val == "1.0.0.4" or source_val.upper() == "UNKNOWN":
                row["source"] = "R1"

    # Remove the "message" column from the header if present
    if "message" in fieldnames:
        fieldnames.remove("message")

    # Process each row:
    # - Replace empty "label" values with "NORMAL"
    # - Remove the "message" field from each row
    for row in rows:
        if "label" in row:
            if row["label"].strip() == "":
                row["label"] = "NORMAL"
        if "message" in row:
            del row["message"]

    # Function to check if a string can be converted to a float
    def is_float(value):
        try:
            float(value)
            return True
        except:
            return False

    # Determine attribute types for each field:
    # If all non-empty values in a column can be converted to float, mark it as numeric.
    # Otherwise, treat it as a nominal attribute listing all distinct values.
    attr_types = {}
    for col in fieldnames:
        values = [row[col] for row in rows if row[col].strip() != ""]
        if values and all(is_float(val) for val in values):
            attr_types[col] = "numeric"
        else:
            distinct_vals = sorted(set(values))
            nominal_list = "{" + ",".join("'" + val.replace("'", "\\'") + "'" for val in distinct_vals) + "}"
            attr_types[col] = nominal_list

    # Write the ARFF file
    with open(output_arff, 'w', encoding='utf-8') as f_out:
        # Write the relation header
        f_out.write(f"@relation {base_name}\n\n")
        # Write attribute declarations
        for col in fieldnames:
            f_out.write(f"@attribute {col} {attr_types[col]}\n")
        f_out.write("\n@data\n")
        # Write each data row. Empty fields are replaced by "?".
        for row in rows:
            row_values = []
            for col in fieldnames:
                value = row[col].strip()
                if value == "":
                    row_values.append("?")
                else:
                    if attr_types[col] == "numeric":
                        row_values.append(value)
                    else:
                        row_values.append("'" + value.replace("'", "\\'") + "'")
            f_out.write(",".join(row_values) + "\n")
    print(f"ARFF file created: {output_arff}")
