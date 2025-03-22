import csv

def csv_to_arff(csv_input, arff_output, relation_name="my_relation"):
    """
    Converts a CSV file to an ARFF file.
    - csv_input: path to the input CSV file.
    - arff_output: path to the output ARFF file.
    - relation_name: name of the relation (for the ARFF header).
    """
    
    # Read the CSV file
    with open(csv_input, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        # Retrieve the header (column names)
        headers = next(reader)
        # Retrieve all data rows
        data = [row for row in reader]

    # Transpose the data to process each column separately
    columns = list(zip(*data)) if data else []

    # Determine the type of each attribute (numeric or nominal)
    col_types = []
    
    def is_float(value):
        try:
            float(value)
            return True
        except ValueError:
            return False

    for i, col in enumerate(columns):
        # We ignore empty fields (missing values) for type detection
        non_empty_values = [val for val in col if val.strip() != '']
        
        # Check if all non-empty fields can be converted to float
        if all(is_float(val) for val in non_empty_values):
            col_types.append("numeric")
        else:
            # If it is not purely numeric, consider the column as nominal
            # Then list all distinct values (excluding empty values)
            distinct_vals = sorted(set(non_empty_values))
            
            # Build a nominal list in the format {val1,val2,...}
            # To avoid formatting issues, escape apostrophes and commas
            # or simply enclose each value in quotes.
            # Here, we leave them without quotes, but you can adapt if necessary.
            distinct_vals_escaped = [val.replace(",", "_").replace("'", "\\'") for val in distinct_vals]
            nominal_list = "{" + ",".join(distinct_vals_escaped) + "}"
            col_types.append(nominal_list)

    # Write the ARFF file
    with open(arff_output, 'w', encoding='utf-8') as f_out:
        # ARFF file header
        f_out.write(f"@relation {relation_name}\n\n")

        # Declare the attributes
        for header, col_type in zip(headers, col_types):
            f_out.write(f"@attribute {header} {col_type}\n")
        
        f_out.write("\n@data\n")
        
        # Write the data
        for row in data:
            row_str = []
            for val, col_type in zip(row, col_types):
                # If the value is empty, replace it with ?
                if val.strip() == '':
                    row_str.append('?')
                else:
                    if col_type == "numeric":
                        # We assume the value is a number
                        row_str.append(val)
                    else:
                        # For a nominal attribute, we can enclose the value in single quotes
                        val_escaped = val.replace("'", "\\'")
                        row_str.append(f"'{val_escaped}'")
            f_out.write(",".join(row_str) + "\n")


# Example usage
if __name__ == "__main__":
    # Path to the CSV file to convert
    csv_input_file = "system_stats-interface_stats.csv"
    # Path to the output ARFF file
    arff_output_file = "system_stats-interface_stats.arff"
    csv_to_arff(csv_input_file, arff_output_file, relation_name="MaRelation")
    print("Conversion complete!")
