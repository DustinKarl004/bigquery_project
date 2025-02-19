import pandas as pd
import re

# Function to clean column names
def clean_column_name(col_name):
    col_name = re.sub(r'[^a-zA-Z0-9_]', '_', col_name)  # Replace unsupported characters
    return col_name.strip()

# Function to log errors
def log_error(row_number, error_message):
    with open("error_log.txt", "a") as log_file:
        log_file.write(f"Row {row_number}: {error_message}\n")

# Read the CSV file and clean data
cleaned_data = []
with open("Order_Week2.csv", "r", encoding='utf-8') as file:  # Specify encoding
    header = file.readline().strip().split(",")  # Read header
    cleaned_header = [clean_column_name(col) for col in header]  # Clean header

    for row_number, line in enumerate(file, start=2):  # Start from 2 to account for header
        try:
            # Split the line into columns
            columns = line.strip().split(",")
            # Clean each column
            cleaned_columns = [col.replace('"', '').strip() for col in columns]
            
            # Check if the "RowNumber" column is numeric
            try:
                cleaned_columns[0] = int(cleaned_columns[0])  # Convert to int
            except (ValueError, IndexError):
                cleaned_columns[0] = None  # Set to None if conversion fails
            
            # Check if the "TotPackages" column is numeric
            try:
                cleaned_columns[14] = float(cleaned_columns[14])  # Convert to float
            except (ValueError, IndexError):
                cleaned_columns[14] = None  # Set to None if conversion fails
            
            # Check if the "MarkForIdAndName" column is numeric
            try:
                cleaned_columns[13] = int(cleaned_columns[13])  # Convert to int
            except (ValueError, IndexError):
                cleaned_columns[13] = None  # Set to None if conversion fails
            
            # Check if the "ParcelLabelType" column is numeric
            try:
                cleaned_columns[15] = float(cleaned_columns[15])  # Convert to float
            except (ValueError, IndexError):
                cleaned_columns[15] = None  # Set to None if conversion fails
            
            # Ensure the number of columns matches the header
            if len(cleaned_columns) > len(cleaned_header):
                cleaned_columns = cleaned_columns[:len(cleaned_header)]  # Truncate extra columns
            elif len(cleaned_columns) < len(cleaned_header):
                cleaned_columns += [None] * (len(cleaned_header) - len(cleaned_columns))  # Pad with None
            
            cleaned_data.append(cleaned_columns)
        except Exception as e:
            log_error(row_number, str(e))  # Log the error

# Create DataFrame from cleaned data
df_orders = pd.DataFrame(cleaned_data, columns=cleaned_header)

# Save cleaned data
df_orders.to_csv("cleaned_orders.csv", index=False)
print("✅ Cleaned Orders saved as: cleaned_orders.csv") 