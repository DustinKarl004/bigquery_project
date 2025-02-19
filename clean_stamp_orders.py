import pandas as pd
import re
import logging
import time

# Configure logging for error tracking
logging.basicConfig(filename="csv_cleaning.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

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
with open("stamp_orders_Week6.csv", "r", encoding='utf-8') as file:  # Specify encoding
    header = file.readline().strip().split(",")  # Read header
    cleaned_header = [clean_column_name(col) for col in header]  # Clean header

    for row_number, line in enumerate(file, start=2):  # Start from 2 to account for header
        try:
            # Split the line into columns
            columns = line.strip().split(",")
            # Clean each column
            cleaned_columns = [col.replace('"', '').strip() for col in columns]
            
            # Check if the "RowNumber" column is numeric (assuming it's the first column)
            try:
                cleaned_columns[0] = int(cleaned_columns[0])  # Convert to int
            except (ValueError, IndexError):
                cleaned_columns[0] = None  # Set to None if conversion fails
            
            # Check if the "TotPackages" column is numeric (assuming it's at index 14)
            try:
                cleaned_columns[14] = float(cleaned_columns[14])  # Convert to float
            except (ValueError, IndexError):
                cleaned_columns[14] = None  # Set to None if conversion fails
            
            # Check if the "MarkForIdAndName" column is numeric (assuming it's at index 13)
            try:
                cleaned_columns[13] = int(cleaned_columns[13])  # Convert to int
            except (ValueError, IndexError):
                cleaned_columns[13] = None  # Set to None if conversion fails
            
            # Check if the "ParcelLabelType" column is numeric (assuming it's at index 15)
            try:
                cleaned_columns[15] = float(cleaned_columns[15])  # Convert to float
            except (ValueError, IndexError):
                cleaned_columns[15] = None  # Set to None if conversion fails
            
            # Check if the "Tracking__" column is valid (assuming it's at index 6)
            if not isinstance(cleaned_columns[6], str) or not cleaned_columns[6].startswith("LV"):
                cleaned_columns[6] = None  # Set to None if invalid
            
            # Check if the "Date_Delivered" column is valid (assuming it's at index 7)
            try:
                # You can implement a more robust date parsing if needed
                pd.to_datetime(cleaned_columns[7], errors='raise')  # Validate date
            except (ValueError, IndexError):
                cleaned_columns[7] = None  # Set to None if conversion fails
            
            # Check if the "Date_Printed" column is valid (assuming it's at index 8)
            try:
                pd.to_datetime(cleaned_columns[8], errors='raise')  # Validate date
            except (ValueError, IndexError):
                cleaned_columns[8] = None  # Set to None if conversion fails
            
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

# Validate and convert the "Date_Delivered" column (assuming it's at index 7)
try:
    df_orders['Date_Delivered'] = pd.to_datetime(df_orders['Date_Delivered'], errors='coerce')  # Validate date
except Exception as e:
    log_error("Date Conversion", str(e))  # Log any errors during date conversion

# Validate and convert the "Date_Printed" column (assuming it's at index 8)
try:
    df_orders['Date_Printed'] = pd.to_datetime(df_orders['Date_Printed'], errors='coerce')  # Validate date
    df_orders['Date_Printed'] = df_orders['Date_Printed'].dt.date  # Convert to date only
except Exception as e:
    log_error("Date Conversion", str(e))  # Log any errors during date conversion

# Save cleaned data
df_orders.to_csv("cleaned_stamp_orders.csv", index=False)
print("✅ Cleaned Stamp Orders saved as: cleaned_stamp_orders.csv")