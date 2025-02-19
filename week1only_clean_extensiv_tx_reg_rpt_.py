import csv
import pandas as pd
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(filename='extensiv_tx_reg_rpt_cleaning.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Define the expected number of columns
expected_columns = 22

# Lists to store valid and problematic rows
valid_rows = []
problematic_rows = []

def convert_timestamp(value):
    try: 
        # Convert the timestamp to a standard format
        return datetime.strptime(value, '%m/%d/%Y %I:%M:%S %p').strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        logging.error(f"Invalid timestamp format: {value}")
        return None  # Return None for invalid timestamps

try:
    # Open the CSV file and read it line by line
    with open('ExtensivTxRegRpt_week1.csv', 'r', encoding='utf-8') as file: # Only Working with Week 1
        reader = csv.reader(file)
        headers = next(reader)  # Read the header row
        
        # Ensure the headers match the expected number of columns
        if len(headers) != expected_columns:
            logging.warning(f"Header row has {len(headers)} columns, but expected {expected_columns}.")
            # Adjust headers to match expected columns
            if len(headers) < expected_columns:
                headers = headers + [f"Column_{i+1}" for i in range(len(headers), expected_columns)]
            else:
                headers = headers[:expected_columns]  # Truncate extra columns
        
        for row_num, row in enumerate(reader, start=1):
            try:
                # Skip empty rows
                if not row:
                    logging.warning(f"Skipping empty row at line {row_num}")
                    continue
                    
                # Clean the row data
                cleaned_row = []
                for col_idx, value in enumerate(row):
                    # Remove any newlines, carriage returns, and extra whitespace
                    cleaned_value = value.strip().replace('\n', ' ').replace('\r', '')
                    
                    # Convert timestamp if it's in the "StartDate" or "EndDate" column
                    if headers[col_idx] in ["StartDate", "EndDate"]:
                        cleaned_value = convert_timestamp(cleaned_value)
                    
                    # Check for TransactionID column and ensure it's an integer
                    if headers[col_idx] == "TransactionID":
                        try:
                            cleaned_value = int(cleaned_value)  # Convert to integer
                        except ValueError:
                            logging.error(f"Invalid TransactionID format at row {row_num}: {cleaned_value}")
                            cleaned_value = None  # Set to None if conversion fails
                    
                    # Check for "QtyIn" column and ensure it's an integer
                    if headers[col_idx] == "QtyIn":
                        try:
                            cleaned_value = int(cleaned_value)  # Convert to integer
                        except ValueError:
                            logging.error(f"Invalid QtyIn format at row {row_num}: {cleaned_value}")
                            cleaned_value = None  # Set to None if conversion fails
                    
                    # Check for "QtyOut" column and ensure it's an integer
                    if headers[col_idx] == "QtyOut":
                        try:
                            cleaned_value = int(cleaned_value)  # Convert to integer
                        except ValueError:
                            logging.error(f"Invalid QtyOut format at row {row_num}: {cleaned_value}")
                            cleaned_value = None  # Set to None if conversion fails
                    
                    # Check for "Textbox80" column and ensure it's an integer
                    if headers[col_idx] == "Textbox80":
                        try:
                            cleaned_value = int(cleaned_value)  # Convert to integer
                        except ValueError:
                            logging.error(f"Error while reading data, error message: Unable to parse; line_number: {row_num} byte_offset_to_start_of_line: {0} column_index: {col_idx} column_name: 'Textbox80' column_type: INT64 value: '{cleaned_value}'")
                            cleaned_value = None  # Set to None if conversion fails
                    
                    # Check for "Textbox69" column and ensure it's an integer
                    if headers[col_idx] == "textbox69":
                        try:
                            cleaned_value = int(cleaned_value)  # Convert to integer
                        except ValueError:
                            logging.error(f"Error while reading data, error message: Unable to parse; line_number: {row_num} byte_offset_to_start_of_line: {0} column_index: {col_idx} column_name: 'Textbox69' column_type: INT64 value: '{cleaned_value}'")
                            cleaned_value = None  # Set to None if conversion fails
                    
                    # Check for "Handling" column and ensure it's a float
                    if headers[col_idx] == "Handling":
                        try:
                            cleaned_value = float(cleaned_value)  # Convert to float
                        except ValueError:
                            logging.error(f"Error while reading data, error message: Unable to parse; line_number: {row_num} byte_offset_to_start_of_line: {0} column_index: {col_idx} column_name: 'Handling' column_type: DOUBLE value: '{cleaned_value}'")
                            cleaned_value = None  # Set to None if conversion fails
                    
                    # Check for "Materials" column and ensure it's a float
                    if headers[col_idx] == "Materials":
                        try:
                            cleaned_value = float(cleaned_value)  # Convert to float
                        except ValueError:
                            logging.error(f"Error while reading data, error message: Unable to parse; line_number: {row_num} byte_offset_to_start_of_line: {0} column_index: {col_idx} column_name: 'Materials' column_type: DOUBLE value: '{cleaned_value}'")
                            cleaned_value = None  # Set to None if conversion fails
                    
                    # Check for "Storage" column and ensure it's an integer
                    if headers[col_idx] == "Storage":
                        try:
                            cleaned_value = int(cleaned_value)  # Convert to integer
                        except ValueError:
                            logging.error(f"Error while reading data, error message: Unable to parse; line_number: {row_num} byte_offset_to_start_of_line: {0} column_index: {col_idx} column_name: 'Storage' column_type: INT64 value: '{cleaned_value}'")
                            cleaned_value = None  # Set to None if conversion fails
                    
                    # Check for "Special" column and ensure it's a float
                    if headers[col_idx] == "Special":
                        try:
                            cleaned_value = float(cleaned_value)  # Convert to float
                        except ValueError:
                            logging.error(f"Error while reading data, error message: Unable to parse; line_number: {row_num} byte_offset_to_start_of_line: {0} column_index: {col_idx} column_name: 'Special' column_type: DOUBLE value: '{cleaned_value}'")
                            cleaned_value = None  # Set to None if conversion fails
                    
                    # Check for "FreightPP" column and ensure it's a float
                    if headers[col_idx] == "FreightPP":
                        try:
                            cleaned_value = float(cleaned_value)  # Convert to float
                        except ValueError:
                            logging.error(f"Error while reading data, error message: Unable to parse; line_number: {row_num} byte_offset_to_start_of_line: {0} column_index: {col_idx} column_name: 'FreightPP' column_type: DOUBLE value: '{cleaned_value}'")
                            cleaned_value = None  # Set to None if conversion fails
                    
                    # Check for "Freight3" column and ensure it's an integer
                    if headers[col_idx] == "Freight3":
                        try:
                            cleaned_value = int(cleaned_value)  # Convert to integer
                        except ValueError:
                            logging.error(f"Error while reading data, error message: Unable to parse; line_number: {row_num} byte_offset_to_start_of_line: {0} column_index: {col_idx} column_name: 'Freight3' column_type: INT64 value: '{cleaned_value}'")
                            cleaned_value = None  # Set to None if conversion fails
                    
                    # Check for "Total" column and ensure it's a float
                    if headers[col_idx] == "Total":
                        try:
                            cleaned_value = float(cleaned_value)  # Convert to float
                        except ValueError:
                            logging.error(f"Error while reading data, error message: Unable to parse; line_number: {row_num} byte_offset_to_start_of_line: {0} column_index: {col_idx} column_name: 'Total' column_type: DOUBLE value: '{cleaned_value}'")
                            cleaned_value = None  # Set to None if conversion fails
                    
                    cleaned_row.append(cleaned_value)
                
                # Ensure the row has the expected number of columns
                if len(cleaned_row) == expected_columns:
                    valid_rows.append(cleaned_row)
                else:
                    if len(cleaned_row) > expected_columns:
                        # Truncate rows that are too long
                        cleaned_row = cleaned_row[:expected_columns]
                        logging.warning(f"Row {row_num} has {len(row)} columns. Truncated to {expected_columns} columns.")
                    else:
                        # Pad rows that are too short
                        cleaned_row = cleaned_row + [""] * (expected_columns - len(cleaned_row))
                        logging.warning(f"Row {row_num} has {len(row)} columns. Padded to {expected_columns} columns.")
                    
                    valid_rows.append(cleaned_row)
                    problematic_rows.append((row_num, row))

            except Exception as e:
                logging.error(f"Error processing row {row_num}: {str(e)}")
                problematic_rows.append((row_num, f"Error processing row: {str(e)}"))

    # Convert valid rows to a DataFrame
    df = pd.DataFrame(valid_rows, columns=headers)

    # Attempt to convert the "Date" column to datetime format
    if "Date" in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')  # Coerce errors to NaT
        if df['Date'].isnull().all():
            logging.warning("All values in 'Date' column could not be parsed. Please check the input data.")

    # Remove any completely empty rows
    df = df.dropna(how='all')

    # Log problematic rows
    if problematic_rows:
        logging.warning(f"Found {len(problematic_rows)} problematic rows that required cleaning.")
        for row_num, row in problematic_rows:
            logging.warning(f"Row {row_num}: {row}")

    # Check for overall error count and raise an error if too many issues were found
    if len(problematic_rows) > 0:
        logging.error(f"CSV processing encountered too many errors, giving up. Rows: {len(valid_rows) + len(problematic_rows)}; errors: {len(problematic_rows)}; max bad: 0; error percent: {len(problematic_rows) / (len(valid_rows) + len(problematic_rows)) * 100:.2f}%")
        raise Exception(f"CSV processing encountered too many errors, giving up. Total rows: {len(valid_rows) + len(problematic_rows)}; Total errors: {len(problematic_rows)}.")

    # Save the cleaned DataFrame with proper quoting and string formatting
    df.to_csv('clean_extensiv_tx_reg_rpt.csv', index=False, quoting=csv.QUOTE_ALL)
    logging.info("✅ Data cleaned and saved successfully.")
    print("✅ Data cleaned and saved successfully.")

except FileNotFoundError:
    logging.error("❌ The file 'ExtensivTxRegRpt_week6.csv' was not found.")
    print("❌ The file 'ExtensivTxRegRpt_week6.csv' was not found.")
except Exception as e:
    logging.error(f"❌ An unexpected error occurred: {str(e)}")
    print(f"❌ An unexpected error occurred: {str(e)}")