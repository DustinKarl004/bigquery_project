import csv
import pandas as pd
import logging
import numpy as np  # Import numpy

# Configure logging
logging.basicConfig(filename='stamp_orders_cleaning.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Define the expected number of columns
expected_columns = 44

# Lists to store valid and problematic rows
valid_rows = []
problematic_rows = []

try:
    # Open the CSV file and read it line by line
    with open('stamp_orders_week6.csv', 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        headers = next(reader)  # Read the header row
        
        # Replace problematic field name
        headers = [h.replace('State/Province', 'State_Province') for h in headers]
        
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
                    
                    # Convert tracking numbers to string type and handle special cases
                    if col_idx == 6:  # Tracking # column
                        # Always store tracking numbers as strings with ="number" format
                        cleaned_value = f'"{cleaned_value}"' if cleaned_value else ''
                        
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

    # Ensure Tracking # column is string type with ="number" format
    df['Tracking #'] = df['Tracking #'].astype(str)
    df['Tracking #'] = df['Tracking #'].apply(lambda x: f'="{x}"' if x else '')

    # Clean the "Postal Code" column
    if "Postal Code" in df.columns:
        # Replace "NA" or "N/A" with NaN
        df['Postal Code'] = df['Postal Code'].replace(['NA', 'N/A', ''], np.nan)

        # Convert to integer type; errors='coerce' will convert invalid values to NaN
        df['Postal Code'] = pd.to_numeric(df['Postal Code'], errors='coerce').fillna(0).astype(int)

    # Replace specified columns with #NUM! if all values are empty
    columns_to_check = ['Extra Services', 'Cost Code', 'Refund Request Date', 'Refund Status', 
                        'Refund Requested', 'Reference 1', 'Order ID', 'Store', 
                        'Order Date', 'Order Total', 'Item SKUs', 'Items', 
                        'Product Total', 'Shipping Paid', 'Tax Paid']
    
    for col in columns_to_check:
        if col in df.columns:
            df[col] = df[col].where(df[col].notna(), '')
            if df[col].eq('').all():
                df[col] = '#NUM!'

    # Handle Address 2 and Address 3
    if 'Address 2' in df.columns:
        df['Address 2'] = df['Address 2'].where(df['Address 2'].notna(), '')
        if df['Address 2'].eq('').all():
            df['Address 2'] = '#NUM!'
    
    if 'Address 3' in df.columns:
        df['Address 3'] = df['Address 3'].where(df['Address 3'].notna(), '')
        if df['Address 3'].eq('').all():
            df['Address 3'] = '#NUM!'

    # Remove any completely empty rows
    df = df.dropna(how='all')

    # Log problematic rows
    if problematic_rows:
        logging.warning(f"Found {len(problematic_rows)} problematic rows that required cleaning.")
        for row_num, row in problematic_rows:
            logging.warning(f"Row {row_num}: {row}")

    # Save the cleaned DataFrame with proper quoting and string formatting
    df.to_csv('cleaned_stamp_orders.csv', index=False, quoting=csv.QUOTE_ALL)
    logging.info("✅ Data cleaned and saved successfully.")
    print("✅ Data cleaned and saved successfully.")

except FileNotFoundError:
    logging.error("❌ The file 'stamp_orders_Week6.csv' was not found.")
    print("❌ The file 'stamp_orders_Week6.csv' was not found.")
except Exception as e:
    logging.error(f"❌ An unexpected error occurred: {str(e)}")
    print(f"❌ An unexpected error occurred: {str(e)}")
