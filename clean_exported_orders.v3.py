import csv
import pandas as pd
import logging

# Configure logging
logging.basicConfig(filename='csv_cleaning.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Define the expected number of columns
expected_columns = 33  # Updated to 33 columns

# Lists to store valid and problematic rows
valid_rows = []
problematic_rows = []

try:
    # Open the CSV file and read it line by line
    with open('Order_Week7.csv', 'r', encoding='utf-8') as file: #ONLY WORKING WITH WEEK 7
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
                for value in row:
                    # Remove any newlines, carriage returns, and extra whitespace
                    cleaned_value = value.strip().replace('\n', ' ').replace('\r', '')
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

    # Clean the SmallParcelShipDate column
    if 'SmallParcelShipDate' in df.columns:
        # Attempt to convert SmallParcelShipDate to datetime, coercing errors to NaT
        df['SmallParcelShipDate'] = pd.to_datetime(df['SmallParcelShipDate'], errors='coerce')
        
        # Log rows with invalid SmallParcelShipDate values
        invalid_ship_dates = df[df['SmallParcelShipDate'].isna()]
        if not invalid_ship_dates.empty:
            logging.warning(f"Found {len(invalid_ship_dates)} rows with invalid SmallParcelShipDate values.")
            for index, row in invalid_ship_dates.iterrows():
                logging.warning(f"Row {index + 2}: SmallParcelShipDate = {row['SmallParcelShipDate']} (invalid)")

        # Drop rows with invalid SmallParcelShipDate values
        df = df.dropna(subset=['SmallParcelShipDate'])

    # Convert numeric columns to appropriate types
    numeric_columns = ['TotWeightImperial']  # Add other numeric columns as needed
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Remove any completely empty rows
    df = df.dropna(how='all')

    # Log problematic rows
    if problematic_rows:
        logging.warning(f"Found {len(problematic_rows)} problematic rows that required cleaning.")
        for row_num, row in problematic_rows:
            logging.warning(f"Row {row_num}: {row}")

    # Save the cleaned DataFrame with proper quoting
    df.to_csv('cleaned_orders.csv', index=False, quoting=csv.QUOTE_ALL)
    logging.info("✅ Data cleaned and saved successfully.")
    print("✅ Data cleaned and saved successfully.")

except FileNotFoundError:
    logging.error("❌ The file was not found.")
    print("❌ The file was not found.")
except Exception as e:
    logging.error(f"❌ An unexpected error occurred: {str(e)}")
    print(f"❌ An unexpected error occurred: {str(e)}")