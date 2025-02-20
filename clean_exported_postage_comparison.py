import csv
import pandas as pd
import logging
from google.cloud import bigquery  # Import BigQuery client
import os  # Import os for file handling

# Configure logging
logging.basicConfig(filename='postage_comparison_cleaning.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Define the expected number of columns
expected_columns = 10

# Lists to store valid and problematic rows
valid_rows = []
problematic_rows = []

# Prompt user for file path
file_path = input("Please drop the file path of the CSV file to process: ")

# Prompt user for table name
table_name = input("Please enter a name for the table: ")

try:
    # Open the CSV file and read it line by line
    with open(file_path, 'r', encoding='utf-8') as file:
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
                    
                    # Ensure OrderId is an integer
                    if headers[col_idx] == "OrderId":
                        try:
                            cleaned_value = int(cleaned_value)
                        except ValueError:
                            logging.error(f"Invalid OrderId format at row {row_num}: {cleaned_value}")
                            cleaned_value = None  # Set to None if conversion fails
                    
                    # Convert specific columns to float
                    float_columns = ["TotVolumeImperial", "Postage Cost"]
                    if headers[col_idx] in float_columns:
                        try:
                            cleaned_value = float(cleaned_value)  # Convert to float
                        except ValueError:
                            logging.error(f"Invalid {headers[col_idx]} format at row {row_num}: {cleaned_value}")
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

    # Remove any completely empty rows
    df = df.dropna(how='all')

    # Log problematic rows
    if problematic_rows:
        logging.warning(f"Found {len(problematic_rows)} problematic rows that required cleaning.")
        for row_num, row in problematic_rows:
            logging.warning(f"Row {row_num}: {row}")

    # Save the cleaned DataFrame in the specified folder
    output_folder = r'C:\Users\Elevate\bigquery_project\clean_exported_postage_comparison'
    output_path = os.path.join(output_folder, f'{table_name}.csv')
    df.to_csv(output_path, index=False, quoting=csv.QUOTE_ALL)
    
    logging.info("✅ Data cleaned and saved successfully.")
    print("✅ Data cleaned and saved successfully.")

    # Upload the cleaned data to BigQuery
    client = bigquery.Client()  # Initialize BigQuery client
    dataset_id = 'postage-calculator-tool.pct'  # Fixed dataset

    # Define full table ID
    table_id = f"{dataset_id}.{table_name}"  # Use user-defined table name

    # Load job configuration with auto schema detection
    job_config = bigquery.LoadJobConfig(
        autodetect=True,  # Auto-detect schema
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Overwrite if table exists
    )

    # Upload the DataFrame to BigQuery
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)

    # Wait for the upload job to complete
    job.result()

    print(f"✅ Data uploaded successfully to BigQuery: {table_id} (Schema auto-detected)")

except FileNotFoundError:
    logging.error("❌ The file was not found.")
    print("❌ The file was not found.")
except Exception as e:
    logging.error(f"❌ An unexpected error occurred: {str(e)}")
    print(f"❌ An unexpected error occurred: {str(e)}")
