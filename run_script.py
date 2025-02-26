import csv
import pandas as pd
import logging
from google.cloud import bigquery
import os
import numpy as np
import re

# Configure logging
logging.basicConfig(filename='data_cleaning.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def clean_column_name(column_name):
    if not column_name:  # Check if column_name is empty
        return '_empty_'
    # Replace spaces and special characters with underscores
    cleaned_name = re.sub(r'[^a-zA-Z0-9_]', '_', column_name)
    # Ensure the name starts with a letter or underscore
    if not cleaned_name[0].isalpha() and cleaned_name[0] != '_':
        cleaned_name = '_' + cleaned_name
    return cleaned_name

def process_exported_orders(file_path, week):
    expected_columns = 33
    valid_rows = []
    problematic_rows = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            headers = next(reader)
            
            # Clean the headers
            headers = [clean_column_name(h) for h in headers]
            
            if len(headers) != expected_columns:
                logging.warning(f"Header row has {len(headers)} columns, but expected {expected_columns}.")
                if len(headers) < expected_columns:
                    headers += [f"Column_{i+1}" for i in range(len(headers), expected_columns)]
                else:
                    headers = headers[:expected_columns]

            for row_num, row in enumerate(reader, start=1):
                try:
                    if not row:
                        logging.warning(f"Skipping empty row at line {row_num}")
                        continue
                        
                    cleaned_row = []
                    for value in row:
                        cleaned_value = value.strip().replace('\n', ' ').replace('\r', '')
                        cleaned_row.append(cleaned_value)
                    
                    if len(cleaned_row) == expected_columns:
                        valid_rows.append(cleaned_row)
                    else:
                        if len(cleaned_row) > expected_columns:
                            cleaned_row = cleaned_row[:expected_columns]
                            logging.warning(f"Row {row_num} has {len(row)} columns. Truncated to {expected_columns} columns.")
                        else:
                            cleaned_row += [""] * (expected_columns - len(cleaned_row))
                            logging.warning(f"Row {row_num} has {len(row)} columns. Padded to {expected_columns} columns.")
                        
                        valid_rows.append(cleaned_row)
                        problematic_rows.append((row_num, row))

                except Exception as e:
                    logging.error(f"Error processing row {row_num}: {str(e)}")
                    problematic_rows.append((row_num, f"Error processing row: {str(e)}"))

        df = pd.DataFrame(valid_rows, columns=headers)

        numeric_columns = {
            'RowNumber': 'int',
            'OrderId': 'int', 
            'CreationDate': 'datetime64[ns]',
            'BatchOrderId': 'int',
            'TotPackages': 'int',
            'ParcelLabelType': 'int',
            'SmallParcelShipDate': 'datetime64[ns]',
            'TotalItemQty': 'int',
            'TotVolumeImperial': 'float'
        }

        for col, dtype in numeric_columns.items():
            if col in df.columns:
                if dtype == 'int':
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                elif dtype == 'float':
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                elif dtype == 'datetime64[ns]':
                    df[col] = pd.to_datetime(df[col], errors='coerce')

        df = df.dropna(how='all')
        
        return df

    except Exception as e:
        logging.error(f"Error processing Exported Orders: {str(e)}")
        raise

def process_stamp_orders(file_path, week):
    expected_columns = 44
    valid_rows = []
    problematic_rows = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            headers = next(reader)
            
            # Clean the headers
            headers = [clean_column_name(h) for h in headers]
            
            if len(headers) != expected_columns:
                logging.warning(f"Header row has {len(headers)} columns, but expected {expected_columns}.")
                if len(headers) < expected_columns:
                    headers += [f"Column_{i+1}" for i in range(len(headers), expected_columns)]
                else:
                    headers = headers[:expected_columns]

            for row_num, row in enumerate(reader, start=1):
                try:
                    if not row:
                        logging.warning(f"Skipping empty row at line {row_num}")
                        continue
                        
                    cleaned_row = []
                    for value in row:
                        cleaned_value = value.strip().replace('\n', ' ').replace('\r', '')
                        cleaned_row.append(cleaned_value)
                    
                    if len(cleaned_row) == expected_columns:
                        valid_rows.append(cleaned_row)
                    else:
                        if len(cleaned_row) > expected_columns:
                            cleaned_row = cleaned_row[:expected_columns]
                            logging.warning(f"Row {row_num} has {len(row)} columns. Truncated to {expected_columns} columns.")
                        else:
                            cleaned_row += [""] * (expected_columns - len(cleaned_row))
                            logging.warning(f"Row {row_num} has {len(row)} columns. Padded to {expected_columns} columns.")
                        
                        valid_rows.append(cleaned_row)
                        problematic_rows.append((row_num, row))

                except Exception as e:
                    logging.error(f"Error processing row {row_num}: {str(e)}")
                    problematic_rows.append((row_num, f"Error processing row: {str(e)}"))

        df = pd.DataFrame(valid_rows, columns=headers)

        if "Postal_Code" in df.columns:
            df['Postal_Code'] = df['Postal_Code'].replace(['NA', 'N/A', ''], np.nan)
            df['Postal_Code'] = pd.to_numeric(df['Postal_Code'], errors='coerce').fillna(0).astype(int)

        date_columns = ['Date_Printed', 'Date_Delivered']
        float_columns = ['Quoted_Amount', 'Extra_Services']
        integer_columns = ['Origin_Zip', 'Insured_For', 'Duties_and_Taxes_Amount']

        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        for col in float_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        for col in integer_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

        columns_to_check = ['Extra_Services', 'Cost_Code', 'Refund_Request_Date', 'Refund_Status',
                            'Refund_Requested', 'Reference_1', 'Order_ID', 'Store',
                            'Order_Date', 'Order_Total', 'Item_SKUs', 'Items',
                            'Product_Total', 'Shipping_Paid', 'Tax_Paid']

        for col in columns_to_check:
            if col in df.columns:
                df[col] = df[col].where(df[col].notna(), '')
                if df[col].eq('').all():
                    df[col] = '#NUM!'

        for addr in ['Address_2', 'Address_3']:
            if addr in df.columns:
                df[addr] = df[addr].where(df[addr].notna(), '')
                if df[addr].eq('').all():
                    df[addr] = '#NUM!'

        df = df.dropna(how='all')
        
        return df

    except Exception as e:
        logging.error(f"Error processing Stamp Orders: {str(e)}")
        raise

def process_extensiv_txregrpt(file_path, week):
    expected_columns = 22
    valid_rows = []
    problematic_rows = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.reader(file)
            headers = next(reader)
            
            # Clean the headers
            headers = [clean_column_name(h) for h in headers]
            
            if len(headers) != expected_columns:
                logging.warning(f"Header row has {len(headers)} columns, but expected {expected_columns}.")
                if len(headers) < expected_columns:
                    headers += [f"Column_{i+1}" for i in range(len(headers), expected_columns)]
                else:
                    headers = headers[:expected_columns]

            for row_num, row in enumerate(reader, start=1):
                try:
                    if not row:
                        logging.warning(f"Skipping empty row at line {row_num}")
                        continue
                        
                    cleaned_row = []
                    for value in row:
                        cleaned_value = value.strip().replace('\n', ' ').replace('\r', '')
                        cleaned_row.append(cleaned_value)
                    
                    if len(cleaned_row) == expected_columns:
                        valid_rows.append(cleaned_row)
                    else:
                        if len(cleaned_row) > expected_columns:
                            cleaned_row = cleaned_row[:expected_columns]
                            logging.warning(f"Row {row_num} has {len(row)} columns. Truncated to {expected_columns} columns.")
                        else:
                            cleaned_row += [""] * (expected_columns - len(cleaned_row))
                            logging.warning(f"Row {row_num} has {len(row)} columns. Padded to {expected_columns} columns.")
                        
                        valid_rows.append(cleaned_row)
                        problematic_rows.append((row_num, row))

                except Exception as e:
                    logging.error(f"Error processing row {row_num}: {str(e)}")
                    problematic_rows.append((row_num, f"Error processing row: {str(e)}"))

        df = pd.DataFrame(valid_rows, columns=headers)
        df = df.dropna(how='all')

        if problematic_rows:
            logging.warning(f"Found {len(problematic_rows)} problematic rows that required cleaning.")
            for row_num, row in problematic_rows:
                logging.warning(f"Row {row_num}: {row}")

        if len(problematic_rows) > 0:
            logging.error(f"CSV processing encountered too many errors, giving up. Rows: {len(valid_rows) + len(problematic_rows)}; errors: {len(problematic_rows)}; max bad: 0; error percent: {len(problematic_rows) / (len(valid_rows) + len(problematic_rows)) * 100:.2f}%")
            raise Exception(f"CSV processing encountered too many errors, giving up. Total rows: {len(valid_rows) + len(problematic_rows)}; Total errors: {len(problematic_rows)}.")

        return df

    except Exception as e:
        logging.error(f"Error processing Extensiv TxRegRpt: {str(e)}")
        raise

def main():
    # Get folder path from user
    folder_path = input("Please drag and drop the folder containing the CSV files: ").strip('"')
    
    # Get week number from user
    week = input("Please enter the week number (e.g., week1): ")
    
    # Create cleaned folder if it doesn't exist
    cleaned_folder = os.path.join(os.getcwd(), 'cleaned')
    if not os.path.exists(cleaned_folder):
        os.makedirs(cleaned_folder)
    
    # Expected file names
    expected_files = {
        'Exported Orders.csv': process_exported_orders,
        'Stamps Orders.csv': process_stamp_orders,
        'ExtensivTxRegRpt.csv': process_extensiv_txregrpt
    }
    
    # Process each file
    for filename, process_func in expected_files.items():
        file_path = os.path.join(folder_path, filename)
        if os.path.exists(file_path):
            try:
                # Process the file
                df = process_func(file_path, week)
                
                # Create output filename with week prefix
                output_filename = f"{week} {filename}"
                output_path = os.path.join(cleaned_folder, output_filename)
                
                # Save to CSV
                df.to_csv(output_path, index=False, quoting=csv.QUOTE_ALL)
                logging.info(f"✅ {filename} cleaned and saved successfully.")
                print(f"✅ {filename} cleaned and saved successfully.")
                
                # Upload to BigQuery
                client = bigquery.Client()
                dataset_id = 'postage-calculator-tool.pct'
                table_name = output_filename.replace('.csv', '').replace(' ', '_')
                table_id = f"{dataset_id}.{table_name}"
                
                job_config = bigquery.LoadJobConfig(
                    autodetect=True,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
                )
                
                job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
                job.result()
                
                print(f"✅ {filename} uploaded successfully to BigQuery: {table_id}")
                
            except Exception as e:
                logging.error(f"❌ Error processing {filename}: {str(e)}")
                print(f"❌ Error processing {filename}: {str(e)}")
        else:
            logging.warning(f"⚠️ File not found: {filename}")
            print(f"⚠️ File not found: {filename}")

if __name__ == "__main__":
    main()