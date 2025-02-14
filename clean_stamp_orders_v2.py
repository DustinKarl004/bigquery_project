import pandas as pd
import logging
from datetime import datetime
import csv

# Configure logging
logging.basicConfig(filename='csv_cleaning.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Define the expected number of columns
expected_columns = 44  # Updated to 44 columns

try:
    # Read the fixed CSV file
    df = pd.read_csv('stamp_orders_Week6_fixed.csv', dtype={'Tracking #': str}, low_memory=False)

    # Ensure the DataFrame has the expected number of columns
    if len(df.columns) != expected_columns:
        logging.warning(f"CSV file has {len(df.columns)} columns, but expected {expected_columns}.")
        # Adjust columns to match expected columns
        if len(df.columns) < expected_columns:
            for i in range(len(df.columns), expected_columns):
                df[f"Column_{i+1}"] = ""  # Add missing columns
        else:
            df = df.iloc[:, :expected_columns]  # Truncate extra columns

    # Rename problematic columns
    df.columns = [col.replace('/', '_') for col in df.columns]

    # Convert columns to appropriate types
    # Date columns
    date_columns = ['Date Printed', 'Date Delivered', 'Ship Date', 'Refund Request Date', 'Order Date']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')  # Convert to datetime, invalid parsing -> NaT

    # Numeric columns (excluding Tracking # since it contains alphanumeric values)
    numeric_columns = [
        'Amount Paid', 'Adjusted Amount', 'Quoted Amount', 'Extra Services', 'Insured For',
        'Cost Code', 'Refund Requested', 'Reference 1', 'Order ID', 'Store', 'Order Total',
        'Item SKUs', 'Items', 'Product Total', 'Shipping Paid', 'Tax Paid', 'Duties and Taxes Amount'
    ]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')  # Convert to numeric, invalid parsing -> NaN

    # String columns (explicitly including Tracking #)
    string_columns = [
        'Payment Type', 'Shipment Status', 'Tracking #', 'Recipient', 'Name', 'Address 1',
        'Address 2', 'Address 3', 'City', 'State_Province', 'Country', 'Weight', 'Carrier',
        'Service', 'Tracking Confirmation', 'Printed Message', 'User', 'Refund Type',
        'Refund Status', 'Insurance Provider'
    ]
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)  # Ensure all values are strings

    # Postal Code and Origin Zip (convert to integer)
    if 'Postal Code' in df.columns:
        df['Postal Code'] = pd.to_numeric(df['Postal Code'], errors='coerce').astype('Int64')  # Handle NaN
    if 'Origin Zip' in df.columns:
        df['Origin Zip'] = pd.to_numeric(df['Origin Zip'], errors='coerce').astype('Int64')  # Handle NaN

    # Remove any completely empty rows
    df = df.dropna(how='all')

    # Save the cleaned DataFrame with proper quoting
    df.to_csv('cleaned_stamp_orders.csv', index=False, quoting=csv.QUOTE_ALL)
    logging.info("✅ Data cleaned and saved successfully.")
    print("✅ Data cleaned and saved successfully.")

except FileNotFoundError:
    logging.error("❌ The file 'stamp_orders_Week6_fixed.csv' was not found.")
    print("❌ The file 'stamp_orders_Week6_fixed.csv' was not found.")
except Exception as e:
    logging.error(f"❌ An unexpected error occurred: {str(e)}")
    print(f"❌ An unexpected error occurred: {str(e)}")