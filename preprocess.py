import csv

input_file = 'stamp_orders_Week6.csv'
output_file = 'stamp_orders_Week6_fixed.csv'

# Open the input and output files
with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', newline='', encoding='utf-8') as outfile:
    reader = csv.reader(infile)
    writer = csv.writer(outfile, quoting=csv.QUOTE_ALL)  # Ensure all fields are quoted

    # Write the header row
    headers = next(reader)
    writer.writerow(headers)

    # Process each row
    for row_num, row in enumerate(reader, start=1):
        try:
            # Ensure the row has the expected number of columns
            if len(row) < len(headers):
                # Pad the row with empty strings for missing columns
                row += [''] * (len(headers) - len(row))
            elif len(row) > len(headers):
                # Truncate the row to the expected number of columns
                row = row[:len(headers)]

            # Clean the "Printed Message" column (index 25, assuming 0-based indexing)
            if len(row) > 25:
                # Replace tabs and other special characters with spaces
                row[25] = row[25].replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
                # Enclose the value in quotes
                row[25] = f'"{row[25]}"'

            # Write the cleaned row to the output file
            writer.writerow(row)

        except Exception as e:
            print(f"Error processing row {row_num}: {e}")
            continue

print(f"✅ Fixed CSV saved to {output_file}")