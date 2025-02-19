import pandas as pd
import re

# Function to clean column names
def clean_column_name(col_name):
    col_name = re.sub(r'[^a-zA-Z0-9_]', '_', col_name)  # Replace unsupported characters
    return col_name.strip()

# Read the CSV file
df_postage = pd.read_csv("Postage Comparison.csv")

# Clean column names
df_postage.columns = [clean_column_name(col) for col in df_postage.columns]

# Clean data (remove quotes and trim whitespace)
for col in df_postage.columns:
    df_postage[col] = df_postage[col].astype(str).str.replace('"', '').str.strip()

# Save cleaned data
df_postage.to_csv("cleaned_postage_comparison.csv", index=False)
print("✅ Cleaned Postage Comparison saved as: cleaned_postage_comparison.csv") 