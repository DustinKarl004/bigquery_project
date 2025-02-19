import pandas as pd
import re

# Function to clean column names
def clean_column_name(col_name):
    col_name = re.sub(r'[^a-zA-Z0-9_]', '_', col_name)  # Replace unsupported characters
    return col_name.strip()

# Read the CSV file
df_extensiv = pd.read_csv("ExtensivTxRegRpt.csv")

# Clean column names
df_extensiv.columns = [clean_column_name(col) for col in df_extensiv.columns]

# Clean data (remove quotes and trim whitespace)
for col in df_extensiv.columns:
    df_extensiv[col] = df_extensiv[col].astype(str).str.replace('"', '').str.strip()

# Save cleaned data
df_extensiv.to_csv("cleaned_extensiv_tx_reg_rpt.csv", index=False)
print("✅ Cleaned Extensiv Tx Reg Report saved as: cleaned_extensiv_tx_reg_rpt.csv") 