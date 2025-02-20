import os
import subprocess

#run this script first

# before running this, run this for terminal: echo $env:GOOGLE_APPLICATION_CREDENTIALS
# for command prompt: echo %GOOGLE_APPLICATION_CREDENTIALS%

def choose_csv_to_clean():
    print("Choose a csv to clean:")
    print("1. Exported Orders")
    print("2. Stamp Orders")
    print("3. Extensive TxtRegRpt")
    print("4. Postage Comparison")

    choice = input("Enter the number corresponding to your choice: ")
    
    script_paths = {
        '1': r"C:\Users\Elevate\bigquery_project\clean_exported_orders.py",
        '2': r"C:\Users\Elevate\bigquery_project\clean_exported_stamp_orders.py",
        '3': r"C:\Users\Elevate\bigquery_project\clean_exported_extensiv_txregrpt.py",
        '4': r"C:\Users\Elevate\bigquery_project\clean_exported_postage_comparison.py"
    }

    if choice in script_paths:
        run_script(script_paths[choice])
    else:
        print("Invalid choice. Please select a valid option.")

def run_script(script_file):
    if os.path.exists(script_file):
        # Use the Python interpreter from the virtual environment
        python_executable = os.path.join(os.getcwd(), 'venv', 'Scripts', 'python')
        subprocess.run([python_executable, script_file])
    else:
        print("The specified file does not exist. Please check the path.")

# Call the function to start the process
choose_csv_to_clean()
