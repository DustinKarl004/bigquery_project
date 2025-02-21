import os
import subprocess

# Run this script first

# Before running this, run this for terminal: echo $env:GOOGLE_APPLICATION_CREDENTIALS
# For command prompt: echo %GOOGLE_APPLICATION_CREDENTIALS%

def choose_csv_to_clean():
    print("Choose a csv to clean:")
    print("1. Exported Orders")
    print("2. Stamp Orders")
    print("3. Extensive TxtRegRpt")
    print("4. Postage Comparison")

    choice = input("Enter the number corresponding to your choice: ")
    
    script_filenames = {
        '1': "clean_exported_orders.py",
        '2': "clean_exported_stamp_orders.py",
        '3': "clean_exported_extensiv_txregrpt.py",
        '4': "clean_exported_postage_comparison.py"
    }

    if choice in script_filenames:
        script_file = os.path.join(os.getcwd(), script_filenames[choice])
        run_script(script_file)
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