1️⃣ Setting the Environment Variable in Command Prompt (cmd.exe)

Temporary (Current Session Only)

If you need to set the environment variable only for the current session, run this command in Command Prompt:

set GOOGLE_APPLICATION_CREDENTIALS=C:\Users\Elevate\bigquery_project\service-account-file.json

Permanent (System-Wide)

To make the variable persist across reboots:

Open the Start Menu, search for "Environment Variables", and select "Edit the system environment variables."

In the System Properties window, click "Environment Variables".

Under "System variables" (or "User variables"), click "New".

Set:

Variable name: GOOGLE_APPLICATION_CREDENTIALS

Variable value: C:\Users\Elevate\bigquery_project\service-account-file.json

Click OK and restart the Command Prompt.

To verify, run:

echo %GOOGLE_APPLICATION_CREDENTIALS%

2️⃣ Setting the Environment Variable in PowerShell

By default, PowerShell does not always recognize system-wide environment variables immediately. To ensure it works correctly, follow these steps:

Temporary (Current PowerShell Session Only)

Run this command inside PowerShell:

$env:GOOGLE_APPLICATION_CREDENTIALS = "C:\Users\Elevate\bigquery_project\service-account-file.json" #change it with the file path

Verify:

echo $env:GOOGLE_APPLICATION_CREDENTIALS

Permanent (Persistent Across PowerShell Sessions)

If the environment variable resets after closing PowerShell, you need to add it to your PowerShell profile.

Step 1: Create or Open Your PowerShell Profile

First, check if a profile exists:

echo $PROFILE

If the file does not exist, create it:

New-Item -Path $PROFILE -ItemType File -Force

Now, open it in Notepad:

notepad $PROFILE

Step 2: Add the Environment Variable to the Profile

Add the following line at the bottom of the profile file:

$env:GOOGLE_APPLICATION_CREDENTIALS = [System.Environment]::GetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", "Machine")

Save the file and close Notepad.

Step 3: Restart PowerShell

Close PowerShell and reopen it, then verify:

echo $env:GOOGLE_APPLICATION_CREDENTIALS

If the path appears correctly, your setup is complete!