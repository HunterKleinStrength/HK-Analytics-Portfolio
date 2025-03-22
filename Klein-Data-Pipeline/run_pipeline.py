"""
Automates the Klein Data Pipeline for data fetching and processing.

This script runs the pipeline to fetch data from Firebase and process it into CSVs. It demonstrates automation, data pipeline design, and data processing skills.
"""

import subprocess

# Step 1: Fetch Firebase Realtime Database JSON
print("Step 1: Fetching Firebase Realtime Database data...")
subprocess.run(['python', 'scripts/fetch_firebase_data.py'])

# Step 2: Fetch Firebase Auth data
print("Step 2: Fetching Firebase Auth data...")
subprocess.run(['python', 'scripts/fetch_auth_data.py'])

# Step 3: Process raw data into CSVs
print("Step 3: Processing raw data into CSVs...")
subprocess.run(['python', 'scripts/process_raw_to_csv.py'])

print("Pipeline complete! CSVs are in data/processed/. Next step: Manually upload to MySQL.")
