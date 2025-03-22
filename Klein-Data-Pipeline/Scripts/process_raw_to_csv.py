"""
Processes raw Firebase JSON and auth data into structured CSVs.

This script cleans and transforms Firebase data into four CSVs for analysis: auth_data.csv, subscriptions.csv, user_profiles.csv, and my_gym.csv. It demonstrates data cleaning, transformation, and error handling skills.
"""

import sys
import subprocess
import os
import json
import pandas as pd
from datetime import date

# Debug: Check the environment
print("DEBUG: Checking environment...")
print(f"Current directory: {os.getcwd()}")
print(f"'data/raw/json' exists: {os.path.exists('data/raw/json')}")
print(f"'data/raw/auth' exists: {os.path.exists('data/raw/auth')}")
print(f"'data/processed' exists: {os.path.exists('data/processed')}")

# Define test accounts and flagged strings for filtering
test_accounts = [
    'jR3UB09kczdJQtCGtKHHkHjhVVO2', 
    'QxjvzDIiQsXdaw75X4Y8SVKEsq52', 
    '9tOJ5ZlfRoWnbNmiaDporsJv39V2', 
    'Onr5ALx1EXh9Pl7q0cIiVFHyzhd2', 
    'dFI1IXGR0pWkEvZMneJTyr05eK52', 
    'jYLJccV2lVZKMouzjU6u7NXZs3x1'
]
flagged_strings = ['uat', 'builduat', 'uatbuild', 'hkleeiin']

def is_flagged(user_id, user_info):
    """Check if a user should be filtered out based on their email or user ID."""
    email = user_info.get("email", "").lower()
    for flag in flagged_strings:
        if flag in email:
            return True
    if user_id in test_accounts:
        return True
    return False

# Optionally fetch new Firebase data if 'update' argument is provided
if len(sys.argv) > 1 and sys.argv[1] == "update":
    subprocess.run(["python", "scripts/fetch_firebase_data.py"])

# Load the most recent JSON and auth files
json_files = sorted([f for f in os.listdir('data/raw/json')], reverse=True)
auth_files = sorted([f for f in os.listdir('data/raw/auth')], reverse=True)
print(f"DEBUG: JSON files found: {json_files}")
print(f"DEBUG: Auth files found: {auth_files}")

if not json_files or not auth_files:
    print("Error: Missing JSON or auth files.")
    sys.exit(1)

# Load the data
with open(f"data/raw/json/{json_files[0]}") as json_file:
    data = json.load(json_file)
auth_data = pd.read_csv(f"data/raw/auth/{auth_files[0]}")
print(f"DEBUG: JSON data type: {type(data)}")
print(f"DEBUG: Auth data shape: {auth_data.shape}")

# Process auth data: Filter test accounts and save
auth_data.columns = ['user_id', 'email', 'creation_date', 'last_sign_in']
auth_data = auth_data[~auth_data['user_id'].isin(test_accounts)]
auth_data.to_csv('data/processed/auth_data.csv', index=False)
print("Saved: data/processed/auth_data.csv")

# Process subscriptions: Extract transaction details
subscriptions = pd.DataFrame(columns=[
    'user_id', 'purchase_date', 'expiration_date', 
    'original_purchase_date', 'product_id', 'num_transactions'
])
total_invalid_transactions = 0  # Track invalid entries

userIDs = data.keys()
for i in userIDs:
    if 'userInfo' not in data[i]:
        continue
    user_info = data[i]['userInfo']
    if is_flagged(i, user_info):
        continue
    if 'latestReceiptInfo' in user_info:
        transactions = user_info['latestReceiptInfo']
        num_transactions = len(transactions)
        for y in transactions:
            if not isinstance(y, dict):
                print(f"DEBUG: Skipping invalid transaction for user {i}: {y}")
                total_invalid_transactions += 1
                continue
            subscriptions.loc[len(subscriptions)] = [
                i, 
                y.get('purchase_date_ms'), 
                y.get('expires_date_ms'), 
                y.get('
