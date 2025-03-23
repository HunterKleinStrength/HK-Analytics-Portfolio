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
                y.get('original_purchase_date_ms'), 
                y.get('product_id'), 
                num_transactions
            ]

# Save the subscriptions data
subscriptions.to_csv('data/processed/subscriptions.csv', index=False)
print("Saved: data/processed/subscriptions.csv")
print(f"Total invalid transactions skipped: {total_invalid_transactions}")

# Process user profiles: Extract user info
user_profiles = pd.DataFrame(columns=[
    'user_id', 'email', 'country', 'city', 'height', 'weight', 'gender', 'age', 'active', 'level'
])
for user_id in userIDs:
    if 'userInfo' not in data[user_id]:
        continue
    user_info = data[user_id]['userInfo']
    if is_flagged(user_id, user_info):
        continue
    user_profiles.loc[len(user_profiles)] = [
        user_id,
        user_info.get('email'),
        user_info.get('country'),
        user_info.get('city'),
        user_info.get('height'),
        user_info.get('weight'),
        user_info.get('gender'),
        user_info.get('age'),
        user_info.get('active'),
        user_info.get('level')
    ]

# Save the user profiles data
user_profiles.to_csv('data/processed/user_profiles.csv', index=False)
print("Saved: data/processed/user_profiles.csv")

# Define preference mapping for my_gym
preference_mapping = {
    'DA': 'Dumbbells',
    'K': 'Kettlebells',
    'KA': 'Kettlebells',
    'B': 'Barbell & Plates',
    'C': 'Cable Machines',
    'J': 'Boxes',
    'M': 'Med Ball',
    'S': 'Swiss Ball',
    'R': 'Bands',
    'N': 'Bodyweight',
    'HB': 'Hexbar',
    'H': 'Hexbar',
    'X': 'Back Extension Machine',
    'Y': 'Assault Bike',
    'P': 'Weighted Plate',
    'D': 'Dumbbells'
}

# Process my_gym preferences
my_gym = pd.DataFrame(columns=['user_id', 'creation_date', 'preferences', 'translated'])
for user_id in userIDs:
    if 'userInfo' not in data[user_id]:
        continue
    user_info = data[user_id]['userInfo']
    if is_flagged(user_id, user_info):
        continue
    creation_date = auth_data[auth_data['user_id'] == user_id]['creation_date'].iloc[0] if user_id in auth_data['user_id'].values else None
    if 'myGymPreferences' in user_info:
        preferences = user_info['myGymPreferences']
        pref_list = []
        
        # Step 1: Extract the preferences into a list
        if isinstance(preferences, list):
            pref_list = preferences
        elif isinstance(preferences, str):
            pref_list = [pref.strip() for pref in preferences.split(',')]
        
        # Step 2: Deduplicate preferences
        # - Replace 'D' with 'DA' (both are Dumbbells)
        # - Replace 'KA' with 'K' (both are Kettlebells)
        # - Use a set to ensure no duplicates while preserving order
        deduped_prefs = []
        seen = set()
        for pref in pref_list:
            # Standardize Dumbbells: 'D' -> 'DA'
            if pref == 'D':
                pref = 'DA'
            # Standardize Kettlebells: 'KA' -> 'K'
            if pref == 'KA':
                pref = 'K'
            # Only add if not already seen
            if pref not in seen:
                seen.add(pref)
                deduped_prefs.append(pref)
        
        # Step 3: Translate the deduplicated preferences
        translated_list = [preference_mapping.get(pref, '') for pref in deduped_prefs]
        
        # Step 4: Join the preferences and translated values into comma-separated strings
        preferences_str = ','.join(deduped_prefs)
        translated_str = ','.join(translated_list)
        
        # Step 5: Add a single row for the user
        my_gym.loc[len(my_gym)] = [
            user_id,
            creation_date,
            preferences_str,
            translated_str
        ]

# Save the my_gym data
my_gym.to_csv('data/processed/my_gym.csv', index=False)
print("Saved: data/processed/my_gym.csv")
