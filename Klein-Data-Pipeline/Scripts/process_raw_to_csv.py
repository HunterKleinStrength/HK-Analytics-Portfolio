"""
Processes raw Firebase JSON and auth data into structured CSVs.
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

# Ensure required directories exist
required_dirs = ['data/raw/json', 'data/raw/auth', 'data/processed']
for dir_path in required_dirs:
    if not os.path.exists(dir_path):
        print(f"ERROR: Directory '{dir_path}' does not exist. Creating it...")
        os.makedirs(dir_path)
    print(f"'{dir_path}' exists: {os.path.exists(dir_path)}")

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
    print("DEBUG: Running fetch_firebase_data.py to update data...")
    try:
        subprocess.run(["python", "scripts/fetch_firebase_data.py"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Failed to run fetch_firebase_data.py: {e}")
        sys.exit(1)
    except FileNotFoundError:
        print("ERROR: scripts/fetch_firebase_data.py not found. Please ensure it exists or remove the 'update' argument.")
        sys.exit(1)

# Load the most recent JSON and auth files
try:
    json_files = sorted([f for f in os.listdir('data/raw/json')], reverse=True)
    auth_files = sorted([f for f in os.listdir('data/raw/auth')], reverse=True)
except FileNotFoundError as e:
    print(f"ERROR: Directory not found: {e}")
    sys.exit(1)

print(f"DEBUG: JSON files found: {json_files}")
print(f"DEBUG: Auth files found: {auth_files}")

if not json_files or not auth_files:
    print("ERROR: Missing JSON or auth files in 'data/raw/json' or 'data/raw/auth'.")
    sys.exit(1)

# Load the data
try:
    with open(f"data/raw/json/{json_files[0]}") as json_file:
        data = json.load(json_file)
    auth_data = pd.read_csv(f"data/raw/auth/{auth_files[0]}")
except FileNotFoundError as e:
    print(f"ERROR: File not found: {e}")
    sys.exit(1)
except json.JSONDecodeError as e:
    print(f"ERROR: Invalid JSON format in {json_files[0]}: {e}")
    sys.exit(1)
except pd.errors.EmptyDataError as e:
    print(f"ERROR: Auth CSV file {auth_files[0]} is empty: {e}")
    sys.exit(1)

print(f"DEBUG: JSON data type: {type(data)}")
print(f"DEBUG: Auth data shape: {auth_data.shape}")

# Process auth data: Filter test accounts and save
auth_data.columns = ['user_id', 'email', 'creation_date', 'last_sign_in']
auth_data = auth_data[~auth_data['user_id'].isin(test_accounts)]
try:
    auth_data.to_csv('data/processed/auth_data.csv', index=False)
    print("Saved: data/processed/auth_data.csv")
except PermissionError as e:
    print(f"ERROR: Permission denied when writing to data/processed/auth_data.csv: {e}")
    sys.exit(1)

# Process subscriptions: Extract transaction details
subscriptions = pd.DataFrame(columns=[
    'user_id', 'purchase_date', 'expiration_date', 
    'original_purchase_date', 'product_id', 'num_transactions'
])
total_invalid_transactions = 0
total_users = 0
skipped_no_userinfo = 0
skipped_flagged = 0
processed_subscriptions = 0

userIDs = data.keys()
for i in userIDs:
    total_users += 1
    if 'userInfo' not in data[i]:
        skipped_no_userinfo += 1
        print(f"DEBUG: Skipping user {i} - no userInfo (subscriptions)")
        continue
    user_info = data[i]['userInfo']
    if is_flagged(i, user_info):
        skipped_flagged += 1
        print(f"DEBUG: Skipping user {i} - flagged (email: {user_info.get('email', 'N/A')}) (subscriptions)")
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
        processed_subscriptions += 1

# Log subscriptions summary
print(f"DEBUG: Subscriptions - Total users: {total_users}")
print(f"DEBUG: Subscriptions - Skipped (no userInfo): {skipped_no_userinfo}")
print(f"DEBUG: Subscriptions - Skipped (flagged): {skipped_flagged}")
print(f"DEBUG: Subscriptions - Processed users: {processed_subscriptions}")
print(f"DEBUG: Subscriptions - Total invalid transactions skipped: {total_invalid_transactions}")

# Save the subscriptions data
try:
    subscriptions.to_csv('data/processed/subscriptions.csv', index=False)
    print("Saved: data/processed/subscriptions.csv")
except PermissionError as e:
    print(f"ERROR: Permission denied when writing to data/processed/subscriptions.csv: {e}")
    sys.exit(1)

# Process user profiles: Extract user info
user_profiles = pd.DataFrame(columns=[
    'user_id', 'email', 'country', 'city', 'height', 'weight', 'gender', 'age', 'active', 'level'
])
total_users = 0
skipped_no_userinfo = 0
skipped_flagged = 0
processed_profiles = 0

for user_id in userIDs:
    total_users += 1
    if 'userInfo' not in data[user_id]:
        skipped_no_userinfo += 1
        print(f"DEBUG: Skipping user {user_id} - no userInfo (user_profiles)")
        continue
    user_info = data[user_id]['userInfo']
    if is_flagged(user_id, user_info):
        skipped_flagged += 1
        print(f"DEBUG: Skipping user {user_id} - flagged (email: {user_info.get('email', 'N/A')}) (user_profiles)")
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
    processed_profiles += 1

# Log user profiles summary
print(f"DEBUG: User Profiles - Total users: {total_users}")
print(f"DEBUG: User Profiles - Skipped (no userInfo): {skipped_no_userinfo}")
print(f"DEBUG: User Profiles - Skipped (flagged): {skipped_flagged}")
print(f"DEBUG: User Profiles - Processed users: {processed_profiles}")

# Save the user profiles data
try:
    user_profiles.to_csv('data/processed/user_profiles.csv', index=False)
    print("Saved: data/processed/user_profiles.csv")
except PermissionError as e:
    print(f"ERROR: Permission denied when writing to data/processed/user_profiles.csv: {e}")
    sys.exit(1)

# Process my_gym preferences
my_gym = pd.DataFrame(columns=['user_id', 'creation_date', 'preferences', 'translated'])
total_users = 0
skipped_no_userinfo = 0
skipped_flagged = 0
skipped_no_preferences = 0
skipped_invalid_preferences = 0
processed_users = 0

# Preference mapping
preference_mapping = {
    'A': 'Full Gym',
    'N': 'Bodyweight',
    'DA': 'Dumbbells',
    'KA': 'Kettlebells',
    'K': 'Kettlebells',  # Added to handle deduplication
    'B': 'Barbell & Plates',
    'C': 'Cable Machines',
    'J': 'Boxes',
    'M': 'Med Ball',
    'S': 'Swiss Ball',
    'R': 'Bands',
    'HB': 'Hexbar',
    'H': 'Hexbar',
    'X': 'Back Extension Machine',
    'Y': 'Assault Bike',
    'P': 'Weighted Plate',
    'D': 'Dumbbells'  # Added to handle deduplication
}

# Possible field names for gym preferences
possible_preference_fields = ['myGym', 'myGymPreferences', 'gymPreferences', 'my_gym_preferences']

for user_id in userIDs:
    total_users += 1
    if 'userInfo' not in data[user_id]:
        skipped_no_userinfo += 1
        print(f"DEBUG: Skipping user {user_id} - no userInfo (my_gym)")
        continue
    user_info = data[user_id]['userInfo']
    if is_flagged(user_id, user_info):
        skipped_flagged += 1
        print(f"DEBUG: Skipping user {user_id} - flagged (email: {user_info.get('email', 'N/A')}) (my_gym)")
        continue
    creation_date = auth_data[auth_data['user_id'] == user_id]['creation_date'].iloc[0] if user_id in auth_data['user_id'].values else None
    
    # Check for any of the possible preference field names
    preferences = None
    found_field = None
    for field in possible_preference_fields:
        if field in user_info:
            preferences = user_info[field]
            found_field = field
            break
    
    if preferences is not None:
        pref_list = []
        
        # Step 1: Extract the preferences into a list
        if preferences is None:  # Handle None case
            pref_list = []
        elif isinstance(preferences, list):
            pref_list = preferences
        elif isinstance(preferences, str):
            pref_list = [pref.strip() for pref in preferences.split(',')]
        else:
            skipped_invalid_preferences += 1
            print(f"DEBUG: Skipping user {user_id} - {found_field} is not a list or string: {preferences}")
            continue
        
        # Step 2: Normalize and standardize preferences
        normalized_prefs = []
        for pref in pref_list:
            if not pref:  # Skip empty strings
                continue
            # Normalize: strip whitespace and convert to uppercase
            pref = pref.strip().upper()
            # Standardize preferences
            if pref == 'D':
                pref = 'DA'
            if pref == 'KA':
                pref = 'K'
            normalized_prefs.append(pref)
        
        # Step 3: Deduplicate preferences
        deduped_prefs = []
        seen = set()
        for pref in normalized_prefs:
            if pref not in seen:
                seen.add(pref)
                deduped_prefs.append(pref)
        
        # Step 4: Create comma-separated strings for preferences and translated values
        if deduped_prefs:
            preferences_str = ','.join(deduped_prefs)
            translated_list = [preference_mapping.get(pref, '') for pref in deduped_prefs]
            translated_str = ','.join(translated_list)
            
            # Step 5: Add a single row for the user
            my_gym.loc[len(my_gym)] = [
                user_id,
                creation_date,
                preferences_str,
                translated_str
            ]
            processed_users += 1
            print(f"DEBUG: Processed user {user_id} - preferences: {preferences_str}, translated: {translated_str}")
    else:
        skipped_no_preferences += 1
        print(f"DEBUG: Skipping user {user_id} - no gym preferences field found (tried: {possible_preference_fields})")

# Log my_gym summary
print(f"DEBUG: My Gym - Total users: {total_users}")
print(f"DEBUG: My Gym - Skipped (no userInfo): {skipped_no_userinfo}")
print(f"DEBUG: My Gym - Skipped (flagged): {skipped_flagged}")
print(f"DEBUG: My Gym - Skipped (no gym preferences field): {skipped_no_preferences}")
print(f"DEBUG: My Gym - Skipped (invalid preferences type): {skipped_invalid_preferences}")
print(f"DEBUG: My Gym - Processed users: {processed_users}")

# Save the my_gym data
try:
    my_gym.to_csv('data/processed/my_gym.csv', index=False)
    print("Saved: data/processed/my_gym.csv")
except PermissionError as e:
    print(f"ERROR: Permission denied when writing to data/processed/my_gym.csv: {e}")
    sys.exit(1)
