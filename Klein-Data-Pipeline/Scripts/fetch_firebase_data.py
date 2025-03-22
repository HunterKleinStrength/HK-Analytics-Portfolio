import os
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
import json
from datetime import date

print("DEBUG: Current working directory:", os.getcwd())
print("DEBUG: Does 'data/raw/json' folder exist?", os.path.exists("data/raw/json"))

"""
Fetches user data from Firebase Realtime Database and saves it as a JSON file.

This script connects to the Firebase Realtime Database, retrieves user data, and saves it as a JSON file for further processing in the Klein Data Pipeline.
"""

import os
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
import json
from datetime import date

# Debug: Check working directory and folder existence
print("DEBUG: Current working directory:", os.getcwd())
print("DEBUG: Does 'data/raw/json' folder exist?", os.path.exists("data/raw/json"))

# Initialize Firebase Admin
cred = credentials.Certificate("config/firebase_admin_key.json")
# Note: Sensitive Firebase credentials are hidden for security.
# Replace with your own credentials to use this script.
config = {
    "apiKey": "HIDDEN_FOR_SECURITY",
    "authDomain": "HIDDEN_FOR_SECURITY",
    "databaseURL": "HIDDEN_FOR_SECURITY",
    "projectId": "HIDDEN_FOR_SECURITY",
    "storageBucket": "HIDDEN_FOR_SECURITY",
    "databaseAuthVariableOverride": {
        'uid': 'HIDDEN_FOR_SECURITY'
    }
}

app = firebase_admin.initialize_app(cred, config)

# Fetch data from /users endpoint
userDict = db.reference("/users/").get()
print("DEBUG: Fetched data type:", type(userDict))
if userDict:
    print("DEBUG: Number of keys in userDict:", len(userDict))
    # Uncomment next line to see all keys (may be large):
    # print("DEBUG: userDict keys:", list(userDict.keys()))
else:
    print("DEBUG: userDict is empty or None")

# Prepare to write JSON
file_path = f"data/raw/json/{date.today()}.json"
print("DEBUG: Will write JSON to:", os.path.abspath(file_path))

# Write JSON to file
with open(file_path, "w") as f:
    json.dump(userDict, f, indent=4)
    print("Wrote data to", file_path)
