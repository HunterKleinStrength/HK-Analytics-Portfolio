import os
import pandas as pd
import firebase_admin
from firebase_admin import credentials
from firebase_admin import auth
from datetime import date

print("DEBUG: Current working directory:", os.getcwd())
print("DEBUG: Does 'data/raw/auth' folder exist?", os.path.exists("data/raw/auth"))

cred = credentials.Certificate('config/firebase_admin_key.json')
firebase_admin.initialize_app(cred)

batch_size = 1000
page = auth.list_users(max_results=batch_size)

df = pd.DataFrame(columns=['user_id', 'email', 'creation_date', 'last_sign_in'])

# Iterate over all users
user_count = 0
while page:
    for user in page.users:
        user_count += 1
        df.loc[len(df)] = [
            user.uid,
            user.email,
            user.user_metadata.creation_timestamp,
            user.user_metadata.last_sign_in_timestamp
        ]
    page = page.get_next_page()

print(f"DEBUG: Total users fetched: {user_count}")

# Remove fake emails
fake_emails = ['uatbuild.com', 'uat.com']
df['email'] = df['email'].astype(str)
for e in fake_emails:
    mask = ~df['email'].str.endswith(e)
    df = df[mask]

# Convert timestamps
df.creation_date = pd.to_datetime(df.creation_date, unit='ms', origin='unix')
df.last_sign_in = pd.to_datetime(df.last_sign_in, unit='ms', origin='unix')

# Prepare output path
file_path = f"data/raw/auth/{date.today()}.csv"
print("DEBUG: Will write auth CSV to:", os.path.abspath(file_path))

df.to_csv(file_path, index=False)
print("Uploaded auth data to", file_path)
