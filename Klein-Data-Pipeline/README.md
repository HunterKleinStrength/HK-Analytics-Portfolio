# Klein Data Pipeline

This folder contains the scripts and documentation for converting raw Firebase data into cleaned CSVs for analysis. The pipeline handles both Firebase Auth data and a massive Firebase Realtime Database JSON file.

## Pipeline Steps

1. **Fetch Auth Data:**  
   Retrieve user authentication data (user IDs, emails, creation dates, last sign-in timestamps) from Firebase Auth and generate `auth_data.csv`.

2. **Pull JSON Data:**  
   Download the full, heavy JSON file from the Firebase Realtime Database and store it locally (e.g., in `data/raw/json/`).

3. **Clean and Parse JSON:**  
   - Filter out test accounts and remove flagged emails based on predefined criteria.
   - Convert Unix millisecond timestamps into readable date formats.
   - Parse the JSON into logical groups:
     - **Subscriptions:** Extract transaction details into `subscriptions.csv`.
     - **User Profiles:** Extract personal and movement data into `user_profiles.csv`.
     - **Gym Preferences:** Translate gym preference codes into `my_gym.csv`.

4. **Upload to SQL:**  
   Import all cleaned CSV files (including `auth_data.csv`, `subscriptions.csv`, `user_profiles.csv`, and `my_gym.csv`) into MySQL to form a relational model keyed on `user_id`.

## Usage

- `python fetch_auth_data.py`: Pulls Firebase Auth data and saves it as `auth_data.csv`.
- `python process_json_to_csv.py`: Cleans and parses the massive JSON file into `subscriptions.csv`, `user_profiles.csv`, and `my_gym.csv`.
- `python run_pipeline.py`: Runs the full pipeline end-to-end (if you have multiple steps to execute in sequence).

## Notes

- Requires Python 3.7+ and packages such as `pandas`.
- Adjust file paths as needed in each script.
- This pipeline provides an end-to-end solution for transforming unstructured Firebase data into a relational format for further SQL analysis.
