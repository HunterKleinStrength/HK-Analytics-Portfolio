# Klein Data Pipeline

## Overview

This project demonstrates an end-to-end data pipeline I built to transform raw Firebase data into a relational MySQL database for analysis. The pipeline fetches user authentication and profile data from Firebase, processes it into structured CSVs, and uploads the data into MySQL, enabling efficient querying and analysis. I developed this project to streamline data processing for a fitness app, showcasing my skills in Python, data engineering, and database management.

## Skills Demonstrated

- **Data Engineering**: Designed and implemented a pipeline to extract, transform, and load (ETL) data from Firebase to MySQL.
- **Python Programming**: Wrote modular scripts using `pandas` for data processing and `firebase-admin` for API integration.
- **Database Management**: Structured data into a relational model in MySQL, optimizing for querying and analysis.
- **Error Handling and Debugging**: Added debug statements and robust error handling to ensure pipeline reliability.
- **Automation**: Automated the entire process with a single `run_pipeline.py` script, improving efficiency.
- **Data Cleaning**: Filtered out test accounts and flagged emails, converted timestamps, and handled missing data.

## Project Structure

The pipeline consists of the following steps:

1. **Fetch Auth Data**:
   - Used the Firebase Auth API to retrieve user authentication data (user IDs, emails, creation dates, last sign-in timestamps).
   - Saved as `data/raw/auth/YYYY-MM-DD.csv`.

2. **Fetch Firebase Data**:
   - Pulled a large JSON file from the Firebase Realtime Database containing user profiles and transaction data.
   - Saved as `data/raw/json/YYYY-MM-DD.json`.

3. **Process Raw Data**:
   - Cleaned and parsed the JSON and auth data:
     - Filtered out test accounts and flagged emails (e.g., containing "uat", "builduat").
     - Converted Unix millisecond timestamps to readable dates.
     - Structured data into four CSVs: `auth_data.csv`, `subscriptions.csv`, `user_profiles.csv`, `my_gym.csv`.
   - Added debug statements to track data processing and handle errors (e.g., invalid transactions).

4. **Upload to MySQL**:
   - Uploaded the CSVs to MySQL, creating a relational model keyed on `user_id`.
   - Dynamically generated table schemas based on CSV column types.

## Key Features

- **Modular Design**: Separated the pipeline into distinct scripts (`fetch_auth_data.py`, `fetch_firebase_data.py`, `process_raw_to_csv.py`) for maintainability.
- **Automation**: Created `run_pipeline.py` to execute the entire pipeline with a single command.
- **Data Integrity**: Implemented filtering to remove test accounts and invalid data, ensuring clean datasets.
- **Scalability**: Handled large JSON files and batched Firebase Auth requests to manage API limits.

## Technologies Used

- **Python**: Core language for scripting and automation.
- **Libraries**:
  - `pandas`: Data manipulation and CSV generation.
  - `firebase-admin`: Interacting with Firebase Auth and Realtime Database.
- **Firebase**: Source of raw data (Auth and Realtime Database).
- **MySQL**: Relational database for storing processed data.
- **Git/GitHub**: Version control and project hosting.

## Project Files
KleinDataPipeline/
├── scripts/
│   ├── fetch_auth_data.py        # Fetches Firebase Auth data
│   ├── fetch_firebase_data.py    # Fetches Firebase Realtime Database JSON
│   └── process_raw_to_csv.py     # Processes JSON and auth data into CSVs
├── run_pipeline.py               # Runs the full pipeline
├── upload_to_mysql.py            # Optional: Uploads CSVs to MySQL
├── requirements.txt              # Python dependencies
├── .gitignore                    # Git ignore file
└── README.md                     # Project documentation


**Note**: The `data/` folder (containing raw and processed data) and `config/firebase_admin_key.json` (containing sensitive credentials) are excluded from this repository for security reasons.

## How It Works

1. **Fetch Data**:
   - `fetch_auth_data.py` retrieves user authentication data from Firebase Auth.
   - `fetch_firebase_data.py` pulls user profile and transaction data from the Firebase Realtime Database.

2. **Process Data**:
   - `process_raw_to_csv.py` cleans and transforms the data into four CSVs:
     - `auth_data.csv`: User authentication details.
     - `subscriptions.csv`: Transaction history.
     - `user_profiles.csv`: User profiles with an `email` column.
     - `my_gym.csv`: Translated gym preferences.

3. **Upload to MySQL**:
   - No automation here yet. Simple table creation in mySQL.
   - Want to use `mysql-connector-python` in the future.

## Challenges and Solutions

- **Challenge**: Handling large JSON files from Firebase Realtime Database.
  - **Solution**: Used `pandas` for efficient data processing and added debug statements to monitor progress.
- **Challenge**: Filtering out test accounts and invalid data.
  - **Solution**: Implemented a robust `is_flagged` function to remove test accounts and flagged emails.

## Future Improvements

- Add data validation checks before uploading to MySQL.
- Optimize performance for larger datasets by parallelizing data processing.
- Add Python to MySQL automation

## How to View the Code

The scripts are organized in the `scripts/` folder and the root directory. Key files to review:
- `scripts/process_raw_to_csv.py`: Showcases data cleaning, transformation, and error handling.
- `run_pipeline.py`: Demonstrates automation and integration of the entire pipeline.
- `upload_to_mysql.py`: Highlights database interaction and schema generation.

## Contact

For inquiries or to discuss this project further, please reach out via [LinkedIn](https://www.linkedin.com/in/yourprofile) or email at [your.email@example.com].

---

**This project is a portfolio piece to demonstrate my data engineering skills and is not intended for public use due to the absence of sensitive configuration files.**
