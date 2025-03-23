# Klein Training MyGym Preferences

This project analyzes user equipment selections in the MyGym feature to help prioritize workout content for the Klein Training App. The goal is to identify the top equipment (e.g., dumbbells, kettlebells) so the strength and conditioning department can better allocate resources.

## Overview

The project workflow includes:

1. **Data Fetching**  
   - **Python Scripts:**  
     - **`fetch_firebase_data.py`**: Fetches raw JSON data from Firebase.  
     - **`fetch_auth_data.py`**: Retrieves authentication data and saves it as CSV.

2. **Data Processing**  
   - **Python Script:**  
     - **`process_raw_to_csv.py`**: Processes the raw JSON and CSV files into structured CSVs, including `my_gym.csv` which contains user gym preferences.

3. **Data Analysis**  
   - **SQL:**  
     - **`equipment.sql`**: Contains SQL queries to clean, parse, and calculate key metrics from the processed data.

4. **Visualization**  
   - **Tableau:**  
     - A dashboard (published soon) visualizes the equipment preferences.  
     - [View Dashboard](https://public.tableau.com/views/EquipmentPreferencesandExerciseAvailabilityAnalysis/D[…]sid=&:redirect=auth&:display_count=n&:origin=viz_share_link)  
     - [Screenshot](../screenshots/mygym.png)

## Tools & Files

- **Python Scripts:**  
  - [fetch_firebase_data.py](fetch_firebase_data.py)  
  - [fetch_auth_data.py](fetch_auth_data.py)  
  - [process_raw_to_csv.py](process_raw_to_csv.py)

- **SQL Code:**  
  - [equipment.sql](equipment.sql)

- **Visualization:**  
  - Tableau dashboard ([link](https://public.tableau.com/views/EquipmentPreferencesandExerciseAvailabilityAnalysis/D[…]sid=&:redirect=auth&:display_count=n&:origin=viz_share_link))  
  - [Screenshot](../screenshots/mygym.png)

## Workflow

1. **Fetch Data:**  
   Run the Firebase fetch scripts to obtain JSON and auth data.

2. **Process Data:**  
   Use `process_raw_to_csv.py` to convert raw data into structured CSVs (including `my_gym.csv`).

3. **Analyze Data:**  
   Use the SQL script (`equipment.sql`) to clean and analyze the processed data.

4. **Visualize Results:**  
   Upload the final tables into Tableau to build an interactive dashboard for equipment preference analysis.
