# Klein Training MyGym Preferences

**Excuse the mess.** A lot of learning happened during this process, and it taught us the cost of not building scalable data pipelines from day one.

This project analyzes user equipment selections in the MyGym feature to help prioritize workout content for the Klein Training App. The main goal is to identify the top equipment (e.g., dumbbells, kettlebells) so the strength and conditioning department can better allocate resources.

---

## Overview

1. **Data Fetching**  
   - **[`[fetch_firebase_data.py](https://github.com/HunterKleinschmidt/HK-Analytics-Portfolio/blob/main/Klein-Data-Pipeline/Scripts/fetch_firebase_data.py)](https://github.com/HunterKleinschmidt/HK-Analytics-Portfolio/blob/main/Klein-Data-Pipeline/Scripts/fetch_firebase_data.py)`](Scripts/fetch_firebase_data.py)** Fetches raw JSON data from Firebase.  
   - **[`fetch_auth_data.py`](Scripts/fetch_auth_data.py)**  
     Retrieves authentication data (user emails, creation dates, etc.) and saves it as CSV.

2. **Data Processing**  
   - **[`process_raw_to_csv.py`](Scripts/process_raw_to_csv.py)**  
     Converts the raw JSON and CSV files into structured CSVs. It also handles filtering out test accounts and merges user info into a final `my_gym.csv`.

3. **Data Analysis**  
   - **[`equipment.sql`](equipment.sql)**  
     Example SQL script to parse the `my_gym.csv` data, calculate how many users do NOT use each equipment type, and produce metrics for further analysis.  
   - **[`mygym_analysis_view.sql`](mygym_analysis_view.sql)**  
     Creates a view (using a recursive CTE) that splits comma-separated equipment lists into individual items, then counts the number of distinct users per equipment and calculates percentages.

4. **Visualization**  
   - **Tableau Dashboard**  
     I used Tableau to build an interactive dashboard for the final equipment analysis.  
     **Link:**  
     https://public.tableau.com/views/EquipmentPreferencesandExerciseAvailabilityAnalysis/Dashboard1?:language=en-US&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link

