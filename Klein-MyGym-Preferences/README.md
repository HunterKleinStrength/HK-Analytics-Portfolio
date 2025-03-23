# Klein Training MyGym Preferences

**Excuse the mess.** A lot of learning happened during this process, and it taught us the cost of not building scalable data pipelines from day one.

This project analyzes user equipment selections in the MyGym feature to help prioritize workout content for the Klein Training App. The main goal is to identify the top equipment (e.g., dumbbells, kettlebells) so the strength and conditioning department can better allocate resources.

---

# Klein Training MyGym Preferences

This project analyzes user equipment selections in the MyGym feature to help prioritize workout content for the Klein Training App. The analysis identifies top equipment (such as dumbbells and kettlebells) to guide resource allocation for the strength and conditioning department.

---

## SQL Analysis

The primary SQL file for this analysis is [mygym_analysis_view.sql](mygym_analysis_view.sql). This script creates a view using a recursive CTE to:

- Split comma-separated equipment values in the `translated` column into individual items.
- Count the distinct number of users per equipment type.
- Calculate the percentage of users for each equipment relative to the total in the `my_gym` table.

### SQL Code

```sql
CREATE ALGORITHM=UNDEFINED 
DEFINER=`root`@`localhost` 
SQL SECURITY DEFINER 
VIEW `have equipment analysis` AS
WITH RECURSIVE split_translated AS (
    SELECT 
        `my_gym`.`user_id` AS `user_id`,
        SUBSTRING_INDEX(`my_gym`.`translated`, ',', 1) AS `equipment`,
        IF((LOCATE(',', `my_gym`.`translated`) > 0),
           SUBSTR(`my_gym`.`translated`, (LOCATE(',', `my_gym`.`translated`) + 1)),
           NULL) AS `remaining`,
        `my_gym`.`translated` AS `translated`
    FROM `my_gym`
    WHERE (`my_gym`.`translated` IS NOT NULL)
    
    UNION ALL
    
    SELECT 
        `split_translated`.`user_id` AS `user_id`,
        SUBSTRING_INDEX(`split_translated`.`remaining`, ',', 1) AS `equipment`,
        IF((LOCATE(',', `split_translated`.`remaining`) > 0),
           SUBSTR(`split_translated`.`remaining`, (LOCATE(',', `split_translated`.`remaining`) + 1)),
           NULL) AS `remaining`,
        `split_translated`.`translated` AS `translated`
    FROM `split_translated`
    WHERE (`split_translated`.`remaining` IS NOT NULL)
)
SELECT 
    `split_translated`.`equipment` AS `equipment`,
    COUNT(DISTINCT `split_translated`.`user_id`) AS `user_count`,
    ((COUNT(DISTINCT `split_translated`.`user_id`) * 100.0) / (SELECT COUNT(0) FROM `my_gym`)) AS `percentage`,
    (SELECT COUNT(0) FROM `my_gym`) AS `total_users`
FROM `split_translated`
GROUP BY `split_translated`.`equipment`;


**Visualization**  
   - **Tableau Dashboard**  
     I used Tableau to build an interactive dashboard for the final equipment analysis.  
     **Link:**  
     https://public.tableau.com/views/EquipmentPreferencesandExerciseAvailabilityAnalysis/Dashboard1?:language=en-US&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link

