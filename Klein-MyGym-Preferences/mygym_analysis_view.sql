-- Excuse the mess. A lot of learning during this process and a hard lesson on the costs of not building scalable data pipelines from startup day one.

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

