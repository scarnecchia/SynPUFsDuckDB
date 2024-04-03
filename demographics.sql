CREATE OR REPLACE TEMP TABLE dem_l3_tmp AS
SELECT (CAST(FLOOR(CAST(DATESUB('month', Birth_date, '2010-12-31') AS INTEGER) / 12) AS INTEGER)) as age
      , sex
      , hispanic
      , race
      , count(*) as count
FROM 'C:\Users\Dscarnec\Downloads\SynPUFs-parquet\demographic-snappy.parquet'
GROUP BY age, sex, hispanic, race;

CREATE OR REPLACE TEMP TABLE dem_l3_agecat_catvars AS
SELECT CASE WHEN age IS NULL THEN '00. MISSING'
            WHEN age < 0 THEN '00. NEGATIVE'
            WHEN AGE BETWEEN 2 AND 4   THEN '02. 2-4 yrs'
            WHEN AGE BETWEEN 5 AND 9  THEN '03. 5-9 yrs'
            WHEN AGE BETWEEN 10 AND 14 THEN '04. 10-14 yrs'
            WHEN AGE BETWEEN 15 AND 18 THEN '05. 15-18 yrs'
            WHEN AGE BETWEEN 19 AND 21 THEN '06. 19-21 yrs'
            WHEN AGE BETWEEN 22 AND 24 THEN '07. 22-24 yrs'
            WHEN AGE BETWEEN 25 AND 34 THEN '08. 25-34 yrs'
            WHEN AGE BETWEEN 35 AND 44 THEN '09. 35-44 yrs'
            WHEN AGE BETWEEN 45 AND 54 THEN '10. 45-54 yrs'
            WHEN AGE BETWEEN 55 AND 59 THEN '11. 55-59 yrs'
            WHEN AGE BETWEEN 60 AND 64 THEN '12. 60-64 yrs'
            WHEN AGE BETWEEN 65 AND 69 THEN '13. 65-69 yrs'
            WHEN AGE BETWEEN 70 AND 74 THEN '14. 70-74 yrs'
            ELSE '15. 75+' END as agecat_years
     , sex
     , hispanic
     , race
     , sum(count) as count
FROM dem_l3_tmp
GROUP BY agecat_years, sex, hispanic, race
ORDER BY agecat_years;

CREATE OR REPLACE TEMP TABLE dem_l3_agecat AS
SELECT agecat_years
     , sum(count) as count
FROM dem_l3_agecat_catvars
GROUP BY agecat_years
ORDER BY agecat_years;

SELECT * FROM dem_l3_agecat_catvars;
SELECT * FROM dem_l3_agecat;