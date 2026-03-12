CREATE TABLE raw_daily_activity (
    Id BIGINT,
    ActivityDate VARCHAR(50), -- Imported as text to prevent errors
    TotalSteps INT,
    TotalDistance FLOAT,
    TrackerDistance FLOAT,
    LoggedActivitiesDistance FLOAT,
    VeryActiveDistance FLOAT,
    ModeratelyActiveDistance FLOAT,
    LightActiveDistance FLOAT,
    SedentaryActiveDistance FLOAT,
    VeryActiveMinutes INT,
    FairlyActiveMinutes INT,
    LightlyActiveMinutes INT,
    SedentaryMinutes INT,
    Calories INT
);


CREATE TABLE raw_sleep_day (
    Id BIGINT,
    SleepDay VARCHAR(50), -- Imported as text because it has hours/minutes attached
    TotalSleepRecords INT,
    TotalMinutesAsleep INT,
    TotalTimeInBed INT
);

select * from raw_sleep_day;

SELECT DISTINCT
    a.Id,
    STR_TO_DATE(a.ActivityDate, '%m/%d/%Y') AS Clean_Activity_Date,
    a.TotalSteps,
    a.Calories,
    s.TotalMinutesAsleep
FROM raw_daily_activity a
LEFT JOIN raw_sleep_day s
    ON a.Id = s.Id 
    AND STR_TO_DATE(a.ActivityDate, '%m/%d/%Y') = STR_TO_DATE(SUBSTRING(s.SleepDay, 1, 10), '%m/%d/%Y')
ORDER BY a.Id, Clean_Activity_Date;

CREATE TABLE raw_hourly_steps (
    Id BIGINT,
    ActivityHour VARCHAR(50), -- Imported as text because it contains both date and time
    StepTotal INT
);

select * from raw_hourly_steps;

SELECT 
    h.Id,
    STR_TO_DATE(SUBSTRING(h.ActivityHour, 1, 10), '%m/%d/%Y') AS Clean_Date,
    SUBSTRING(h.ActivityHour, 12, 11) AS Time_of_Day,
    h.StepTotal AS Hourly_Steps,
    a.TotalSteps AS Daily_Total_Steps,
    a.Calories AS Daily_Calories,
    s.TotalMinutesAsleep
FROM raw_hourly_steps h
LEFT JOIN raw_daily_activity a 
    ON h.Id = a.Id 
    AND STR_TO_DATE(SUBSTRING(h.ActivityHour, 1, 10), '%m/%d/%Y') = STR_TO_DATE(a.ActivityDate, '%m/%d/%Y')
LEFT JOIN raw_sleep_day s
    ON h.Id = s.Id 
    AND STR_TO_DATE(SUBSTRING(h.ActivityHour, 1, 10), '%m/%d/%Y') = STR_TO_DATE(SUBSTRING(s.SleepDay, 1, 10), '%m/%d/%Y')
ORDER BY h.Id, Clean_Date, Time_of_Day;

SELECT 
    SUBSTRING(ActivityHour,12, 11) AS Time_of_Day,
    ROUND(AVG(StepTotal), 0) AS Average_Steps
FROM raw_hourly_steps
GROUP BY Time_of_Day
ORDER BY Average_Steps desc
LIMIT 5;

SELECT 
    Id,
    COUNT(SleepDay) AS Total_Sleep_Logs,
    ROUND(AVG(TotalMinutesAsleep), 0) AS Avg_Sleep_Minutes
FROM raw_sleep_day
GROUP BY Id
HAVING Avg_Sleep_Minutes < 360 
ORDER BY Avg_Sleep_Minutes ASC;

WITH Clean_Joined_Data AS (
    -- Step 1: The CTE (This cleans and joins the data temporarily)
    SELECT 
        a.Id,
        STR_TO_DATE(a.ActivityDate, '%m/%d/%Y') AS Activity_Date,
        a.TotalSteps,
        s.TotalMinutesAsleep
    FROM raw_daily_activity a
    LEFT JOIN raw_sleep_day s
        ON a.Id = s.Id 
        AND STR_TO_DATE(a.ActivityDate, '%m/%d/%Y') = STR_TO_DATE(SUBSTRING(s.SleepDay, 1, 10), '%m/%d/%Y')
)
-- Step 2: The Window Function (Querying from the CTE we just built)
SELECT 
    Id,
    Activity_Date,
    TotalSteps,
    TotalMinutesAsleep,
    ROUND(AVG(TotalSteps) OVER (
        PARTITION BY Id 
        ORDER BY Activity_Date 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 0) AS Rolling_3_Day_Avg_Steps
FROM Clean_Joined_Data
WHERE TotalMinutesAsleep IS NOT NULL;

CREATE TABLE raw_hourly_calories (
    Id BIGINT,
    ActivityHour VARCHAR(50), 
    Calories INT
);

WITH Hourly_Activity_Metrics AS (
    -- Step 1: The CTE joins the two hourly tables together
    SELECT 
        s.Id,
        s.ActivityHour,
        SUBSTRING(s.ActivityHour, 12, 11) AS Time_of_Day,
        s.StepTotal,
        c.Calories
    FROM raw_hourly_steps s
    INNER JOIN raw_hourly_calories c
        ON s.Id = c.Id 
        AND s.ActivityHour = c.ActivityHour
)
-- Step 2: The Window Function calculates the running total of calories
SELECT 
    Id,
    ActivityHour,
    Time_of_Day,
    StepTotal,
    Calories AS Hourly_Calories_Burned,
    SUM(Calories) OVER (
        PARTITION BY Id, SUBSTRING(ActivityHour, 1, 10) 
        ORDER BY ActivityHour
    ) AS Cumulative_Daily_Calories
FROM Hourly_Activity_Metrics
WHERE Id = 1503960366; 



