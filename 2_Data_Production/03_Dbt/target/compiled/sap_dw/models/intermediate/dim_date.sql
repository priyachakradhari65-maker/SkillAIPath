-- dbt Intermediate Model: Date Dimension
-- Complete date dimension for time-based analysis



WITH date_spine AS (
  SELECT EXPLODE(SEQUENCE(
    TO_DATE('2020-01-01'),
    TO_DATE('2030-12-31'),
    INTERVAL 1 DAY
  )) AS Date
),

date_attributes AS (
  SELECT
    Date,
    YEAR(Date) AS Year,
    QUARTER(Date) AS QuarterNum,
    MONTH(Date) AS MonthNumber,
    DAYOFMONTH(Date) AS DayOfMonth,
    DAYOFWEEK(Date) AS DayOfWeekNumber,
    WEEKOFYEAR(Date) AS WeekOfYear,
    DATE_FORMAT(Date, 'MMMM') AS MonthName,
    DATE_FORMAT(Date, 'EEEE') AS DayOfWeek,
    CASE WHEN DAYOFWEEK(Date) IN (1, 7) THEN 1 ELSE 0 END AS IsWeekend
  FROM date_spine
)

SELECT
  ROW_NUMBER() OVER (ORDER BY Date) AS DateID,
  Date,
  Year,
  CONCAT('Q', QuarterNum) AS Quarter,
  MonthName,
  MonthNumber,
  DayOfMonth,
  DayOfWeek,
  DayOfWeekNumber,
  IsWeekend,
  WeekOfYear,
  Year AS FiscalYear,
  CONCAT('FQ', QuarterNum) AS FiscalQuarter
FROM date_attributes