-- Create or replace a view with the specified schema and table names
CREATE OR REPLACE VIEW {sink_schema}.{sink_table} AS 

-- Define a common table expression (CTE) to calculate weekly vote counts
WITH weekly_votes AS (
  SELECT 
    EXTRACT(year FROM CreationDate) AS year,  -- Extract the year from the CreationDate
    strftime('%W', CreationDate :: date) AS week_number,  -- Extract the week number from the CreationDate
    COUNT(*) AS vote_count  -- Count the number of votes for each week
  FROM 
    {source_schema}.{source_table}  -- Source table containing vote data
  GROUP BY 
    1,  -- Group by year
    2   -- Group by week_number
), 

-- Define a CTE to calculate the average vote count per year
overall_avg AS (
  SELECT 
    year,  -- Year from weekly_votes CTE
    AVG(vote_count) AS avg_vote_count  -- Calculate the average vote count for each year
  FROM 
    weekly_votes  -- Use the results from the weekly_votes CTE
  GROUP BY 
    1   -- Group by year
), 

-- Define a CTE to identify outliers based on the average vote count
outliers AS (
  SELECT 
    w.year,  -- Year from weekly_votes CTE
    w.week_number,  -- Week number from weekly_votes CTE
    w.vote_count,  -- Vote count from weekly_votes CTE
    o.avg_vote_count  -- Average vote count from overall_avg CTE
  FROM 
    weekly_votes w  -- Use the weekly_votes CTE
    INNER JOIN overall_avg o ON w.year = o.year  -- Join with overall_avg to get the average vote count
  WHERE 
    ABS(1.0 - (w.vote_count / o.avg_vote_count)) > 0.2  -- Identify outliers where vote count deviates more than 20% from the average
) 

-- Select and order the final results of the outliers
SELECT 
  year,  -- Year of the outlier vote count
  week_number,  -- Week number of the outlier vote count
  vote_count  -- Outlier vote count
FROM 
  outliers  -- Use the outliers CTE
ORDER BY 
  year,  -- Order by year
  week_number;  -- Order by week number
