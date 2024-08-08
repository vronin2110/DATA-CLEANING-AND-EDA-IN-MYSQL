-- PORTFOLIO PROJECT ( SQL DATA CLEANING AND EDA )

-- PART 1 : DATA CLEANING
SELECT * 
FROM world_layoffs.layoffs;

-- Remove Duplicates
-- Standardize the data
-- Check Null values or blank values
-- Remove any column 

-- 1. Removing Duplicates
-- Creating duplicate table to work on 
CREATE TABLE layoffs2
Like layoffs;

Select *
From layoffs2;

Insert layoffs2
Select *
From layoffs;

-- Checking for Duplicate data using ROW_NUMBER
Select Company,location,industry,percentage_laid_off,`date`,stage,country,funds_raised_millions,
ROW_NUMBER() OVER( PARTITION By Company,location,industry,percentage_laid_off,`date`,stage,country,funds_raised_millions ) as ROW_NUM
From layoffs2;

With TEMP_CTE as (
Select Company,location,industry,percentage_laid_off,`date`,stage,country,funds_raised_millions,
ROW_NUMBER() OVER( PARTITION By Company,location,industry,percentage_laid_off,`date`,stage,country,funds_raised_millions ) as ROW_NUM
From layoffs2
) 
Select *
From  TEMP_CTE
Where ROW_NUM > 1
;

-- Create a Temp table to delete ROW_NUM > 2 values
CREATE TABLE `world_layoffs`.`layoffs_temp` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);

INSERT INTO `world_layoffs`.`layoffs_temp`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs2;
        
-- Lets delete now from layoffs_temp
DELETE FROM world_layoffs.layoffs_temp
WHERE row_num >= 2;


-- 2 Standardizing the data
Select *
From layoffs_temp;

-- Clear Blank Spaces
select company, TRIM(company)
From layoffs_temp;

Update layoffs_temp
Set company = trim(company);

-- Filling up blank spaces
select company,industry
from layoffs_temp
where industry = ''
Or industry is null;

Update layoffs_temp
set industry = null
where industry = '';

select company,location,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions
from layoffs_temp
where location = ''
Or total_laid_off = ''
or percentage_laid_off = ''
or `date` = ''
or stage = ''
or country = ''
or funds_raised_millions = '';

-- now we need to populate those nulls if possible

UPDATE layoffs_temp t1
JOIN layoffs_temp t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;


-- ---------------------------------------------------

-- I also noticed the Crypto has multiple different variations. We need to standardize that - let's say all to Crypto
SELECT DISTINCT industry
FROM world_layoffs.layoffs_temp
ORDER BY industry;

UPDATE layoffs_temp
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

SELECT *
FROM world_layoffs.layoffs_temp;

-- everything looks good except apparently we have some "United States" and some "United States." with a period at the end. Let's standardize this.
SELECT DISTINCT country
FROM world_layoffs.layoffs_temp
ORDER BY country;

UPDATE layoffs_temp
SET country = TRIM(TRAILING '.' FROM country);

-- Let's also fix the date columns:
SELECT *
FROM world_layoffs.layoffs_temp;

-- we can use str to date to update this field
UPDATE layoffs_temp
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- now we can convert the data type properly
ALTER TABLE layoffs_temp
MODIFY COLUMN `date` DATE;

-- Delete Useless data we can't really use
DELETE FROM world_layoffs.layoffs_temp
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_temp
DROP COLUMN row_num;


-- Rename the table as cleaning is finished and drop tables that are now useless 
RENAME TABLE layoffs_temp TO Layoffs_Cleaned;
DROP TABLE layoffs2;


-- PART 2 : LETS DO SOME EDA ON THIS CLEANED DATA --
SELECT MAX(total_laid_off)
FROM world_layoffs.Layoffs_Cleaned;
-- Max laid off in a single day was 12k.

SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM world_layoffs.Layoffs_Cleaned
WHERE  percentage_laid_off IS NOT NULL;


-- Which companies had 1 which is basically 100 percent of they company laid off
SELECT *
FROM world_layoffs.Layoffs_Cleaned
WHERE  percentage_laid_off = 1;

select percentage_laid_off, Count(*) as count
FROM world_layoffs.Layoffs_Cleaned
where percentage_laid_off = 1
Group by percentage_laid_off;
-- Shows 116 companies had completely laid off
-- these are mostly startups it looks like who all went out of business during this time
-- if we order by funcs_raised_millions we can see how big some of these companies were
SELECT *
FROM world_layoffs.Layoffs_Cleaned
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;


-- Lets see the top 10 companies that did their layoffs including all their industries 
SELECT company, SUM(total_laid_off)
FROM world_layoffs.Layoffs_Cleaned
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;
-- Shows Amazon,Google,Meta has the extreme most layoffs

-- Lets check companies with most layoffs in last 3 years according to year segregated so we can see who was affected in which year ! 
WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM Layoffs_Cleaned
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;

-- Rolling Total of Layoffs Per Month
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM Layoffs_Cleaned
GROUP BY dates
ORDER BY dates ASC;

WITH DATE_CTE AS 
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM Layoffs_Cleaned
GROUP BY dates
ORDER BY dates ASC
)
SELECT dates, SUM(total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE
ORDER BY dates ASC;