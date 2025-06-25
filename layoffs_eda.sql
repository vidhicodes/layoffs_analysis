-- Exploratory Data Analysis on Layoffs

SELECT *
FROM layoffs_staging3;

SELECT MAX(total_laid_off) , MAX(percentage_laid_off)
FROM layoffs_staging3;

SELECT * 
FROM layoffs_staging3
WHERE percentage_laid_off = 1; -- tells that which companies completely went down

SELECT * 
FROM layoffs_staging3
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC; -- tells which company went down with largest to smallest people laid off i.e. in desc order

SELECT * 
FROM layoffs_staging3
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;-- tells which company went down in order of decreasing funds raised

SELECT company,SUM(total_laid_off)
FROM layoffs_staging3
GROUP BY company
ORDER BY SUM(total_laid_off) DESC; -- total layoffs for each company summed across the timeline of dataset -- Amazon topped

SELECT MIN(`date`),MAX(`date`)
FROM layoffs_staging3; -- 2020-03-11 to 2023-03-06 timeline of layoffs

SELECT industry,SUM(total_laid_off)
FROM layoffs_staging3
GROUP BY industry
ORDER BY SUM(total_laid_off) DESC; -- total layoffs for each industry summed across the timeline of dataset -- Consumer topped

SELECT country,SUM(total_laid_off)
FROM layoffs_staging3
GROUP BY country
ORDER BY SUM(total_laid_off) DESC; -- total layoffs for each country summed across the timeline of dataset -- US topped

SELECT YEAR(`date`),SUM(total_laid_off)
FROM layoffs_staging3
GROUP BY YEAR(`date`)
ORDER BY YEAR(`date`) DESC; -- we have only 3 months data of 2023 forecasting makes it seem that 2023 will have maximum layoffs in the whole timeline

SELECT stage,SUM(total_laid_off)
FROM layoffs_staging3
GROUP BY stage
ORDER BY stage DESC; -- summed layoffs by the stage in which companies were

SELECT SUBSTRING(`date`,1,7) AS 'MONTH',SUM(total_laid_off)
FROM layoffs_staging3
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY SUBSTRING(`date`,1,7)
ORDER BY SUBSTRING(`date`,1,7);

-- we are going to create rolling total ordered by months

WITH Rolling_Total_CTE AS
(
SELECT SUBSTRING(`date`,1,7) AS 'MONTH',SUM(total_laid_off) AS month_sum_laid_off
FROM layoffs_staging3
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY SUBSTRING(`date`,1,7)
ORDER BY SUBSTRING(`date`,1,7)
)
SELECT `MONTH`,month_sum_laid_off, SUM(month_sum_laid_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total_CTE; -- cumulative sum of total layoffs progressing by one month at a time

SELECT company,YEAR(`date`),SUM(total_laid_off)
FROM layoffs_staging3
GROUP BY company,YEAR(`date`)
ORDER BY company; -- sum of total layoffs for a particular company in a particular year ordered alphabetically by company name

WITH Ranking_Year(company,layoff_year,total_laid_off) AS
(
SELECT company,YEAR(`date`),SUM(total_laid_off)
FROM layoffs_staging3
GROUP BY company,YEAR(`date`)
ORDER BY 3 DESC -- grouping by company,year and total layoff acc to this grouping
), Company_Year_Rank AS
(
SELECT *, DENSE_RANK() OVER(PARTITION BY layoff_year ORDER BY total_laid_off DESC) AS ranking
FROM Ranking_Year
WHERE layoff_year IS NOT NULL -- take results of previous make new cte to add dense rank on year ordered by no. of layoffs in desc order getting highest first
)
SELECT * 
FROM Company_Year_Rank
WHERE ranking <=5; -- top 5 companies with highest layoffs in each year (therefore desc used)