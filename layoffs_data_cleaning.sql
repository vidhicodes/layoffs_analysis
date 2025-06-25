-- Data Cleaning

SELECT *
FROM layoffs;

-- Remove Duplicates
-- Standardize the data
-- Null or Blank values
-- Remove any columns

-- creating staging table to avoid changes in the original raw data

CREATE TABLE layoffs_staging
LIKE layoffs; -- copying schema

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging 
SELECT * FROM layoffs; -- inserting the data

SELECT *
FROM layoffs_staging;


-- REMOVING DUPLICATES
WITH duplicate_cte as
(
SELECT *, ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num >1 ;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` bigint DEFAULT NULL,
  `percentage_laid_off` bigint DEFAULT NULL,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT *, ROW_NUMBER() OVER(PARTITION BY company,location,industry,total_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging ;

SELECT *
FROM layoffs_staging2
WHERE row_num>1; -- cross check the data you are trying to delete oncee

DELETE 
FROM layoffs_staging2
WHERE row_num>1;

SELECT *
FROM layoffs_staging2;

-- STANDARDIZING THE DATA

SELECT company,TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT company
FROM layoffs_staging2; -- checked

SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY location; -- checked

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%'; -- variations of Crypto industry found

UPDATE 
layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'; -- variations standardized

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1; -- variations in United States found

UPDATE
layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States%'; -- FIXED

SELECT `date`,STR_TO_DATE(`date`,'%m/%d/%Y')
FROM layoffs_staging2; -- converting date column to standard format

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2; -- date format fixedd

 -- ALTER TABLE and modify column datatype (perform only on staging tables)
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- FILLING NULL AND BLANK VALUES

SELECT *
FROM layoffs_staging2;

-- Encountered blank/null values in 'industry' can populate using other entries with the same company where 'industry' is not null/blank.
SELECT *
FROM layoffs_staging2
WHERE industry = '' OR industry IS NULL;

-- Using self join to populate other entries of the same company eith industry column value not null/blank

SELECT t1.company,t1.industry,t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company=t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND (t2.industry IS NOT NULL AND t2.industry != '');

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company=t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL  ;

SELECT *
FROM layoffs_staging2; -- null/blank values handled which could have been filled

CREATE TABLE layoffs_staging3
LIKE layoffs_staging2;

SELECT *
FROM layoffs_staging3;

INSERT INTO layoffs_staging3
SELECT * 
FROM layoffs_staging2;

-- REMOVED COLUMNS
ALTER TABLE layoffs_staging3
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging3
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL; -- removing data where total_laid_off and percentage_laid_off are null

DELETE 
FROM layoffs_staging3
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL; -- done for future EDA on the dataset

SELECT *
FROM layoffs_staging3; -- final cleaned data