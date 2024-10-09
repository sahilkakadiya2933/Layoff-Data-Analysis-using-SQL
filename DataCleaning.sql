-- Data Cleaning:

select *
from layoffs;

-- 1. Remove duplicate data
-- 2. Standardize the data
-- 3. Null Values or Blank Values
-- 4. Remove any column which is unnecessory

create table layoff_staging
like layoffs;

select *
from layoff_staging;

insert layoff_staging
select *
from layoffs;

select *
from layoff_staging;

-- 1) remove duplicates

-- using row number function to find row number of every row
 select *,
 Row_NUMBER() over(
 partition by company, industry, total_laid_off, percentage_laid_off, `date` 
 ) as row_num
 from layoff_staging;
 
 -- check the duplicates in table using etc means with function
 -- if there is row number is grater than 2 thenits gonna be duplicate
 
 with duplicate_cte as
 (
  select *,
 Row_NUMBER() over(
 partition by company,location, industry, total_laid_off, 
 percentage_laid_off, `date`, stage, country, 
 funds_raised_millions 
 ) as row_num
 from layoff_staging
 )
select*
from duplicate_cte
where row_num > 1;

-- check the table 
 select *,
 Row_NUMBER() over(
 partition by company, industry, total_laid_off, percentage_laid_off, `date` 
 ) as row_num
 from layoff_staging;

-- create a second table like layoff and in that table add row name is roe_num
CREATE TABLE `layoff_staging2` (
    `company` TEXT,
    `location` TEXT,
    `industry` TEXT,
    `total_laid_off` INT DEFAULT NULL,
    `percentage_laid_off` TEXT,
    `date` TEXT,
    `stage` TEXT,
    `country` TEXT,
    `funds_raised_millions` INT DEFAULT NULL,
    `row_num` INT
)  ENGINE=INNODB DEFAULT CHARSET=UTF8MB4 COLLATE = UTF8MB4_0900_AI_CI;

select *
from layoff_staging2;

insert into layoff_staging2
 select *,
 Row_NUMBER() over(
 partition by company, industry, total_laid_off, percentage_laid_off, `date` 
 ) as row_num
 from layoff_staging;
 
select *
from layoff_staging2;

SET SQL_SAFE_UPDATES = 0;

-- now delete the row where roe_num > 1
DELETE
from layoff_staging2
where row_num > 1;

-- check if somwthing remain or not
select *
from layoff_staging2
where row_num > 1;

select *
from layoff_staging2;

-- now its completing removes duplicate data

-- 2) Standardizing Data

-- remove the space in company name
select company, (trim(company))
from layoff_staging2;

-- update company column
update layoff_staging2
set company = trim(company);

-- update industry like CryptoCurrency to Crypto beausemost of the are Crypto
select distinct industry
from layoff_staging2;

update layoff_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

-- update coutry 
select distinct country
from layoff_staging2
order by 1;

update layoff_staging2
set country = 'United States'
where country like 'United States.';

select distinct country, trim(country)
from layoff_staging2
order by 1;

-- changind format to string (text) into date format
select `date`
from layoff_staging2;

update layoff_staging2
set `date` = str_to_date(`date` ,  '%m/%d/%Y') ;


alter table layoff_staging2
modify column `date` DATE;

select *
from layoff_staging2;


-- 3) null and blank values



update layoff_staging2
set industry = null
where industry = '';

select distinct *
from layoff_staging2
where industry is null
or industry = 'null';
 
select distinct *
from layoff_staging2
where company = 'Airbnb';

select t1.industry, t2.industry
from layoff_staging2 t1
join layoff_staging2 t2
	on t1.company = t2.company
    and t1.location = t2.location
where (t1.industry = 'null')
and t2.industry is not null;



update layoff_staging2 t1
join layoff_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry = 'null'
and t2.industry is not null;

update layoff_staging2
set industry = 'Travel'
where industry = 'null'and company = 'Aribnb';

select *
from layoff_staging2
where total_laid_off is null
and percentage_laid_off is null ;

delete 
from layoff_staging2
where total_laid_off is null
and percentage_laid_off is null ;

select *
from layoff_staging2;

alter table layoff_staging2
drop column row_num;
