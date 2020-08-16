Strategy
1. Upload data to S3. Can have seperate DAG
2. Run etl with AWS EMR S3
3. Store transformed tables on S3
4. Perform sample query on data

US Immigration Database

## Raw Data
US Cities
  - clean it by seperating data into their individual columns. Can be used to merge with immigration data on cities.
  - Must do.
i94 port to city name
  - Generate dataset from i94 immigration dataset description. 
  - Map i94 port to town and city names. 
  - Can write python script to convert to csv. Can write python script to convert to csv.
  - Must do.
i94 immigration data
  - Must have.
i94 Country Code table (No need)
  - Generate dataset from i94 immigration dataset description. 
  - i94 codes to country name. 
  - Can write python script to convert to csv. 
  - May not be necessary if not planning to add country info.

Other Approach
Read i94 immigration data into Spark
Read original IATA airport code data into Spark
Read original ISO country code data into Spark. This uses ISO-3166-1 and ISO-3166-2 Country and Dependent Territories Lists with UN Regional Codes.
Read and clean i94 airport code into Spark. This is different from airport code and contains same airport codes as immigration data.
Read and clean i94 country codes into Spark. This is different from country code and contains same country codes as immigration data.
Create staging tables
  - Write i94 immigration data into parquet file using Spark
  - Write i94 airport code into parquet file using Spark
  - Write i94 country code into parquet file using Spark
  - Write IATA Airport data to parquet file using Spark
  - Write ISO-3166 Country Code data to parquet file using Spark
Read the parquet files back to Spark and perform data cleaning 
  - Fill na values in immigration data and ISO country code with 0.0 or NA
  - No further cleaning to i94 airport code and i94 country codes
Plan data modelling
  - Staging Tables
    - i94 immigration
    - IATA airport
    - i94 airport code
    - i94 country code
    - ISO country code
  - Fact Tables
    - Immigration. Contains all foreign keys of the top 4 tables
  - Dimension Tables
    - Admissions
    - Countries
    - Airports
    - Time
Create admission tables and write to parquet. Created from i94 immigrations df
  - admnum AS admission_nbr,
  - i94res AS country_code, 
  - i94bir AS age, 
  - i94visa AS visa_code, 
  - visatype AS visa_type, 
  - gender AS person_gender
Created countries dim table by joining i94 country code and ISO country code
  - i94_cit          AS country_code,
  - i94_country_name AS country_name,
  - iso_country_code AS iso_ccode,
  - alpha_2          AS iso_alpha_2,
  - alpha_3          AS iso_alpha_3,
  - iso_3166_2       AS iso_3166_2_code,
  - name             AS iso_country_name,
  - region           AS iso_region,
  - sub_region       AS iso_sub_region,
  - region_code      AS iso_region_code,
  - sub_region_code  AS iso_sub_region_code
Create airport dim table from i94 airport codes
  - i94_port          AS airport_id, 
  - i94_airport_name  AS airport_name,
  - i94_airport_state AS airport_state
Create time dim table from i94 immigrations data
  - arrival_time             AS arrival_ts, 
  - hour(arrival_time)       AS hour, 
  - day(arrival_time)        AS day, 
  - weekofyear(arrival_time) AS week,
  - month(arrival_time)      AS month,
  - year(arrival_time)       AS year,
  - dayofweek(arrival_time)  AS weekday
Create immigrations fact table from i94 immigration, i94 country code, i94 airport code and time tables
  - immigration_id AS immigration_id, 
  - arrival_time   AS arrival_time, foreign key to time table
  - year           AS arrival_year,
  - month          AS arrival_month,
  - i94_port       AS airport_id, foreign key to airport table
  - i94_cit        AS country_code, foreign key for country table
  - admnum         AS admission_nbr, foreign key for admissions table
  - i94mode        AS arrival_mode,
  - departure_date AS departure_date,
  - airline        AS airline,
  - fltno          AS flight_nbr
Perform data quality checks
  - Check that all primary and secondary keys in star schema dimension and fact tables have values. No nulls and empty values
  - Check that all tables have more than 0 rows.