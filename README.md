# Complete documentation for the queries used and the source tables
  KODEKLOUD DATA ANALYSIS
DATA PREPARATION: 
Source databases: 
using the “mixpanel.mp_master_event” from the kodekloud account for getting the initial dataset, and applying the filter [mp_event_name = "Sign Up (server)"] to get the the signup leads
Using the stripe.invoices table for getting the purchase information
We are extracting the data starting from 1-Dec’24 to the present date
CREATING THE TABLES FOR PURCHASE DATA 

Lead Queries




Extracting all the useful columns from the mixtape data related to "Sign Up (server)" event,  by applying the filters:

-Date- after December 2024
Table used: 
“Mixpanel.mp_master_event” from the kk dataset
Extracting columns:
Useremail,
Lead_month,
lead_year,
utm_campaign_first as lead_utm_campaign_first,
utm_campaign_latest as lead_utm_campaign_latest,
time as lead_submission_date,
utm_source_first,
utm_source_latest
query 1
create or replace table kk-data-analytics.TB_Customer_Analytics.Lead_List_2025_v4_1 as

with a as (select useremail,time, extract (month from time) as lead_month, extract (year from time) as lead_year,utm_campaign_first,utm_campaign_latest,utm_source_first, utm_source_latest
from kk-data-analytics.mixpanel.mp_master_event
where mp_event_name = "Sign Up (server)"
and
(
(EXTRACT(YEAR FROM time) >= 2025) OR
(EXTRACT(YEAR FROM time) = 2024 AND EXTRACT(MONTH FROM time) = 12)
)

#and utm_campaign_first is not null
#and utm_campaign_latest is not null
)

select

useremail,lead_month,lead_year,utm_campaign_first as lead_utm_campaign_first,utm_campaign_latest as lead_utm_campaign_latest,time as lead_submission_date,utm_source_first, utm_source_latest
from a
order by time asc
Adding another column ‘id’ by left joining the current table with the stripe table to get the customers id so we can extract the purchases data later using the id
Query 2
create or replace table kk-data-analytics.TB_Customer_Analytics.Lead_List_2025_v4_2 as

select a.*, b.id from kk-data-analytics.TB_Customer_Analytics.Lead_List_2025_v4_1 as a
left join kk-data-analytics.stripe.customers as b
on a.useremail = b.email
Just copying the data into other table
Query 3
create or replace table kk-data-analytics.TB_Customer_Analytics.Lead_List_2025_v4_4 as

select * from kk-data-analytics.TB_Customer_Analytics.Lead_List_2025_v4_2
Identifying if the lead is a new addition or not, if it is then referring it to ‘1’ if not then it is ‘0’
Query 4
create or replace table kk-data-analytics.TB_Customer_Analytics.Lead_List_2025_v4_5 as
-- Step 1: Create a temp table or CTE with earliest submission date from v1.1
WITH earliest_leads_v1 AS (
SELECT
useremail,
MIN(lead_submission_date) AS earliest_submission_date
FROM
`kk-data-analytics.TB_Customer_Analytics.Lead_List_2025_v4_1`
GROUP BY
useremail
)

-- Step 2: Add new column using SELECT with join
SELECT
v4.*,
CASE
WHEN v4.lead_submission_date = elv1.earliest_submission_date THEN 1
ELSE 0
END AS is_new_addition
FROM
`kk-data-analytics.TB_Customer_Analytics.Lead_List_2025_v4_4` v4
LEFT JOIN
earliest_leads_v1 elv1
ON
v4.useremail = elv1.useremail;




Purchases Queries




Creating the tables with the necessary columns from the stripe data by applying the date filter as to get the purchase data from after Dec’24
Query 1
create or replace table kk-data-analytics.TB_Customer_Analytics.Revenue_2025_v1_1 as


SELECT customer_id,received_at,currency,paid,subtotal, tax, total, FROM stripe.invoices
where

(EXTRACT(YEAR FROM received_at) = 2025) OR
(EXTRACT(YEAR FROM received_at) = 2024 AND EXTRACT(MONTH FROM received_at) = 12)
Now selecting only those entries where the payment is completed
Query 2
create or replace table kk-data-analytics.TB_Customer_Analytics.Revenue_2025_v1_2 as

SELECT * from kk-data-analytics.`TB_Customer_Analytics.Revenue_2025_v1_1`
where
paid = true
Currency conversion is happening here, converting all the currency columns into the USD
Query 3
CREATE OR REPLACE TABLE kk-data-analytics.TB_Customer_Analytics.Revenue_2025_v1_3 AS

SELECT
c.*,
(c.subtotal * b.rate*1/100) AS subtotal_USD,
(c.tax * b.rate*1/100) AS tax_USD,
(c.total * b.rate*1/100) AS total_USD

FROM
kk-data-analytics.TB_Customer_Analytics.Revenue_2025_v1_2 AS c
LEFT JOIN
kk-data-analytics.TB_Customer_Analytics.Currency_v1 AS b
ON
c.currency = b.currency;
Copying the necessary columns into a new table
Query 4
create or replace table kk-data-analytics.TB_Customer_Analytics.Revenue_2025_v1_4 as select distinct customer_id,received_at,currency,paid,subtotal,tax,total, subtotal_USD, tax_USD, total_USD from kk-data-analytics.TB_Customer_Analytics.Revenue_2025_v1_3


CREATING THE TABLES FOR COURSE ENROLLMENT DATA

Lead Query
Creating a table for the data where the “lead_utm_campaign_first’ or “lead_utm_campaign_latest” contains the words ‘KK’ because we want to analyse only our campaigns and then finding if a lead is a new addition or not; 1 for yes and 0 for no 
create or replace table kk-data-analytics.TB_Customer_Analytics.Lead_List_2025_v3_1 as
with a as (select * from kk-data-analytics.TB_Customer_Analytics.Lead_List_2025_v4_1),
earliest_leads_v1 AS (
SELECT
useremail, MIN(lead_submission_date) AS earliest_submission_date
FROM
`kk-data-analytics.TB_Customer_Analytics.Lead_List_2025_v4_1`
GROUP BY
useremail
)
-- Step 2: Add new column using SELECT with join
SELECT
a.*,
CASE
WHEN a.lead_submission_date = elv1.earliest_submission_date THEN 1 ELSE 0
END AS is_new_addition
FROM a
LEFT JOIN
earliest_leads_v1 elv1
ON
a.useremail = elv1.useremail;
Course Enrollment Query
Extracting all the columns that we require from the tables : “course_progress_records” and “student_records” starting from the month of January’25
CREATE OR REPLACE TABLE `kk-data-analytics.TB_Customer_Analytics.Course_enrollment_2025_v1_1` AS
SELECT DISTINCT
c.id AS id,
c.uuid AS uuid,
s.email AS email,
c.course_uuid AS course_uuid,
c.student_uuid AS student_uuid,
c.certificate_uuid AS certificate_uuid,
c.status AS status,
c.progress AS progress,
c.started_at AS started_at,
c.completed_at AS completed_at,
c.created_at AS created_at,
c.updated_at AS updated_at,
c.last_lesson_uuid AS last_lesson_uuid,
c.lessons_count AS lessons_count,
c.lessons_completed AS lessons_completed
FROM
`kk-data-analytics.v3_prod_dataset.course_progress_records` AS c
LEFT JOIN
`kk-data-analytics.v3_prod_dataset.student_records` AS s
ON c.student_uuid = s.uuid
WHERE
c.created_at >= '2025-01-01';


2. PoC for Purchases data: (link_for_the_notebook)
      Aim: To get the purchase data for a rolling window 
      Inputs: The leads and the purchase tables, the start and end_data, and the     
                    campaign names you want to get the purchase information
       Leads Table: `kk-data-analytics.TB_Customer_Analytics.Lead_List_2025_v4_5`
       Revenue Table: `kk-data-analytics.TB_Customer_Analytics.Revenue_2025_v1_4`
     (The complete process and all the assumptions taken are mentioned in the notebook)

3. PoC for Course Enrollment Data: (link_for the_notebook)
      Aim: To get the enrollment data for a rolling window 
      Inputs: The leads and the enrollment tables, the start and end_data, and the     
                    campaign names you want to get the enrollment information
      Leads Table: `kk-data-analytics.TB_Customer_Analytics.Lead_List_2025_v3_1`
     Revenue Table:‘kk-data-analytics.TB_Customer_Analytics.Course_enrollment_2025_v1_1`
     (The complete process and all the assumptions taken are mentioned in the notebook)



# Purchase_data_rolling_window_functions
# Course enrollment_rolling_window_functions
