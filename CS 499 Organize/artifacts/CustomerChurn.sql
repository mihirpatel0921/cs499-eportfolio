-- churn_customer_view.sql

CREATE OR REPLACE TABLE raw_customer_data (
    customerid STRING,
    gender STRING,
    seniorcitizen BOOLEAN,
    tenure INT,
    monthlycharges FLOAT,
    totalcharges FLOAT,
    techsupport STRING,
    churn STRING
);


CREATE OR REPLACE TABLE churn_features (
    customerid STRING,
    average_monthly_charge FLOAT,
    tenure_bucket STRING,
    tech_support_flag INT,
    churn_risk_score FLOAT
);


GRANT USAGE ON DATABASE CUSTOMERCHURN TO ROLE ACCOUNTADMIN;
GRANT USAGE ON SCHEMA public TO ROLE ACCOUNTADMIN;
GRANT SELECT, INSERT ON TABLE raw_customer_data TO ROLE ACCOUNTADMIN;

INSERT INTO churn_features
SELECT
    customerid,
    
    -- Avoid division by zero
    CASE 
        WHEN tenure = 0 THEN 0
        ELSE totalcharges / tenure
    END AS average_monthly_charge,
    
    -- Bucket tenure
    CASE 
        WHEN tenure <= 12 THEN 'New'
        WHEN tenure BETWEEN 13 AND 36 THEN 'Established'
        WHEN tenure BETWEEN 37 AND 72 THEN 'Loyal'
        ELSE 'Veteran'
    END AS tenure_bucket,
    
    -- Tech support flag
    CASE 
        WHEN LOWER(techsupport) = 'yes' THEN 1
        ELSE 0
    END AS tech_support_flag,
    
    -- Churn risk score (custom logic — can be improved)
    monthlycharges * 
    CASE 
        WHEN LOWER(techsupport) = 'yes' THEN 1
        ELSE 0
    END AS churn_risk_score

FROM raw_customer_data;



CREATE OR REPLACE VIEW churn_analysis_view AS
SELECT
    c.customerid,
    c.gender,
    c.seniorcitizen,
    c.tenure,
    f.tenure_bucket,
    f.average_monthly_charge,
    f.churn_risk_score,
    c.churn
FROM raw_customer_data c
JOIN churn_features f ON c.customerid = f.customerid
WHERE c.churn IS NOT NULL;

