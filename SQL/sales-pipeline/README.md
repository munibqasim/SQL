Sales Pipeline Query
Located in: /SQL/sales-pipeline/
Overview
SQL query that tracks changes in Salesforce opportunity fields over time, providing a comprehensive view of the sales pipeline. The query handles historical tracking of various opportunity fields including stages, owners, and deal values.
Features

Historical field change tracking
Duplicate record handling
Date-based scaffolding
Support for multiple fields (Stage, Owner, CloseDate, Amount, Forecast ACV)

Technical Details

Database: Snowflake
Schema: REVOPS.SALESFORCE
Main Tables:

OPPORTUNITYFIELDHISTORY1
OPPORTUNITY
USER
ACCOUNT



Key Components

Date Scaffolding

Creates a date range from 2023-01-01 to current date
Focuses on 1st and 15th of each month, plus Sundays


Duplicate Handling

Identifies and manages duplicate field history records
Uses window functions for temporal analysis


Field History Tracking

Tracks changes in:
Stage Name
Owner
Close Date
Amount
Forecast ACV
Total ACV



Usage
sqlCopy-- Execute the query to get pipeline changes
SELECT
    scaffold_date,
    FUNCTION_TYPE,
    OPPORTUNITY_ID,
    OPPORTUNITY_NAME,
    -- other fields
FROM final
ORDER BY OPPORTUNITY_NAME, scaffold_date;
