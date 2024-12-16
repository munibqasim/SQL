Shopfloor Metrics
Located in: /SQL/shopfloor-metrics/
Overview
A suite of Snowflake stored procedures that calculate and maintain various shopfloor operational metrics, including monthly, month-over-month (MOM), and year-over-year (YOY) calculations.
Procedures

REFRESH_SHOPFLOOR_METRICS

Calculates base metrics
Handles inbound/outbound transfers
Tracks finished goods and work orders


REFRESH_SHOPFLOOR_MOM_METRICS

Calculates month-over-month changes
Provides percentage changes for key metrics
Handles historical comparisons


REFRESH_SHOPFLOOR_YOY_METRICS

Calculates year-over-year metrics
Provides rolling 12-month calculations
Handles YOY percentage changes



Key Metrics

Finished Good Pallets Produced
Shipments Shipped
Receipts Received
Work Orders Created
Inbound Stock Transfers (IST)
Outbound Stock Transfers (OST)

Technical Details

Database: Snowflake
Schema: REVOPS.VITALLY
Main Tables:

SHOPFLOOR_METRICS_TABLE
SHOPFLOOR_MOM_METRICS_TABLE
SHOPFLOOR_YOY_METRICS_TABLE



Usage
sqlCopy-- Refresh base metrics
CALL REVOPS.VITALLY.REFRESH_SHOPFLOOR_METRICS();

-- Refresh MOM calculations
CALL REVOPS.VITALLY.REFRESH_SHOPFLOOR_MOM_METRICS();

-- Refresh YOY calculations
CALL REVOPS.VITALLY.REFRESH_SHOPFLOOR_YOY_METRICS();