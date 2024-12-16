CREATE OR REPLACE PROCEDURE REVOPS.VITALLY.REFRESH_SHOPFLOOR_METRICS()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN
  -- Use a temporary table to hold the aggregated results
  CREATE OR REPLACE TEMPORARY TABLE temp_shopfloor_metrics AS
  WITH recursive months AS (
      SELECT date_trunc(''MONTH'', ''2020-01-01''::timestamp) AS month_start
      UNION ALL
      SELECT dateadd(month, 1, month_start) AS month_start
      FROM months
      WHERE month_start <= CURRENT_DATE()
  ),
  calendar AS (
      SELECT month_start
      FROM months
      ORDER BY month_start
  ),
  names AS (
      SELECT
          sites.name AS site_name,
          accounts.name AS account_name,
          companies.name AS company_name,
          companies.company_id,
          accounts.account_id,
          sites.site_id
      FROM pm_prod_edw.LATEST_MULTITENANT.sites
      LEFT JOIN pm_prod_edw.LATEST_MULTITENANT.accounts ON accounts.account_id = sites.account_id
      LEFT JOIN pm_prod_edw.LATEST_MULTITENANT.companies ON accounts.company_id = companies.company_id
  ),
  tenancy AS (
      SELECT *
      FROM calendar
      CROSS JOIN names
  ),
  ist_count AS (
      SELECT
          DATE_TRUNC(''month'', created_at) AS created_date_month,
          INBOUND_STOCK_TRANSFERS.site_id,
          COUNT(inbound_stock_transfer_id) AS ist_count
      FROM PM_PROD_EDW.LATEST_MULTITENANT.INBOUND_STOCK_TRANSFERS
      WHERE TRANSFER_STATUS = ''completed''
          AND EXTRACT(year FROM created_date_month) >= 2020
      GROUP BY 1, 2
  ),
  ost_count AS (
      SELECT
          DATE_TRUNC(''month'', created_at) AS created_date_month,
          OUTBOUND_STOCK_TRANSFERS.site_id,
          COUNT(OUTBOUND_STOCK_TRANSFER_ID) AS ost_count
      FROM PM_PROD_EDW.LATEST_MULTITENANT.OUTBOUND_STOCK_TRANSFERS
      WHERE status = 1
          AND EXTRACT(year FROM created_date_month) >= 2020
      GROUP BY 1, 2
  ),
  finished_good_pallets_produced AS (
      SELECT
          DATE_TRUNC(''month'', p.created_at) AS pallets_created_date,
          s.site_id,
          COUNT(DISTINCT pr.pallet_id) AS sum_of_produced_pallets
      FROM pm_prod_edw.LATEST_MULTITENANT.pallets p
      LEFT JOIN pm_prod_edw.LATEST_MULTITENANT.productions pr ON p.pallet_id = pr.pallet_id
      LEFT JOIN pm_prod_edw.LATEST_MULTITENANT.receipt_items ri ON ri.pallet_id = p.pallet_id
      LEFT JOIN pm_prod_edw.LATEST_MULTITENANT.sites s ON p.site_id = s.site_id
      WHERE EXTRACT(year FROM p.created_at) >= 2020
      GROUP BY 1, 2
  ),
  shipments_shipped AS (
      SELECT
          shipments.site_id,
          DATE_TRUNC(''month'', created_at) AS created_date_month,
          COUNT(*) AS shipments_shipped
      FROM PM_PROD_EDW.LATEST_MULTITENANT.shipments
      WHERE shipped = TRUE
          AND EXTRACT(year FROM created_date_month) >= 2020
      GROUP BY shipments.site_id, created_date_month
  ),
  receipts_received AS (
      SELECT
          receipts.site_id,
          DATE_TRUNC(''month'', created_at) AS created_date_month,
          COUNT(*) AS receipts_received
      FROM PM_PROD_EDW.LATEST_MULTITENANT.receipts
      WHERE status = ''completed''
          AND EXTRACT(year FROM created_date_month) >= 2020
      GROUP BY receipts.site_id, created_date_month
  ),
  work_orders AS (
      SELECT
          DATE_TRUNC(''month'', created_at) AS created_date_month,
          WORK_ORDERS.site_id,
          COUNT(work_order_id) AS work_orders_created
      FROM PM_PROD_EDW.LATEST_MULTITENANT.WORK_ORDERS
      WHERE EXTRACT(year FROM created_date_month) >= 2020
      GROUP BY WORK_ORDERS.site_id, created_date_month
  )
  
  SELECT
      tenancy.*,
      COALESCE(finished_good_pallets_produced.sum_of_produced_pallets, 0) AS finished_good_pallets_produced,
      COALESCE(shipments_shipped.shipments_shipped, 0) AS shipments_shipped,
      COALESCE(receipts_received.receipts_received, 0) AS receipts_received,
      COALESCE(work_orders.work_orders_created, 0) AS work_orders_created,
      COALESCE(ist_count.ist_count, 0) AS ist_count,
      COALESCE(ost_count.ost_count, 0) AS ost_count
  FROM tenancy
  LEFT JOIN finished_good_pallets_produced ON tenancy.site_id = finished_good_pallets_produced.site_id
      AND tenancy.month_start = finished_good_pallets_produced.pallets_created_date
  LEFT JOIN shipments_shipped ON tenancy.site_id = shipments_shipped.site_id
      AND tenancy.month_start = shipments_shipped.created_date_month
  LEFT JOIN receipts_received ON tenancy.site_id = receipts_received.site_id
      AND tenancy.month_start = receipts_received.created_date_month
  LEFT JOIN ist_count ON tenancy.site_id = ist_count.site_id
      AND tenancy.month_start = ist_count.created_date_month
  LEFT JOIN ost_count ON tenancy.site_id = ost_count.site_id
      AND tenancy.month_start = ost_count.created_date_month
  LEFT JOIN work_orders ON tenancy.site_id = work_orders.site_id
      AND tenancy.month_start = work_orders.created_date_month;
  
  -- Insert data from temporary table to target table in vitally schema
  INSERT INTO vitally.SHOPFLOOR_METRICS_TABLE
  (MONTH_START, SITE_NAME, ACCOUNT_NAME, COMPANY_NAME, COMPANY_ID, ACCOUNT_ID, SITE_ID, 
   FINISHED_GOOD_PALLETS_PRODUCED, SHIPMENTS_SHIPPED, RECEIPTS_RECEIVED, 
   WORK_ORDERS_CREATED, IST_COUNT, OST_COUNT, UPDATED_AT)
  SELECT 
      MONTH_START, SITE_NAME, ACCOUNT_NAME, COMPANY_NAME, COMPANY_ID, ACCOUNT_ID, SITE_ID, 
      FINISHED_GOOD_PALLETS_PRODUCED, SHIPMENTS_SHIPPED, RECEIPTS_RECEIVED, 
      WORK_ORDERS_CREATED, IST_COUNT, OST_COUNT, 
      CURRENT_TIMESTAMP
  FROM 
      temp_shopfloor_metrics
    WHERE 
    (MONTH_START, SITE_ID) NOT IN 
    (
        SELECT MONTH_START, SITE_ID 
        FROM vitally.SHOPFLOOR_METRICS_TABLE
    )
    OR MONTH_START = DATE_TRUNC("MONTH", CURRENT_DATE());
  
  RETURN ''Data refreshed successfully'';
END;
';