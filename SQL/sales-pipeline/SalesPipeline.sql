with 
recursive datescaffold AS
(
       SELECT '2023-01-01'::date AS scaffold_date
       UNION ALL
       SELECT dateadd(day, 1, scaffold_date)
       FROM   datescaffold
       WHERE  scaffold_date <= CURRENT_DATE() ), scaffolded AS
(
         SELECT   scaffold_date
         FROM     datescaffold
         WHERE    dayofweek(scaffold_date) = 1
         OR       dayofmonth(scaffold_date) = 1
         OR       dayofmonth(scaffold_date) = 15
         ORDER BY scaffold_date),

duplicate_times AS
(
         SELECT   opportunityfieldhistory1.*,
                  Lead(createddate) OVER (partition BY opportunity_id, field ORDER BY createddate ASC )                AS next_created_date,
                  Lag(createddate) OVER (partition BY opportunity_id, field ORDER BY createddate ASC )                 AS last_created_date,
                  Dense_rank() OVER (partition BY opportunity_id, field, createddate::date ORDER BY createddate DESC ) AS dense_rank,
                  createddate::date                                                                                    AS created_date_only
         FROM     revops.salesforce.opportunityfieldhistory1
         ORDER BY field,
                  createddate), duplicate_opportunities AS
(
       SELECT duplicate_times.*
       FROM   duplicate_times
       WHERE  (
                     createddate = next_created_date
              OR     createddate = last_created_date)
       AND    dense_rank =1
       AND    field != 'Owner'), final1 AS
(
         SELECT   opportunity_id,
                  createddate,
                  field,
                  new_value,
                  old_value,
                  Lead(old_value) OVER (partition BY opportunity_id, field,   Date(createddate) ORDER BY createddate ASC ) AS next_old_value,
                  Lead(old_value,2) OVER (partition BY opportunity_id, field, Date(createddate) ORDER BY createddate ASC ) AS next_2_old_value,
                  Lag(old_value) OVER (partition BY opportunity_id, field,    Date(createddate) ORDER BY createddate ASC ) AS last_old_value,
                  Lag(old_value,2) OVER (partition BY opportunity_id, field,  Date(createddate) ORDER BY createddate ASC ) AS last_2_old_value,
                  CASE
                           WHEN (
                                             new_value = next_old_value)
                           OR       (
                                             new_value = last_old_value)
                           OR       (
                                             new_value = next_2_old_value)
                           OR       (
                                             new_value = last_2_old_value) THEN 2
                           ELSE 1
                  END AS rank1
         FROM     duplicate_opportunities
         ORDER BY opportunity_id ,
                  field,
                  createddate,
                  rank1), final2 AS
(
         SELECT   final1.*,
                  Row_number() OVER (partition BY opportunity_id, field, Date(createddate) ORDER BY rank1) AS rn
         FROM     final1 ),      
RankedHistory1 AS (
  SELECT
    ofh.OPPORTUNITY_ID,
    ofh.FIELD,
    opp.CREATED_DATE as Opp_CREATED_DATE,
    Date(ofh.CREATEDDATE) as CREATEDDATE,
    ofh.NEW_VALUE,
    ofh.Old_Value,
    ROW_NUMBER() OVER (PARTITION BY ofh.Opportunity_id, ofh.Field ORDER BY ofh.CREATEDDATE ASC) AS RowNum1,
    ROW_NUMBER() OVER (PARTITION BY ofh.Opportunity_id, ofh.Field, Date(ofh.CREATEDDATE) ORDER BY ofh.CREATEDDATE desc) AS RowNum2
  FROM REVOPS.SALESFORCE.OPPORTUNITYFIELDHISTORY1 as ofh
  LEFT JOIN REVOPS.SALESFORCE.OPPORTUNITY as opp
  ON opp.OPPORTUNITY_ID = ofh.OPPORTUNITY_ID
  where ofh.ID not in ( select ID from duplicate_opportunities)
),
new_ofh as (
  SELECT
    Opportunity_id,
    Field,
    Opp_CREATED_DATE as CREATEDDATE,
    Old_Value as NEW_VALUE,
    Null as Old_Value
  FROM RankedHistory1
  WHERE RowNum1 = 1
  UNION
  SELECT
    Opportunity_id,
    Field,
    CREATEDDATE,
    NEW_VALUE,
    Old_Value
  FROM RankedHistory1
  WHERE RowNum2 = 1
  UNION
  select 
    Opportunity_id,
    Field,
    CREATEDDATE,
    NEW_VALUE,
    Old_Value
    from 
    final2 
    where RN=1
),
         no_stage_ofh as (
select opportunity_id, opportunity_name from revops.salesforce.opportunity where opportunity_id not in (
select opportunity_id from revops.salesforce.OPPORTUNITYFIELDHISTORY1
where field = 'StageName')
),

 no_owner_ofh as (
select opportunity_id, opportunity_name from revops.salesforce.opportunity where opportunity_id not in (
select opportunity_id from revops.salesforce.OPPORTUNITYFIELDHISTORY1
where field = 'Owner')
),
 no_closedate_ofh as (
select opportunity_id, opportunity_name from revops.salesforce.opportunity where opportunity_id not in (
select opportunity_id from revops.salesforce.OPPORTUNITYFIELDHISTORY1
where field = 'CloseDate')
),
no_amount_ofh as (
select opportunity_id, opportunity_name from revops.salesforce.opportunity where opportunity_id not in (
select opportunity_id from revops.salesforce.OPPORTUNITYFIELDHISTORY1
where field = 'Amount')
),
no_forecast_acv_ofh as (
select opportunity_id, opportunity_name from revops.salesforce.opportunity where opportunity_id not in (
select opportunity_id from revops.salesforce.OPPORTUNITYFIELDHISTORY1
where field = 'ForecastACV__c')
),
no_forecast_total_acv_ofh as (
select opportunity_id, opportunity_name from revops.salesforce.opportunity where opportunity_id not in (
select opportunity_id from revops.salesforce.OPPORTUNITYFIELDHISTORY1
where field = 'Forecast_Total_ACV_Backup__c')
),

main as (
          SELECT
    opp.OPPORTUNITY_ID,
    opp.opportunity_name,
    ac.function_type,
    COALESCE(LEAD(ofh.createddate) OVER (PARTITION BY opp.opportunity_name,ofh.field ORDER BY ofh.createddate ASC), DATEADD(DAY, 1, CURRENT_DATE())) AS next_status_change_date,
    CASE
        WHEN no_stage_ofh.opportunity_id IS not NULL THEN opp.stage
        when ofh.field = 'StageName' then ofh.new_value
        else null
    END AS main_stage, 

    CASE
        when no_owner_ofh.opportunity_id IS not NULL then USER.Name
        WHEN ofh.opportunity_id IS not NULL and ofh.field = 'Owner' THEN ofh.new_value
        else null
    END AS Owner,

    CASE
        when no_closedate_ofh.opportunity_id IS not NULL then opp.CLOSE_DATE
        WHEN ofh.opportunity_id IS not NULL and ofh.field = 'CloseDate' THEN ofh.new_value
        else null
    END AS CloseDate,

CASE
        when no_amount_ofh.opportunity_id IS not NULL then opp.AMOUNT
        WHEN ofh.opportunity_id IS not NULL and ofh.field = 'Amount' THEN ofh.new_value
        else null
    END AS Amount, 
    CASE
        when no_forecast_acv_ofh.opportunity_id IS not NULL then opp.FORECAST_ACV
        WHEN ofh.opportunity_id IS not NULL and ofh.field = 'ForecastACV__c' THEN ofh.new_value
        else null
    END AS FORECAST_ACV, 
   CASE
        when no_forecast_total_acv_ofh.opportunity_id IS not NULL then opp.FORECAST_TOTAL_ACV_BACKUP
        WHEN ofh.opportunity_id IS not NULL and ofh.field = 'Forecast_Total_ACV_Backup__c' THEN ofh.new_value
        else NULL
    END AS FORECAST_TOTAL_ACV_BACKUP,

    CASE
        WHEN ofh.opportunity_id IS NULL THEN opp.created_date
        ELSE ofh.createddate
    END AS main_created_date,
    CASE
        WHEN ofh.opportunity_id IS NULL THEN DATEADD(DAY, 1, CURRENT_DATE())
        ELSE next_status_change_date
    END AS main_next_status_change_date,

     CASE
        WHEN ofh.opportunity_id IS not NULL THEN field
        ELSE null
    END AS Field,
    opp.EXISTING_ACV as Existing_ACV,
    ofh.new_value,
    ofh.old_value,
    opp.opportunity_currency,
    opp.created_date AS opportunity_created_date,
    opp.opportunity_record_type,
    opp.deal_type as opportunity_deal_type,
    us.NAME AS opportunity_owner_name,
    us.role AS opportunity_owner_role,
    us.active AS opportunity_owner_active,
    opp.revenue_type AS revenue_type
FROM new_ofh AS ofh
RIGHT JOIN revops.salesforce.opportunity AS opp
    ON opp.opportunity_id = ofh.opportunity_id and new_value is not null
left join REVOPS.SALESFORCE.USER on opp.OPPORTUNITY_OWNER = USER.ID
LEFT JOIN revops.salesforce.USER us
    ON opp.opportunity_owner = us.id
LEFT JOIN revops.salesforce.account ac
    ON opp.account_id = ac.id
LEFT JOIN no_stage_ofh
    ON opp.opportunity_id = no_stage_ofh.opportunity_id
LEFT JOIN no_amount_ofh
    ON opp.opportunity_id = no_amount_ofh.opportunity_id
LEFT JOIN no_forecast_acv_ofh
    ON opp.opportunity_id = no_forecast_acv_ofh.opportunity_id
LEFT JOIN no_forecast_total_acv_ofh
    ON opp.opportunity_id = no_forecast_total_acv_ofh.opportunity_id
LEFT JOIN no_owner_ofh
    ON opp.opportunity_id = no_owner_ofh.opportunity_id
LEFT JOIN no_closedate_ofh
    ON opp.opportunity_id = no_closedate_ofh.opportunity_id
   
),
final as (
   SELECT    *
FROM      scaffolded
LEFT JOIN main
ON        scaffolded.scaffold_date >= main_created_date
AND       scaffolded.scaffold_date <= main_next_status_change_date)
SELECT
    scaffold_date,
    FUNCTION_TYPE,
    OPPORTUNITY_ID,
    OPPORTUNITY_NAME,
    OPPORTUNITY_CURRENCY,
    OPPORTUNITY_CREATED_DATE,
    OPPORTUNITY_RECORD_TYPE,
    OPPORTUNITY_OWNER_NAME,
    OPPORTUNITY_OWNER_ROLE,
    OPPORTUNITY_OWNER_ACTIVE,
    opportunity_deal_type,
    REVENUE_TYPE,
    Existing_ACV,
    max(Owner) as Owner,
    max(CloseDate) as Close_Date,
    max(Amount) as Amount ,
    max(FORECAST_ACV) as FORECAST_ACV,
    max(FORECAST_TOTAL_ACV_BACKUP) as FORECAST_TOTAL_ACV_BACKUP ,
    max(MAIN_STAGE) as main_stage
FROM final
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
order by OPPORTUNITY_NAME ,scaffold_date