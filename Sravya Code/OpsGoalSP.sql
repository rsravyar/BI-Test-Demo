CREATE OR REPLACE PROCEDURE OPERATIONALGOALS_DATA_LOAD(INTEGER, INTEGER, CHARACTER VARYING(150), CHARACTER VARYING(150))
RETURNS INTEGER
LANGUAGE NZPLSQL AS
BEGIN_PROC
 DECLARE v_iETL_Date 
ALIAS for $1;

v_iETL_Job_ID 
ALIAS for $2;

v_strSP_Name 
ALIAS for $3;

v_strPackage_Name 
ALIAS for $4;

v_iSeverity integer := 0;

v_iRow_Count integer := 0;

v_strMessage varchar(250);

v_row_nbr SMALLINT;

v_max_row_nbr SMALLINT;

BEGIN v_strMessage := 'Start of ACE_REPORTS_DB..OPERATIONALGOALS_DATA_LOAD';

Call CreateAuditEvent(v_iETL_Date, v_iETL_Job_ID, v_iSeverity, v_iRow_Count, v_strSP_Name, v_strPackage_Name, v_strMessage);

v_strMessage := 'CREATE TEMP TABLE OPERATIONALGOALS_temp_dates';

CREATE TEMP TABLE OPERATIONALGOALS_temp_dates AS SELECT extr_dt.calendar_date AS ExtractDate , extr_dt.calendar_date_id AS extract_date_ID , expire_dt.calendar_date_id AS expiration_date_id , CURRENT_TIMESTAMP AS l_CreateDate , CURRENT_USER AS l_CreateBy , CURRENT_TIMESTAMP AS l_ModifyDate , CURRENT_USER AS l_ModifyBy 
FROM acedw..calendar_date extr_dt 
INNER JOIN acedw..calendar_date expire_dt ON extr_dt.calendar_date_id = v_iETL_Date 
AND expire_dt.calendar_date = DATE_TRUNC('Month', extr_dt.calendar_date -1 - INTERVAL '1 Month') 
DISTRIBUTE ON random;

v_iRow_Count := ROW_COUNT;

CREATE TEMP TABLE CENTERS_DTE AS SELECT DISTINCT  A.CENTER, B.YYYYMMDDZ AS DTE 
FROM ACE_ODS. 
ADMIN.ACEHIERV01 A, ACE_ODS. 
ADMIN.CALENDAR B 
WHERE ((( (SQLDATE 
between to_date(YEAR(add_months(date_trunc('month',current_date-2),-1)) || 
case 
when length(MONTH(add_months(date_trunc('month',current_date-2),-1))-1) = 1 then 0 || MONTH(add_months(date_trunc('month',current_date-2),0)) - 1 
WHEN length(MONTH(add_months(date_trunc('month',current_date-2),-1))-1) = 2 then CAST(MONTH(add_months(date_trunc('month',current_date-2),0) - 1) AS VARCHAR(2)) 
END || '01', 'YYYYMMDD') 
and CASE 
WHEN day(CURRENT_DATE)<=2 THEN current_date-2 ELSE CURRENT_DATE-1 
END ) 
AND (A.CENTER >= 25)) 
AND ((A.CENTER < 5000) 
OR (A.CENTER > 5999))) ) 
ORDER BY A.CENTER, B.YYYYMMDDZ;

v_strMessage := 'CREATE TEMP TABLE NPS' || v_iETL_Date;

CREATE TEMP TABLE NPS AS WITH nps as ( 
SELECT A.CENTER AS CENTER, A.PERIOD , SUM( 
CASE 
WHEN A.ACCOUNT 
IN ('CTUITM') THEN DATA_VALUE ELSE 0 
END ) AS DBACHECKS, SUM( 
CASE 
WHEN A.ACCOUNT 
IN ('Promoters') THEN DATA_VALUE ELSE 0 
END ) AS PROMOTERS_NPS, SUM( 
CASE 
WHEN A.ACCOUNT 
IN ('Detractors') THEN DATA_VALUE ELSE 0 
END ) AS DETRACTORS_NPS, SUM( 
CASE 
WHEN A.ACCOUNT 
IN ('Passive') THEN DATA_VALUE ELSE 0 
END ) AS PASSIVE_NPS 
FROM VENDOR_ESSBASE. 
ADMIN.ES_DT_CHK_TYPE A 
INNER JOIN ( 
SELECT PERIOD, CENTER, ACCOUNT,MAX(EXTRACT_DATE_ID) AS MAXID 
FROM VENDOR_ESSBASE. 
ADMIN.ES_DT_CHK_TYPE 
WHERE ACCOUNT 
IN ('CTUITM','Promoters','Detractors','Passive') 
GROUP BY PERIOD, CENTER, ACCOUNT ) B ON (A.PERIOD = B.PERIOD) 
AND (A.CENTER = B.CENTER) 
AND (A.ACCOUNT = B.ACCOUNT) 
AND (A.EXTRACT_DATE_ID = B.MAXID) 
WHERE TO_DATE(A.PERIOD, 'YYYYMMDD')>= Date_Trunc('MONTH', ADD_MONTHS(CURRENT_DATE-2, -1)) 
AND TO_DATE(a.period, 'YYYYMMDD')<= 
CASE 
WHEN day(CURRENT_DATE)<=2 THEN CURRENT_DATE-2 ELSE CURRENT_DATE-1 
END AND A.CENTER >= 25 
AND A.CENTER 
NOT BETWEEN 5000 
AND 5999 
GROUP BY A.CENTER, A.PERIOD ) , prevmonDATA AS ( 
SELECT A.DTE PERIOD, A.CENTER, B.DBACHECKS,B.PROMOTERS_NPS,B.DETRACTORS_NPS,B.PASSIVE_NPS, C.DBACHECKS DBACHECKS_ly, C.PROMOTERS_NPS PROMOTERS_NPS_ly, C.DETRACTORS_NPS DETRACTORS_NPS_ly, C.PASSIVE_NPS PASSIVE_NPS_ly 
FROM ((CENTERS_DTE A 
LEFT JOIN nps B ON (((A.CENTER = B.CENTER) 
AND (A.DTE = B.PERIOD)))) 
LEFT JOIN OPERATIONALGOALS C ON (((A.CENTER = C.CENTER) 
AND (date(to_date(A.DTE,'YYYYMMDD')- cast('12 month' as interval)) = TO_DATE("VARCHAR"(C.PERIOD), 'YYYYMMDD'::"VARCHAR")) ))) 
where to_date(a.dte,'YYYYMMDD') < date_trunc('MONTH',CURRENT_DATE - 2) ), DATA AS ( 
SELECT A.DTE AS PERIOD, A.CENTER, B.DBACHECKS,B.PROMOTERS_NPS,B.DETRACTORS_NPS,B.PASSIVE_NPS, C.DBACHECKS DBACHECKS_ly,C.PROMOTERS_NPS PROMOTERS_NPS_ly,C.DETRACTORS_NPS DETRACTORS_NPS_ly,C.PASSIVE_NPS PASSIVE_NPS_ly 
from CENTERS_DTE A 
LEFT JOIN NPS B ON A.CENTER = B.CENTER 
AND A.DTE = B.PERIOD 
left outer join OPERATIONALGOALS c on a.center = c.center 
AND ((TO_DATE("VARCHAR"(A.DTE), 'YYYYMMDD'::"VARCHAR") - 364) = TO_DATE("VARCHAR"(C.PERIOD), 'YYYYMMDD'::"VARCHAR")) 
where to_date(a.dte,'YYYYMMDD') >= date_trunc('MONTH',CURRENT_DATE - 2)) 
select * 
from prevmonDATA 
union all SELECT * 
FROM DATA;

v_iRow_Count := ROW_COUNT;

v_strMessage := 'CREATE TEMP TABLE hoursstaff ' || v_iETL_Date;

CREATE TEMP TABLE hoursstaff as WITH hoursstaff as ( 
SELECT CAST(CENTER AS INT) AS CENTER, A.PERIOD , SUM(CAST(HOURSOVERSTAFFED AS FLOAT)) AS HOURSOVERSTAFFED, SUM(CAST(HOURSUNDERSTAFFED AS FLOAT)) AS HOURSUNDERSTAFFED, SUM(CAST(SCHEDULEDHOURS AS FLOAT)) AS SCHEDULEDHOURS, SUM(CAST(TOTALCMOT AS FLOAT)) AS CM_OT, SUM(CAST(TOTALSAOT AS FLOAT)) AS SA_OT, SUM(CAST(PAYDATEDAYS AS FLOAT)) AS PAYDATE_DAYS, SUM(CAST(CORPORATEAUDITSCOREDENOMINATOR AS FLOAT)) AS CORP_SCORE_DEN, SUM(CAST(CORPORATEAUDITSCORENUMERATOR AS FLOAT)) AS CORP_SCORE_NUM, SUM(CAST(CHECKITEMS AS FLOAT)) AS CHECKITEMS 
FROM ACE_REPORTS_DB..ACE_HERP_DAILY_METRICS_DATA_VW A 
WHERE TO_DATE(A.PERIOD, 'YYYYMMDD')>= Date_Trunc('MONTH', ADD_MONTHS(CURRENT_DATE-2, -1)) 
AND TO_DATE(a.period, 'YYYYMMDD')<= 
CASE 
WHEN day(CURRENT_DATE)<=2 THEN CURRENT_DATE-2 ELSE CURRENT_DATE-1 
END AND CENTER >=25 
AND CENTER 
NOT BETWEEN 5000 
AND 5100 
GROUP BY CENTER, A.PERIOD) , prevmonDATA AS ( 
SELECT A.DTE PERIOD, A.CENTER, B.HOURSOVERSTAFFED,B.HOURSUNDERSTAFFED,B.SCHEDULEDHOURS,B.CM_OT,B.SA_OT,B.PAYDATE_DAYS,B.CORP_SCORE_DEN,B.CORP_SCORE_NUM, B.CHECKITEMS, C.HOURSOVERSTAFFED HOURSOVERSTAFFED_ly, C.HOURSUNDERSTAFFED HOURSUNDERSTAFFED_ly, C.SCHEDULEDHOURS SCHEDULEDHOURS_ly, C.CM_OT CM_OT_ly, C.SA_OT SA_OT_LY, C.PAYDATE_DAYS PAYDATE_DAYS_LY, C.CORP_SCORE_DEN CORP_SCORE_DEN_LY, C.CORP_SCORE_NUM CORP_SCORE_NUM_LY, C.CHECKITEMS CHECKITEMS_LY 
FROM ((CENTERS_DTE A 
LEFT JOIN hoursstaff B ON (((A.CENTER = B.CENTER) 
AND (A.DTE = B.PERIOD)))) 
LEFT JOIN ACE_REPORTS_DB..OPERATIONALGOALS C ON (((A.CENTER = C.CENTER) 
AND (date(to_date(A.DTE,'YYYYMMDD')- cast('12 month' as interval)) = TO_DATE("VARCHAR"(C.PERIOD), 'YYYYMMDD'::"VARCHAR")) ))) 
where to_date(a.dte,'YYYYMMDD') < date_trunc('MONTH',CURRENT_DATE - 2) ) , DATA AS ( 
SELECT A.DTE AS PERIOD, A.CENTER, B.HOURSOVERSTAFFED,B.HOURSUNDERSTAFFED,B.SCHEDULEDHOURS,B.CM_OT,B.SA_OT,B.PAYDATE_DAYS,B.CORP_SCORE_DEN,B.CORP_SCORE_NUM,B.CHECKITEMS, C.HOURSOVERSTAFFED HOURSOVERSTAFFED_ly,C.HOURSUNDERSTAFFED HOURSUNDERSTAFFED_ly,C.SCHEDULEDHOURS SCHEDULEDHOURS_ly,C.CM_OT CM_OT_ly,C.SA_OT SA_OT_LY,C.PAYDATE_DAYS PAYDATE_DAYS_LY,C.CORP_SCORE_DEN CORP_SCORE_DEN_LY, C.CORP_SCORE_NUM CORP_SCORE_NUM_LY, C.CHECKITEMS CHECKITEMS_LY 
from CENTERS_DTE A 
LEFT JOIN hoursstaff B ON A.CENTER = B.CENTER 
AND A.DTE = B.PERIOD 
left outer join ACE_REPORTS_DB..OPERATIONALGOALS c on a.center = c.center 
AND ((TO_DATE("VARCHAR"(A.DTE), 'YYYYMMDD'::"VARCHAR") - 364) = TO_DATE("VARCHAR"(C.PERIOD), 'YYYYMMDD'::"VARCHAR")) 
where to_date(a.dte,'YYYYMMDD') >= date_trunc('MONTH',CURRENT_DATE - 2)) 
select * 
from prevmonDATA 
union all SELECT * 
FROM DATA;

v_iRow_Count := ROW_COUNT;

v_strMessage := 'CREATE TEMP TABLE DDS ' || v_iETL_Date;

CREATE TEMP TABLE DDS AS WITH DD AS ( 
SELECT A.CENTER, A.PERIOD, SUM(A.DATA_VALUE) AS FIRSTTIME_DDS 
FROM (VENDOR_ESSBASE. 
ADMIN.ES_DT_NETDDP A 
JOIN ( 
SELECT A.CENTER, A.PERIOD, MAX(A.EXTRACT_DATE_ID) AS MAXID 
FROM VENDOR_ESSBASE. 
ADMIN.ES_DT_NETDDP A 
WHERE (A."ACCOUNT" IN (('Netspend First Time Direct Deposit Items - OTH'::"VARCHAR")::VARCHAR(50), ('Netspend First Time Direct Deposit Items - BNF'::"VARCHAR")::VARCHAR(50), ('Netspend First Time Direct Deposit Items - TAX'::"VARCHAR")::VARCHAR(50))) 
GROUP BY A.CENTER, A.PERIOD) B ON ((((A.CENTER = B.CENTER) 
AND (A.PERIOD = B.PERIOD)) 
AND (A.EXTRACT_DATE_ID = B.MAXID)))) 
WHERE ((A."ACCOUNT" IN (('Netspend First Time Direct Deposit Items - OTH'::"VARCHAR")::VARCHAR(50), ('Netspend First Time Direct Deposit Items - BNF'::"VARCHAR")::VARCHAR(50), ('Netspend First Time Direct Deposit Items - TAX'::"VARCHAR")::VARCHAR(50))) 
AND TO_DATE(A.PERIOD, 'YYYYMMDD')>= Date_Trunc('MONTH', ADD_MONTHS(CURRENT_DATE-2, -1)) 
AND TO_DATE(a.period, 'YYYYMMDD')<= 
CASE 
WHEN day(CURRENT_DATE)<=2 THEN CURRENT_DATE-2 ELSE CURRENT_DATE-1 
END ) 
GROUP BY A.CENTER, A.PERIOD ), prevmonDATA AS ( 
SELECT A.DTE PERIOD, A.CENTER, B.FIRSTTIME_DDS FIRSTTIME_DDS_CY, C.FIRSTTIME_DDS_CY FIRSTTIME_DDS_LY 
FROM ((CENTERS_DTE A 
LEFT JOIN DD B ON (((A.CENTER = B.CENTER) 
AND (A.DTE = B.PERIOD)))) 
LEFT JOIN ACE_REPORTS_DB..OPERATIONALGOALS C ON (((A.CENTER = C.CENTER) 
AND (date(to_date(A.DTE,'YYYYMMDD')- cast('12 month' as interval)) = TO_DATE("VARCHAR"(C.PERIOD), 'YYYYMMDD'::"VARCHAR")) ))) 
where to_date(a.dte,'YYYYMMDD') < date_trunc('MONTH',CURRENT_DATE - 2) ) , DATA AS ( 
SELECT A.DTE AS PERIOD, A.CENTER, B.FIRSTTIME_DDS FIRSTTIME_DDS_CY, C.FIRSTTIME_DDS_CY AS FIRSTTIME_DDS_LY 
from CENTERS_DTE A 
LEFT JOIN DD B ON A.CENTER = B.CENTER 
AND A.DTE = B.PERIOD 
left outer join ACE_REPORTS_DB..OPERATIONALGOALS c on a.center = c.center 
AND ((TO_DATE("VARCHAR"(A.DTE), 'YYYYMMDD'::"VARCHAR") - 364) = TO_DATE("VARCHAR"(C.PERIOD), 'YYYYMMDD'::"VARCHAR")) 
where to_date(a.dte,'YYYYMMDD') >= date_trunc('MONTH',CURRENT_DATE - 2) ) 
select * 
from prevmonDATA 
union all select * 
from data 
WHERE to_date(PERIOD,'YYYYMMDD') < CURRENT_DATE -1;

v_iRow_Count := ROW_COUNT;

v_strMessage := 'create temp table goal ' || v_iETL_Date;

create temp table goal as SELECT CAST(A.CENTER AS INT) CENTER, CAST(A.PERIOD AS INT) PERIOD, SUM( 
CASE 
WHEN to_date(a.PERIOD,'YYYYMMDD') < 
CASE 
WHEN day(CURRENT_DATE)<=2 THEN CURRENT_DATE-2 ELSE CURRENT_DATE-1 
END AND A.ACCOUNT 
IN ('Netspend First Time Direct Deposit Items BUD') THEN GOAL_VALUE ELSE 0 
END) AS FTDD_GOAL_MTD, SUM( 
CASE 
WHEN A.ACCOUNT 
IN ('FAP Personal Card Sale Items') THEN GOAL_VALUE ELSE 0 
END) AS CARDSALES_GOAL_MTD, SUM( 
CASE 
WHEN A.ACCOUNT 
IN ('#Days') THEN GOAL_VALUE ELSE 0 
END) AS days_MTD, sum( 
case 
when A.ACCOUNT 
IN ('Net Payday Buy Backs BUD','Net Installment Buy Backs BUD','Net Title Buy Backs BUD') THEN GOAL_VALUE ELSE 0 
END) BBK_ITEMS_GOAL_MTD ,sum( 
case 
when A.ACCOUNT 
IN ('Net Installment Buy Back Opportunities BUD','Net Payday Buy Back Opportunities BUD','Net Title Buy Back Opportunities BUD') THEN GOAL_VALUE ELSE 0 
END) BBK_ITEMS_OPPS_GOAL_MTD 
FROM VENDOR_ESSBASE. 
ADMIN.GOAL_TRENDS_CUBE A 
INNER JOIN ( 
SELECT PERIOD, CENTER, ACCOUNT, MAX(EXTRACT_DATE_ID) AS MAXID 
FROM VENDOR_ESSBASE. 
ADMIN.GOAL_TRENDS_CUBE 
GROUP BY PERIOD, CENTER, ACCOUNT )B ON (A.CENTER = B.CENTER) 
AND (A.PERIOD = B.PERIOD) 
AND (A.ACCOUNT = B.ACCOUNT) 
AND (A.EXTRACT_DATE_ID = B.MAXID) 
join OPERATIONALGOALS_temp_dates d ON A.PERIOD >= d.EXPIRATION_DATE_ID 
AND A.PERIOD <= 
CASE 
WHEN day(CURRENT_DATE)<=2 THEN d.EXTRACT_DATE_ID -1 ELSE d.EXTRACT_DATE_ID 
END WHERE A.CENTER 
NOT LIKE '%Centers' AND A.CENTER 
NOT LIKE 'D%' GROUP BY A.CENTER, A.PERIOD;

v_iRow_Count := ROW_COUNT;

v_strMessage := 'create temp table dda' || v_iETL_Date;

create temp table dda as WITH DDA AS ( 
SELECT A.CENTER, A.PERIOD, SUM( 
CASE 
WHEN (A."ACCOUNT" = 'DDA Upgrade Card Sale Items'::"VARCHAR") THEN A.DATA_VALUE ELSE '0'::"NUMERIC" END) AS DDAUPGRADES, SUM( 
CASE 
WHEN (A."ACCOUNT" = 'DDA New Card Sale Items'::"VARCHAR") THEN A.DATA_VALUE ELSE '0'::"NUMERIC" END) AS DDANEWCARD 
FROM (VENDOR_ESSBASE. 
ADMIN.ES_DT_DDA_DATA A 
JOIN ( 
SELECT A.CENTER, A.PERIOD, MAX(A.EXTRACT_DATE_ID) AS MAXID 
FROM VENDOR_ESSBASE. 
ADMIN.ES_DT_DDA_DATA A 
WHERE (A."ACCOUNT" IN (('DDA New Card Sale Items'::"VARCHAR")::VARCHAR(100) , ('DDA Upgrade Card Sale Items'::"VARCHAR")::VARCHAR(100))) 
AND TO_DATE(A.PERIOD, 'YYYYMMDD')>= Date_Trunc('MONTH', ADD_MONTHS(CURRENT_DATE-2, -1)) 
AND TO_DATE(a.period, 'YYYYMMDD')<= 
CASE 
WHEN day(CURRENT_DATE)<=2 THEN CURRENT_DATE-2 ELSE CURRENT_DATE-1 
END GROUP BY A.CENTER, A.PERIOD) B ON ((((A.CENTER = B.CENTER) 
AND (A.PERIOD = B.PERIOD)) 
AND (A.EXTRACT_DATE_ID = B.MAXID)))) 
WHERE ((A."ACCOUNT" IN (('DDA New Card Sale Items'::"VARCHAR")::VARCHAR(100), ('DDA Upgrade Card Sale Items'::"VARCHAR")::VARCHAR(100))) 
AND TO_DATE(A.PERIOD, 'YYYYMMDD')>= Date_Trunc('MONTH', ADD_MONTHS(CURRENT_DATE-2, -1)) 
AND TO_DATE(a.period, 'YYYYMMDD')<= 
CASE 
WHEN day(CURRENT_DATE)<=2 THEN CURRENT_DATE-2 ELSE CURRENT_DATE-1END ) 
GROUP BY A.CENTER, A.PERIOD ), PREVMONDATA AS ( 
SELECT A.DTE PERIOD, A.CENTER, B.DDAUPGRADES, C.DDAUPGRADES DDAUPGRADES_LY, B.DDANEWCARD, C.DDANEWCARD DDANEWCARD_LY 
FROM ((CENTERS_DTE A 
LEFT JOIN DDA B ON (((A.CENTER = B.CENTER) 
AND (A.DTE = B.PERIOD)))) 
LEFT JOIN ACE_REPORTS_DB..OPERATIONALGOALS C ON (((A.CENTER = C.CENTER) 
AND (date(to_date(A.DTE,'YYYYMMDD')- cast('12 month' as interval)) = TO_DATE("VARCHAR"(C.PERIOD), 'YYYYMMDD'::"VARCHAR")) ))) 
where to_date(a.dte,'YYYYMMDD') < date_trunc('MONTH',CURRENT_DATE - 2)) , DATA AS ( 
SELECT A.DTE AS PERIOD, A.CENTER, B.DDAUPGRADES, C.DDAUPGRADES AS DDAUPGRADES_LY, B.DDANEWCARD, C.DDANEWCARD AS DDANEWCARD_LY 
from CENTERS_DTE A 
LEFT JOIN DDA B ON A.CENTER = B.CENTER 
AND A.DTE = B.PERIOD 
left outer join ACE_REPORTS_DB..OPERATIONALGOALS c on a.center = c.center 
AND ((TO_DATE("VARCHAR"(A.DTE), 'YYYYMMDD'::"VARCHAR") - 364) = TO_DATE("VARCHAR"(C.PERIOD), 'YYYYMMDD'::"VARCHAR")) 
where to_date(a.dte,'YYYYMMDD') >= date_trunc('MONTH',CURRENT_DATE - 2)) 
SELECT DISTINCT A.CENTER, a.period, coalesce(DDAUPGRADES,0) DDAUPGRADES, coalesce(DDAUPGRADES_ly,0) DDAUPGRADES_ly, coalesce(DDANEWCARD ,0) DDANEWCARD , coalesce(DDANEWCARD_ly,0) DDANEWCARD_ly 
FROM PREVMONdata a 
UNION ALL SELECT DISTINCT A.CENTER, a.period, coalesce(DDAUPGRADES,0) DDAUPGRADES, coalesce(DDAUPGRADES_ly,0) DDAUPGRADES_ly, coalesce(DDANEWCARD ,0) DDANEWCARD , coalesce(DDANEWCARD_ly,0) DDANEWCARD_ly 
FROM data a;

v_iRow_Count := ROW_COUNT;

v_strMessage := 'create temp table PREPAID ' || v_iETL_Date;

create temp table PREPAID as WITH PREPAID AS ( 
SELECT A.CENTER, A.PERIOD, SUM(A.DATA_VALUE) AS PREPAIDCARDSALEITEMS 
FROM (VENDOR_ESSBASE. 
ADMIN.ES_DT_PREPAID_DATA A 
JOIN ( 
SELECT A.CENTER, A.PERIOD, MAX(A.EXTRACT_DATE_ID) AS MAXID 
FROM VENDOR_ESSBASE. 
ADMIN.ES_DT_PREPAID_DATA A 
WHERE (A."ACCOUNT" IN (('Business Card Sale Items'::"VARCHAR")::VARCHAR(100) , ('FAP Personal Card Sale Items'::"VARCHAR")::VARCHAR(100) , ('PG Personal Card Sale Items'::"VARCHAR")::VARCHAR(100))) 
AND TO_DATE(A.PERIOD, 'YYYYMMDD')>= Date_Trunc('MONTH', ADD_MONTHS(CURRENT_DATE-2, -1)) 
AND TO_DATE(a.period, 'YYYYMMDD')<= 
CASE 
WHEN day(CURRENT_DATE)<=2 THEN CURRENT_DATE-2 ELSE CURRENT_DATE-1 
END GROUP BY A.CENTER, A.PERIOD) B ON ((((A.CENTER = B.CENTER) 
AND (A.PERIOD = B.PERIOD)) 
AND (A.EXTRACT_DATE_ID = B.MAXID)))) 
WHERE ((A."ACCOUNT" IN (('Business Card Sale Items'::"VARCHAR")::VARCHAR(100), ('FAP Personal Card Sale Items'::"VARCHAR")::VARCHAR(100), ('PG Personal Card Sale Items'::"VARCHAR")::VARCHAR(100))) 
AND TO_DATE(A.PERIOD, 'YYYYMMDD')>= Date_Trunc('MONTH', ADD_MONTHS(CURRENT_DATE-2, -1)) 
AND TO_DATE(a.period, 'YYYYMMDD')<= 
CASE 
WHEN day(CURRENT_DATE)<=2 THEN CURRENT_DATE-2 ELSE CURRENT_DATE-1 
END) 
GROUP BY A.CENTER, A.PERIOD) , prevmonDATA AS ( 
SELECT A.DTE PERIOD, A.CENTER, B.PREPAIDCARDSALEITEMS, C.PREPAIDCARDSALEITEMS PREPAIDCARDSALEITEMS_LY 
FROM ((CENTERS_DTE A 
LEFT JOIN PREPAID B ON (((A.CENTER = B.CENTER) 
AND (A.DTE = B.PERIOD)))) 
LEFT JOIN ACE_REPORTS_DB..OPERATIONALGOALS C ON (((A.CENTER = C.CENTER) 
AND (date(to_date(A.DTE,'YYYYMMDD')- cast('12 month' as interval)) = TO_DATE("VARCHAR"(C.PERIOD), 'YYYYMMDD'::"VARCHAR")) ))) 
where to_date(a.dte,'YYYYMMDD') < date_trunc('MONTH',CURRENT_DATE - 2)) , DATA AS ( 
SELECT A.DTE AS PERIOD, A.CENTER, B.PREPAIDCARDSALEITEMS, C.PREPAIDCARDSALEITEMS AS PREPAIDCARDSALEITEMS_LY 
from CENTERS_DTE A 
LEFT JOIN PREPAID B ON A.CENTER = B.CENTER 
AND A.DTE = B.PERIOD 
left outer join ACE_REPORTS_DB..OPERATIONALGOALS c on a.center = c.center 
AND ((TO_DATE("VARCHAR"(A.DTE), 'YYYYMMDD'::"VARCHAR") - 364) = TO_DATE("VARCHAR"(C.PERIOD), 'YYYYMMDD'::"VARCHAR")) 
where to_date(a.dte,'YYYYMMDD') >= date_trunc('MONTH',CURRENT_DATE - 2) ) 
SELECT DISTINCT A.CENTER, A.PERIOD,COALESCE(PREPAIDCARDSALEITEMS,0) PREPAIDCARDSALEITEMS ,COALESCE(PREPAIDCARDSALEITEMS_LY,0) PREPAIDCARDSALEITEMS_LY 
FROM prevmonDATA A 
union all SELECT DISTINCT A.CENTER, A.PERIOD,COALESCE(PREPAIDCARDSALEITEMS,0) PREPAIDCARDSALEITEMS,COALESCE(PREPAIDCARDSALEITEMS_LY,0) PREPAIDCARDSALEITEMS_LY 
FROM DATA A;

v_iRow_Count := ROW_COUNT;

v_strMessage := 'CREATE TEMP TABLE NC4S ' || v_iETL_Date;

CREATE TEMP TABLE NC4S AS WITH NC4S AS ( 
SELECT A.CENTER, A.PERIOD, SUM( 
CASE 
WHEN A.ACCOUNT 
IN ('New Check Cashing Customer Debit Card Sales') THEN DATA_VALUE ELSE 0 
END) AS NC4S_SALES_CY, SUM( 
CASE 
WHEN A.ACCOUNT 
IN ('New Check Cashing Opportunities') THEN DATA_VALUE ELSE 0 
END) AS NC4S_OPPS_CY 
FROM VENDOR_ESSBASE. 
ADMIN.ES_DT_DDA_DATA A 
INNER JOIN ( 
SELECT PERIOD, CENTER, ACCOUNT, MAX(EXTRACT_DATE_ID) AS MAXID 
FROM VENDOR_ESSBASE. 
ADMIN.ES_DT_DDA_DATA 
GROUP BY PERIOD, CENTER, ACCOUNT )B ON (A.CENTER = B.CENTER) 
AND (A.PERIOD = B.PERIOD) 
AND (A.ACCOUNT = B.ACCOUNT) 
AND (A.EXTRACT_DATE_ID = B.MAXID) 
where TO_DATE(A.PERIOD, 'YYYYMMDD')>= Date_Trunc('MONTH', ADD_MONTHS(CURRENT_DATE-2, -1)) 
AND TO_DATE(a.period, 'YYYYMMDD')<= 
CASE 
WHEN day(CURRENT_DATE)<=2 THEN CURRENT_DATE-2 ELSE CURRENT_DATE-1 
END AND a.CENTER >=25 
AND a.CENTER 
NOT BETWEEN 5000 
AND 5100 
GROUP BY A.CENTER, A.PERIOD) , prevmonDATA AS ( 
SELECT A.dte PERIOD, A.CENTER, b.NC4S_SALES_CY,b.NC4S_OPPS_CY, c.NC4S_SALES_CY NC4S_SALES_LY, c.NC4S_OPPS_CY NC4S_OPPS_LY 
FROM ((CENTERS_DTE A 
LEFT JOIN NC4S B ON (((A.CENTER = B.CENTER) 
AND (A.DTE = B.PERIOD)))) 
LEFT JOIN ACE_REPORTS_DB..OPERATIONALGOALS C ON (((A.CENTER = C.CENTER) 
AND (date(to_date(A.DTE,'YYYYMMDD')- cast('12 month' as interval)) = TO_DATE("VARCHAR"(C.PERIOD), 'YYYYMMDD'::"VARCHAR")) ))) 
where to_date(a.dte,'YYYYMMDD') < date_trunc('MONTH',CURRENT_DATE - 2) ) , DATA AS ( 
SELECT A.DTE AS PERIOD, A.CENTER, B.NC4S_SALES_CY,B.NC4S_OPPS_CY,C.NC4S_SALES_CY NC4S_SALES_LY,C.NC4S_OPPS_CY NC4S_OPPS_LY 
from CENTERS_DTE A 
LEFT JOIN nc4s B ON A.CENTER = B.CENTER 
AND A.DTE = B.PERIOD 
left outer join ACE_REPORTS_DB..OPERATIONALGOALS c on a.center = c.center 
AND ((TO_DATE("VARCHAR"(A.DTE), 'YYYYMMDD'::"VARCHAR") - 364) = TO_DATE("VARCHAR"(C.PERIOD), 'YYYYMMDD'::"VARCHAR")) 
where to_date(a.dte,'YYYYMMDD') >= date_trunc('MONTH',CURRENT_DATE - 2) ) 
select * 
from prevmonDATA 
union all select * 
from data;

v_iRow_Count := ROW_COUNT;

v_strMessage := 'CREATE TEMP TABLE BBK ' || v_iETL_Date;

CREATE TEMP TABLE BBK AS WITH bbk as ( 
SELECT CENTER,PERIOD, COALESCE(STOREFRONT_PAYDAY_BUY_BACKS,0) + COALESCE(STOREFRONT_INSTALLMENT_BUY_BACKS,0) + COALESCE(STOREFRONT_TITLE_BUY_BACKS,0) AS BBK_ITEMS_CY , COALESCE(STOREFRONT_PAYDAY_BUY_BACK_OPPORTUNITIES,0) + COALESCE(STOREFRONT_INSTALLMENT_BUY_BACK_OPPORTUNITIES, 0) + COALESCE(STOREFRONT_TITLE_BUY_BACK_OPPORTUNITIES ,0) AS BBK_OPP_ITEMS_CY, CASH_SHORTS_PSD,INTERNAL_AUDIT 
FROM ( 
SELECT A.CENTER, A.PERIOD, SUM(A.STOREFRONT_PAYDAY_BUY_BACKS) AS STOREFRONT_PAYDAY_BUY_BACKS, SUM(A.STOREFRONT_PAYDAY_BUY_BACK_OPPORTUNITIES) AS STOREFRONT_PAYDAY_BUY_BACK_OPPORTUNITIES, SUM(A.STOREFRONT_INSTALLMENT_BUY_BACKS) AS STOREFRONT_INSTALLMENT_BUY_BACKS, SUM(A.STOREFRONT_INSTALLMENT_BUY_BACK_OPPORTUNITIES) AS STOREFRONT_INSTALLMENT_BUY_BACK_OPPORTUNITIES, SUM(A.STOREFRONT_TITLE_BUY_BACKS) AS STOREFRONT_TITLE_BUY_BACKS, SUM(A.STOREFRONT_TITLE_BUY_BACK_OPPORTUNITIES) AS STOREFRONT_TITLE_BUY_BACK_OPPORTUNITIES, sum(CASH_SHORTS_PSD) CASH_SHORTS_PSD,sum(INTERNAL_AUDIT) INTERNAL_AUDIT 
FROM ACE_REPORTS_DB. 
ADMIN.ACE_HERP_DM_VISIT_DATA_DAILY_VW A 
where TO_DATE(A.PERIOD, 'YYYYMMDD')>= Date_Trunc('MONTH', ADD_MONTHS(CURRENT_DATE-2, -1)) 
AND TO_DATE(a.period, 'YYYYMMDD')<= 
CASE 
WHEN day(CURRENT_DATE)<=2 THEN CURRENT_DATE-2 ELSE CURRENT_DATE-1 
END AND a.CENTER >=25 
AND a.CENTER 
NOT BETWEEN 5000 
AND 5100 
GROUP BY A.CENTER, A.PERIOD ) A) , prevmonDATA as ( 
select a.CENTER, a.dte PERIOD, b.BBK_ITEMS_CY, b.BBK_OPP_ITEMS_CY, b.CASH_SHORTS_PSD, b.INTERNAL_AUDIT, c.BBK_ITEMS_CY BBK_ITEMS_LY , c.BBK_OPP_ITEMS_CY BBK_OPP_ITEMS_LY, c.CASH_SHORTS_PSD CASH_SHORTS_PSD_LY, c.INTERNAL_AUDIT INTERNAL_AUDIT_LY 
FROM ((CENTERS_DTE A 
LEFT JOIN bbk B ON (((A.CENTER = B.CENTER) 
AND (A.DTE = B.PERIOD)))) 
LEFT JOIN ACE_REPORTS_DB..OPERATIONALGOALS C ON (((A.CENTER = C.CENTER) 
AND (date(to_date(A.DTE,'YYYYMMDD')- cast('12 month' as interval)) = TO_DATE("VARCHAR"(C.PERIOD), 'YYYYMMDD'::"VARCHAR")) ))) 
where to_date(a.dte,'YYYYMMDD') < date_trunc('MONTH',CURRENT_DATE - 2) ), DATA AS ( 
select a.CENTER, a.dte PERIOD, b.BBK_ITEMS_CY, b.BBK_OPP_ITEMS_CY, b.CASH_SHORTS_PSD, b.INTERNAL_AUDIT, c.BBK_ITEMS_CY BBK_ITEMS_LY , c.BBK_OPP_ITEMS_CY BBK_OPP_ITEMS_LY, c.CASH_SHORTS_PSD CASH_SHORTS_PSD_LY, c.INTERNAL_AUDIT INTERNAL_AUDIT_LY 
from CENTERS_DTE A 
LEFT JOIN bbk B ON A.CENTER = B.CENTER 
AND A.DTE = B.PERIOD 
left outer join ACE_REPORTS_DB..OPERATIONALGOALS c on a.center = c.center 
AND ((TO_DATE("VARCHAR"(A.DTE), 'YYYYMMDD'::"VARCHAR") - 364) = TO_DATE("VARCHAR"(C.PERIOD), 'YYYYMMDD'::"VARCHAR")) 
where to_date(a.dte,'YYYYMMDD') >= date_trunc('MONTH',CURRENT_DATE - 2)) 
SELECT * 
FROM PrevmonDATA 
UNION ALL select * 
from data;

v_iRow_Count := ROW_COUNT;

v_strMessage := 'CREATE TEMP TABLE CASHSHORTS ' || v_iETL_Date;

CREATE TEMP TABLE CASHSHORTS AS WITH cash as ( 
SELECT A.CENTER ,A.PERIOD,SUM(DATA_VALUE) AS AUDCASHSHORTS 
FROM VENDOR_ESSBASE. 
ADMIN.ES_DT_AUDCASH A 
INNER JOIN ( 
SELECT CENTER,substring(period,1,6) period, ACCOUNT, MAX(EXTRACT_DATE_ID) AS MAXID 
FROM VENDOR_ESSBASE. 
ADMIN.ES_DT_AUDCASH 
GROUP BY CENTER, ACCOUNT,substring(period,1,6) )B ON (A.CENTER = B.CENTER) 
AND (substring(a.period,1,6) = B.PERIOD) 
AND (A.ACCOUNT = B.ACCOUNT) 
AND (A.EXTRACT_DATE_ID = B.MAXID) 
where TO_DATE(A.PERIOD, 'YYYYMMDD')>= Date_Trunc('MONTH', ADD_MONTHS(CURRENT_DATE-2, -1)) 
AND TO_DATE(a.period, 'YYYYMMDD')<= 
CASE 
WHEN day(CURRENT_DATE)<=2 THEN CURRENT_DATE-2 ELSE CURRENT_DATE-1 
END AND a.CENTER >=25 
AND a.CENTER 
NOT BETWEEN 5000 
AND 5100 
GROUP BY A.CENTER ,A.PERIOD) , PrevmonDATA AS ( 
SELECT A.dte PERIOD, A.CENTER, b.AUDCASHSHORTS, c.AUDCASHSHORTS AUDCASHSHORTS_ly 
FROM ((CENTERS_DTE A 
LEFT JOIN cash B ON (((A.CENTER = B.CENTER) 
AND (A.DTE = B.PERIOD)))) 
LEFT JOIN ACE_REPORTS_DB..OPERATIONALGOALS C ON (((A.CENTER = C.CENTER) 
AND (date(to_date(A.DTE,'YYYYMMDD')- cast('12 month' as interval)) = TO_DATE("VARCHAR"(C.PERIOD), 'YYYYMMDD'::"VARCHAR")) ))) 
where to_date(a.dte,'YYYYMMDD') < date_trunc('MONTH',CURRENT_DATE - 2) ) , DATA AS ( 
SELECT A.DTE AS PERIOD, A.CENTER, b.AUDCASHSHORTS,c.AUDCASHSHORTS AUDCASHSHORTS_ly 
from CENTERS_DTE A 
LEFT JOIN cash B ON A.CENTER = B.CENTER 
AND A.DTE = B.PERIOD 
left outer join ACE_REPORTS_DB..OPERATIONALGOALS c on a.center = c.center 
AND ((TO_DATE("VARCHAR"(A.DTE), 'YYYYMMDD'::"VARCHAR") - 364) = TO_DATE("VARCHAR"(C.PERIOD), 'YYYYMMDD'::"VARCHAR")) 
where to_date(a.dte,'YYYYMMDD') >= date_trunc('MONTH',CURRENT_DATE - 2)) 
SELECT * 
FROM PrevmonDATA 
UNION ALL select * 
from data;

v_iRow_Count := ROW_COUNT;

v_strMessage := 'CREATE TEMP TABLE CHECKS ' || v_iETL_Date;

CREATE TEMP TABLE CHECKS as WITH checks as ( 
SELECT CENTER,PERIOD , SUM(CAST(FORGERYRETURNEDVOLUME AS FLOAT)) - SUM(CAST(FORGERYCOLLECTVOLUME AS FLOAT)) AS FORGERIES_VOL, SUM(CAST(CHECKCASHINGVOLUME AS FLOAT)) AS CHECK_CASHING_VOL, SUM(CAST(OPENLATE AS FLOAT)) AS OPEN_LATE, SUM(CAST(CLOSEEARLY AS FLOAT)) AS CLOSE_EARLY, SUM(NO_OF_DAYS) AS NO_DAYS_CY 
FROM ACE_REPORTS_DB..ACE_HERP_DAILY_METRICS_CENTER_DATA_VW A 
where TO_DATE(A.PERIOD, 'YYYYMMDD')>= Date_Trunc('MONTH', ADD_MONTHS(CURRENT_DATE-2, -1)) 
AND TO_DATE(a.period, 'YYYYMMDD')<= 
CASE 
WHEN day(CURRENT_DATE)<=2 THEN CURRENT_DATE-2 ELSE CURRENT_DATE-1 
END AND a.CENTER >=25 
AND a.CENTER 
NOT BETWEEN 5000 
AND 5100 
GROUP BY CENTER,PERIOD ) , PrevmonDATA AS ( 
SELECT A.dte PERIOD, A.CENTER, b.FORGERIES_VOL,c.FORGERIES_VOL FORGERIES_VOL_ly,b.CHECK_CASHING_VOL,c.CHECK_CASHING_VOL CHECK_CASHING_VOL_ly,b.OPEN_LATE,c.OPEN_LATE OPEN_LATE_ly,b.CLOSE_EARLY,c.CLOSE_EARLY CLOSE_EARLY_ly, b.NO_DAYS_CY,c.NO_DAYS_CY NO_DAYS_ly 
FROM ((CENTERS_DTE A 
LEFT JOIN checks B ON (((A.CENTER = B.CENTER) 
AND (A.DTE = B.PERIOD)))) 
LEFT JOIN ACE_REPORTS_DB..OPERATIONALGOALS C ON (((A.CENTER = C.CENTER) 
AND (date(to_date(A.DTE,'YYYYMMDD')- cast('12 month' as interval)) = TO_DATE("VARCHAR"(C.PERIOD), 'YYYYMMDD'::"VARCHAR")) ))) 
where to_date(a.dte,'YYYYMMDD') < date_trunc('MONTH',CURRENT_DATE - 2) ) , DATA AS ( 
SELECT A.DTE AS PERIOD, A.CENTER, b.FORGERIES_VOL,c.FORGERIES_VOL FORGERIES_VOL_ly,b.CHECK_CASHING_VOL,c.CHECK_CASHING_VOL CHECK_CASHING_VOL_ly,b.OPEN_LATE,c.OPEN_LATE OPEN_LATE_ly,b.CLOSE_EARLY,c.CLOSE_EARLY CLOSE_EARLY_ly, b.NO_DAYS_CY,c.NO_DAYS_CY NO_DAYS_ly 
from CENTERS_DTE A 
LEFT JOIN checks B ON A.CENTER = B.CENTER 
AND A.DTE = B.PERIOD 
left outer join ACE_REPORTS_DB..OPERATIONALGOALS c on a.center = c.center 
AND ((TO_DATE("VARCHAR"(A.DTE), 'YYYYMMDD'::"VARCHAR") - 364) = TO_DATE("VARCHAR"(C.PERIOD), 'YYYYMMDD'::"VARCHAR")) 
where to_date(a.dte,'YYYYMMDD') >= date_trunc('MONTH',CURRENT_DATE - 2)) 
SELECT * 
FROM PrevmonDATA 
UNION ALL select * 
from data;

v_iRow_Count := ROW_COUNT;

v_strMessage := 'CREATE TEMP TABLE employees ' || v_iETL_Date;

CREATE TEMP TABLE employees AS WITH employees AS ( 
SELECT A.CENTER, A.PERIOD, SUM( 
CASE 
WHEN A.ACCOUNT 
IN ('Active Employee Items') THEN DATA_VALUE ELSE 0 
END) AS ActiveEmployee, SUM( 
CASE 
WHEN A.ACCOUNT 
IN ('Terminated Employee Items') THEN DATA_VALUE ELSE 0 
END) AS TerminatedEmployee 
FROM VENDOR_ESSBASE. 
ADMIN.ES_DT_EMP_ITEM A 
INNER JOIN ( 
SELECT PERIOD, CENTER, ACCOUNT, MAX(EXTRACT_DATE_ID) AS MAXID 
FROM VENDOR_ESSBASE. 
ADMIN.ES_DT_EMP_ITEM 
GROUP BY PERIOD, CENTER, ACCOUNT )B ON (A.CENTER = B.CENTER) 
AND (A.PERIOD = B.PERIOD) 
AND (A.ACCOUNT = B.ACCOUNT) 
AND (A.EXTRACT_DATE_ID = B.MAXID) 
where TO_DATE(A.PERIOD, 'YYYYMMDD')>= Date_Trunc('MONTH', ADD_MONTHS(CURRENT_DATE-2, -1)) 
AND TO_DATE(a.period, 'YYYYMMDD')<= 
CASE 
WHEN day(CURRENT_DATE)<=2 THEN CURRENT_DATE-2 ELSE CURRENT_DATE-1 
END AND a.CENTER >=25 
AND a.CENTER 
NOT BETWEEN 5000 
AND 5100 
GROUP BY A.CENTER, A.PERIOD) , PrevmonDATA AS ( 
SELECT A.DTE AS PERIOD, A.CENTER, B.ActiveEmployee,B.TerminatedEmployee,C.ActiveEmployee ActiveEmployee_LY,C.TerminatedEmployee TerminatedEmployee_LY 
FROM ((CENTERS_DTE A 
LEFT JOIN employees B ON (((A.CENTER = B.CENTER) 
AND (A.DTE = B.PERIOD)))) 
LEFT JOIN ACE_REPORTS_DB..OPERATIONALGOALS C ON (((A.CENTER = C.CENTER) 
AND (date(to_date(A.DTE,'YYYYMMDD')- cast('12 month' as interval)) = TO_DATE("VARCHAR"(C.PERIOD), 'YYYYMMDD'::"VARCHAR")) ))) 
where to_date(a.dte,'YYYYMMDD') < date_trunc('MONTH',CURRENT_DATE - 2) ) , DATA AS ( 
SELECT A.DTE AS PERIOD, A.CENTER, B.ActiveEmployee,B.TerminatedEmployee,C.ActiveEmployee ActiveEmployee_LY,C.TerminatedEmployee TerminatedEmployee_LY 
from CENTERS_DTE A 
LEFT JOIN employees B ON A.CENTER = B.CENTER 
AND A.DTE = B.PERIOD 
left outer join ACE_REPORTS_DB..OPERATIONALGOALS c on a.center = c.center 
AND ((TO_DATE("VARCHAR"(A.DTE), 'YYYYMMDD'::"VARCHAR") - 364) = TO_DATE("VARCHAR"(C.PERIOD), 'YYYYMMDD'::"VARCHAR")) 
where to_date(a.dte,'YYYYMMDD') >= date_trunc('MONTH',CURRENT_DATE - 2)) 
SELECT * 
FROM PrevmonDATA 
UNION ALL select * 
from data;

v_iRow_Count := ROW_COUNT;

v_strMessage := 'CREATE TEMP TABLE SHOPS ' || v_iETL_Date;

CREATE TEMP TABLE SHOPS AS WITH SHOPS AS ( 
SELECT A.CENTER, A.PERIOD, SUM( 
CASE 
WHEN A.ACCOUNT 
IN ('Experience Shop Score Denominator') THEN DATA_VALUE ELSE 0 
END) AS EXPERIENCESHOPDENOMINATOR, SUM( 
CASE 
WHEN A.ACCOUNT 
IN ('Experience Shop Score Numerator') THEN DATA_VALUE ELSE 0 
END) AS EXPERIENCESHOPNUMERATOR, SUM( 
CASE 
WHEN A.ACCOUNT 
IN ('Loan Shop Score Denominator') THEN DATA_VALUE ELSE 0 
END) AS LOANSHOPDENOMINATOR, SUM( 
CASE 
WHEN A.ACCOUNT 
IN ('Loan Shop Score Numerator') THEN DATA_VALUE ELSE 0 
END) AS LOANSHOPNUMERATOR, SUM( 
CASE 
WHEN A.ACCOUNT 
IN ('Phone Shop Score Denominator') THEN DATA_VALUE ELSE 0 
END) AS PHONESHOPDENOMINATOR, SUM( 
CASE 
WHEN A.ACCOUNT 
IN ('Phone Shop Score Numerator') THEN DATA_VALUE ELSE 0 
END) AS PHONESHOPNUMERATOR 
FROM VENDOR_ESSBASE. 
ADMIN.ES_DT_MYS_PH_SHOP A 
INNER JOIN ( 
SELECT PERIOD, CENTER, ACCOUNT, MAX(EXTRACT_DATE_ID) AS MAXID 
FROM VENDOR_ESSBASE. 
ADMIN.ES_DT_MYS_PH_SHOP 
GROUP BY PERIOD, CENTER, ACCOUNT )B ON (A.CENTER = B.CENTER) 
AND (A.PERIOD = B.PERIOD) 
AND (A.ACCOUNT = B.ACCOUNT) 
AND (A.EXTRACT_DATE_ID = B.MAXID) 
where TO_DATE(A.PERIOD, 'YYYYMMDD')>= Date_Trunc('MONTH', ADD_MONTHS(CURRENT_DATE-2, -1)) 
AND TO_DATE(a.period, 'YYYYMMDD')<= 
CASE 
WHEN day(CURRENT_DATE)<=2 THEN CURRENT_DATE-2 ELSE CURRENT_DATE-1 
END AND a.CENTER >=25 
AND a.CENTER 
NOT BETWEEN 5000 
AND 5100 
GROUP BY A.CENTER, A.PERIOD) , prevmonDATA AS ( 
SELECT A.DTE AS PERIOD, A.CENTER, B.EXPERIENCESHOPDENOMINATOR, C.EXPERIENCESHOPDENOMINATOR as EXPERIENCESHOPDENOMINATOR_LY, B.EXPERIENCESHOPNUMERATOR, C.EXPERIENCESHOPNUMERATOR as EXPERIENCESHOPNUMERATOR_LY, B.LOANSHOPDENOMINATOR, C.LOANSHOPDENOMINATOR as LOANSHOPDENOMINATOR_LY, B.LOANSHOPNUMERATOR, C.LOANSHOPNUMERATOR as LOANSHOPNUMERATOR_LY,B.PHONESHOPDENOMINATOR, C.PHONESHOPDENOMINATOR as PHONESHOPDENOMINATOR_LY, b.PHONESHOPNUMERATOR, c.PHONESHOPNUMERATOR as PHONESHOPNUMERATOR_ly 
FROM ((CENTERS_DTE A 
LEFT JOIN SHOPS B ON (((A.CENTER = B.CENTER) 
AND (A.DTE = B.PERIOD)))) 
LEFT JOIN ACE_REPORTS_DB..OPERATIONALGOALS C ON (((A.CENTER = C.CENTER) 
AND (date(to_date(A.DTE,'YYYYMMDD')- cast('12 month' as interval)) = TO_DATE("VARCHAR"(C.PERIOD), 'YYYYMMDD'::"VARCHAR")) ))) 
where to_date(a.dte,'YYYYMMDD') < date_trunc('MONTH',CURRENT_DATE - 2) ) , DATA AS ( 
SELECT A.DTE AS PERIOD, A.CENTER, B.EXPERIENCESHOPDENOMINATOR, C.EXPERIENCESHOPDENOMINATOR as EXPERIENCESHOPDENOMINATOR_LY, B.EXPERIENCESHOPNUMERATOR, C.EXPERIENCESHOPNUMERATOR as EXPERIENCESHOPNUMERATOR_LY, B.LOANSHOPDENOMINATOR, C.LOANSHOPDENOMINATOR as LOANSHOPDENOMINATOR_LY, B.LOANSHOPNUMERATOR, C.LOANSHOPNUMERATOR as LOANSHOPNUMERATOR_LY,B.PHONESHOPDENOMINATOR, C.PHONESHOPDENOMINATOR as PHONESHOPDENOMINATOR_LY, b.PHONESHOPNUMERATOR, c.PHONESHOPNUMERATOR as PHONESHOPNUMERATOR_ly 
from CENTERS_DTE A 
LEFT JOIN shops B ON A.CENTER = B.CENTER 
AND A.DTE = B.PERIOD 
left outer join ACE_REPORTS_DB..OPERATIONALGOALS c on a.center = c.center 
AND ((TO_DATE("VARCHAR"(A.DTE), 'YYYYMMDD'::"VARCHAR") - 364) = TO_DATE("VARCHAR"(C.PERIOD), 'YYYYMMDD'::"VARCHAR")) 
where to_date(a.dte,'YYYYMMDD') >= date_trunc('MONTH',CURRENT_DATE - 2)) 
SELECT * 
FROM PrevmonDATA 
UNION ALL select * 
from data;

v_iRow_Count := ROW_COUNT;

v_strMessage := 'CREATE TEMP TABLE FINDATA ' || v_iETL_Date;

CREATE TEMP TABLE FINaldATAset AS SELECT COALESCE(A.CENTER,B.CENTER,C.CENTER,D.CENTER,E.CENTER,F.CENTER,G.CENTER,H.CENTER,I.CENTER,J.CENTER,K.CENTER,L.CENTER) CENTER, COALESCE(A.PERIOD,B.PERIOD,C.PERIOD,D.PERIOD,E.PERIOD,F.PERIOD,G.PERIOD,H.PERIOD,I.PERIOD,J.PERIOD,K.PERIOD,L.PERIOD) PERIOD, DBACHECKS,PROMOTERS_NPS,DETRACTORS_NPS,PASSIVE_NPS,DBACHECKS_LY,PROMOTERS_NPS_LY,DETRACTORS_NPS_LY,PASSIVE_NPS_LY, HOURSOVERSTAFFED,HOURSUNDERSTAFFED,SCHEDULEDHOURS,CM_OT,SA_OT,PAYDATE_DAYS,CORP_SCORE_DEN,CORP_SCORE_NUM,CHECKITEMS, HOURSOVERSTAFFED_LY,HOURSUNDERSTAFFED_LY,SCHEDULEDHOURS_LY,CM_OT_LY,SA_OT_LY,PAYDATE_DAYS_LY,CORP_SCORE_DEN_LY,CORP_SCORE_NUM_LY,CHECKITEMS_LY, FIRSTTIME_DDS_CY,FIRSTTIME_DDS_LY,FTDD_GOAL_MTD,CARDSALES_GOAL_MTD,days_MTD,BBK_ITEMS_GOAL_MTD,BBK_ITEMS_OPPS_GOAL_MTD ,DDAUPGRADES,DDAUPGRADES_LY,DDANEWCARD,DDANEWCARD_LY,PREPAIDCARDSALEITEMS_LY,PREPAIDCARDSALEITEMS, BBK_ITEMS_CY,BBK_OPP_ITEMS_CY,BBK_ITEMS_LY,BBK_OPP_ITEMS_LY,CASH_SHORTS_PSD,INTERNAL_AUDIT, CASH_SHORTS_PSD_LY,INTERNAL_AUDIT_LY, AUDCASHSHORTS,AUDCASHSHORTS_LY, FORGERIES_VOL,FORGERIES_VOL_LY,CHECK_CASHING_VOL,NO_DAYS_CY,OPEN_LATE,CLOSE_EARLY,CHECK_CASHING_VOL_LY,NO_DAYS_LY,OPEN_LATE_LY,CLOSE_EARLY_LY, NC4S_OPPS_CY,NC4S_SALES_CY,NC4S_OPPS_LY,NC4S_SALES_LY,ActiveEmployee,ActiveEmployee_LY,TerminatedEmployee,TerminatedEmployee_LY, EXPERIENCESHOPDENOMINATOR, EXPERIENCESHOPNUMERATOR, LOANSHOPDENOMINATOR, LOANSHOPNUMERATOR, PHONESHOPDENOMINATOR, PHONESHOPNUMERATOR, EXPERIENCESHOPDENOMINATOR_LY, EXPERIENCESHOPNUMERATOR_LY, LOANSHOPDENOMINATOR_LY, LOANSHOPNUMERATOR_LY, PHONESHOPDENOMINATOR_LY, PHONESHOPNUMERATOR_LY, coalesce(EXPERIENCESHOPDENOMINATOR,0) + coalesce(LOANSHOPDENOMINATOR,0) + coalesce(PHONESHOPDENOMINATOR,0) TOTALSHOPSDEN, coalesce(EXPERIENCESHOPNUMERATOR,0) + coalesce(LOANSHOPNUMERATOR,0) + coalesce(PHONESHOPNUMERATOR,0) TOTALSHOPSNUM, coalesce(EXPERIENCESHOPDENOMINATOR_LY,0) + coalesce(LOANSHOPDENOMINATOR_LY,0) + coalesce(PHONESHOPDENOMINATOR_LY,0) TOTALSHOPSDEN_LY, coalesce(EXPERIENCESHOPNUMERATOR_LY,0) + coalesce(LOANSHOPNUMERATOR_LY,0) + coalesce(PHONESHOPNUMERATOR_LY,0) TOTALSHOPSNUM_LY 
FROM NPS A 
FULL OUTER JOIN hoursstaff B ON A.CENTER = B.CENTER 
AND A.PERIOD = B.PERIOD 
FULL OUTER JOIN DDS C ON COALESCE(A.CENTER,B.CENTER) = C.CENTER 
AND COALESCE(A.PERIOD,B.PERIOD) = C.PERIOD 
FULL OUTER JOIN GOAL D ON COALESCE(A.CENTER,B.CENTER,C.CENTER) = D.CENTER 
AND COALESCE(A.PERIOD,B.PERIOD,C.PERIOD) = D.PERIOD 
FULL OUTER JOIN DDA E ON COALESCE(A.CENTER,B.CENTER,C.CENTER,D.CENTER) = E.CENTER 
AND COALESCE(A.PERIOD,B.PERIOD,C.PERIOD,D.PERIOD) = E.PERIOD 
FULL OUTER JOIN PREPAID F ON COALESCE(A.CENTER,B.CENTER,C.CENTER,D.CENTER,E.CENTER) = F.CENTER 
AND COALESCE(A.PERIOD,B.PERIOD,C.PERIOD,D.PERIOD,E.PERIOD) = F.PERIOD 
FULL OUTER JOIN BBK G ON COALESCE(A.CENTER,B.CENTER,C.CENTER,D.CENTER,E.CENTER,F.CENTER) = G.CENTER 
AND COALESCE(A.PERIOD,B.PERIOD,C.PERIOD,D.PERIOD,E.PERIOD,F.PERIOD) = G.PERIOD 
FULL OUTER JOIN CASHSHORTS H ON COALESCE(A.CENTER,B.CENTER,C.CENTER,D.CENTER,E.CENTER,F.CENTER,G.CENTER) = H.CENTER 
AND COALESCE(A.PERIOD,B.PERIOD,C.PERIOD,D.PERIOD,E.PERIOD,F.PERIOD,G.PERIOD) = H.PERIOD 
FULL OUTER JOIN CHECKS I ON COALESCE(A.CENTER,B.CENTER,C.CENTER,D.CENTER,E.CENTER,F.CENTER,G.CENTER,H.CENTER) = I.CENTER 
AND COALESCE(A.PERIOD,B.PERIOD,C.PERIOD,D.PERIOD,E.PERIOD,F.PERIOD,G.PERIOD,H.PERIOD) = I.PERIOD 
FULL OUTER JOIN NC4S J ON COALESCE(A.CENTER,B.CENTER,C.CENTER,D.CENTER,E.CENTER,F.CENTER,G.CENTER,H.CENTER,I.CENTER) = J.CENTER 
AND COALESCE(A.PERIOD,B.PERIOD,C.PERIOD,D.PERIOD,E.PERIOD,F.PERIOD,G.PERIOD,H.PERIOD,I.PERIOD) = J.PERIOD 
FULL OUTER JOIN employees K ON COALESCE(A.CENTER,B.CENTER,C.CENTER,D.CENTER,E.CENTER,F.CENTER,G.CENTER,H.CENTER,I.CENTER,J.CENTER) = K.CENTER 
AND COALESCE(A.PERIOD,B.PERIOD,C.PERIOD,D.PERIOD,E.PERIOD,F.PERIOD,G.PERIOD,H.PERIOD,I.PERIOD,J.PERIOD) = K.PERIOD 
FULL OUTER JOIN shops L ON COALESCE(A.CENTER,B.CENTER,C.CENTER,D.CENTER,E.CENTER,F.CENTER,G.CENTER,H.CENTER,I.CENTER,J.CENTER,K.CENTER) = L.CENTER 
AND COALESCE(A.PERIOD,B.PERIOD,C.PERIOD,D.PERIOD,E.PERIOD,F.PERIOD,G.PERIOD,H.PERIOD,I.PERIOD,J.PERIOD,K.PERIOD) = L.PERIOD;

v_iRow_Count := ROW_COUNT;

v_strMessage := 'Delete from ACE_REPORTS_DB..OPERATIONALGOALS where extract_date_ID = ' || v_iETL_Date;

DELETE FROM OPERATIONALGOALS 
WHERE period >= ( 
select EXPIRATION_DATE_ID 
from OPERATIONALGOALS_temp_dates);

v_iRow_Count := ROW_COUNT;

v_strMessage := 'Insert into TABLE ACE_REPORTS_DB..OPERATIONALGOALS ' || v_iETL_Date;

INSERT INTO OPERATIONALGOALS 
SELECT a.center,period,to_date(period,'YYYYMMDD') DATES, DBACHECKS,PROMOTERS_NPS,DETRACTORS_NPS,PASSIVE_NPS,DBACHECKS_LY,PROMOTERS_NPS_LY,DETRACTORS_NPS_LY,PASSIVE_NPS_LY, HOURSOVERSTAFFED,HOURSUNDERSTAFFED,SCHEDULEDHOURS,CM_OT,SA_OT,PAYDATE_DAYS,CORP_SCORE_DEN,CORP_SCORE_NUM,CHECKITEMS, HOURSOVERSTAFFED_LY,HOURSUNDERSTAFFED_LY,SCHEDULEDHOURS_LY,CM_OT_LY,SA_OT_LY,PAYDATE_DAYS_LY,CORP_SCORE_DEN_LY,CORP_SCORE_NUM_LY,CHECKITEMS_LY, FIRSTTIME_DDS_CY,FIRSTTIME_DDS_LY,FTDD_GOAL_MTD,CARDSALES_GOAL_MTD,days_MTD,BBK_ITEMS_GOAL_MTD,BBK_ITEMS_OPPS_GOAL_MTD,DDAUPGRADES,DDAUPGRADES_LY,DDANEWCARD,DDANEWCARD_LY,PREPAIDCARDSALEITEMS_LY,PREPAIDCARDSALEITEMS, DDANEWCARD + PREPAIDCARDSALEITEMS CARDSALES_CY, DDANEWCARD_LY + PREPAIDCARDSALEITEMS_LY CARDSALES_LY, BBK_ITEMS_CY,BBK_OPP_ITEMS_CY,BBK_ITEMS_LY,BBK_OPP_ITEMS_LY,CASH_SHORTS_PSD,INTERNAL_AUDIT, CASH_SHORTS_PSD_LY,INTERNAL_AUDIT_LY, AUDCASHSHORTS,AUDCASHSHORTS_LY, FORGERIES_VOL,FORGERIES_VOL_LY,CHECK_CASHING_VOL,NO_DAYS_CY,OPEN_LATE,CLOSE_EARLY,CHECK_CASHING_VOL_LY,NO_DAYS_LY,OPEN_LATE_LY,CLOSE_EARLY_LY, NC4S_OPPS_CY,NC4S_SALES_CY,NC4S_OPPS_LY,NC4S_SALES_LY,ActiveEmployee,ActiveEmployee_LY,TerminatedEmployee,TerminatedEmployee_LY , EXPERIENCESHOPDENOMINATOR , EXPERIENCESHOPNUMERATOR , LOANSHOPDENOMINATOR , LOANSHOPNUMERATOR , PHONESHOPDENOMINATOR , PHONESHOPNUMERATOR , EXPERIENCESHOPDENOMINATOR_LY , EXPERIENCESHOPNUMERATOR_LY , LOANSHOPDENOMINATOR_LY , LOANSHOPNUMERATOR_LY , PHONESHOPDENOMINATOR_LY , PHONESHOPNUMERATOR_LY , TOTALSHOPSDEN , TOTALSHOPSNUM , TOTALSHOPSDEN_LY , TOTALSHOPSNUM_LY , d.EXTRACT_DATE_ID, "TIMESTAMP"('now'::"VARCHAR") DW_MOD_DATETIME 
FROM FINaldATAset A 
JOIN OPERATIONALGOALS_temp_dates D ON PERIOD>=expiration_date_id 
AND A.PERIOD <= 
CASE 
WHEN day(CURRENT_DATE)<=2 THEN d.EXTRACT_DATE_ID-1 ELSE d.EXTRACT_DATE_ID 
END;

v_iRow_Count := ROW_COUNT;

v_strMessage := 'Procedure Completed Successfully';

CALL CreateAuditEvent(v_iETL_Date, v_iETL_Job_ID, v_iSeverity, v_iRow_Count, v_strSP_Name, v_strPackage_Name, v_strMessage);

RETURN (0);

EXCEPTION WHEN OTHERS THEN v_strMessage := v_strMessage || SQLERRM;

CALL CreateAuditEvent(v_iETL_Date, v_iETL_Job_ID, v_iSeverity, v_iRow_Count, v_strSP_Name, v_strPackage_Name, v_strMessage);

RAISE EXCEPTION '%', v_strMessage;

RETURN (1);

END;

END_PROC;

