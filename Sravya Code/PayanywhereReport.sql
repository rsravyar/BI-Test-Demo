CREATE OR REPLACE PROCEDURE PAY_ANYWHERE_REPORT_LOAD_SP(INTEGER, INTEGER, CHARACTER VARYING(150), CHARACTER VARYING(150))
RETURNS INTEGER
LANGUAGE NZPLSQL AS
BEGIN_PROC
 DECLARE v_ietl_date 
ALIAS for $1;

v_iETL_Job_ID 
alias FOR $2;

v_strSP_Name 
alias FOR $3;

v_strPackage_Name 
alias FOR $4;

v_iSeverity integer := 0;

v_iRow_Count integer := 0;

v_strMessage varchar(250);

v_row_nbr smallint;

v_max_row_nbr smallint;

BEGIN v_strmessage := 'Start of PAY_ANYWHERE_REPORT_LOAD_SP ';

call createauditevent(v_ietl_date, v_ietl_job_id, v_iseverity, v_irow_count, v_strsp_name, v_strpackage_name, v_strmessage);

v_strmessage := 'CREATE TEMP TABLE temp_dates';

create temp TABLE temp_dates AS SELECT extr_dt.calendar_date AS extractdate , extr_dt.calendar_date_id AS extract_date_id , expire_dt.calendar_date_id AS expiration_date_id , CURRENT_TIMESTAMP AS l_createdate , CURRENT_USER AS l_createby , CURRENT_TIMESTAMP AS l_modifydate , CURRENT_USER AS l_modifyby 
FROM acedw..calendar_date extr_dt 
JOIN acedw..calendar_date expire_dt ON extr_dt.calendar_date_id = v_ietl_date 
AND expire_dt.calendar_date = extr_dt.calendar_date - 1 
distribute ON random;

v_irow_count := row_count;

call createauditevent(v_ietl_date, v_ietl_job_id, v_iseverity, v_irow_count, v_strsp_name, v_strpackage_name, v_strmessage);

v_irow_count := 0;

v_strmessage := 'CREATE TEMP TABLE temp_loans1 TO PULL PRINCIPLA AMOUNT AND FEE';

create temp TABLE temp_loans AS SELECT gtcmp, orgcenter as gtcntr, gttrnt, to_date(gttrdt,'YYYYMMDD') gttrdt , gtgln_nbr, sum(gtpamt) principal_amount, sum(gtofee) fee, 
case 
when essbase_group_description 
is null then 'Other' else essbase_group_description 
end AS customer_type, 
CASE 
WHEN gtcntr=gmcntr THEN 'Orig Center' WHEN gtcntr<>gmcntr THEN 'Remote Center' END AS centers, tran_type, 
CASE 
WHEN tender_type = 'CASH' THEN 'Cash' WHEN tender_type='DEBIT' THEN 'Debit' WHEN tender_type='ACH' THEN 'ACH' ELSE 'Other' END AS tender_types, 
CASE 
WHEN source 
IN ('NON-ORIGINATING STORE', 'ORIGINATING STORE') 
AND GTCNTR <> 9001 THEN 'In Person' WHEN GTCNTR = 9001 
AND SOURCE <> 'PHONE' THEN 'Online' WHEN source ='PHONE' THEN 'Phone' ELSE source 
END AS sources, 
CASE 
WHEN category='PAYMENT' THEN 'Payment' WHEN category='PAYOFF' AND TRAN_TYPE <> 'MBK' THEN 'BuyBack' WHEN category='PAYOFF' AND TRAN_TYPE = 'MBK' THEN 'MBK' ELSE 
category END AS categories , gttrn_nbr, count( 
DISTINCT gnbdtl_id) AS count 
FROM acedw..gnbdtl_paw a 
INNER JOIN ace_ods..gnbmst b ON gmgln_nbr= gtgln_nbr 
INNER JOIN ( 
select GTCNTR orgcenter,gtgln_nbr orgloan 
from ace_ods..GNBDTL 
where gTtrnt = 'ORG') e ON A.GTGLN_NBR = e.orgloan 
INNER JOIN acedw..center c ON E.orgcenter=c.s01ctr 
LEFT JOIN acedw..loan_application_dim d ON gtgln_nbr=d.loan_nbr 
AND d.end_date='9999-12-31 00:00:00' LEFT JOIN acedw..loan_customer_type cu ON customer_type_code = loan_customer_type_code 
WHERE gttrdt>=20151001 
AND s01st 
IN ('TX') 
AND c.end_date ='9999-12-31 00:00:00' AND gtcmp 
IN ('C51', 'C52', 'C55') 
AND tender_type 
IN ('CASH', 'DEBIT', 'ACH') 
AND TRIM(SOURCE) 
NOT IN ('PHONE') 
GROUP BY gtcntr,orgcenter, gmcntr, gttrnt, gttrdt , gtgln_nbr, tran_type, tender_type, source, 
category, gtcmp, essbase_group_description, gttrn_nbr 
UNION ALL SELECT gtcmp,f.orgcenter as gtcntr, gttrnt, to_date(gttrdt,'YYYYMMDD') gttrdt , gtgln_nbr, sum(gtpamt) principal_amount, sum(gtofee) fee, 
case 
when essbase_group_description 
is null then 'Other' else essbase_group_description 
end AS customer_type, 
case 
when gtcntr=gmcntr 
AND gtcntr = PMTCNTR then 'Orig Center' WHEN gtcntr<>gmcntr 
or gtcntr <>PMTCNTR then 'Remote Center' END AS CENTERS, tran_type, 
CASE 
WHEN tender_type = 'CASH' THEN 'Cash' WHEN tender_type='DEBIT' THEN 'Debit' WHEN tender_type='ACH' THEN 'ACH' ELSE 'Other' END AS tender_types, 
CASE 
WHEN source 
IN ('NON-ORIGINATING STORE', 'ORIGINATING STORE') 
AND GTCNTR <> 9001 THEN 'In Person' WHEN GTCNTR = 9001 
AND SOURCE <> 'PHONE' THEN 'Online' WHEN source ='PHONE' THEN 'Phone' ELSE source 
END AS sources, 
CASE 
WHEN category='PAYMENT' THEN 'Payment' WHEN category='PAYOFF' AND TRAN_TYPE <> 'MBK' THEN 'BuyBack' WHEN category='PAYOFF' AND TRAN_TYPE = 'MBK' THEN 'MBK' ELSE 
category END AS categories, gttrn_nbr, count( 
DISTINCT gnbdtl_id) AS count 
FROM acedw..gnbdtl_paw a 
INNER JOIN ace_ods..gnbmst b ON gmgln_nbr= gtgln_nbr 
INNER JOIN ( 
select GTCNTR orgcenter,gtgln_nbr orgloan 
from ace_ods..GNBDTL 
where gTtrnt = 'ORG') f ON A.GTGLN_NBR = f.orgloan 
INNER JOIN acedw..center c ON F.orgcenter=c.s01ctr 
LEFT JOIN acedw..loan_application_dim d ON gtgln_nbr=d.loan_nbr 
AND d.end_date='9999-12-31 00:00:00' LEFT JOIN acedw..loan_customer_type cu ON customer_type_code = loan_customer_type_code 
WHERE gttrdt>=20151001 
AND s01st 
IN ('TX') 
AND c.end_date ='9999-12-31 00:00:00' AND gtcmp 
IN ('C51', 'C52', 'C55') 
AND tender_type 
IN ('CASH', 'DEBIT', 'ACH') 
AND TRIM(SOURCE) 
IN ('PHONE') 
GROUP BY gtcntr, f.orgcenter,gmcntr, gttrnt, gttrdt , gtgln_nbr, tran_type, tender_type, source, 
category, gtcmp, essbase_group_description, gttrn_nbr,PMTCNTR;

v_irow_count := row_count;

call createauditevent(v_ietl_date, v_ietl_job_id, v_iseverity, v_irow_count, v_strsp_name, v_strpackage_name, v_strmessage);

v_irow_count := 0;

v_strmessage := 'CREATE TEMP TABLE temp_loans5 TO PULL PRINCIPLA AMOUNT AND FEE';

create temp TABLE temp_loans5 AS SELECT gtcmp, e.orgcenter as gtcntr, gttrnt, to_date(gttrdt,'YYYYMMDD') gttrdt , gtgln_nbr, sum(gtpamt) principal_amount, sum(gtofee) fee, 
case 
when essbase_group_description 
is null then 'Other' else essbase_group_description 
end AS customer_type, 
CASE 
WHEN gtcntr=gmcntr THEN 'Orig Center' WHEN gtcntr<>gmcntr THEN 'Remote Center' END AS centers, tran_type, 
CASE 
WHEN tender_type = 'CASH' THEN 'Cash' WHEN tender_type='DEBIT' THEN 'Debit' WHEN tender_type='ACH' THEN 'ACH' ELSE 'Other' END AS tender_types, 
CASE 
WHEN source 
IN ('NON-ORIGINATING STORE', 'ORIGINATING STORE') 
AND GTCNTR <> 9001 THEN 'In Person' WHEN GTCNTR = 9001 
AND SOURCE <> 'PHONE' THEN 'Online' WHEN source ='PHONE' THEN 'Phone' ELSE source 
END AS sources, 
CASE 
WHEN category='PAYMENT' THEN 'Payment' WHEN category='PAYOFF' AND TRAN_TYPE <> 'MBK' THEN 'BuyBack' WHEN category='PAYOFF' AND TRAN_TYPE = 'MBK' THEN 'MBK' ELSE 
category END AS categories, gttrn_nbr, count( 
DISTINCT gnbdtl_id) AS count 
FROM acedw..gnbdtl_paw a 
INNER JOIN ace_ods..gnbmst b ON gmgln_nbr= gtgln_nbr 
INNER JOIN ( 
select GTCNTR orgcenter,gtgln_nbr orgloan 
from ace_ods..GNBDTL 
where gTtrnt = 'ORG') e ON A.GTGLN_NBR = e.orgloan 
INNER JOIN acedw..center c ON E.orgcenter=c.s01ctr 
LEFT JOIN acedw..loan_application_dim d ON gtgln_nbr=d.loan_nbr 
AND d.end_date='9999-12-31 00:00:00' LEFT JOIN acedw..loan_customer_type cu ON customer_type_code = loan_customer_type_code 
WHERE gttrdt>=20151001 
AND s01st 
IN ('FL','KS','CA','IN','TN') 
AND c.end_date ='9999-12-31 00:00:00' AND gtcmp 
IN ('ACE') 
AND tender_type 
IN ('CASH', 'DEBIT', 'ACH') 
AND TRIM(SOURCE) 
NOT IN ('PHONE') 
GROUP BY gtcntr,e.orgcenter, gmcntr, gttrnt, gttrdt , gtgln_nbr, tran_type, tender_type, source, 
category, gtcmp, essbase_group_description, gttrn_nbr 
UNION ALL SELECT gtcmp, f.orgcenter as gtcntr, gttrnt, to_date(gttrdt,'YYYYMMDD') gttrdt , gtgln_nbr, sum(gtpamt) principal_amount, sum(gtofee) fee, 
case 
when essbase_group_description 
is null then 'Other' else essbase_group_description 
end AS customer_type, 
case 
when gtcntr=gmcntr 
AND gtcntr = PMTCNTR then 'Orig Center' WHEN gtcntr<>gmcntr 
or gtcntr <>PMTCNTR then 'Remote Center' END AS CENTERS, tran_type, 
CASE 
WHEN tender_type = 'CASH' THEN 'Cash' WHEN tender_type='DEBIT' THEN 'Debit' WHEN tender_type='ACH' THEN 'ACH' ELSE 'Other' END AS tender_types, 
CASE 
WHEN source 
IN ('NON-ORIGINATING STORE', 'ORIGINATING STORE') 
AND GTCNTR <> 9001 THEN 'In Person' WHEN GTCNTR = 9001 
AND SOURCE <> 'PHONE' THEN 'Online' WHEN source ='PHONE' THEN 'Phone' ELSE source 
END AS sources, 
CASE 
WHEN category='PAYMENT' THEN 'Payment' WHEN category='PAYOFF' AND TRAN_TYPE <> 'MBK' THEN 'BuyBack' WHEN category='PAYOFF' AND TRAN_TYPE = 'MBK' THEN 'MBK' ELSE 
category END AS categories, gttrn_nbr, count( 
DISTINCT gnbdtl_id) AS count 
FROM acedw..gnbdtl_paw a 
INNER JOIN ace_ods..gnbmst b ON gmgln_nbr= gtgln_nbr 
INNER JOIN ( 
select GTCNTR orgcenter,gtgln_nbr orgloan 
from ace_ods..GNBDTL 
where gTtrnt = 'ORG') f ON A.GTGLN_NBR = f.orgloan 
INNER JOIN acedw..center c ON F.orgcenter=c.s01ctr 
LEFT JOIN acedw..loan_application_dim d ON gtgln_nbr=d.loan_nbr 
AND d.end_date='9999-12-31 00:00:00' LEFT JOIN acedw..loan_customer_type cu ON customer_type_code = loan_customer_type_code 
WHERE gttrdt>=20151001 
AND s01st 
IN ('FL','KS','CA','IN','TN') 
AND c.end_date ='9999-12-31 00:00:00' AND gtcmp 
IN ('ACE') 
AND tender_type 
IN ('CASH', 'DEBIT', 'ACH') 
AND TRIM(SOURCE) 
IN ('PHONE') 
GROUP BY gtcntr, f.orgcenter,gmcntr, gttrnt, gttrdt , gtgln_nbr, tran_type, tender_type, source, 
category, gtcmp, essbase_group_description, gttrn_nbr,PMTCNTR;

v_irow_count := row_count;

call createauditevent(v_ietl_date, v_ietl_job_id, v_iseverity, v_irow_count, v_strsp_name, v_strpackage_name, v_strmessage);

v_irow_count := 0;

v_strmessage := 'CREATE TEMP TABLE temp_loans1 TO PULL PRINCIPLA AMOUNT AND FEE';

create temp TABLE temp_loans1 AS SELECT * 
FROM temp_loans 
UNION ALL SELECT * 
FROM TEMP_LOANS5;

v_irow_count := row_count;

call createauditevent(v_ietl_date, v_ietl_job_id, v_iseverity, v_irow_count, v_strsp_name, v_strpackage_name, v_strmessage);

v_irow_count := 0;

v_strmessage := 'CREATE TEMP TABLE temp_loans2 TO PULL REBATE AMOUNT AND FEE';

create temp TABLE temp_loans2 AS SELECT a.gtgln_nbr, a.gttrn_nbr, a.gtcntr, a.gttrdt, sum(gtpamt) + sum(gtofee) payment 
FROM temp_loans1 a 
INNER JOIN acedw..gnbdtl_paw b ON a.gttrn_nbr=b.gttrn_nbr 
AND a.gtcntr=b.gtcntr 
AND a.gttrdt = to_date(b.gttrdt,'YYYYMMDD') 
AND a.gtgln_nbr=b.gtgln_nbr 
WHERE b.gttrnt='REB' GROUP BY a.gtgln_nbr, a.gttrn_nbr, a.gtcntr, a.gttrdt;

v_irow_count := row_count;

call createauditevent(v_ietl_date, v_ietl_job_id, v_iseverity, v_irow_count, v_strsp_name, v_strpackage_name, v_strmessage);

v_irow_count := 0;

v_strmessage := 'CREATE TEMP TABLE temp_loans3 PBK AND BBK LOANS TO CALCULATE REFINANCE AMOUNT';

create temp TABLE temp_loans3 AS SELECT gtgln_nbr 
FROM temp_loans1 
WHERE gttrnt 
IN ('PBK', 'BBK') 
GROUP BY gtgln_nbr, gttrdt 
HAVING count( 
DISTINCT gttrnt) > 1;

v_irow_count := row_count;

call createauditevent(v_ietl_date, v_ietl_job_id, v_iseverity, v_irow_count, v_strsp_name, v_strpackage_name, v_strmessage);

v_irow_count := 0;

v_strmessage := 'CREATE TEMP TABLE temp_loans4 CGO Loans';

create temp TABLE temp_loans4 AS SELECT gtcmp, f.orgcenter as gtcntr, gttrnt, to_date(gttrdt,'YYYYMMDD') gttrdt , gtgln_nbr, sum(gtpamt) principal_amount, sum(gtofee) fee, 
case 
when essbase_group_description 
is null then 'Other' else essbase_group_description 
end AS customer_type, 
CASE 
WHEN gtcntr=gmcntr THEN 'Orig Center' WHEN gtcntr<>gmcntr THEN 'Remote Center' END AS centers, tran_type, 
CASE 
WHEN tender_type = 'CASH' THEN 'Cash' WHEN tender_type='DEBIT' THEN 'Debit' WHEN tender_type='ACH' THEN 'ACH' ELSE 'Other' END AS tender_types, 
CASE 
WHEN source 
IN ('NON-ORIGINATING STORE', 'ORIGINATING STORE') 
AND GTCNTR <> 9001 THEN 'In Person' WHEN GTCNTR = 9001 
AND SOURCE <> 'PHONE' THEN 'Online' WHEN source ='PHONE' THEN 'Phone' ELSE source 
END AS sources, 
CASE 
WHEN category='PAYMENT' THEN 'Payment' WHEN category='PAYOFF' AND TRAN_TYPE <> 'MBK' THEN 'BuyBack' WHEN category='PAYOFF' AND TRAN_TYPE = 'MBK' THEN 'MBK' ELSE 
category END AS categories, gttrn_nbr, count( 
DISTINCT gnbdtl_id) AS count 
FROM acedw..gnbdtl_paw a 
INNER JOIN ace_ods..gnbmst b ON gmgln_nbr= gtgln_nbr 
INNER JOIN ( 
select GTCNTR orgcenter,gtgln_nbr orgloan 
from ace_ods..GNBDTL 
where gTtrnt = 'ORG') f ON A.GTGLN_NBR = f.orgloan 
INNER JOIN acedw..center c ON F.orgcenter=c.s01ctr 
LEFT JOIN acedw..loan_application_dim d ON gtgln_nbr=d.loan_nbr 
AND d.end_date='9999-12-31 00:00:00' LEFT JOIN acedw..loan_customer_type cu ON customer_type_code = loan_customer_type_code 
WHERE gttrdt>=20151001 
AND s01st 
IN ('TX') 
AND c.end_date ='9999-12-31 00:00:00' AND gtcmp 
IN ( 'C55') 
AND GTTRNT='CGO' GROUP BY gtcntr, f.orgcenter,gmcntr, gttrnt, gttrdt , gtgln_nbr, tran_type, tender_type, source, 
category, gtcmp, essbase_group_description, gttrn_nbr 
ORDER BY gttrdt, gtcntr;

v_irow_count := row_count;

call createauditevent(v_ietl_date, v_ietl_job_id, v_iseverity, v_irow_count, v_strsp_name, v_strpackage_name, v_strmessage);

v_irow_count := 0;

v_strmessage := 'TRUNCATE PAY ANYWHERE REPORT TABLE';

truncate TABLE pay_anywhere_report;

v_irow_count := row_count;

call createauditevent(v_ietl_date, v_ietl_job_id, v_iseverity, v_irow_count, v_strsp_name, v_strpackage_name, v_strmessage);

v_irow_count := 0;

v_strmessage := 'INSERT INTO PAY ANYWHERE REPORT TABLE FROM THE ABOVE TEMP TABLES';

insert INTO pay_anywhere_report ( gtcmp , gtcntr , gttrnt , gttrdt , gtgln_nbr , reb_ln_nbr , ref_ln_nbr , principalamount , actual_fee , calculated_fee , payment , customer_type , center , tran_type , tender_type , source , 
category , extract_date ) 
SELECT a.gtcmp, a.gtcntr, a.gttrnt, a.gttrdt , a.gtgln_nbr, b.gtgln_nbr AS reb_ln_nbr , c.gtgln_nbr AS ref_ln_nbr, a.principal_amount AS principalamount, a.fee AS actual_fee, 
CASE 
WHEN b.gtgln_nbr 
IS NULL THEN a.fee ELSE (a.fee - b.payment) 
END AS calculated_fee, 
CASE 
WHEN b.gtgln_nbr 
IS NULL THEN (a.principal_amount + a.fee) ELSE (a.principal_amount + a.fee) - b.payment 
END AS payment, a.customer_type, a.centers AS center, a.tran_type, a.tender_types AS tender_type, a.sources AS source, 
CASE 
WHEN a.gttrnt='BBK' AND c.gtgln_nbr 
IS NOT NULL AND b.gtgln_nbr 
IS NULL THEN 'BuyBack-REFI' ELSE a.categories 
END AS category, d.extract_date_id 
FROM temp_loans1 a 
LEFT OUTER JOIN temp_loans2 b ON a.gttrn_nbr=b.gttrn_nbr 
AND a.gtcntr=b.gtcntr 
AND a.gttrdt = b.gttrdt 
AND a.gtgln_nbr=b.gtgln_nbr 
LEFT OUTER JOIN temp_loans3 c ON a.gtgln_nbr=c.gtgln_nbr 
CROSS JOIN temp_dates d 
WHERE a.gtgln_nbr 
NOT IN ( 
SELECT DISTINCT LOAN 
FROM ACE_ODS..PDAPPL) 
UNION ALL SELECT a.gtcmp, a.gtcntr, a.gttrnt, a.gttrdt , a.gtgln_nbr, 
NULL AS reb_ln_nbr , 
NULL AS ref_ln_nbr, a.principal_amount AS principalamount, a.fee AS actual_fee, A.FEE AS calculated_fee, (a.principal_amount + a.fee) payment, a.customer_type, a.centers AS center, a.tran_type, 
CASE 
WHEN a.tender_types='Other' then 'Default' else a.tender_types 
end AS tender_type, a.sources AS source, a.categories AS category ,d.extract_date_id 
FROM temp_loans4 a 
CROSS JOIN temp_dates d 
WHERE a.gtgln_nbr 
NOT IN ( 
SELECT DISTINCT LOAN 
FROM ACE_ODS..PDAPPL);

v_irow_count := row_count;

call createauditevent(v_ietl_date, v_ietl_job_id, v_iseverity, v_irow_count, v_strsp_name, v_strpackage_name, v_strmessage);

v_irow_count := 0;

v_strmessage := 'Procedure Completed Successfully';

call createauditevent(v_ietl_date, v_ietl_job_id, v_iseverity, v_irow_count, v_strsp_name, v_strpackage_name, v_strmessage);

return (0);

exception WHEN others THEN v_strmessage := v_strmessage || sqlerrm;

call createauditevent(v_ietl_date, v_ietl_job_id, v_iseverity, v_irow_count, v_strsp_name, v_strpackage_name, v_strmessage);

raise exception '%' ,v_strmessage;

return (1);

end;

END_PROC;

