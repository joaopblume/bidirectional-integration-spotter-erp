--------------------------------------------------------
--  File created - Friday-October-11-2024   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for View VIEW_UPDATE_CONTACT_IN_CRM
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "APP_SCHEMA"."VIEW_UPDATE_CONTACT_IN_CRM" ("ID", "NAME", "EMAIL", "PHONE", "JOB_TITLE", "IS_PRIMARY", "LEAD_ID") AS 
  select
C.ID
,upper(C.NAME) as NAME
,upper(C.EMAIL) as EMAIL,
C.PHONE as PHONE,
upper(C.JOB_TITLE) as JOB_TITLE,
upper(C.IS_PRIMARY) as IS_PRIMARY,
L.ID AS LEAD_ID
from CONTACTS C, LEADS L
where C.CLIENT_CODE = L.CLIENT_CODE

minus 

select
con.ID_CRM as ID,
upper(con.NAME) as NAME
, upper(con.EMAIL) as EMAIL
, con.PHONE AS PHONE
, upper(con.JOB_TITLE) as JOB_TITLE
, upper(con.IS_PRIMARY) as IS_PRIMARY,
cli.ID_CRM as LEAD_ID
from VW_CRM_CLIENTS_CONTACTS CON, VW_CRM_CLIENTS CLI
where con.ID_CRM is not null
and CON.CLIENT_TAXID = CLI.TAXID
and con.ID_CRM in
(select id from contacts)

union

select
con.ID_CRM as ID,
upper(con.NAME) as NAME
, upper(con.EMAIL) as EMAIL
, con.PHONE AS PHONE
, upper(con.JOB_TITLE) as JOB_TITLE
, upper(con.IS_PRIMARY) as IS_PRIMARY,
cli.ID_CRM as LEAD_ID
from VW_CRM_CLIENTS_CONTACTS CON, VW_CRM_CLIENTS CLI
where CON.ID_CRM is not null
and CON.CLIENT_TAXID = CLI.TAXID
and con.ID_CRM in
(select id from contacts)

minus 

  select
C.ID
,upper(C.NAME) as NAME
,upper(C.EMAIL) as EMAIL,
C.PHONE as PHONE,
upper(C.JOB_TITLE) as JOB_TITLE,
upper(C.IS_PRIMARY) as IS_PRIMARY,
L.ID AS LEAD_ID
from CONTACTS C, LEADS L
where C.CLIENT_CODE = L.CLIENT_CODE
;
