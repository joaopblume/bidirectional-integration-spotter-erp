--------------------------------------------------------
--  File created - Friday-October-11-2024   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for View VIEW_UPDATE_LEAD_IN_CRM
--------------------------------------------------------

CREATE OR REPLACE FORCE EDITIONABLE VIEW "APP_SCHEMA"."VIEW_UPDATE_LEAD_IN_CRM" ("ID", "NAME", "TAXID", "ADDRESS_STREET", "ADDRESS_ZIPCODE", "ADDRESS_NUMBER", "ADDRESS_COMPLEMENT", "ADDRESS_DISTRICT", "CITY", "STATE", "SALES_REP", "CLIENT_CODE", "TAX_REGISTRATION", "FINANCIAL_ANALYSIS") AS 
select
    ID,
    upper(NAME) as NAME,
    lpad(TAXID, 14, '0') as TAXID,
    upper(ADDRESS_STREET) as ADDRESS_STREET,
    ADDRESS_ZIPCODE,
    ADDRESS_NUMBER,
    upper(ADDRESS_COMPLEMENT) as ADDRESS_COMPLEMENT,
    upper(ADDRESS_DISTRICT) as ADDRESS_DISTRICT,
    upper(CITY) as CITY,
    upper(STATE) as STATE,
    SALES_REP,
    CLIENT_CODE,
    TAX_REGISTRATION,
    FINANCIAL_ANALYSIS
from LEADS
where STAGE in ('Development/Budget', 'Activation', 'Retention/Expansion')

minus 

select
    ID_CRM as ID,
    upper(COMPANY_NAME) as NAME,
    lpad(TAXID, 14, '0') as TAXID,
    upper(ADDRESS_STREET) as ADDRESS_STREET,
    ADDRESS_ZIPCODE,
    ADDRESS_NUMBER,
    upper(ADDRESS_COMPLEMENT) as ADDRESS_COMPLEMENT,
    upper(ADDRESS_DISTRICT) as ADDRESS_DISTRICT,
    upper(CITY) as CITY,
    upper(STATE) as STATE,
    SALES_REP,
    CLIENT_CODE,
    TAX_REGISTRATION,
    FINANCIAL_ANALYSIS
from VW_CRM_CLIENTS
where ID_CRM is not null
AND ID_CRM IN 
    (SELECT ID FROM LEADS)

union

select
    ID_CRM as ID,
    upper(COMPANY_NAME) as NAME,
    lpad(TAXID, 14, '0') as TAXID,
    upper(ADDRESS_STREET) as ADDRESS_STREET,
    ADDRESS_ZIPCODE,
    ADDRESS_NUMBER,
    upper(ADDRESS_COMPLEMENT) as ADDRESS_COMPLEMENT,
    upper(ADDRESS_DISTRICT) as ADDRESS_DISTRICT,
    upper(CITY) as CITY,
    upper(STATE) as STATE,
    SALES_REP,
    CLIENT_CODE,
    TAX_REGISTRATION,
    FINANCIAL_ANALYSIS
from VW_CRM_CLIENTS
where ID_CRM is not null
AND ID_CRM IN 
    (SELECT ID FROM LEADS)

minus 

select
    ID,
    upper(NAME) as NAME,
    lpad(TAXID, 14, '0') as TAXID,
    upper(ADDRESS_STREET) as ADDRESS_STREET,
    ADDRESS_ZIPCODE,
    ADDRESS_NUMBER,
    upper(ADDRESS_COMPLEMENT) as ADDRESS_COMPLEMENT,
    upper(ADDRESS_DISTRICT) as ADDRESS_DISTRICT,
    upper(CITY) as CITY,
    upper(STATE) as STATE,
    SALES_REP,
    CLIENT_CODE,
    TAX_REGISTRATION,
    FINANCIAL_ANALYSIS
from LEADS
where STAGE in ('Development/Budget', 'Activation', 'Retention/Expansion')
;
