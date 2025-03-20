--------------------------------------------------------
--  DDL for View VIEW_INSERT_LEAD_IN_CRM
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "APP_SCHEMA"."VIEW_INSERT_LEAD_IN_CRM" ("NAME", "TAXID", "ADDRESS", "DISTRICT", "ZIPCODE", "NUMBER", "CITY", "PHONE", "COMPLEMENT", "STATE", "COUNTRY", "INDUSTRY", "SALES_REP", "CLIENT_CODE", "TAX_REGISTRATION") AS 
  select COMPANY_NAME as name,
    TAXID as taxid,
    ADDRESS_STREET as address,
    ADDRESS_DISTRICT as district,
    ADDRESS_ZIPCODE as zipcode,
    ADDRESS_NUMBER as NUMBER,
    CITY as city,
    PHONE as phone,
    ADDRESS_COMPLEMENT as COMPLEMENT,
    STATE as state,
    COUNTRY as country,
    INDUSTRY as industry,
    SALES_REP as sales_rep,
    CLIENT_CODE AS CLIENT_CODE,
    TAX_REGISTRATION
    from VW_CRM_CLIENTS where INTEGRATE_CRM = 'Y' AND ID_CRM IS NULL;
