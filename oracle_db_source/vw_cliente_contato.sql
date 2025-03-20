--------------------------------------------------------
--  File created - Friday-October-11-2024   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for View VW_CRM_CLIENTS_CONTACTS
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "APP_SCHEMA"."VW_CRM_CLIENTS_CONTACTS" ("ID", "NAME", "EMAIL", "PHONE", "JOB_TITLE", "CLIENT_TAXID", "ID_CRM", "IS_PRIMARY") AS 
  select c.code id
      ,c.name
      ,c.email
      ,c.phone
      ,NVL(c.job_title_desc, g.title) job_title
      ,f.taxid CLIENT_TAXID
      ,c.ID_CRM
      ,c.is_primary
  from contact c
      ,branches f
      ,contact_job_titles g
where c.branch_client = f.client
  and c.branch_code = f.code
  and c.job_title_code = g.job_title_code (+)

;
