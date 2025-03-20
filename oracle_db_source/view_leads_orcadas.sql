--------------------------------------------------------
--  File created - Friday-October-11-2024   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for View VIEW_LEADS_BUDGETED
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "APP_SCHEMA"."VIEW_LEADS_BUDGETED" ("ID", "BUDGET_VALUE") AS 
  SELECT l.id, e.budget_value
FROM leads l
JOIN vw_crm_clients e ON l.id = e.id_crm
WHERE e.budget_value IS NOT NULL
  AND COALESCE(l.budget_value, -1) <> e.budget_value
;
