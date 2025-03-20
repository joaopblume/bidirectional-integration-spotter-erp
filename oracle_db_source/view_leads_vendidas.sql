--------------------------------------------------------
--  File created - Friday-October-11-2024   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for View VIEW_LEADS_SOLD
--------------------------------------------------------

  CREATE OR REPLACE FORCE EDITIONABLE VIEW "APP_SCHEMA"."VIEW_LEADS_SOLD" ("ID", "ORDER_VALUE", "SALES_REP_EMAIL") AS 
  select crm.id
    , erp.order_value
    , erp.sales_rep_email
from leads crm
join vw_crm_clients erp
on crm.id = erp.id_crm
WHERE erp.budget_value IS NOT NULL
and crm.stage <> 'Activation'
 AND COALESCE(crm.order_value, -1) <> erp.order_value
;
