--------------------------------------------------------
--  File created - Friday-October-11-2024   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Trigger TRG_LOG_EXECUTION_CRM
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE TRIGGER "APP_SCHEMA"."TRG_LOG_EXECUTION_CRM" 
BEFORE INSERT ON LOG_EXECUTION_CRM
FOR EACH ROW
BEGIN
  IF :NEW.LOG_ID IS NULL THEN
    SELECT SEQ_LOG_EXECUTION_CRM.NEXTVAL
    INTO :NEW.LOG_ID
    FROM DUAL;
  END IF;
END;
/
ALTER TRIGGER "APP_SCHEMA"."TRG_LOG_EXECUTION_CRM" ENABLE;
--------------------------------------------------------
--  DDL for Trigger TRG_CRM_UPDATE_CONTACT_ERP
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE TRIGGER "APP_SCHEMA"."TRG_CRM_UPDATE_CONTACT_ERP" AFTER
    INSERT OR UPDATE ON app_schema.contacts
    FOR EACH ROW
BEGIN
    PRC_UPDATE_CLIENT_CONTACTS(
         P_CLIENT => :NEW.CLIENT_CODE,
         P_NAME => :NEW.NAME,
         P_EMAIL => :NEW.EMAIL,
         P_PHONE => :NEW.PHONE,
         P_JOB_TITLE => :NEW.JOB_TITLE,
         P_IS_PRIMARY => :NEW.IS_PRIMARY,
         P_ID_CRM => :NEW.ID,
         P_OPERATION => CASE WHEN INSERTING THEN 'A'
    WHEN updating THEN
        'A'
    WHEN deleting THEN
        'D'
END
    );
END;
/
ALTER TRIGGER "APP_SCHEMA"."TRG_CRM_UPDATE_CONTACT_ERP" ENABLE;
--------------------------------------------------------
--  DDL for Trigger TRG_CRM_UPDATE_ERP
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE TRIGGER "APP_SCHEMA"."TRG_CRM_UPDATE_ERP" 
BEFORE INSERT OR UPDATE ON app_schema.leads
FOR EACH ROW
DECLARE
BEGIN
-- Call client update procedure
    PRC_UPDATE_CLIENTS(
        P_TAXID => lpad(:NEW.taxid,14,0),
        P_ID_CRM => :NEW.ID,
        P_COMPANY_NAME => :NEW.NAME,
        P_INDUSTRY => :NEW.INDUSTRY,
        P_TAX_REGISTRATION => :NEW.TAX_REGISTRATION,
        P_ADDRESS_STREET => :NEW.ADDRESS_STREET,
        P_ADDRESS_NUMBER => :NEW.ADDRESS_NUMBER,
        P_ADDRESS_COMPLEMENT => :NEW.ADDRESS_COMPLEMENT,
        P_ADDRESS_DISTRICT => :NEW.ADDRESS_DISTRICT,
        P_ADDRESS_ZIPCODE => :NEW.ADDRESS_ZIPCODE,
        P_CITY => :NEW.CITY,
        P_STATE => :NEW.STATE,
        P_PHONE => :NEW.PHONE,
        P_SALES_REP => :NEW.SALES_REP,
        P_INITIAL_LOAD => 'N',
        P_FINANCIAL_ANALYSIS => :NEW.FINANCIAL_ANALYSIS
    );

EXCEPTION
    WHEN OTHERS THEN
            pkg_error_mgr.log_error (
            'ERROR - INSERT_LEAD -> ' ||
            'TAXID: ' || :NEW.taxid ||
            ' - ID: ' || :NEW.ID ||
            ' - Name: ' || :NEW.NAME ||
            ' - Industry: ' || :NEW.INDUSTRY ||
            ' - Tax Registration: ' || :NEW.TAX_REGISTRATION ||
            ' - Address Street: ' || :NEW.ADDRESS_STREET ||
            ' - Address Number: ' || :NEW.ADDRESS_NUMBER ||
            ' - Address Complement: ' || :NEW.ADDRESS_COMPLEMENT ||
            ' - Address District: ' || :NEW.ADDRESS_DISTRICT ||
            ' - Address Zipcode: ' || :NEW.ADDRESS_ZIPCODE ||
            ' - City: ' || :NEW.CITY ||
            ' - State: ' || :NEW.STATE ||
            ' - Phone: ' || :NEW.PHONE ||
            ' - Sales Rep: ' || :NEW.SALES_REP ||
            ' - ERROR: ' || SQLERRM || 
            ' (Code: ' || SQLCODE || ')'
            );  
            raise_application_error(-20020, 'ERROR, '||SQLCODE||' - '||sqlerrm);
            
END;
/
ALTER TRIGGER "APP_SCHEMA"."TRG_CRM_UPDATE_ERP" ENABLE;
