--------------------------------------------------------
--  File created - Friday-October-11-2024   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure PRC_UPDATE_CLIENTS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "APP_SCHEMA"."PRC_UPDATE_CLIENTS" (P_TAXID              varchar2
                                                       ,P_ID_CRM             number
                                                       ,P_COMPANY_NAME       varchar2
                                                       ,P_INDUSTRY           varchar2
                                                       ,P_TAX_REGISTRATION   varchar2
                                                       ,P_ADDRESS_STREET     varchar2
                                                       ,P_ADDRESS_NUMBER     in number
                                                       ,P_ADDRESS_COMPLEMENT in varchar2
                                                       ,P_ADDRESS_DISTRICT   in varchar2
                                                       ,P_ADDRESS_ZIPCODE    in number
                                                       ,P_CITY               varchar2
                                                       ,P_STATE              varchar2
                                                       ,P_PHONE              number
                                                       ,P_SALES_REP          number
                                                       ,P_INITIAL_LOAD       varchar2
                                                       ,P_FINANCIAL_ANALYSIS varchar2) is
  cursor C1 is
    select *
      from APP_SCHEMA.VW_CRM_CLIENTS C
     where LTRIM(LPAD(C.TAXID, 14, '0')) = P_TAXID;
  V_UPDATED varchar2(1) := 'N';
  --
  V_CLIENT_CODE number;
  --
  cursor C_CITIES is
    select C.ID           CITY_CODE
          ,C.CITY_NAME    CITY_NAME
          ,C.STATE_CODE
          ,P.CODE         COUNTRY_CODE
          ,P.NAME         COUNTRY_NAME
      from CITIES C
          ,COUNTRIES P
          ,STATES E
     where UPPER(UTL_PKG.REMOVE_ACCENTS(C.CITY_NAME)) = UPPER(UTL_PKG.REMOVE_ACCENTS(P_CITY))
       and C.STATE_CODE = E.CODE
       and UPPER(UTL_PKG.REMOVE_ACCENTS(E.NAME)) = UPPER(UTL_PKG.REMOVE_ACCENTS(P_STATE))
       and P.CODE = C.COUNTRY_CODE;
  --   
  R_CITY C_CITIES%rowtype;
  --
begin
  if P_INITIAL_LOAD = 'S' then
    update BRANCHES
       set ID_CRM      = P_ID_CRM
          ,INTEGRATE_CRM = 'S'
     where TAXID = P_TAXID;
  else
    open C_CITIES;
    fetch C_CITIES
      into R_CITY;
    close C_CITIES;
    DBMS_OUTPUT.PUT_LINE('CITY:' || R_CITY.CITY_CODE || '-' || R_CITY.STATE_CODE || '-' || R_CITY.COUNTRY_CODE);
    for R in C1 loop
      begin
        V_UPDATED := 'S';
        if UPPER(R.COMPANY_NAME) <> UPPER(P_COMPANY_NAME) or UPPER(R.ADDRESS_STREET) <> UPPER(P_ADDRESS_STREET) or R.ADDRESS_NUMBER <> P_ADDRESS_NUMBER or
           UPPER(R.ADDRESS_COMPLEMENT) <> UPPER(P_ADDRESS_COMPLEMENT) or UPPER(R.ADDRESS_DISTRICT) <> UPPER(P_ADDRESS_DISTRICT) or
           UPPER(R_CITY.CITY_NAME) || '-' || R_CITY.STATE_CODE <> UPPER(P_CITY) || '-' || P_STATE or R.ADDRESS_ZIPCODE <> P_ADDRESS_ZIPCODE then
          
          update ERP_SCHEMA.BRANCHES F
             set F.TAX_REGISTRATION = UTL_PKG.FORMAT_TAX_REGISTRATION(P_TAX_REGISTRATION, NVL(R_CITY.STATE_CODE, 'XX'))
           where F.CLIENT = R.CLIENT_CODE
             and F.CODE = R.BRANCH_CODE;
        end if;
        
        if R.SALES_REP <> P_SALES_REP or R.PHONE <> P_PHONE then
          update ERP_SCHEMA.BRANCHES F
             set F.SALES_REP = P_SALES_REP
                ,F.PHONE     = P_PHONE
           where F.CLIENT = R.CLIENT_CODE
             and F.CODE = R.BRANCH_CODE;
        end if;
        if UPPER(UTL_PKG.REMOVE_ACCENTS(R.INDUSTRY)) <> UPPER(UTL_PKG.REMOVE_ACCENTS(P_INDUSTRY)) then
          begin
            update ERP_SCHEMA.CLIENT_INDUSTRIES C
               set C.INDUSTRY = NVL((select min(M.INDUSTRY)
                                     from ERP_SCHEMA.INDUSTRIES M
                                    where UPPER(UTL_PKG.REMOVE_ACCENTS(M.DESCRIPTION)) = UPPER(UTL_PKG.REMOVE_ACCENTS(P_INDUSTRY))),
                                   10)
             where C.CLIENT = R.CLIENT_CODE
               and C.BRANCH = R.BRANCH_CODE;
          exception
            when DUP_VAL_ON_INDEX then
              null;
          end;
        end if;
        update ERP_SCHEMA.BRANCHES F
           set F.INTEGRATE_CRM = 'S'
              ,ID_CRM        = P_ID_CRM
         where F.CLIENT = R.CLIENT_CODE
           and F.CODE = R.BRANCH_CODE
           and (NVL(F.INTEGRATE_CRM, 'N') = 'N' or ID_CRM is null);
      exception
        when others then
          RAISE_APPLICATION_ERROR(-20012, 'Error updating client: ' || sqlerrm);
      end;
      
      if P_FINANCIAL_ANALYSIS is not null then
        declare
          V1 number;
        begin
          select NVL(max(SEQUENCE), 0) + 1
            into V1
            from CREDIT_LIMIT_HISTORY
           where CLIENT_CODE = R.CLIENT_CODE;
          
          insert into ERP_SCHEMA.CREDIT_LIMIT_HISTORY
            (CLIENT_CODE
            ,SEQUENCE
            ,DATE
            ,OBSERVATIONS
            ,USER)
          values
            (R.CLIENT_CODE
            ,V1
            ,sysdate
            ,P_FINANCIAL_ANALYSIS
            ,user);
        exception
          when DUP_VAL_ON_INDEX then
            null;
        end;
      end if;
      
    end loop;
    if V_UPDATED = 'N' then
      declare
        cursor C_IND is
          select M.INDUSTRY
            from ERP_SCHEMA.INDUSTRIES M
           where UPPER(UTL_PKG.REMOVE_ACCENTS(M.DESCRIPTION)) = UPPER(UTL_PKG.REMOVE_ACCENTS(P_INDUSTRY));
        V_IND ERP_SCHEMA.INDUSTRIES.INDUSTRY%type;
      begin
        V_CLIENT_CODE := ERP_SCHEMA.GET_CLIENT_CODE;
        insert into ERP_SCHEMA.CLIENTS
          (CLIENT_CODE
          ,COMPANY_NAME
          ,CLIENT_TYPE
          ,TRADING_NAME
          ,ADDRESS
          ,DISTRICT
          ,CITY
          ,STATE_CODE
          ,ZIPCODE
          ,COUNTRY
          ,CREDIT_LIMIT
          ,IS_INTERCOMPANY
          ,PERSON_TYPE
          ,PLANT_CODE
          ,COUNTRY_CODE
          ,ACCOUNTING_INDICATOR
          ,COST_CENTER
          ,PROTEST_INDICATOR
          ,INTEREST_INDICATOR
          ,LETTER_PROTEST_INDICATOR
          ,LETTER_INTEREST_INDICATOR
          ,ACCOUNT_LEVEL
          ,PAY_MONDAY
          ,PAY_TUESDAY
          ,PAY_WEDNESDAY
          ,PAY_THURSDAY
          ,PAY_FRIDAY
          ,PAY_ADVANCE_RETREAT
          ,PAY_SATURDAY
          ,PAY_SUNDAY
          ,PRICE_CALCULATION_TYPE
          ,FINANCIAL_COST_INDICATOR
          ,HAS_EDI
          ,COMMISSION_TYPE
          ,CREDIT_LIMIT_INDICATOR
          ,SALE_TYPE
          ,COMMERCIAL_DELIVERIES
          ,BLOCK_BILLING
          ,SHOW_CALCULATED_PRICE_REP
          ,NUMBER
          ,COMPLEMENT
          ,CREATED_BY
          ,CREATED_ON)
        values
          (V_CLIENT_CODE
          ,UPPER(P_COMPANY_NAME)
          ,1
          ,UPPER(P_COMPANY_NAME)
          ,NVL(UPPER(P_ADDRESS_STREET), 'X')
          ,NVL(UPPER(P_ADDRESS_DISTRICT), 'X')
          ,NVL(R_CITY.CITY_CODE, 10000)
          ,NVL(R_CITY.STATE_CODE, 'XX')
          ,P_ADDRESS_ZIPCODE
          ,NVL(R_CITY.COUNTRY_CODE, 'US')
          ,999999999999.99
          ,'S'
          ,'02'
          ,'DUP'
          ,'US'
          ,0
          ,0
          ,'S'
          ,'S'
          ,'S'
          ,'S'
          ,1
          ,'S'
          ,'S'
          ,'S'
          ,'S'
          ,'S'
          ,'A'
          ,'S'
          ,'S'
          ,1
          ,'N'
          ,'N'
          ,0
          ,2
          ,'N'
          ,'N'
          ,'N'
          ,'N'
          ,P_ADDRESS_NUMBER
          ,P_ADDRESS_COMPLEMENT
          ,'CRM'
          ,sysdate);
        
        if P_FINANCIAL_ANALYSIS is not null then
          begin
            insert into ERP_SCHEMA.CREDIT_LIMIT_HISTORY
              (CLIENT_CODE
              ,SEQUENCE
              ,DATE
              ,OBSERVATIONS
              ,USER)
            values
              (V_CLIENT_CODE
              ,1
              ,sysdate
              ,P_FINANCIAL_ANALYSIS
              ,user);
          exception
            when DUP_VAL_ON_INDEX then
              null;
          end;
        end if;
        
        insert into ERP_SCHEMA.BRANCHES
          (CLIENT
          ,CODE
          ,DESCRIPTION
          ,ADDRESS
          ,DISTRICT
          ,ZIPCODE
          ,CITY
          ,STATE
          ,COUNTRY
          ,SALES_REP
          ,PHONE
          ,CONTACT
          ,TAXID
          ,TAX_REGISTRATION
          ,POSTAL_BOX
          ,EMAIL
          ,ACTIVE
          ,LAST_UPDATE_USER
          ,LAST_UPDATE_DATE
          ,FAIN_CODE
          ,HAS_EDI
          ,CHECK_OLD_ORDER_REFERENCE
          ,NUMBER
          ,COMPLEMENT
          ,SEND_INVOICE_WS
          ,CREATED_BY
          ,CREATED_ON
          ,PRINT_PALLET_LABEL
          ,DEFAULT_UNIT_MEASURE
          ,USE_ADDITIONAL_REFERENCE
          ,DEFAULT_INVOICE_ITEMS_ORDER
          ,TAXID_STATUS
          ,QUALITY_STANDARD
          ,ALLOW_SALES_SUPP_ITEM
          ,REQUIRE_DUN_LABEL
          ,TOLERANCE_DISPLAY_TYPE
          ,ALLOW_QUALITY_CORRECTIONS
          ,EXTERNAL_CODE
          ,CLIENT_APPROVAL_PDF
          ,CLIENT_APPROVAL_AB
          ,ID_CRM
          ,INTEGRATE_CRM)
        values
          (V_CLIENT_CODE
          ,1
          ,UPPER(P_COMPANY_NAME)
          ,NVL(UPPER(P_ADDRESS_STREET), 'X')
          ,NVL(UPPER(P_ADDRESS_DISTRICT), 'X')
          ,P_ADDRESS_ZIPCODE
          ,NVL(R_CITY.CITY_CODE, 10000)
          ,NVL(R_CITY.STATE_CODE, 'XX')
          ,NVL(R_CITY.COUNTRY_CODE, 'US')
          ,NVL(P_SALES_REP, 99)
          ,P_PHONE
          ,'CRM'
          ,P_TAXID
          ,UTL_PKG.FORMAT_TAX_REGISTRATION(NVL(P_TAX_REGISTRATION, 'EXEMPT'), NVL(R_CITY.STATE_CODE, 'XX'))
          ,null
          ,null
          ,'S'
          ,'CRM'
          ,sysdate
          ,1
          ,'N'
          ,'S'
          ,1
          ,1
          ,0
          ,'CRM'
          ,sysdate
          ,'S'
          ,'MI'
          ,'N'
          ,'DITB_DITE_CODE'
          ,'A'
          ,'STANDARD'
          ,'N'
          ,'N'
          ,'P'
          ,0
          ,null
          ,'I'
          ,'I'
          ,P_ID_CRM
          ,'S');
        
        begin
          insert into SALES_REP_BRANCHES
            (CLIENT_CODE
            ,BRANCH_CODE
            ,SALES_REP_CODE
            ,EFFECTIVE_DATE
            ,FROM_TO_CLIENT
            ,FROM_TO_BRANCH)
          values
            (V_CLIENT_CODE
            ,1
            ,P_SALES_REP
            ,sysdate
            ,V_CLIENT_CODE
            ,1);
        exception
          when DUP_VAL_ON_INDEX then
            null;
        end;
        
        begin
          insert into BILLING_ADDRESSES
            (BILLING_ADDRESS_SEQ
            ,CLIENT_CODE
            ,BRANCH_CODE
            ,BILLING_ADDRESS
            ,BILLING_ZIPCODE
            ,CITY_CODE
            ,STATE_CODE
            ,COUNTRY_CODE
            ,BILLING_DISTRICT
            ,BILLING_PHONE
            ,EMAIL
            ,COMPLEMENT
            ,NUMBER)
          values
            (1
            ,V_CLIENT_CODE
            ,1
            ,P_ADDRESS_STREET
            ,P_ADDRESS_ZIPCODE
            ,NVL(R_CITY.CITY_CODE, 10000)
            ,NVL(R_CITY.STATE_CODE, 'XX')
            ,NVL(R_CITY.COUNTRY_CODE, 'US')
            ,UPPER(P_ADDRESS_DISTRICT)
            ,P_PHONE
            ,null
            ,P_ADDRESS_COMPLEMENT
            ,P_ADDRESS_NUMBER);
        exception
          when others then
            null;
        end;
        
        open C_IND;
        fetch C_IND
          into V_IND;
        close C_IND;
        
        if V_IND is not null then
          insert into ERP_SCHEMA.CLIENT_INDUSTRIES
            (CLIENT
            ,BRANCH
            ,INDUSTRY
            ,STATUS
            ,ACTIVATION_DATE)
          values
            (V_CLIENT_CODE
            ,1
            ,NVL(V_IND, 10)
            ,'A'
            ,sysdate);
        end if;
      exception
        when DUP_VAL_ON_INDEX then
          null;
        when others then
          RAISE_APPLICATION_ERROR(-20001, 'Error adding client: ' || sqlerrm);
      end;
    end if;
  end if;
end;

/
--------------------------------------------------------
--  DDL for Procedure PRC_UPDATE_CLIENT_CONTACTS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "APP_SCHEMA"."PRC_UPDATE_CLIENT_CONTACTS" (P_CLIENT           number
                                                               ,P_NAME              varchar2
                                                               ,P_EMAIL             varchar2
                                                               ,P_PHONE             number
                                                               ,P_JOB_TITLE         varchar2
                                                               ,P_IS_PRIMARY        varchar2
                                                               ,P_ID_CRM            number
                                                               ,P_OPERATION         in varchar2) is
  cursor C1 is
    select *
      from APP_SCHEMA.VW_CRM_CLIENTS_CONTACTS C
     where C.EMAIL = P_EMAIL;
  V_UPDATED varchar2(1) := 'N';
begin
  /*operation = A : add or update --  D - delete */
  if NVL(P_OPERATION, 'A') <> 'D' then
    if P_CLIENT is not null then
      for R in C1 loop
        update CONTACT_SCHEMA.CONTACTS C
           set C.NAME              = P_NAME
              ,C.PHONE             = P_PHONE
              ,C.JOB_TITLE_DESC    = UPPER(P_JOB_TITLE)
              ,C.IS_PRIMARY        = P_IS_PRIMARY
              ,C.ID_CRM            = P_ID_CRM
         where LOWER(C.EMAIL) = LOWER(P_EMAIL)
            or ID_CRM = P_ID_CRM;
        V_UPDATED := 'S';
      end loop;
      if V_UPDATED = 'N' then
        begin
          insert into CONTACT_SCHEMA.CONTACTS
            (NAME
            ,EMAIL
            ,PHONE
            ,JOB_TITLE_DESC
            ,BRANCH_CLIENT
            ,IS_PRIMARY
            ,ID_CRM)
          values
            (P_NAME
            ,LOWER(P_EMAIL)
            ,P_PHONE
            ,P_JOB_TITLE
            ,P_CLIENT
            ,P_IS_PRIMARY
            ,P_ID_CRM);
        exception
          when DUP_VAL_ON_INDEX then
            null;
          when others then
            RAISE_APPLICATION_ERROR(-20001, 'Error adding client contact: ' || sqlerrm);
        end;
      end if;
    end if;
  else
    delete CONTACT_SCHEMA.CONTACTS C
     where C.ID_CRM = P_ID_CRM
        or LOWER(C.EMAIL) = LOWER(P_EMAIL);
  end if;
end;

/
--------------------------------------------------------
--  DDL for Procedure PRC_STORE_CRM_CLIENT_ID
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "APP_SCHEMA"."PRC_STORE_CRM_CLIENT_ID" (P_TAXID       varchar2
                                                          ,P_ID_CRM number) is
begin
  update BRANCHES
     set ID_CRM      = P_ID_CRM
        ,INTEGRATE_CRM = 'S'
   where TAXID = P_TAXID;
end;

/
--------------------------------------------------------
--  DDL for Procedure PRC_STORE_CRM_CONTACT_ID
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "APP_SCHEMA"."PRC_STORE_CRM_CONTACT_ID" (P_EMAIL      varchar2
                                                               ,P_ID_CRM number) is
begin
  update CONTACT_SCHEMA.CONTACTS C
     set C.ID_CRM = P_ID_CRM
   where LOWER(C.EMAIL) = LOWER(P_EMAIL);
end;

/
