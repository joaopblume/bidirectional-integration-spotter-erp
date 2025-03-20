create or replace package body APP_SCHEMA.PKG_CRM_CLIENT is

  -- Private type declarations
  function HAS_SERVICE_ORDER(P_CLIENT    in number
                            ,P_BRANCH    in number
                            ,P_BASE_DATE in date) return varchar2 is
    cursor C1 is
      select 1
        from SERVICE_ORDER_ITEM I
            ,SERVICE_ORDER      O
       where O.CLIENT = P_CLIENT
         and O.BRANCH = P_BRANCH
         and O.COMPANY_CODE = I.SOI_COMPANY_CODE
         and O.BRANCH_CODE = I.SOI_BRANCH_CODE
         and O.SALES_REP_CODE = I.SOI_SALES_REP_CODE
         and O.NUMBER = I.SOI_NUMBER
         and I.CREATE_DATE >= P_BASE_DATE
         and I.STATUS <> 'C';
    --     
    V1 number;
  begin
    open C1;
    fetch C1
      into V1;
    close C1;
    if V1 > 0 then
      return 'Y';
    end if;
    return 'N';
  end;

  function HAS_BUDGET(P_CLIENT    in number
                     ,P_BRANCH    in number
                     ,P_BASE_DATE in date) return varchar2 is
    cursor C1 is
      select 1
        from BUDGET_ITEM I
            ,BUDGET      O
       where O.CLIENT_BILLING_CODE = P_CLIENT
         and O.BRANCH_BILLING_CODE = P_BRANCH
         and O.NUMBER = I.BDG_NUMBER
         and I.DATE >= P_BASE_DATE
         and I.GENERATED <> 'N';
    V1 number;
  begin
    open C1;
    fetch C1
      into V1;
    close C1;
    if V1 > 0 then
      return 'Y';
    end if;
    return 'N';
  end;

  --
  function GET_BUDGET_VALUE(P_CLIENT    in number
                           ,P_BRANCH    in number
                           ,P_BASE_DATE in date) return number is
    cursor C1 is
      select sum(Q.QUANTITY / 1000 * DECODE(NVL(E.NEGOTIATED_PRICE, 0), 0, E.PRICE, E.NEGOTIATED_PRICE)) VALUE
        from BUDGET_ITEM               I
            ,BUDGET                    O
            ,BUDGET_ITEM_DELIVERY      IE
            ,BUDGET_ITEM_QUANTITY      Q
            ,BUDGET_ITEM_QUANT_DELIVERY E
       where O.CLIENT_BILLING_CODE = P_CLIENT
         and O.BRANCH_BILLING_CODE = P_BRANCH
         and O.NUMBER = I.BDG_NUMBER
         and Q.BIT_BDG_NUMBER = I.BDG_NUMBER
         and Q.BIT_ITEM = I.ITEM
         and Q.QUANTITY = (select min(QUANTITY)
                               from BUDGET_ITEM_QUANTITY Q2
                              where Q2.BIT_BDG_NUMBER = I.BDG_NUMBER
                                and Q2.BIT_ITEM = I.ITEM)
         and E.BID_BDG_NUMBER = Q.BIT_BDG_NUMBER
         and E.BID_ITEM = Q.BIT_ITEM
         and E.BID_QUANTITY = Q.QUANTITY
         and IE.BIT_BDG_NUMBER = I.BDG_NUMBER
         and IE.BIT_ITEM = I.ITEM
         and IE.BILLING_CLIENT = 'Y'
         and IE.APPROVAL_DATE is not null
         and E.CLIENT_CODE = IE.CLIENT_CODE
         and E.BRANCH_CODE = IE.BRANCH_CODE
         and TRUNC(I.DATE) between P_BASE_DATE and P_BASE_DATE + 365
         and I.GENERATED <> 'N';
    V1 number;
  begin
    open C1;
    fetch C1
      into V1;
    close C1;
    --
    if P_CLIENT in (1001) then
      return 10000;
    end if;
    return V1;
  end;

  --
  function HAS_ORDER(P_CLIENT    in number
                    ,P_BRANCH    in number
                    ,P_BASE_DATE in date) return varchar2 is
    cursor C1 is
      select 1
        from PERSON                 D
            ,SALES_ORDER            P
            ,SALES_ORDER_ITEM       I
       where D.CODE = P.PERS_CODE
         and P.COMPANY_CODE = I.SO_COMPANY_CODE
         and P.NUMBER = I.SO_NUMBER
         and I.ITEM_DATE >= P_BASE_DATE
         and D.KEY1 = P_CLIENT
         and D.KEY2 = P_BRANCH;
    --   
    V1 number;
  begin
    open C1;
    fetch C1
      into V1;
    close C1;
    if V1 > 0 then
      return 'Y';
    end if;
    return 'N';
  end;

  --
  function GET_ORDER_VALUE(P_CLIENT    in number
                          ,P_BRANCH    in number
                          ,P_BASE_DATE in date) return number is
    cursor C1 is
      select sum(I.ITEM_QTY * E.UNIT_VALUE) VALUE
        from PERSON                    D
            ,SALES_ORDER               P
            ,SALES_ORDER_ITEM          I
            ,SALES_ORDER_ITEM_DELIVERY E
       where D.CODE = P.PERS_CODE
         and P.COMPANY_CODE = I.SO_COMPANY_CODE
         and P.NUMBER = I.SO_NUMBER
         and I.SO_COMPANY_CODE = E.SOD_COMPANY_CODE
         and I.SO_NUMBER = E.SOD_NUMBER
         and I.SEQUENCE = E.SEQUENCE
         and E.BILLING_CLIENT = 'Y'
         and TRUNC(I.ITEM_DATE) between P_BASE_DATE and P_BASE_DATE + 365
         and D.KEY1 = P_CLIENT
         and D.KEY2 = P_BRANCH;
    --   
    V1 number;
  begin
    open C1;
    fetch C1
      into V1;
    close C1;
    --
    if P_CLIENT = 1002 then
      return 10000;
    end if;
    --
    return V1;
  end;

end PKG_CRM_CLIENT;
