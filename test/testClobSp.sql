CREATE SET TABLE GCFR_Execution_Log ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      Logger_Name VARCHAR(128) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      Logger_Id INTEGER NOT NULL,
      Message_Type INTEGER NOT NULL,
      Stream_Key SMALLINT,
      Stream_Id INTEGER,
      Process_Id INTEGER,
      TD_Session_Id INTEGER NOT NULL,
      Execution_Text VARCHAR(1024) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Calling_API VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL,
      Calling_API_Step CHAR(2) CHARACTER SET UNICODE NOT CASESPECIFIC,
      Sql_Activity_Count INTEGER,
      Sql_Text CLOB(1000000000) CHARACTER SET UNICODE,
      Update_Date DATE FORMAT 'YYYY-MM-DD' NOT NULL DEFAULT DATE ,
      Update_User VARCHAR(128) CHARACTER SET UNICODE NOT CASESPECIFIC NOT NULL DEFAULT USER ,
      Update_Ts TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6))
PRIMARY INDEX ( Logger_Name ,Logger_Id );;

REPLACE PROCEDURE GCFR_BB_ExecutionLog_Set
/* Stored Procedure Parameters */
(
      IN iLogger_Name            VARCHAR(128)
    , IN iLogger_Id              INTEGER
    , IN iMessage_Type           INTEGER
    , IN iStream_Key             SMALLINT
    , IN iStream_Id              INTEGER
    , IN iProcess_Id             INTEGER
    , IN iExecution_Text         VARCHAR(1024)
    , IN iCalling_API            VARCHAR(128)
    , IN iCalling_API_Step       CHAR(4)
    , IN iSql_Activity_Count     INTEGER
    , IN iSql_Text               CLOB(1000000000)
)
BEGIN
    /* Inserting a new execution log row using the given inputs */
    INSERT INTO GCFR_Execution_Log
    (
        Logger_Name
       ,Logger_Id
       ,Message_Type
       ,Stream_Key
       ,Stream_Id
       ,Process_Id
       ,Execution_Text
       ,TD_Session_Id
       ,Calling_API
       ,Calling_API_Step
       ,Sql_Activity_Count
       ,Sql_Text
       ,Update_Date
       ,Update_User
       ,Update_Ts
    )
    VALUES
    (
         :iLogger_Name
        ,:iLogger_Id
        ,:iMessage_Type
        ,:iStream_Key
        ,:iStream_Id
        ,:iProcess_Id
        ,:iExecution_Text
        ,SESSION /* Teradata Session Id */
        ,:iCalling_API
        ,:iCalling_API_Step
        ,:iSql_Activity_Count
        ,:iSql_Text
        ,CURRENT_DATE
        ,USER
        ,CURRENT_TIMESTAMP(6)
    )
    ;

END;
;;
