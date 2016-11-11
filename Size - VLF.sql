SET NOCOUNT ON

DECLARE @CURRENT_DB_ONLY BIT = 0
DECLARE @USER_DBS_ONLY BIT = 0
DECLARE @SYSTEM_DBS_ONLY BIT = 0
DECLARE @DB_INCLUDE_LIST NVARCHAR(1000) = '*'--'TMSDEV, FSSDev, ENTERPRISEWATCH'
DECLARE @DB_EXCLUDE_LIST NVARCHAR(1000) = '*'

DECLARE @sqlmajorver int =  CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff);


DECLARE @NOW DATETIME 
SET @NOW =  GETDATE()   --MAKE SURE ALL RECORDS IN THE BATCH ARE INSERTED WITH SAME VALUE

DECLARE @SERVER_INSTANCE SYSNAME 
SET @SERVER_INSTANCE = CAST(SERVERPROPERTY('SERVERNAME') AS SYSNAME)

---------------------------------------------------------------------------------------------------------

IF OBJECT_ID(N'TEMPDB..#db_list') is not null DROP TABLE #db_list
IF OBJECT_ID(N'TEMPDB..#INCLUDED_DBS') is not null DROP TABLE #INCLUDED_DBS
IF OBJECT_ID(N'TEMPDB..#EXCLUDED_DBS') is not null DROP TABLE #EXCLUDED_DBS

create table #db_list (ROW_NUM int identity (1, 1), name sysname)
create table #INCLUDED_DBS  ( DB_NAME SYSNAME )
create table #EXCLUDED_DBS ( DB_NAME SYSNAME)

/* DEAL WITH CSV LISTS*/
SET @DB_INCLUDE_LIST = upper(ltrim(rtrim(@DB_INCLUDE_LIST)))

IF @DB_INCLUDE_LIST IS NULL OR @DB_INCLUDE_LIST = ''  OR @DB_INCLUDE_LIST = 'ALL' OR @DB_INCLUDE_LIST = '*'  OR @DB_INCLUDE_LIST = 'NULL' 
      BEGIN  
         SET @DB_INCLUDE_LIST = '*'  
      END 

      IF @DB_INCLUDE_LIST <>'*'
         BEGIN
            INSERT INTO #INCLUDED_DBS
               select ltrim(rtrim(item ))
               from (
                     select item = y.i.value('(./text())[1]', 'nvarchar(4000)')
                     from 
                     ( 
                       select x = convert(xml, '<i>' 
                         + replace(@DB_INCLUDE_LIST, ',', '</i><i>') 
                         + '</i>').query('.')
                     ) as a cross apply x.nodes('i') as y(i) ) x

         END

SET @DB_EXCLUDE_LIST = upper(ltrim(rtrim(@DB_EXCLUDE_LIST)))

IF @DB_EXCLUDE_LIST IS NULL OR @DB_EXCLUDE_LIST = ''  OR @DB_EXCLUDE_LIST = 'ALL' OR @DB_EXCLUDE_LIST = '*'  OR @DB_EXCLUDE_LIST = 'NULL' 
      BEGIN  
         SET @DB_EXCLUDE_LIST = '*'  
      END 

      IF @DB_EXCLUDE_LIST <>'*'
         BEGIN
            INSERT INTO #EXCLUDED_DBS
               select ltrim(rtrim(item ))
               from (
                     select item = y.i.value('(./text())[1]', 'nvarchar(4000)')
                     from 
                     ( 
                       select x = convert(xml, '<i>' 
                         + replace(@DB_EXCLUDE_LIST, ',', '</i><i>') 
                         + '</i>').query('.')
                     ) as a cross apply x.nodes('i') as y(i) ) x
         END


insert into #db_list (name)
   select db.name 
   from sys.databases db
   where state_desc = 'online'
   AND (@CURRENT_DB_ONLY = 1 AND NAME = DB_NAME() OR @CURRENT_DB_ONLY = 0)
   AND ((@USER_DBS_ONLY = 1 AND NAME NOT IN ('MASTER', 'MODEL', 'MSDB', 'TEMPDB', 'DISTRIBUTION')) OR @USER_DBS_ONLY = 0)
   AND ((@SYSTEM_DBS_ONLY = 1 AND NAME  IN ('MASTER', 'MODEL', 'MSDB', 'DISTRIBUTION')) OR @SYSTEM_DBS_ONLY = 0)
   AND ((@DB_INCLUDE_LIST <> '*' AND NAME IN (SELECT DB_NAME FROM #INCLUDED_DBS)) OR @DB_INCLUDE_LIST = '*')
   AND ((@DB_EXCLUDE_LIST <> '*' AND NAME NOT IN (SELECT DB_NAME FROM #EXCLUDED_DBS)) OR @DB_EXCLUDE_LIST = '*')
   ORDER BY DB.NAME

DECLARE @COUNTER INT 
DECLARE @MAX_COUNTER INT
DECLARE @DB SYSNAME
SELECT @COUNTER = MIN(ROW_NUM), @MAX_COUNTER = MAX(ROW_NUM)
FROM #db_list
------------------------------------------------------------------------------------------------
IF OBJECT_ID(N'TEMPDB..#VLF_TEMP') is not null DROP TABLE #VLF_TEMP
IF OBJECT_ID(N'TEMPDB..#VLF_TEMP2') is not null DROP TABLE #VLF_TEMP2
IF OBJECT_ID(N'TEMPDB..#VLF_TEMP3') is not null DROP TABLE #VLF_TEMP3

 
CREATE TABLE #VLF_TEMP
(
      RecoveryUnitID int
    , FileId         varchar(3)
    , FileSize       numeric(20,0)
    , StartOffset    bigint
    , FSeqNo         bigint
    , Status         char(1)
    , Parity         varchar(4)
    , CreateLSN      numeric(25,0)
)
  
CREATE TABLE #VLF_TEMP3
(
      Server_Instance nvarchar(128)
    , DB_name         nvarchar(128)
    , FileId          varchar(3)
    , FileSize        numeric(20,0)
    , StartOffset     bigint
    , FSeqNo          bigint
    , Status          char(1)
    , Parity          varchar(4)
    , CreateLSN       numeric(25,0)
)
  

CREATE TABLE #VLF_TEMP2
(
      Server_Instance nvarchar(128)
    , DB_name         nvarchar(128)
    , FileId          varchar(3)
    , Status          int
    , Total_VLF_MB    decimal(38,3)
    , Avg_VLF_MB      decimal(38,3)
    , Max_VLF_MB      decimal(38,3)
    , Min_VLF_MB      decimal(38,3)
    , Vlf_Count       int
)
DECLARE @SQLCMD NVARCHAR(200)

------------------------------------------------------------------------------------------------
WHILE @COUNTER <= @MAX_COUNTER
BEGIN

   SELECT @DB = NAME FROM #db_list WHERE ROW_NUM = @COUNTER

   SELECT @SQLCMD ='DBCC LOGINFO(''' + @DB + ''')'


			IF @sqlmajorver < 11
			BEGIN
				INSERT INTO #VLF_TEMP (FileId, FileSize, StartOffset, FSeqNo, [Status], Parity, CreateLSN)
				EXEC(@SQLCMD)
			END
			ELSE
			BEGIN
				INSERT INTO #VLF_TEMP (RecoveryUnitID, FileId, FileSize, StartOffset, FSeqNo, [Status], Parity, CreateLSN)
				EXEC(@SQLCMD)
			END
          

   insert into #VLF_TEMP3 
      select  @SERVER_INSTANCE AS Server_Instance
          , @DB 
          , FileId
          , FileSize
          , StartOffset
          , FSeqNo
          , Status
          , Parity
          , CreateLSN
      FROM #VLF_TEMP

   INSERT INTO #VLF_TEMP2
         select 
              @SERVER_INSTANCE AS Server_Instance
            , @DB
            , fileid as File_ID
            , '' as status
            , CAST(SUM(FILESIZE)/1024.0/1024 AS DECIMAL(38, 3)) AS total_VLF_MB
            , CAST(AVG(FILESIZE)/1024.0/1024 AS DECIMAL(38, 3)) AS   AVG_VLF_MB
            , CAST(max(filesize)/1024.0/1024 AS DECIMAL(38, 3)) as   max_VLF_MB
            , CAST(min(filesize)/1024.0/1024 AS DECIMAL(38, 3)) as   min_VLF_MB
            , count(*) as vlf_count
         FROM #VLF_TEMP 
        -- WHERE STATUS = 2
         GROUP BY FileID--, status

   TRUNCATE TABLE #VLF_TEMP

   SET @COUNTER = @COUNTER + 1
END

SELECT 'Grain - File' as Output_type
    , @NOW AS Capture_Datetime
    , Server_Instance
    , DB_name
    , FileId
    , vlf_count 
    , total_VLF_MB 
    , avg_VLF_MB
    , min_VLF_MB
    , max_VLF_MB
FROM #VLF_TEMP2

select  'Grain - File/Status' as Output_type
    , GETDATE() AS Collection_Datetime
    , Server_Instance
    , DB_name
    , FileId
    , Status   
    , CASE when Status = 2 then 'Active - Not Reusable' 
           when Status = 0 then 'Empty - Reusable'
      end as Status_Desc
    , MIN(FSeqNo) AS Min_FSeqNo
    , MAX(FSeqNo) AS Max_FSeqNo
    , COUNT(*)    AS VLF_Count
    , cast(SUM(FileSize/1024.0/1024) as decimal(38, 3)) AS Total_VLF_MB
    , cast(avg(FileSize/1024.0/1024) as decimal(38, 3)) AS Avg_VLF_MB
    , cast(min(FileSize/1024.0/1024) as decimal(38, 3)) AS Min_VLF_MB
    , cast(max(FileSize/1024.0/1024) as decimal(38, 3)) AS Max_VLF_MB
from #VLF_TEMP3
GROUP BY 
   Server_Instance
   , DB_Name 
   , FileID
   , Status
   , CASE when Status = 2 then 'Active - Not Reusable' 
              when Status = 0 then 'Empty - Reusable'
         end
ORDER BY 
   DB_Name 
   , FileID
   , Status
   , CASE when Status = 2 then 'Active - Not Reusable' 
              when Status = 0 then 'Empty - Reusable'
         end

select  'Grain - VLF Sizes' as Output_type
    , GETDATE() AS Collection_Datetime
    , Server_Instance
    , DB_name
    , FileId
    , cast(FileSize/1024.0/1024 as decimal(12, 3)) as VLF_Size_MB
    , COUNT(*)    AS VLF_Count

from #VLF_TEMP3
GROUP BY 
   Server_Instance
   , DB_Name 
   , FileID
   , FileSize/1024.0/1024
ORDER BY 
   DB_Name 
   , FileID
   , VLF_Size_MB


SELECT 'VLF Detail' as output_type
, *
from #VLF_TEMP3
order by 
   Server_Instance
   , DB_Name 
   , FileID
   , FSeqNo

   
--DROP TABLE #VLF_TEMP
--DROP TABLE #VLF_TEMP2
--DROP TABLE #VLF_TEMP3
--DROP TABLE #DB

