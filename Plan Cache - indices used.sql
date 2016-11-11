/* scrape all plans in cache looking for presence of specific databases, schemas, tables, or indices.

NOTE - this can be a heavy hitter on systems with a lot of plans in cache.  
investigate sys.dm_exec_cached_plans before running in production

*/


SET DEADLOCK_PRIORITY LOW
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED 

declare @database_name sysname = '*'
declare @schema_name sysname = '*'
declare @table_name sysname = '*'
declare @index_name sysname = '*'


-- DECLARE @IndexName AS NVARCHAR(128) = 'PK__TestTabl__FFEE74517ABC33CD'; 

--— Make sure the name passed is appropriately quoted 
-- IF (LEFT(@IndexName, 1) <> '[' AND RIGHT(@IndexName, 1) <> ']') SET @IndexName = QUOTENAME(@IndexName); 
--–Handle the case where the left or right was quoted manually but not the opposite side 
-- IF LEFT(@IndexName, 1) <> '[' SET @IndexName = '['+@IndexName; 
-- IF RIGHT(@IndexName, 1) <> ']' SET @IndexName = @IndexName + ']'; 

IF OBJECT_ID(N'TEMPDB..#indices_used') IS NOT NULL DROP TABLE #indices_used

--— Dig into the plan cache and find all plans using this index 
 ;WITH XMLNAMESPACES 
    (DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan')    
 SELECT distinct
  objtype
, refcounts
, usecounts

  ,  objectid as object_id
   , OBJECT_NAME(objectid, dbid) as object_name
 , stmt.value('(@StatementId)[1]', 'int')                                AS StatementId
 , stmt.value('(@StatementCompId)[1]', 'int')                            AS StatementCompId

 , stmt.value('(@StatementText)[1]', 'varchar(max)') AS SQL_Text 
 , iobj.value('(@Database)[1]', 'varchar(128)') AS Database_Name 
 , iobj.value('(@Schema)[1]', 'varchar(128)') AS Schema_Name 
 , iobj.value('(@Table)[1]', 'varchar(128)') AS Table_Name 
 , iobj.value('(@Index)[1]', 'varchar(128)') AS Index_Name 
 , iobj.value('(@IndexKind)[1]', 'varchar(128)') AS Index_Kind
 
 --, ix.value('(@Lookup)[1]', 'bit') AS Lookup 
 --, ix.value('(@Ordered)[1]', 'bit') AS Ordered 
-- , ix.value('(@ScanDirection)[1]', 'varchar(10)') AS ScanDirection 
 --, ix.value('(@ForcedIndex)[1]', 'bit') AS ForcedIndex 
 --, ix.value('(@ForcedSeek)[1]', 'bit') AS ForcedSeek
 
 --, ix.value('(@ForcedSeekColumnCount)[1]', 'int') AS ForcedSeekColumnCount 
 --, ix.value('(@ForceScan)[1]', 'bit') AS ForceScan 
 --, ix.value('(@NoExpandHint)[1]', 'bit') AS NoExpandHint 
 , ix.value('(@Storage)[1]', 'varchar(20)') AS Storage 
--, query_hash, 
--query_plan_hash
, cp.plan_handle 
, pool_id
 --,query_plan 
into #indices_used
 FROM sys.dm_exec_cached_plans AS cp 
 CROSS APPLY sys.dm_exec_query_plan(plan_handle) AS qp 
 CROSS APPLY query_plan.nodes('/ShowPlanXML/BatchSequence/Batch/Statements/StmtSimple') AS batch(stmt) 
 OUTER APPLY stmt.nodes('.//IndexScan') AS scan(ix) 
 outer APPLY stmt.nodes('.//IndexScan/Object') AS idx(iobj) 

 OPTION(MAXDOP 1, RECOMPILE); 

update #indices_used 
set Database_Name = replace(REPLACE(Database_Name, '[', ''), ']', '')
   , Schema_Name = replace(REPLACE(Schema_Name, '[', ''), ']', '')
   , Table_Name = replace(REPLACE(Table_Name, '[', ''), ']', '')
   , Index_Name = replace(REPLACE(Index_Name, '[', ''), ']', '')

select * from #indices_used i
where index_name is not null
and (@database_name = '*' or @database_name = i.database_name)
and (@schema_name = '*' or @schema_name = i.schema_name)
and (@table_name = '*' or @table_name = i.table_name)
and (@index_name = '*' or @index_name = i.index_name)
order by database_name, schema_name, table_name, index_name

