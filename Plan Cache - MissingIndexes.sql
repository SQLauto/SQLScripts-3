

SET DEADLOCK_PRIORITY LOW
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED 


IF OBJECT_ID(N'TEMPDB..#missing_details') IS NOT NULL DROP TABLE #missing_details

 ;WITH XMLNAMESPACES 
    (DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan')   

 SELECT objtype
   , rp.name                                                as resource_pool
   , dbid                                                   as database_id
   , db_name(dbid)                                          as database_name
   , objectid                                               as object_id
   , OBJECT_NAME(objectid, dbid)                            as object_name
   , stmt.value('(@StatementId)[1]', 'int')                 AS Statement_Id
   , stmt.value('(@StatementText)[1]', 'varchar(max)')      AS SQL_Text 
   , stmt.value('(@ParameterizedText)[1]', 'nvarchar(max)') AS Parameterized_Text
   , missing.value('(@Schema)[1]', 'varchar(128)')          as Missing_schema
   , missing.value('(@Table)[1]', 'varchar(128)')           as Missing_table
   , missingcolgrp.value('(@Usage)[1]', 'varchar(128)')     as Missing_Usage
   , missingcol.value('(@Name)[1]', 'varchar(128)')         as Missing_Column
   , missinggroup.value('(@Impact)[1]', 'float')            as Missing_Impact
   , cp.plan_handle
   , row_number() over(partition by plan_handle, stmt.value('(@StatementId)[1]', 'int') order by getdate()) as row_num
into #missing_details
 FROM sys.dm_exec_cached_plans AS cp 
 join sys.dm_resource_governor_resource_pools rp on rp.pool_id = cp.pool_id
 CROSS APPLY sys.dm_exec_query_plan(plan_handle) AS qp 
 CROSS APPLY query_plan.nodes('/ShowPlanXML/BatchSequence/Batch/Statements/StmtSimple') AS batch(stmt) 
 outer APPLY stmt.nodes('.//QueryPlan') AS p(pln) 
 outer APPLY pln.nodes('.//MissingIndexes/MissingIndexGroup') AS m2(missinggroup) 
 outer APPLY missinggroup.nodes('.//MissingIndex') AS m(missing) 
 outer APPLY missing.nodes('.//ColumnGroup') AS cg(missingcolgrp) 
 outer APPLY missingcolgrp.nodes('.//Column') AS c(missingcol) 
where pln.value('(@CachedPlanSize)[1]', 'int') is not null 
and dbid <> 32767 -- exclude resource database
and missingcol.value('(@Name)[1]', 'varchar(128)')  is not null
order by plan_handle
 OPTION(MAXDOP 1, RECOMPILE);

IF OBJECT_ID(N'TEMPDB..#missing_header') IS NOT NULL DROP TABLE #missing_header

SELECT
      objtype
    , resource_pool
    , database_id
    , database_name
    , object_id 
    , object_name
    , SQL_Text
    , Parameterized_Text
    , Statement_Id
    , Missing_schema
    , Missing_Table
    , Missing_Impact
    , plan_handle
    , row_number() over(order by getdate()) as row_num
into #missing_header
FROM #missing_details
where row_num = 1

IF OBJECT_ID(N'TEMPDB..#column_csv') IS NOT NULL DROP TABLE #column_csv
create table #column_csv(plan_handle varbinary(64)
, statement_id int
, equality_columns nvarchar(max)
, inequality_columns nvarchar(max)
, include_columns nvarchar(max)
)

declare @equality nvarchar(max) = ''
declare @inequality nvarchar(max) = ''
declare @include nvarchar(max) = ''

declare @Plan_handle varbinary(64)
declare @statement_id int

declare @counter int = 1
declare @max_counter int = (select count(*) from #missing_header)

while @counter <= @max_counter
begin

select @plan_handle = plan_handle
, @statement_id = statement_id
from #missing_header
where row_num = @counter

select @equality = @equality + Missing_Column
from #missing_details md
join #missing_header mh on mh.plan_handle = md.plan_handle 
                           and mh.statement_id = md.statement_id
where mh.row_num = @counter
and md.Missing_Usage = 'equality'

select @inequality = @inequality + Missing_Column
from #missing_details md
join #missing_header mh on mh.plan_handle = md.plan_handle 
                           and mh.statement_id = md.statement_id
where mh.row_num = @counter
and md.Missing_Usage = 'inequality'

select @include = @include + Missing_Column
from #missing_details md
join #missing_header mh on mh.plan_handle = md.plan_handle 
                           and mh.statement_id = md.statement_id
where mh.row_num = @counter
and md.Missing_Usage = 'include'

insert into #column_csv 
   select @Plan_handle
   , @statement_id
   , @equality
   , @inequality
   , @include

set @equality = ''
set @inequality = ''
set @include = ''

set @counter = @counter + 1
end


SELECT
      mh.objtype
    , mh.resource_pool
    , mh.database_id
    , mh.database_name
    , mh.object_id
    , mh.object_name

    , mh.SQL_Text
    , mh.Parameterized_Text
    , mh.Statement_Id
    , mh.Missing_schema
    , mh.Missing_Table
    , cc.equality_columns
    , cc.inequality_columns
    , cc.include_columns
    , mh.Missing_Impact
    , mh.plan_handle
from #missing_header mh
join #column_csv cc on cc.plan_handle = mh.plan_handle
                     and cc.statement_id = mh.Statement_Id



