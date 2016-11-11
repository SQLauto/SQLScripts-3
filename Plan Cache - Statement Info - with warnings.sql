/* Purpose:	    to parse statment-level information from the plan cache.  
   Source:	    John Kauffman, the Jolly DBA
   Date:        
   Explanation: 

   Returns:     
   Parameters:  	
   Notes:	    to simplify xml execution, i'm pulling just statment level (and statment/queryplan) 
                and plan on pulling other levels separately.  then i can join together via plan handle, statment id, etc.
                the different values as needed, without having to shred every aspect of a plan in a single query.

                to join to different outputs, i'd need to load these data into a temp table.  not done yet.

                parameters for db, for object, etc would make sense.

                obviously, a version that would use persisted query plans in the QUERY_OBJECTS model makes a lot of sense.
                in that case, i would create persisted tables for the different levels of the plan, and join as needed.
                
                would also like to get this to work with an actual execution plan.  can do it manually pretty easily
                but would also like to use profiler and extended events as sources so i could look at larger volumes 
                more easily.
               
*/



--— Dig into the plan cache - statment and query plan level
/* note statements without query plans are excluded - removes the 'set nocount on', variable declarations, table creation, etc.*/

SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED 

 ;WITH XMLNAMESPACES 
    (DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan')   

 SELECT objtype
   , rp.name as resource_pool
   , dbid as database_id
   , db_name(dbid) as database_name
   , objectid as object_id
   , OBJECT_NAME(objectid, dbid) as object_name
   , refcounts
   , usecounts
   , cp.plan_handle
   , qp.query_plan
   , pln.value('(@CachedPlanSize)[1]', 'int')                              as CachedPlanSize
   , pln.value('(@CompileTime)[1]', 'int')                                 as CompileTime
   , pln.value('(@CompileCPU)[1]', 'int')                                  as CompileCPU
   , pln.value('(@CompileMemory)[1]', 'int')                               as CompileMemory
   , pln.value('(@EffectiveDegreeOfParallelism)[1]', 'int')                as EffectiveDegreeOfParallelism
   , stmt.value('(@DegreeOfParallelism)[1]', 'int')                        as DegreeOfParallelism
   , pln.value('(@NonParallelPlanReason)[1]', 'varchar(50)')               as NonParallelPlanReason
   , pln.value('(@MemoryGrant)[1]', 'int')                                 as MemoryGrant

   , stmt.value('(@StatementText)[1]', 'varchar(max)')                     AS SQL_Text 
   , stmt.value('(@ParameterizedText)[1]', 'nvarchar(max)')                AS ParameterizedText
   , stmt.value('(@StatementParameterizationType)[1]', 'int')              AS StatementParameterizationType
   , stmt.value('(@StatementId)[1]', 'int')                                AS StatementId
   , stmt.value('(@StatementCompId)[1]', 'int')                            AS StatementCompId
   , stmt.value('(@StatementType)[1]', 'varchar(100)')                     AS StatementType
   , stmt.value('(@RetrievedFromCache)[1]', 'varchar(10)')                 AS RetrievedFromCache
   
   , stmt.value('(@StatementSubTreeCost)[1]', 'float')                     AS StatementSubTreeCost
   
   , stmt.value('(@StatementEstRows)[1]', 'float')                         AS StatementEstRows
   , stmt.value('(@StatementOptmLevel)[1]', 'varchar(20)')                 AS StatementOptmLevel
   , stmt.value('(@StatementOptmEarlyAbortReason)[1]', 'varchar(100)')     AS StatementOptmEarlyAbortReason
   , stmt.value('(@CardinalityEstimationModelVersion)[1]', 'varchar(100)') AS CardinalityEstimVersion
   
   , sttemp.value('(@SpillLevel)[1]', 'int')                               as SpillLevel
   , convrt.value('(@ConvertIssue)[1]', 'varchar(30)')                     as ConvertIssue
   , convrt.value('(@Expression)[1]', 'varchar(4000)')                     as ConvertIssueExpression
   , warn_statcol.value('(@Schema)[1]', 'varchar(128)')                    
     + '.' + warn_statcol.value('(@Table)[1]', 'varchar(128)')                   
     + '.' + warn_statcol.value('(@Column)[1]', 'varchar(128)')            AS NoStats       


   , warn_plan.value('(@NoJoinPredicate)[1]', 'bit')                       as NoJoinPredicate
   , warn_plan.value('(@UnmatchedIndexes)[1]', 'bit')                      as UnmatchedIndexes
   , wait.value('(@WaitType)[1]', 'varchar(30)')                           as WaitType
   , warn_plan.value('(@SpatialGuess)[1]', 'bit')                          as SpatialGuess
   , warn_plan.value('(@FullUpdateForOnlineIndexBuild)[1]', 'bit')         as FullUpdateForOnlineIndexBuild



   , warnrel_sttemp.value('(@SpillLevel)[1]', 'int')                       as SpillLevel
   , warnrel_convrt.value('(@ConvertIssue)[1]', 'varchar(30)')             as ConvertIssue
   , warnrel_convrt.value('(@Expression)[1]', 'varchar(4000)')             as ConvertIssueExpression
   , warnrel_statcol.value('(@Schema)[1]', 'varchar(128)')                    
     + '.' + warnrel_statcol.value('(@Table)[1]', 'varchar(128)')                   
     + '.' + warnrel_statcol.value('(@Column)[1]', 'varchar(128)')        AS NoStats       


   , warn_rel.value('(@NoJoinPredicate)[1]', 'bit')                       as NoJoinPredicate
   , warn_rel.value('(@UnmatchedIndexes)[1]', 'bit')                      as UnmatchedIndexes
   , warnrel_wait.value('(@WaitType)[1]', 'varchar(30)')                           as WaitType
   , warn_rel.value('(@SpatialGuess)[1]', 'bit')                          as SpatialGuess
   , warn_rel.value('(@FullUpdateForOnlineIndexBuild)[1]', 'bit')         as FullUpdateForOnlineIndexBuild
 FROM sys.dm_exec_cached_plans                   AS cp 
 join sys.dm_resource_governor_resource_pools       rp on rp.pool_id = cp.pool_id
 CROSS APPLY sys.dm_exec_query_plan(plan_handle) AS qp 
 CROSS APPLY query_plan.nodes('/ShowPlanXML/BatchSequence/Batch/Statements/StmtSimple') AS batch(stmt) 
 outer APPLY stmt.nodes('.//QueryPlan')                    AS p(pln) 
 outer APPLY pln.nodes('.//Warnings')                      AS wrn_qp(warn_plan) 
 outer APPLY warn_plan.nodes('.//PlanAffectingConvert')    as d(convrt)
 outer APPLY warn_plan.nodes('.//ColumnsWithNoStatistics') as a(warn_cnstat)
 outer APPLY warn_cnstat.nodes('.//ColumnReference')       as a2(warn_statcol)
 outer APPLY warn_plan.nodes('.//SpillToTempDb')           as b(sttemp)
 outer APPLY warn_plan.nodes('.//Wait')                    as c(wait)

 outer APPLY pln.nodes('.//relop')                         AS rel(relop) 
 outer APPLY pln.nodes('.//warning')                       AS wrn_rl(warn_rel) 
 outer APPLY warn_rel.nodes('.//ColumnsWithNoStatistics')  as j(warnrel_cnstat)
 outer APPLY warnrel_cnstat.nodes('.//ColumnReference')    as k(warnrel_statcol)
 outer APPLY warn_rel.nodes('.//SpillToTempDb')            as l(warnrel_sttemp)
 outer APPLY warn_rel.nodes('.//Wait')                     as m(warnrel_wait)
 outer APPLY warn_rel.nodes('.//PlanAffectingConvert')     as n(warnrel_convrt)
where pln.value('(@CachedPlanSize)[1]', 'int') is not null 
and dbid <> 32767 -- exclude resource database
order by plan_handle, statementid, StatementCompId
 OPTION(MAXDOP 1, RECOMPILE);
