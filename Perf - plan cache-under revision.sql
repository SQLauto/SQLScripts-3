/*
script - plan cache analysis 
author - john kauffman, http://sqljohnkauffman.wordpress.com/

disclaimer:
this sample code is provided for the purpose of illustration only and is not intended
to be used in a production environment.  this sample code and any related information are
provided "as is" without warranty of any kind, either expressed or implied, including but
not limited to the implied warranties of merchantability and/or fitness for a particular
purpose.  we grant you a nonexclusive, royalty-free right to use and modify the sample code
and to reproduce and distribute the object code form of the sample code, provided that you
agree: 
(i) to not use our name, logo, or trademarks to market your software product in which
the sample code is embedded; 
(ii) to include a valid copyright notice on your software product
in which the sample code is embedded; 
and (iii) to indemnify, hold harmless, and defend us and
our suppliers from and against any claims or lawsuits, including attorneys fees, that arise or
result from the use or distribution of the sample code.

*/



/* this version of the plan cache query has several goals:
1.  to pull metadata about the plan cache, not just details about the specific queries.
2.  to work across all databases.  it does so by getting sys.objects from all dbs.
3.  to pull all procs, not just a single proc.  this requires better coordination between the proc level and query level outputs
4.  to provide information about ad hoc data.  performance data are aggregated by query hash and query plan hash to show the impact of the statement.
5.  to rank procs and ad hoc statements by various factors so that the proc can pull top 20 of each by various factors
6.  to handle query plans as elegantly as possible.  pulling query plans is parameterized, and only 1 plan per query hash is included.
9.  to isolate functions

/next-gen goals include
7.  to parse the query plans enough to flag those with missing indices, index scans, and implicit conversions, etc.
8.  to capture 2 points in time_ms so that totals can be normalized.  in the short term, consider averaging by time, in addition to by execution count.
10. to get to work for csv list of procs without requiring function.
*/

/* see set_options descriptions  at http://msdn.microsoft.com/en-us/library/ms189472.aspx*/

set nocount on

--set statistics io on
--set statistics time_ms on
--set statistics io off
--set statistics time_ms off
---------------------------------------------------------------------------------------------------------------
begin /*variable declaration*/
---------------------------------------------------------------------------------------------------------------
/* select output types.  can multi-select*/
declare @return_plan_cache_summary       tinyint = 0
declare @show_object_stats               tinyint = 1
declare @show_object_statement_details   tinyint = 1
declare @show_ad_hoc                     tinyint = 1
--declare @show_query_plan_attributes      tinyint = 0 -- multiple rows per object.  may be hard to work with for large result sets.


/*caution:  returning query plans for all procedures can be very resource_intensive.  
use carefully.  for example, use when output has been filtered in some substantial way, 
                              and after knowing how many results the filter returns.
*/
declare @include_object_query_plans                  bit = 0
declare @include_ad_hoc_query_plans                  bit = 0
declare @return_plans_as_xml_or_text        nvarchar(10) = N'xml' -- options are 'xml' and 'text'  large plans don't return xml to ssms window, which has an xml size limit.  
                                                                  -- if plan is blank, try the 'text' option.

---------------------------------------------------------------------------------------------------------------

/* apply various filters for object-based outputs (meaning procs and triggers - not functions yet).

filter by object name, by object type (proc or trigger), or with wildcard searches against object names.
if object name or wildcard filters are applied, ad hoc output is not produced.  i'm assuming you're interested in 
something specific in those cases.*/

declare @object_name_list                        sysname = N'*'  -- enter either'*' or  csv list of names (procs, triggers, functions)
                                                                         -- if specific name is entered, the @show_ad_hoc option is set to 0.  
                                                                         -- if proc name is provided, the assumption is that you're running in that proc's db (maybe - not coded)

declare @object_type                         nvarchar(20) = N'*' -- options include 'sql_stored_procedure', 'sql_trigger', 'clr_table_valued_function', 'sql_inline_table_valued_function', 'sql_scalar_function', 'sql_table_valued_function'', '*'
declare @object_name_list_wildcard_pattern  nvarchar(100) = N''  -- '' - no wildcard search.   
                                                            -- 'text%' -- text is exact at beginning. very fast'
                                                            -- '%text' -- full-on wildcard search.  very likely to be very slow
                                                            -- '%text%' -- full-on wildcard search.  very likely to be very slow


declare @ac_hoc_text                  nvarchar(4000) = N'' -- use with caution. note wildcard options for next parameter
declare @ac_hoc_text_wildcard_pattern nvarchar(100)  = N'' -- options include '' (full text provded - match completely with no wildcards.  fastest option)'
                                                           --                 'text%' -- text is exact at beginning.  also very fast'
                                                           --                 '%text%' -- full-on wildcard search.  very likely to be very slow
                                                           -- not sure how this will work with crlfs, tabs, etc.  will need to test.
---------------------------------------------------------------------------------------------------------------
/* apply other filters beyond names, having to do with the stats or query plans.
not coded */                      
declare @only_objects_with_multiple_plans            bit = 0  --when selected, query plan options that can impact plan generation are included.
declare @return_plans_with_missing_indices       tinyint = 1 -- caution!  if selected, query plans will be pulled.  this will be expensive for large record sets.  it's parsing the xml.
declare @return_plans_with_index_scans           tinyint = 1 -- caution!  if selected, query plans will be pulled.  this will be expensive for large record sets.  it's parsing the xml.
declare @return_plans_with_implicit_conversions  tinyint = 1 -- caution!  if selected, query plans will be pulled.  this will be expensive for large record sets.  it's parsing the xml.

---------------------------------------------------------------------------------------------------------------
/* look for worst offenders for cpu, physical i/o, etc.  
   execution count is useful because it helps you weed out false positives.  if something is the #3 total cpu consumer but #1 in execution count, it's probably a false positive.
   of course, that's reflected in the avg cpu ranking, but still!

   because plans can stay in cache for varying lengths of time, looking at total counts can be misleading.  
*/

/* quick ways to avoid having to set all the individual ranking filters.  "no" top filters wins over "all".  */
declare @apply_all_top_filters tinyint = 1
declare @apply_no_top_filters  tinyint = 1
---------------------------------------------------------
declare @top_n_value                            int = 30

declare @return_top_total_execution_count   tinyint = 1
                                            
declare @return_top_avg_execution_time_ms      tinyint = 0
declare @return_top_total_execution_time_ms    tinyint = 0
declare @return_max_execution_time_ms          tinyint = 0
declare @return_min_execution_time_ms          tinyint = 0
                                            
declare @return_top_avg_cpu                 tinyint = 0
declare @return_top_total_cpu               tinyint = 0
declare @return_max_cpu                     tinyint = 0
declare @return_min_cpu                     tinyint = 0
                                            
declare @return_top_avg_physical_reads      tinyint = 0
declare @return_top_total_physical_reads    tinyint = 0
declare @return_max_physical_reads          tinyint = 0
declare @return_min_physical_reads          tinyint = 0
                                            
declare @return_top_avg_logical_reads       tinyint = 0
declare @return_top_total_logical_reads     tinyint = 0
declare @return_max_logical_reads           tinyint = 0
declare @return_min_logical_reads           tinyint = 0
                                            
declare @return_top_avg_logical_writes      tinyint = 0
declare @return_top_total_logical_writes    tinyint = 0
declare @return_max_logical_writes          tinyint = 0
declare @return_min_logical_writes          tinyint = 0
                                         

/* ad hoc ranking only*/         
declare @return_top_cached_entries          tinyint = 0                     
declare @return_top_avg_rows                tinyint = 0
declare @return_top_total_rows              tinyint = 0
declare @return_max_rows                    tinyint = 0
declare @return_min_rows                    tinyint = 0


end /*variable declaration*/
-----------------------------------------------------------------------------------------------------------
begin /* variable clean up.  
      e.g., won't show object statement details without also showing object.
            if ad hoc selected, have to do basic object statement details to figure out which query stats aren't ad hoc, but need to suppress outputs if not 
               selected by user.
     */
-----------------------------------------------------------------------------------------------------------
   if @show_object_statement_details = 1
      begin
         set @show_object_stats= 1
      end

if @object_name_list_wildcard_pattern = '*' 
   begin
      set @object_name_list_wildcard_pattern = ''
   end

if  @ac_hoc_text_wildcard_pattern = '*' 
   begin
      set @ac_hoc_text_wildcard_pattern = ''
   end

   /* if running for specific objects, don't return ad hoc*/
   if @object_name_list <> '*' or @object_name_list_wildcard_pattern <> ''
      begin
         set @show_ad_hoc = 0
      end

   if @show_object_stats = 0
      begin
         set @include_object_query_plans = 0
         set @only_objects_with_multiple_plans = 0
      end

   if @object_name_list <> '*' 
      begin
         set  @object_name_list_wildcard_pattern = '' -- no wildcard search. 
      end

------------------------------------------------------------------------------------
   declare @top_n_object_rankings_checked      int = 0
   --declare @top_n_statement_rankings_checked   int = 0


   if @apply_all_top_filters = 1
      begin                                      
         set @return_top_total_execution_count   = 1
         set @return_top_avg_execution_time_ms      = 1
         set @return_top_total_execution_time_ms    = 1
         set @return_max_execution_time_ms          = 1
         set @return_min_execution_time_ms          = 1
                                                 
         set @return_top_avg_cpu                 = 1
         set @return_top_total_cpu               = 1
         set @return_max_cpu                     = 1
         set @return_min_cpu                     = 1
                                                 
         set @return_top_avg_physical_reads      = 1
         set @return_top_total_physical_reads    = 1
         set @return_max_physical_reads          = 1
         set @return_min_physical_reads          = 1
                                                 
         set @return_top_avg_logical_reads       = 1
         set @return_top_total_logical_reads     = 1
         set @return_max_logical_reads           = 1
         set @return_min_logical_reads           = 1
                                                 
         set @return_top_avg_logical_writes      = 1
         set @return_top_total_logical_writes    = 1
         set @return_max_logical_writes          = 1
         set @return_min_logical_writes          = 1

         set @top_n_object_rankings_checked = 
                 @return_top_total_execution_count         
               + @return_top_avg_execution_time_ms   + @return_top_total_execution_time_ms + @return_max_execution_time_ms + @return_min_execution_time_ms                                                          
               + @return_top_avg_cpu              + @return_top_total_cpu            + @return_max_cpu            + @return_min_cpu                                                                     
               + @return_top_avg_physical_reads   + @return_top_total_physical_reads + @return_max_physical_reads + @return_min_physical_reads                                                          
               + @return_top_avg_logical_reads    + @return_top_total_logical_reads  + @return_max_logical_reads  + @return_min_logical_reads                  
               + @return_top_avg_logical_writes   + @return_top_total_logical_writes + @return_max_logical_writes + @return_min_logical_writes 

         set @return_top_cached_entries          = 1
         set @return_top_avg_rows                = 1
         set @return_top_total_rows              = 1
         set @return_max_rows                    = 1
         set @return_min_rows                    = 1
      end --if @apply_all_top_filters = 1

   if @apply_no_top_filters = 1
      begin
         set @return_top_total_execution_count    = 0
         set @return_top_avg_execution_time_ms       = 0
         set @return_top_total_execution_time_ms     = 0
         set @return_max_execution_time_ms           = 0
         set @return_min_execution_time_ms           = 0
                                                       
         set @return_top_avg_cpu                  = 0
         set @return_top_total_cpu                = 0
         set @return_max_cpu                      = 0
         set @return_min_cpu                      = 0
                                                       
         set @return_top_avg_physical_reads       = 0
         set @return_top_total_physical_reads     = 0
         set @return_max_physical_reads           = 0
         set @return_min_physical_reads           = 0
                                                        
         set @return_top_avg_logical_reads        = 0
         set @return_top_total_logical_reads      = 0
         set @return_max_logical_reads            = 0
         set @return_min_logical_reads            = 0
                                                        
         set @return_top_avg_logical_writes       = 0
         set @return_top_total_logical_writes     = 0
         set @return_max_logical_writes           = 0
         set @return_min_logical_writes           = 0

         set @top_n_object_rankings_checked = 21 -- all object-level rankings

         set @return_top_cached_entries          = 0
         set @return_top_avg_rows                = 0
         set @return_top_total_rows              = 0
         set @return_max_rows                    = 0
         set @return_min_rows                    = 0

      end --if @apply_no_top_filters = 1

         if @top_n_object_rankings_checked = 0    set @top_n_object_rankings_checked = 21 -- all object-level rankings


end /* variable clean up.*/
-----------------------------------------------------------------------------------------------------------
begin /* drop temp tables */
-----------------------------------------------------------------------------------------------------------

   if object_id(N'tempdb..#object_stats') > 0 drop table #object_stats

end /* drop temp tables */


-----------------------------------------------------------------------------------------------------------
/* outputs */
-----------------------------------------------------------------------------------------------------------

if @return_plan_cache_summary = 1
   begin
      select '@return_plan_cache_summary' as output_type
         , rp.name as resource_pool
         , cacheobjtype as cache_type
         , objtype as object_type
         , case when usecounts = 1 then 'single use' else 'multi use' end as plan_reuse
         , sum(cast(usecounts as bigint)) as use_count
         , count_big(*) as plan_count
         , cast(sum(cast(usecounts as bigint)) * 1.0/count_big(*) as decimal(38, 2)) as avg_uses_per_plan
         , cast(sum(size_in_bytes* 1.0/1048576) as decimal(38, 2)) as total_plan_mb
         , cast(sum(size_in_bytes* 1.0/1048576)/ count_big(*)  as decimal(38, 2))  as avg_plan_mb
         , cast(min(size_in_bytes* 1.0/1048576) as decimal(38, 2)) as min_plan_mb
         , cast(max(size_in_bytes* 1.0/1048576) as decimal(38, 2)) as max_plan_mb
      from sys.dm_exec_cached_plans cp
      join sys.resource_governor_resource_pools rp on rp.pool_id = cp.pool_id
      where cacheobjtype like 'compiled plan%'
      group by cacheobjtype 
         , objtype 
         , case when usecounts = 1 then 'single use' else 'multi use' end
         , rp.name
      order by object_type, plan_reuse, rp.name
   end --if @return_plan_cache_summary = 1


------------------------------------------------------------------------------------------------------

if @show_object_stats = 1 or @show_object_statement_details  = 1
   begin 
      -------------------------------------------------------------------------------------------------
      begin /* pull objects from all databases.  
      this is necessary because the proc, trigger, and query stats dmvs work across databases, but sys.objects is db-specific.
      in systems with large numbers of databases, this would have to be run in each db to get proc names.*/

         declare @debug_only bit = 0 -- by default, the dynamic sql will be printed, not executed.
         declare @all_dbs tinyint = 1
         declare @current_db_only bit = 0
         declare @user_dbs_only bit = 0
         declare @system_dbs_only bit = 0
         declare @db_include_list nvarchar(1000) = '*'--use csv list
         declare @db_exclude_list nvarchar(1000) = 'tempdb'

         if object_id(N'tempdb..#db_list') is not null drop table #db_list
         if object_id(N'tempdb..#included_dbs') is not null drop table #included_dbs
         if object_id(N'tempdb..#excluded_dbs') is not null drop table #excluded_dbs

         create table #db_list (row_num int identity (1, 1), database_id int, database_name sysname)
         create table #included_dbs  ( database_name sysname )
         create table #excluded_dbs ( database_name sysname)

         /* deal with csv lists*/
         set @db_include_list = upper(ltrim(rtrim(@db_include_list)))

         if @db_include_list is null or @db_include_list = ''  or @db_include_list = 'all' or @db_include_list = '*'  or @db_include_list = 'null' 
               begin  
                  set @db_include_list = '*'  
               end 

               if @db_include_list <>'*'
                  begin
                     insert into #included_dbs
                     select ltrim(rtrim(item ))
                     from (
                           select item = y.i.value('(./text())[1]', 'nvarchar(4000)')
                           from 
                           ( 
                             select x = convert(xml, '<i>' 
                               + replace(@db_include_list, ',', '</i><i>') 
                               + '</i>').query('.')
                           ) as a cross apply x.nodes('i') as y(i) ) x
                     where charindex(@db_include_list, item )<> 0 or @db_include_list = '*'
                  end

         set @db_exclude_list = upper(ltrim(rtrim(@db_exclude_list)))

         if @db_exclude_list is null or @db_exclude_list = ''  or @db_exclude_list = 'all' or @db_exclude_list = '*'  or @db_exclude_list = 'null' 
               begin  
                  set @db_exclude_list = '*'  
               end 

               if @db_exclude_list <>'*'
                  begin
                     insert into #excluded_dbs
                     select ltrim(rtrim(item ))
                     from (
                           select item = y.i.value('(./text())[1]', 'nvarchar(4000)')
                           from 
                           ( 
                             select x = convert(xml, '<i>' 
                               + replace(@db_exclude_list, ',', '</i><i>') 
                               + '</i>').query('.')
                           ) as a cross apply x.nodes('i') as y(i) ) x
                     where charindex(@db_exclude_list, item )<> 0 or @db_exclude_list = '*'
                  end


         insert into #db_list (database_id, database_name)
            select db.database_id, db.name 
            from sys.databases db
            where state_desc = 'online'
            and (@all_dbs = 1)
            and (@current_db_only = 1 and name = db_name() or @current_db_only = 0)
            and ((@user_dbs_only = 1 and name not in ('master', 'model', 'msdb', 'tempdb', 'distribution')) or @user_dbs_only = 0)
            and ((@system_dbs_only = 1 and name  in ('master', 'model', 'msdb', 'distribution')) or @system_dbs_only = 0)
            and ((@db_include_list <> '*' and name in (select database_name from #included_dbs)) or @db_include_list = '*')
            and ((@db_exclude_list <> '*' and name not in (select database_name from #excluded_dbs)) or @db_exclude_list = '*')
--and db.name not like '%(%'
--and db.name not like '%-%'
--and db.name not like '%!%'
--and db.name not like '%&%'

         declare @counter int = 1
         declare @max_counter int = (select max(row_num) from #db_list)
         declare @sql_text nvarchar(4000) = ''
         declare @database_name sysname
         declare @database_id int

         if object_id(N'tempdb..#objects') is not null drop table #objects  

         create table #objects (database_id int, database_name sysname, object_id int, name sysname, object_type sysname)


         while @counter <= @max_counter
            begin
               select @database_name = database_name, @database_id = database_id from #db_list where row_num = @counter

               set @sql_text = N'
               print ''--  starting @database_name, @counter of @max_counter   --''
               use [@database_name]
                 insert into #objects
                  select @database_id, ''@database_name'',  o.object_id, o.name, type_desc
                  from  sys.objects o
                  where type_desc <>(''system_table'') and type_desc not like ''%constraint%''

                  '
               set @sql_text = replace(@sql_text, '@database_id', @database_id)
               set @sql_text = replace(@sql_text, '@database_name', @database_name)
               set @sql_text = replace(@sql_text, '@counter', @counter)
               set @sql_text = replace(@sql_text, '@max_counter', @max_counter)

     
               if @debug_only = 0 
                  begin
                     exec  (@sql_text)
                  end

               set @counter = @counter + 1

            end -- while @counter <= @max_counter
      end /* pull objects from all databases.  */


         /* find functions.  not directly available.  need to go to attributes*/

         if object_id(N'tempdb..#attributes') is not null drop table #attributes

         create table #attributes
         (
               plan_handle  varbinary(64)
             , attribute    nvarchar(128)
             , value        sql_variant
             , is_cache_key bit
         )

         insert into #attributes
            select  plan_handle, z.*
            from sys.dm_exec_query_stats x
            cross apply sys.dm_exec_plan_attributes (plan_handle) z
            where attribute in ('dbid',  'objectid')

         create index ix1 on #attributes (plan_handle, attribute) include (value)

         if object_id(N'tempdb..#functions_prep') is not null drop table #functions_prep

         create table #functions_prep
         (
               plan_handle varbinary(64)
             , database_id sql_variant
             , object_id   sql_variant
         )
  

         insert into #functions_prep
            select distinct plan_handle
            , (select distinct value from #attributes a2 where attribute = 'dbid' and a2.plan_handle = a.plan_handle)  as database_id
            , (select distinct value from #attributes a2 where attribute = 'objectid' and a2.plan_handle = a.plan_handle)  as object_id        
            from #attributes a

         if object_id(N'tempdb..#functions') is not null drop table #functions

         create table #functions
         (
               plan_handle varbinary(64)
             , database_id sql_variant
             , object_id   sql_variant
             , name        nvarchar(128)
             , object_type nvarchar(128)
         )

         insert into #functions
         select f.*, o.name, o.object_type
         from #functions_prep f
         join #objects o on o.database_id = f.database_id and o.object_id = f.object_id
                  where o.object_type in 
                     (N'clr_table_valued_function', 
                      N'sql_inline_table_valued_function', 
                      N'sql_scalar_function', 
                      N'sql_table_valued_function')


      if @show_ad_hoc = 1
      -----------------------------------------------------------------------------------------------------
         /* in order to pull ad hoc statements, i need the plan handles for all objects.
             however, i don't want to have to pull query text and plans for everything if i'm applying object-level filters
            so i load the objects into a second table (#all_objects) and then, if ad hoc selected, do a pull from proc/trigger stats just to get the plan handles.*/

      /*need to have plan handles for all objects to exclude from query stats*/
         begin

            if object_id(N'tempdb..#all_objects') is not null drop table #all_objects
            if object_id(N'tempdb..#all_object_stats') is not null drop table #all_object_stats

            create table #all_objects
            (
                  database_id   int
                , database_name nvarchar(128)
                , object_id     int
                , name          nvarchar(128)
                , object_type   nvarchar(128)
            )
  
            insert into #all_objects
               select *
               from #objects

            create table #all_object_stats
            (
                  database_name nvarchar(128)
                , database_id   int
                , name          nvarchar(128)
                , object_id     int
                , object_type   nvarchar(128)
                , plan_handle   varbinary(64)
            )
  

            insert into #all_object_stats
               select p.database_name
                  , p.database_id  
                  , p.name
                  , p.object_id
                  , p.object_type
                  , s.plan_handle
               from (select * from sys.dm_exec_procedure_stats
                     union all
                     select * from sys.dm_exec_trigger_stats) s
               join #all_objects p on p.object_id = s.object_id
               and p.database_id = s.database_id

            insert into #all_object_stats
               select database_name
                  , o.database_id
                  , o.name
                  , o.object_id
                  , o.object_type
                  , plan_handle
                  from #functions  a
                  join #objects o on o.object_id = a.object_id and o.database_id = a.database_id
                  where o.object_type in 
                     (N'clr_table_valued_function', 
                      N'sql_inline_table_valued_function', 
                      N'sql_scalar_function', 
                      N'sql_table_valued_function')

         end --if @show_ad_hoc = 1

      -----------------------------------------------------------------------------------------------------

      /* apply object-level filters */
      if object_id(N'tempdb..#object_list') is not null drop table #object_list

      create table #object_list
      (
            item nvarchar(4000)
      )

      if @object_name_list <> '*'
         begin
            insert into #object_list
               select ltrim(rtrim(item )) as item
               from (
                     select item = y.i.value('(./text())[1]', 'nvarchar(4000)')
                     from 
                     ( 
                        select x = convert(xml, '<i>' 
                           + replace(@object_name_list, ',', '</i><i>') 
                           + '</i>').query('.')
                     ) as a cross apply x.nodes('i') as y(i) ) x

            delete from o
            from #objects o
            left join #object_list ol on ol.item = o.name
            where ol.item is null

         end --if @object_name_list <> '*'

      if @object_type <> '*'
         begin
            delete from #objects where object_type <> @object_type
         end -- if @object_type <> '*'

      if @object_name_list_wildcard_pattern <> ''
         begin
            delete from #objects where name not like @object_name_list_wildcard_pattern
         end


     -------------------------------------------------------------------------------
     /* load object-level statistics*/

      if object_id(N'tempdb..#object_stats_prep') is not null drop table #object_stats_prep

      create table #object_stats_prep
      (  database_name sysname
         , database_id int
         , object_name sysname
         , object_id int
         , plan_instance_id int
         , object_type nvarchar(60)
         , cached_time_ms datetime
         , last_execution_time_ms datetime
         , cache_to_last_exec_minutes int
         , execution_count  bigint
         , avg_time_ms   decimal(38, 2), total_time_ms   decimal(38, 2), min_time_ms   decimal(38, 2), max_time_ms   decimal(38, 2)
         , avg_cpu_ms          decimal(38, 2), total_cpu_ms          decimal(38, 2), min_cpu               decimal(38, 2), max_cpu   decimal(38, 2)
         , avg_logical_reads     decimal(38, 2), total_logical_reads     bigint, min_logical_reads     bigint, max_logical_reads     bigint 
         , avg_physical_reads    decimal(38, 2), total_physical_reads    bigint, min_physical_reads    bigint, max_physical_reads    bigint 
         , avg_logical_writes    decimal(38, 2), total_logical_writes    bigint, min_logical_writes    bigint, max_logical_writes    bigint 
         , plan_handle varbinary(64) 
         )

      insert into #object_stats_prep
         select p.database_name
            , p.database_id  
            , p.name
            , p.object_id
            , row_number() over(partition by p.database_id, p.object_id order by cached_time) 
            , p.object_type
            , cached_time
            , last_execution_time
            , datediff(minute, cached_time, last_execution_time) as cache_to_last_exec_minutes
            , execution_count
            , total_elapsed_time/1000.0/execution_count  
            , total_elapsed_time/1000.0
            , min_elapsed_time/1000.0
            , max_elapsed_time/1000.0
            , total_worker_time/1000.0/execution_count 
            , total_worker_time/1000.0
            , min_worker_time/1000.0
            , max_worker_time/1000.0
            , total_logical_reads*1.0/execution_count 
            , total_logical_reads
            , min_logical_reads
            , max_logical_reads
            , total_physical_reads*1.0/execution_count 
            , total_physical_reads
            , min_physical_reads
            , max_physical_reads
            , total_logical_writes*1.0 /execution_count 
            , total_logical_writes
            , min_logical_writes
            , max_logical_writes
            , plan_handle
         from (select * from sys.dm_exec_procedure_stats where database_id <> 32767
               union  all
               select * from sys.dm_exec_trigger_stats where database_id <> 32767) s
         join #objects p on p.object_id = s.object_id
         and p.database_id = s.database_id

         insert into #object_stats_prep
            select 
              p.database_name
            , p.database_id  
            , p.name
            , p.object_id
            , row_number() over(partition by p.database_id, p.object_id order by creation_time) 
            , p.object_type
            , qs.creation_time
            , qs.last_execution_time
            , datediff(minute, creation_time, qs.last_execution_time) as cache_to_last_exec_minutes
            , sum(qs.execution_count)            as execution_count
            , sum(qs.total_elapsed_time/1000.0)  
               /sum(qs.execution_count)          as avg_time
            , sum(qs.total_elapsed_time/1000.0 ) as  total_time
            , min(qs.min_elapsed_time/1000.0 )   as min_time
            , max(qs.max_elapsed_time/1000.0 )   as max_time
            , sum(qs.total_worker_time/1000.0)    
               /sum(qs.execution_count)          as avg_cpu_ms
            , sum(qs.total_worker_time/1000.0)          as total_cpu_ms
            , min(qs.min_worker_time/1000.0)            as min_cpu_ms
            , max(qs.max_worker_time/1000.0)            as max_cpu_ms
            , sum(qs.total_logical_reads) *1.0   
               /sum(qs.execution_count)          as avg_logical_reads
            , sum(qs.total_logical_reads )       as total_logical_reads
            , min(qs.min_logical_reads )         as min_logical_reads
            , max(qs.max_logical_reads )         as max_logical_reads
            , sum(qs.total_physical_reads *1.0)  
               /sum(qs.execution_count)          as avg_physical_reads
            , sum(qs.total_physical_reads )      as total_physical_reads
            , min(qs.min_physical_reads )        as min_physical_reads
            , max(qs.max_physical_reads )        as max_physical_reads
            , sum(qs.total_logical_writes*1.0)   
               /sum(qs.execution_count)          as avg_logical_writes
            , sum(qs.total_logical_writes )      as total_logical_writes
            , min(qs.min_logical_writes )        as min_logical_writes
            , max(qs.max_logical_writes )        as max_logical_writes
            , max(f.plan_handle)                 as plan_handle
            from sys.dm_exec_query_stats qs 
            join #functions  f on qs.plan_handle = f.plan_handle 
            join #objects    p on p.object_id = f.object_id
                                   and p.database_id = f.database_id
            left join #object_stats_prep osp on osp.database_id = p.database_id and osp.object_id = p.object_id
            where osp.object_id is null
            group by               
              p.database_name
            , p.database_id  
            , p.name
            , p.object_id
            , p.object_type
            , qs.creation_time
            , qs.last_execution_time

      if object_id(N'tempdb..#object_stats') is not null drop table #object_stats

      create table #object_stats 
      (  database_name sysname
         , database_id int
         , object_name sysname
         , object_id int
         , plan_instance_id int
         , object_type nvarchar(60)
         , cached_time_ms datetime
         , last_execution_time_ms datetime
         , cache_to_last_exec_minutes int
         , execution_count  bigint
         , avg_time_ms   decimal(38, 2), total_time_ms   decimal(38, 2), min_time_ms   decimal(38, 2), max_time_ms   decimal(38, 2)
         , avg_cpu_ms       decimal(38, 2), total_cpu_ms       decimal(38, 2), min_cpu            decimal(38, 2), max_cpu            decimal(38, 2)
         , avg_logical_reads  decimal(38, 2), total_logical_reads  bigint, min_logical_reads  bigint, max_logical_reads  bigint 
         , avg_physical_reads decimal(38, 2), total_physical_reads bigint, min_physical_reads bigint, max_physical_reads bigint 
         , avg_logical_writes decimal(38, 2), total_logical_writes bigint, min_logical_writes bigint, max_logical_writes bigint 
         , plan_handle varbinary(64) 
         , execution_count_rank    int
         , avg_time_rank   int  , total_time_rank   int, min_time_rank   int, max_time_rank   int
         , avg_cpu_ms_rank       int  , total_cpu_ms_rank       int, min_cpu_rank            int, max_cpu_rank            int
         , avg_logical_reads_rank  int  , total_logical_reads_rank  int, min_logical_reads_rank  int, max_logical_reads_rank  int 
         , avg_physical_reads_rank int  , total_physical_reads_rank int, min_physical_reads_rank int, max_physical_reads_rank int 
         , avg_logical_writes_rank int  , total_logical_writes_rank int, min_logical_writes_rank int, max_logical_writes_rank int 
         )

         insert into #object_stats
            select osp.*
            , rank() over(order by execution_count desc)
            , rank() over(order by total_time_ms * 1.0/execution_count  desc)
            , rank() over(order by total_time_ms desc)
            , rank() over(order by min_time_ms desc)
            , rank() over(order by max_time_ms desc)
            , rank() over(order by total_cpu_ms * 1.0 / execution_count  desc)
            , rank() over(order by total_cpu_ms desc)
            , rank() over(order by min_cpu desc)
            , rank() over(order by max_cpu desc)
            , rank() over(order by total_logical_reads*1.0/execution_count  desc)
            , rank() over(order by total_logical_reads desc)
            , rank() over(order by min_logical_reads desc)
            , rank() over(order by max_logical_reads desc)
            , rank() over(order by total_physical_reads*1.0/execution_count  desc)
            , rank() over(order by total_physical_reads desc)
            , rank() over(order by min_physical_reads desc)
            , rank() over(order by max_physical_reads desc)
            , rank() over(order by total_logical_writes*1.0 /execution_count  desc)
            , rank() over(order by total_logical_writes desc)
            , rank() over(order by min_logical_writes desc)
            , rank() over(order by max_logical_writes desc)
          from #object_stats_prep osp


      /* assign overall ranking, and include y/n bit value, based on selected criteria*/
      if object_id(N'tempdb..#object_stats2') is not null drop table #object_stats2
      select *
            ,  case when @return_top_total_execution_count        = 1 then execution_count_rank      else 0 end
             + case when @return_top_avg_execution_time_ms           = 1 then avg_time_rank     else 0 end
             + case when @return_top_total_execution_time_ms         = 1 then total_time_rank   else 0 end
             + case when @return_min_execution_time_ms               = 1 then max_time_rank     else 0 end
             + case when @return_max_execution_time_ms               = 1 then min_time_rank     else 0 end
             + case when @return_top_avg_cpu                      = 1 then avg_cpu_ms_rank         else 0 end
             + case when @return_top_total_cpu                    = 1 then total_cpu_ms_rank       else 0 end
             + case when @return_max_cpu                          = 1 then max_cpu_rank              else 0 end
             + case when @return_min_cpu                          = 1 then min_cpu_rank              else 0 end
             + case when @return_top_avg_physical_reads           = 1 then avg_logical_reads_rank    else 0 end            
             + case when @return_top_total_physical_reads         = 1 then total_logical_reads_rank  else 0 end
             + case when @return_max_physical_reads               = 1 then max_logical_reads_rank    else 0 end
             + case when @return_min_physical_reads               = 1 then min_logical_reads_rank    else 0 end
             + case when @return_top_avg_logical_reads            = 1 then avg_physical_reads_rank   else 0 end            
             + case when @return_top_total_logical_reads          = 1 then total_physical_reads_rank else 0 end
             + case when @return_max_logical_reads                = 1 then max_physical_reads_rank   else 0 end
             + case when @return_min_logical_reads                = 1 then min_physical_reads_rank   else 0 end
             + case when @return_top_avg_logical_writes           = 1 then avg_logical_writes_rank   else 0 end            
             + case when @return_top_total_logical_writes         = 1 then total_logical_writes_rank else 0 end
             + case when @return_max_logical_writes               = 1 then max_logical_writes_rank   else 0 end
             + case when @return_min_logical_writes               = 1 then min_logical_writes_rank   else 0 end as overall_score 

            ,  case when @return_top_total_execution_count        = 1 and execution_count_rank      <= @top_n_value then 1 else 0 end
             + case when @return_top_avg_execution_time_ms           = 1 and avg_time_rank     <= @top_n_value then 1 else 0 end 
             + case when @return_top_total_execution_time_ms         = 1 and total_time_rank   <= @top_n_value then 1 else 0 end 
             + case when @return_min_execution_time_ms               = 1 and max_time_rank     <= @top_n_value then 1 else 0 end 
             + case when @return_max_execution_time_ms               = 1 and min_time_rank     <= @top_n_value then 1 else 0 end 
             + case when @return_top_avg_cpu                      = 1 and avg_cpu_ms_rank         <= @top_n_value then 1 else 0 end 
             + case when @return_top_total_cpu                    = 1 and total_cpu_ms_rank       <= @top_n_value then 1 else 0 end 
             + case when @return_max_cpu                          = 1 and max_cpu_rank              <= @top_n_value then 1 else 0 end 
             + case when @return_min_cpu                          = 1 and min_cpu_rank              <= @top_n_value then 1 else 0 end 
             + case when @return_top_avg_physical_reads           = 1 and avg_logical_reads_rank    <= @top_n_value then 1 else 0 end 
             + case when @return_top_total_physical_reads         = 1 and total_logical_reads_rank  <= @top_n_value then 1 else 0 end 
             + case when @return_max_physical_reads               = 1 and max_logical_reads_rank    <= @top_n_value then 1 else 0 end 
             + case when @return_min_physical_reads               = 1 and min_logical_reads_rank    <= @top_n_value then 1 else 0 end 
             + case when @return_top_avg_logical_reads            = 1 and avg_physical_reads_rank   <= @top_n_value then 1 else 0 end 
             + case when @return_top_total_logical_reads          = 1 and total_physical_reads_rank <= @top_n_value then 1 else 0 end 
             + case when @return_max_logical_reads                = 1 and max_physical_reads_rank   <= @top_n_value then 1 else 0 end 
             + case when @return_min_logical_reads                = 1 and min_physical_reads_rank   <= @top_n_value then 1 else 0 end 
             + case when @return_top_avg_logical_writes           = 1 and avg_logical_writes_rank   <= @top_n_value then 1 else 0 end 
             + case when @return_top_total_logical_writes         = 1 and total_logical_writes_rank <= @top_n_value then 1 else 0 end 
             + case when @return_max_logical_writes               = 1 and max_logical_writes_rank   <= @top_n_value then 1 else 0 end 
             + case when @return_min_logical_writes               = 1 and min_logical_writes_rank   <= @top_n_value then 1 else 0 end as top_n_ranking_count 
      into #object_stats2
      from #object_stats

      /* apply top n filters.  can't do this until i have statement-level values.*/
      if @apply_no_top_filters  = 0
         begin
            delete from os2
            from #object_stats2 os2
            where 
               (    (@return_top_total_execution_count  = 1 or execution_count_rank      > @top_n_value) 
                 and (@return_top_avg_execution_time_ms    = 1 or avg_time_rank     > @top_n_value) 
                 and (@return_top_total_execution_time_ms  = 1 or total_time_rank   > @top_n_value)
                 and (@return_min_execution_time_ms        = 1 or max_time_rank     > @top_n_value)
                 and (@return_max_execution_time_ms        = 1 or min_time_rank     > @top_n_value)
                 and (@return_top_avg_cpu               = 1 or avg_cpu_ms_rank         > @top_n_value)
                 and (@return_top_total_cpu             = 1 or total_cpu_ms_rank       > @top_n_value)
                 and (@return_max_cpu                   = 1 or max_cpu_rank              > @top_n_value)
                 and (@return_min_cpu                   = 1 or min_cpu_rank              > @top_n_value)
                 and (@return_top_avg_physical_reads    = 1 or avg_logical_reads_rank    > @top_n_value)
                 and (@return_top_total_physical_reads  = 1 or total_logical_reads_rank  > @top_n_value)
                 and (@return_max_physical_reads        = 1 or max_logical_reads_rank    > @top_n_value)
                 and (@return_min_physical_reads        = 1 or min_logical_reads_rank    > @top_n_value)
                 and (@return_top_avg_logical_reads     = 1 or avg_physical_reads_rank   > @top_n_value)
                 and (@return_top_total_logical_reads   = 1 or total_physical_reads_rank > @top_n_value)
                 and (@return_max_logical_reads         = 1 or max_physical_reads_rank   > @top_n_value)
                 and (@return_min_logical_reads         = 1 or min_physical_reads_rank   > @top_n_value)
                 and (@return_top_avg_logical_writes    = 1 or avg_logical_writes_rank   > @top_n_value)
                 and (@return_top_total_logical_writes  = 1 or total_logical_writes_rank > @top_n_value)
                 and (@return_max_logical_writes        = 1 or max_logical_writes_rank   > @top_n_value)
                 and (@return_min_logical_writes        = 1 or min_logical_writes_rank   > @top_n_value))
         end  --if @apply_no_top_filters  = 0

      if @include_object_query_plans = 1 and @return_plans_as_xml_or_text = N'xml'
         begin
            select 'object-level output - xml query plan' as output_type
             , x.database_name 
             , x.database_id 
             , x.object_name 
             , x.object_id 
             , x.plan_instance_id
             , x.object_type 
             , x.cached_time_ms 
             , x.last_execution_time_ms 
             , x.cache_to_last_exec_minutes 
             , execution_count  
             , avg_time_ms   , total_time_ms   , min_time_ms   , max_time_ms   
             , avg_cpu_ms       , total_cpu_ms       , min_cpu            , max_cpu            
             , avg_logical_reads  , total_logical_reads  , min_logical_reads  , max_logical_reads   
             , avg_physical_reads , total_physical_reads , min_physical_reads , max_physical_reads  
             , avg_logical_writes , total_logical_writes , min_logical_writes , max_logical_writes  
             , plan_handle
             , overall_score
             , dense_rank() over (order by overall_score) as overall_all_score_ranking
             , top_n_ranking_count
             --, case when top_n_ranking_count = 0 then 0 else cast(overall_score * 100.0/top_n_ranking_count as decimal(12, 2))end as rankings_met_pct
             --, execution_count_rank    
             --, avg_time_rank   , total_time_rank   , min_time_rank   , max_time_rank   
             --, avg_cpu_ms_rank       , total_cpu_ms_rank       , min_cpu_rank            , max_cpu_rank            
             --, avg_logical_reads_rank  , total_logical_reads_rank  , min_logical_reads_rank  , max_logical_reads_rank   
             --, avg_physical_reads_rank , total_physical_reads_rank , min_physical_reads_rank , max_physical_reads_rank  
             --, avg_logical_writes_rank , total_logical_writes_rank , min_logical_writes_rank , max_logical_writes_rank  
             --, statement_level_overall_score
             , y.query_plan
            from #object_stats2 x
            outer apply sys.dm_exec_query_plan(plan_handle) y
            where top_n_ranking_count > 0 or @apply_no_top_filters = 1
            order by database_name, x.object_name, plan_handle
         end --if @include_object_query_plans = 1 and @return_plans_as_xml_or_text = n'xml'

      if @include_object_query_plans = 1 and @return_plans_as_xml_or_text = N'text'
         begin
            select 'object-level output - text query plan' as output_type
             , x.database_name 
             , x.database_id 
             , x.object_name 
             , x.object_id 
             , x.plan_instance_id
             , x.object_type 
             , x.cached_time_ms 
             , x.last_execution_time_ms 
             , x.cache_to_last_exec_minutes 
             , execution_count  
             , avg_time_ms   , total_time_ms   , min_time_ms   , max_time_ms   
             , avg_cpu_ms       , total_cpu_ms       , min_cpu            , max_cpu            
             , avg_logical_reads  , total_logical_reads  , min_logical_reads  , max_logical_reads   
             , avg_physical_reads , total_physical_reads , min_physical_reads , max_physical_reads  
             , avg_logical_writes , total_logical_writes , min_logical_writes , max_logical_writes  
             , plan_handle
             , overall_score
             , dense_rank() over(order by overall_score) as overall_all_score_ranking
             , top_n_ranking_count
             , @top_n_object_rankings_checked as top_n_rankings_checked
             , case when top_n_ranking_count = 0 then 0 else cast(overall_score * 100.0/top_n_ranking_count as decimal(12, 2))end as rankings_met_pct
             , execution_count_rank    
             --, avg_time_rank   , total_time_rank   , min_time_rank   , max_time_rank   
             --, avg_cpu_ms_rank       , total_cpu_ms_rank       , min_cpu_rank            , max_cpu_rank            
             --, avg_logical_reads_rank  , total_logical_reads_rank  , min_logical_reads_rank  , max_logical_reads_rank   
             --, avg_physical_reads_rank , total_physical_reads_rank , min_physical_reads_rank , max_physical_reads_rank  
             --, avg_logical_writes_rank , total_logical_writes_rank , min_logical_writes_rank , max_logical_writes_rank
             --, statement_level_overall_score  
             , y.query_plan
            from #object_stats2 x
            outer apply sys.dm_exec_text_query_plan(plan_handle, 0, -1) y
            where top_n_ranking_count > 0 or @apply_no_top_filters = 1
            order by database_name, object_name, plan_handle
         end --if @include_object_query_plans = 1 and @return_plans_as_xml_or_text = n'text'

      if @include_object_query_plans = 0
         begin
            select 'object-level output - no query plan' as output_type
             , x.database_name 
             , x.database_id 
             , x.object_name 
             , x.object_id 
             , x.plan_instance_id
             , x.object_type 
             , x.cached_time_ms 
             , x.last_execution_time_ms 
             , x.cache_to_last_exec_minutes 
             , execution_count  
             , avg_time_ms   , total_time_ms   , min_time_ms   , max_time_ms   
             , avg_cpu_ms       , total_cpu_ms       , min_cpu            , max_cpu            
             , avg_logical_reads  , total_logical_reads  , min_logical_reads  , max_logical_reads   
             , avg_physical_reads , total_physical_reads , min_physical_reads , max_physical_reads  
             , avg_logical_writes , total_logical_writes , min_logical_writes , max_logical_writes  
             , plan_handle
             , overall_score
             , dense_rank() over(order by overall_score) as overall_all_score_ranking
             --, statement_level_overall_score  
             , top_n_ranking_count
             , @top_n_object_rankings_checked as top_n_rankings_checked
             , case when top_n_ranking_count = 0 then 0 else cast(overall_score * 100.0/top_n_ranking_count as decimal(12, 2))end as rankings_met_pct
             , execution_count_rank    
             --, avg_time_rank   , total_time_rank   , min_time_rank   , max_time_rank   
             --, avg_cpu_ms_rank       , total_cpu_ms_rank       , min_cpu_rank            , max_cpu_rank            
             --, avg_logical_reads_rank  , total_logical_reads_rank  , min_logical_reads_rank  , max_logical_reads_rank   
             --, avg_physical_reads_rank , total_physical_reads_rank , min_physical_reads_rank , max_physical_reads_rank  
             --, avg_logical_writes_rank , total_logical_writes_rank , min_logical_writes_rank , max_logical_writes_rank  
            from #object_stats2 x
            where top_n_ranking_count > 0 or @apply_no_top_filters = 1
            order by database_name, object_name, plan_handle
         end -- if @include_object_query_plans = 0

      if @show_object_statement_details = 1
         begin

            set @sql_text =  ''
            declare @sqlmajorver int, @sqlminorver int, @sqlbuild int
            select @sqlmajorver = convert(int, (@@microsoftversion / 0x1000000) & 0xff);
            select @sqlminorver = convert(int, (@@microsoftversion / 0x10000) & 0xff);
            select @sqlbuild = convert(int, @@microsoftversion & 0xffff);

            if (@sqlmajorver = 11 and ((@sqlminorver = 0 and @sqlbuild >= 6020) or @sqlminorver >= 1))
               or 
               (@sqlmajorver = 12 and (@sqlminorver >= 1))
               or 
               (@sqlmajorver = 13)
                  begin
                     set @sql_text = '

 select ''object statement-level output'' as output_type
    , x.database_name
    , x.object_name
    , x.object_type
    , x.plan_instance_id
    , qs.last_execution_time
    , row_number() over(partition by qs.plan_handle order by qs.statement_start_offset) as row_num
    , qs.execution_count
    , qs.total_elapsed_time/1000.0/qs.execution_count  as avg_time_ms
    , qs.total_elapsed_time/1000.0                     as total_time_ms
    , qs.min_elapsed_time/1000.0                       as min_time_ms
    , qs.max_elapsed_time/1000.0                       as max_time_ms
    , qs.total_worker_time/1000.0/qs.execution_count   as avg_cpu_ms
    , qs.total_worker_time/1000.0                      as total_cpu_ms
    , qs.min_worker_time/1000.0                        as min_cpu_ms
    , qs.max_worker_time/1000.0                        as max_cpu_ms
    , qs.total_logical_reads/qs.execution_count  *1.0  as avg_logical_reads
    , qs.total_logical_reads
    , qs.min_logical_reads
    , qs.max_logical_reads
    , qs.total_physical_reads/qs.execution_count  *1.0 as avg_physical_reads
    , qs.total_physical_reads
    , qs.min_physical_reads
    , qs.max_physical_reads
    , qs.total_logical_writes/qs.execution_count *1.0  as avg_logical_writes
    , qs.total_logical_writes
    , qs.min_logical_writes
    , qs.max_logical_writes
    , qs.total_rows
    , qs.last_rows
    , qs.min_rows
    , qs.max_rows

    , qs.total_dop              
    , qs.last_dop               
    , qs.min_dop                
    , qs.max_dop                
    , qs.total_grant_kb         
    , qs.last_grant_kb          
    , qs.min_grant_kb           
    , qs.max_grant_kb           
    , qs.total_used_grant_kb    
    , qs.last_used_grant_kb     
    , qs.min_used_grant_kb      
    , qs.max_used_grant_kb      
    , qs.total_ideal_grant_kb   
    , qs.last_ideal_grant_kb    
    , qs.min_ideal_grant_kb     
    , qs.max_ideal_grant_kb     
    , qs.total_reserved_threads 
    , qs.last_reserved_threads  
    , qs.min_reserved_threads   
    , qs.max_reserved_threads   
    , qs.total_used_threads     
    , qs.last_used_threads      
    , qs.min_used_threads       
    , qs.max_used_threads  

    , qs.plan_handle
    , replace(replace(replace(     substring(qt.text,qs.statement_start_offset/2 +1, 
               (case when qs.statement_end_offset = -1 
                     then len(convert(nvarchar(max), qt.text)) * 2 
                     else qs.statement_end_offset end -
                          qs.statement_start_offset
               )/2
           ), char(10), '' ''), char(13), '' ''), char(9) , '' '') as query_text
 from #object_stats2 x 
 --join #statement_level sl on sl.plan_handle = x.plan_handle
 left join sys.dm_exec_query_stats qs on qs.plan_handle = x.plan_handle-- and sl.sql_handle = qs.sql_handle and sl.statement_start_offset = qs.statement_start_offset and qs.statement_end_offset = sl.statement_end_offset
 outer apply sys.dm_exec_sql_text(qs.sql_handle) as qt 
 where top_n_ranking_count > 0 or @apply_no_top_filters = 1
 order by object_name, plan_handle, row_num'
                    
                  end
               else
                  begin
                     set @sql_text = '

 select ''object statement-level output'' as output_type
    , x.database_name
    , x.object_name
    , x.object_type
    , x.plan_instance_id
    , qs.last_execution_time
    , row_number() over(partition by qs.plan_handle order by qs.statement_start_offset) as row_num
    , qs.execution_count
    , qs.total_elapsed_time/1000.0/qs.execution_count  as avg_time_ms
    , qs.total_elapsed_time/1000.0                     as total_time_ms
    , qs.min_elapsed_time/1000.0                       as min_time_ms
    , qs.max_elapsed_time/1000.0                       as max_time_ms
    , qs.total_worker_time/1000.0/qs.execution_count   as avg_cpu_ms
    , qs.total_worker_time/1000.0                      as total_cpu_ms
    , qs.min_worker_time/1000.0                        as min_cpu_ms
    , qs.max_worker_time/1000.0                        as max_cpu_ms
    , qs.total_logical_reads/qs.execution_count  *1.0  as avg_logical_reads
    , qs.total_logical_reads
    , qs.min_logical_reads
    , qs.max_logical_reads
    , qs.total_physical_reads/qs.execution_count  *1.0 as avg_physical_reads
    , qs.total_physical_reads
    , qs.min_physical_reads
    , qs.max_physical_reads
    , qs.total_logical_writes/qs.execution_count *1.0  as avg_logical_writes
    , qs.total_logical_writes
    , qs.min_logical_writes
    , qs.max_logical_writes
    , qs.total_rows
    , qs.last_rows
    , qs.min_rows
    , qs.max_rows

    , null as total_dop              
    , null as last_dop               
    , null as min_dop                
    , null as max_dop                
    , null as total_grant_kb         
    , null as last_grant_kb          
    , null as min_grant_kb           
    , null as max_grant_kb           
    , null as total_used_grant_kb    
    , null as last_used_grant_kb     
    , null as min_used_grant_kb      
    , null as max_used_grant_kb      
    , null as total_ideal_grant_kb   
    , null as last_ideal_grant_kb    
    , null as min_ideal_grant_kb     
    , null as max_ideal_grant_kb     
    , null as total_reserved_threads 
    , null as last_reserved_threads  
    , null as min_reserved_threads   
    , null as max_reserved_threads   
    , null as total_used_threads     
    , null as last_used_threads      
    , null as min_used_threads       
    , null as max_used_threads  

    , qs.plan_handle
    , replace(replace(replace(     substring(qt.text,qs.statement_start_offset/2 +1, 
               (case when qs.statement_end_offset = -1 
                     then len(convert(nvarchar(max), qt.text)) * 2 
                     else qs.statement_end_offset end -
                          qs.statement_start_offset
               )/2
           ), char(10), '' ''), char(13), '' ''), char(9) , '' '') as query_text
 from #object_stats2 x 
 --join #statement_level sl on sl.plan_handle = x.plan_handle
 left join sys.dm_exec_query_stats qs on qs.plan_handle = x.plan_handle-- and sl.sql_handle = qs.sql_handle and sl.statement_start_offset = qs.statement_start_offset and qs.statement_end_offset = sl.statement_end_offset
 outer apply sys.dm_exec_sql_text(qs.sql_handle) as qt 
 where top_n_ranking_count > 0 or @apply_no_top_filters = 1
 order by object_name, plan_handle, row_num'

                  end

            set @sql_text = replace(@sql_text, '@apply_no_top_filters', @apply_no_top_filters)
            print @sql_text
            exec (@sql_text)
                     end --if @show_object_statement_details = 1'

      if @show_ad_hoc = 1
         begin
            if object_id(N'tempdb..#all_query_stats_entries') is not null drop table #all_query_stats_entries

            set @sql_text =  ''
            select @sqlmajorver = convert(int, (@@microsoftversion / 0x1000000) & 0xff);
            select @sqlminorver = convert(int, (@@microsoftversion / 0x10000) & 0xff);
            select @sqlbuild = convert(int, @@microsoftversion & 0xffff);

            if (@sqlmajorver = 11 and ((@sqlminorver = 0 and @sqlbuild >= 6020) or @sqlminorver >= 1))
               or 
               (@sqlmajorver = 12 and (@sqlminorver >= 1))
               or 
               (@sqlmajorver = 13)
                  begin
                     set @sql_text = '
select  query_hash
, query_plan_hash
, max(plan_generation_num) as max_plan_generation_num
, count(*) as cache_enties
, sum(qs.execution_count) as execution_count
, min(qs.creation_time) as min_cache_datetime
, max(qs.creation_time)  as max_cache_datetime
, min(qs.last_execution_time) as min_last_execution_time
, max(qs.last_execution_time) as max_last_execution_time
, sum(qs.total_elapsed_time/1000.0)/sum(qs.execution_count)  as avg_time_ms
, sum(qs.total_elapsed_time/1000.0 ) as  total_time_ms
, min(qs.min_elapsed_time/1000.0 ) as min_time_ms
, max(qs.max_elapsed_time/1000.0 ) as max_time_ms
, sum(qs.total_worker_time/1000.0)/sum(qs.execution_count)   as avg_cpu_ms
, sum(qs.total_worker_time/1000.0) as total_cpu_ms
, min(qs.min_worker_time/1000.0) as min_cpu_ms
, max(qs.max_worker_time/1000.0) as max_cpu_ms
, sum(qs.total_logical_reads) *1.0/sum(qs.execution_count)  as avg_logical_reads
, sum(qs.total_logical_reads ) as total_logical_reads
, min(qs.min_logical_reads ) as min_logical_reads
, max(qs.max_logical_reads ) as max_logical_reads
, sum(qs.total_physical_reads *1.0)/sum(qs.execution_count)  as avg_physical_reads
, sum(qs.total_physical_reads ) as total_physical_reads
, min(qs.min_physical_reads ) as min_physical_reads
, max(qs.max_physical_reads ) as max_physical_reads
, sum(qs.total_logical_writes*1.0)/sum(qs.execution_count)   as avg_logical_writes
, sum(qs.total_logical_writes ) as total_logical_writes
, min(qs.min_logical_writes ) as min_logical_writes
, max(qs.max_logical_writes ) as max_logical_writes
, sum(qs.total_rows ) as total_rows
, sum(qs.total_rows*1.0)/sum(qs.execution_count) as avg_rows
, min(qs.min_rows ) as min_rows
, max(qs.max_rows ) as max_rows
, min(qs.min_dop) as min_dop
, max(qs.max_dop) as max_dop
, cast(sum(qs.total_grant_kb/1024.0) as decimal(38, 3)) as total_memory_grant_kb

, cast(min(qs.min_grant_kb/1024.0) as decimal(38, 3)) as min_memory_grant_kb
, cast(max(qs.max_grant_kb/1024.0) as decimal(38, 3)) as max_memory_grant_kb
, cast(sum(qs.total_grant_kb/1024.0) as decimal(38, 3)) as total_used_memory_grant_kb
, cast(min(qs.total_grant_kb/1024.0) as decimal(38, 3)) as min_used_memory_grant_kb
, cast(max(qs.total_grant_kb/1024.0) as decimal(38, 3)) as max_used_memory_grant_kb
, cast(sum(qs.total_grant_kb/1024.0) as decimal(38, 3)) as total_ideal_memory_grant_kb
, cast(min(qs.total_grant_kb/1024.0) as decimal(38, 3)) as min_ideal_memory_grant_kb
, cast(max(qs.total_grant_kb/1024.0) as decimal(38, 3)) as max_ideal_memory_grant_kb
, sum(qs.total_reserved_threads) as  total_reserved_threads
, min(qs.min_reserved_threads) as  min_reserved_threads  
, max(qs.max_reserved_threads) as  max_reserved_threads  
, sum(qs.total_used_threads) as  total_used_threads    
, min(qs.min_used_threads ) as  min_used_threads      
, max(qs.max_used_threads ) as  max_used_threads      
, max(qs.sql_handle) as sample_sql_handle
, max(qs.plan_handle) as sample_plan_handle
, rank() over(order by count(*)  desc)                as cache_enties_rank
, rank() over(order by sum(qs.execution_count)  desc) as execution_count_rank
, rank() over(order by sum(qs.total_elapsed_time)*1.0 / sum(qs.execution_count) desc)    as avg_time_rank
, rank() over(order by sum(qs.total_elapsed_time )  desc) as total_time_rank
, rank() over(order by min(qs.min_elapsed_time )  desc)   as min_time_rank
, rank() over(order by max(qs.max_elapsed_time )  desc)   as max_time_rank
, rank() over(order by sum(qs.total_worker_time*1.0)/sum(qs.execution_count)    desc)    as avg_cpu_ms_rank
, rank() over(order by sum(qs.total_worker_time)   desc) as total_cpu_ms_rank
, rank() over(order by min(qs.min_worker_time)  desc)    as min_cpu_ms_rank
, rank() over(order by max(qs.max_worker_time)  desc)    as max_cpu_ms_rank
, rank() over(order by sum(qs.total_logical_reads) *1.0/sum(qs.execution_count)  desc)   as avg_logical_reads_rank
, rank() over(order by sum(qs.total_logical_reads )  desc) as total_logical_reads_rank
, rank() over(order by min(qs.min_logical_reads )   desc)  as min_logical_reads_rank
, rank() over(order by max(qs.max_logical_reads )   desc)  as max_logical_reads_rank
, rank() over(order by sum(qs.total_physical_reads *1.0)/sum(qs.execution_count)   desc) as avg_physical_reads_rank
, rank() over(order by sum(qs.total_physical_reads )  desc) as total_physical_reads_rank
, rank() over(order by min(qs.min_physical_reads )  desc)   as min_physical_reads_rank
, rank() over(order by max(qs.max_physical_reads ) desc)    as max_physical_reads_rank
, rank() over(order by sum(qs.total_logical_writes*1.0)/sum(qs.execution_count)  desc)   as avg_logical_writes_rank
, rank() over(order by sum(qs.total_logical_writes ) desc) as total_logical_writes_rank
, rank() over(order by min(qs.min_logical_writes )  desc)  as min_logical_writes_rank
, rank() over(order by max(qs.max_logical_writes ) desc)   as max_logical_writes_rank
, rank() over(order by sum(qs.total_rows )  desc)          as total_rows_rank
, rank() over(order by sum(qs.total_rows*1.0)/sum(qs.execution_count)  desc) as avg_rows_rank
, rank() over(order by min(qs.min_rows ) desc) as min_rows_rank
, rank() over(order by max(qs.max_rows ) desc) as max_rows_rank
into #all_query_stats_entries       
from sys.dm_exec_query_stats qs 
left join #all_object_stats o on qs.plan_handle = o.plan_handle 
where o.plan_handle is null
group by query_hash
, query_plan_hash
'
end
else
begin
set @sql_text = '
select  query_hash
, query_plan_hash
, max(plan_generation_num) as max_plan_generation_num
, count(*) as cache_enties
, sum(qs.execution_count) as execution_count
, min(qs.creation_time) as min_cache_datetime
, max(qs.creation_time)  as max_cache_datetime
, min(qs.last_execution_time) as min_last_execution_time
, max(qs.last_execution_time) as max_last_execution_time
, sum(qs.total_elapsed_time/1000.0)/sum(qs.execution_count)  as avg_time_ms
, sum(qs.total_elapsed_time/1000.0 ) as  total_time_ms
, min(qs.min_elapsed_time/1000.0 ) as min_time_ms
, max(qs.max_elapsed_time/1000.0 ) as max_time_ms
, sum(qs.total_worker_time/1000.0)/sum(qs.execution_count)   as avg_cpu_ms
, sum(qs.total_worker_time/1000.0) as total_cpu_ms
, min(qs.min_worker_time/1000.0) as min_cpu_ms
, max(qs.max_worker_time/1000.0) as max_cpu_ms
, sum(qs.total_logical_reads) *1.0/sum(qs.execution_count)  as avg_logical_reads
, sum(qs.total_logical_reads ) as total_logical_reads
, min(qs.min_logical_reads ) as min_logical_reads
, max(qs.max_logical_reads ) as max_logical_reads
, sum(qs.total_physical_reads *1.0)/sum(qs.execution_count)  as avg_physical_reads
, sum(qs.total_physical_reads ) as total_physical_reads
, min(qs.min_physical_reads ) as min_physical_reads
, max(qs.max_physical_reads ) as max_physical_reads
, sum(qs.total_logical_writes*1.0)/sum(qs.execution_count)   as avg_logical_writes
, sum(qs.total_logical_writes ) as total_logical_writes
, min(qs.min_logical_writes ) as min_logical_writes
, max(qs.max_logical_writes ) as max_logical_writes
, sum(qs.total_rows ) as total_rows
, sum(qs.total_rows*1.0)/sum(qs.execution_count) as avg_rows
, min(qs.min_rows ) as min_rows
, max(qs.max_rows ) as max_rows
, min(qs.min_dop) as min_dop
, max(qs.max_dop) as max_dop
, cast(sum(qs.total_grant_kb/1024.0) as decimal(38, 3)) as total_memory_grant_kb

, null as min_memory_grant_kb
, null as max_memory_grant_kb
, null as total_used_memory_grant_kb
, null as min_used_memory_grant_kb
, null as max_used_memory_grant_kb
, null as total_ideal_memory_grant_kb
, null as min_ideal_memory_grant_kb
, null as max_ideal_memory_grant_kb
, null as total_reserved_threads
, null as min_reserved_threads  
, null as max_reserved_threads  
, null as total_used_threads    
, null as min_used_threads      
, null as max_used_threads      

, max(qs.sql_handle) as sample_sql_handle
, max(qs.plan_handle) as sample_plan_handle

, rank() over(order by count(*)  desc)                                                   as cache_enties_rank
, rank() over(order by sum(qs.execution_count)  desc)                                    as execution_count_rank
, rank() over(order by sum(qs.total_elapsed_time)*1.0 / sum(qs.execution_count) desc)        as avg_time_rank
, rank() over(order by sum(qs.total_elapsed_time )  desc)                                as total_time_rank
, rank() over(order by min(qs.min_elapsed_time )  desc)                                  as min_time_rank
, rank() over(order by max(qs.max_elapsed_time )  desc)                                  as max_time_rank
, rank() over(order by sum(qs.total_worker_time*1.0)/sum(qs.execution_count)    desc)    as avg_cpu_ms_rank
, rank() over(order by sum(qs.total_worker_time)   desc)                                 as total_cpu_ms_rank
, rank() over(order by min(qs.min_worker_time)  desc)                                    as min_cpu_ms_rank
, rank() over(order by max(qs.max_worker_time)  desc)                                    as max_cpu_ms_rank
, rank() over(order by sum(qs.total_logical_reads) *1.0/sum(qs.execution_count)  desc)   as avg_logical_reads_rank
, rank() over(order by sum(qs.total_logical_reads )  desc)                               as total_logical_reads_rank
, rank() over(order by min(qs.min_logical_reads )   desc)                                as min_logical_reads_rank
, rank() over(order by max(qs.max_logical_reads )   desc)                                as max_logical_reads_rank
, rank() over(order by sum(qs.total_physical_reads *1.0)/sum(qs.execution_count)   desc) as avg_physical_reads_rank
, rank() over(order by sum(qs.total_physical_reads )  desc)                              as total_physical_reads_rank
, rank() over(order by min(qs.min_physical_reads )  desc)                                as min_physical_reads_rank
, rank() over(order by max(qs.max_physical_reads ) desc)                                 as max_physical_reads_rank
, rank() over(order by sum(qs.total_logical_writes*1.0)/sum(qs.execution_count)  desc)   as avg_logical_writes_rank
, rank() over(order by sum(qs.total_logical_writes ) desc)                               as total_logical_writes_rank
, rank() over(order by min(qs.min_logical_writes )  desc)                                as min_logical_writes_rank
, rank() over(order by max(qs.max_logical_writes ) desc)                                 as max_logical_writes_rank
, rank() over(order by sum(qs.total_rows )  desc)                                        as total_rows_rank
, rank() over(order by sum(qs.total_rows*1.0)/sum(qs.execution_count)  desc)             as avg_rows_rank
, rank() over(order by min(qs.min_rows ) desc)                                           as min_rows_rank
, rank() over(order by max(qs.max_rows ) desc)                                           as max_rows_rank
into #all_query_stats_entries       
from sys.dm_exec_query_stats qs 
left join #all_object_stats o on qs.plan_handle = o.plan_handle 
where o.plan_handle is null
group by query_hash
      , query_plan_hash
'
end


            set @sql_text = replace(@sql_text, '@apply_no_top_filters', @apply_no_top_filters)
            print @sql_text
            exec (@sql_text)


         if object_id(N'tempdb..#ad_hoc_rank_filtered') is not null drop table #ad_hoc_rank_filtered

         select *
            ,  case when @return_top_cached_entries          = 1 then cache_enties_rank         else 0 end
             + case when @return_top_total_execution_count   = 1 then execution_count_rank      else 0 end
             + case when @return_top_avg_execution_time_ms   = 1 then avg_time_rank             else 0 end
             + case when @return_top_total_execution_time_ms = 1 then total_time_rank           else 0 end
             + case when @return_min_execution_time_ms       = 1 then max_time_rank             else 0 end
             + case when @return_max_execution_time_ms       = 1 then min_time_rank             else 0 end
             + case when @return_top_avg_cpu                 = 1 then avg_cpu_ms_rank           else 0 end
             + case when @return_top_total_cpu               = 1 then total_cpu_ms_rank         else 0 end
             + case when @return_max_cpu                     = 1 then max_cpu_ms_rank           else 0 end
             + case when @return_min_cpu                     = 1 then min_cpu_ms_rank           else 0 end
             + case when @return_top_avg_physical_reads      = 1 then avg_logical_reads_rank    else 0 end            
             + case when @return_top_total_physical_reads    = 1 then total_logical_reads_rank  else 0 end
             + case when @return_max_physical_reads          = 1 then max_logical_reads_rank    else 0 end
             + case when @return_min_physical_reads          = 1 then min_logical_reads_rank    else 0 end
             + case when @return_top_avg_logical_reads       = 1 then avg_physical_reads_rank   else 0 end            
             + case when @return_top_total_logical_reads     = 1 then total_physical_reads_rank else 0 end
             + case when @return_max_logical_reads           = 1 then max_physical_reads_rank   else 0 end
             + case when @return_min_logical_reads           = 1 then min_physical_reads_rank   else 0 end
             + case when @return_top_avg_logical_writes      = 1 then avg_logical_writes_rank   else 0 end            
             + case when @return_top_total_logical_writes    = 1 then total_logical_writes_rank else 0 end
             + case when @return_max_logical_writes          = 1 then max_logical_writes_rank   else 0 end
             + case when @return_min_logical_writes          = 1 then min_logical_writes_rank   else 0 end 
             + case when @return_top_avg_rows                = 1 then total_rows_rank           else 0 end 
             + case when @return_top_total_rows              = 1 then avg_rows_rank             else 0 end 
             + case when @return_max_rows                    = 1 then min_rows_rank             else 0 end 
             + case when @return_min_rows                    = 1 then max_rows_rank             else 0 end as overall_score 
                                                             
            ,  case when @return_top_cached_entries          = 1 and cache_enties_rank         <= @top_n_value then 1 else 0 end
             + case when @return_top_total_execution_count   = 1 and execution_count_rank      <= @top_n_value then 1 else 0 end
             + case when @return_top_avg_execution_time_ms   = 1 and avg_time_rank             <= @top_n_value then 1 else 0 end 
             + case when @return_top_total_execution_time_ms = 1 and total_time_rank           <= @top_n_value then 1 else 0 end 
             + case when @return_min_execution_time_ms       = 1 and max_time_rank             <= @top_n_value then 1 else 0 end 
             + case when @return_max_execution_time_ms       = 1 and min_time_rank             <= @top_n_value then 1 else 0 end 
             + case when @return_top_avg_cpu                 = 1 and avg_cpu_ms_rank           <= @top_n_value then 1 else 0 end 
             + case when @return_top_total_cpu               = 1 and total_cpu_ms_rank         <= @top_n_value then 1 else 0 end 
             + case when @return_max_cpu                     = 1 and max_cpu_ms_rank           <= @top_n_value then 1 else 0 end 
             + case when @return_min_cpu                     = 1 and min_cpu_ms_rank           <= @top_n_value then 1 else 0 end 
             + case when @return_top_avg_physical_reads      = 1 and avg_logical_reads_rank    <= @top_n_value then 1 else 0 end 
             + case when @return_top_total_physical_reads    = 1 and total_logical_reads_rank  <= @top_n_value then 1 else 0 end 
             + case when @return_max_physical_reads          = 1 and max_logical_reads_rank    <= @top_n_value then 1 else 0 end 
             + case when @return_min_physical_reads          = 1 and min_logical_reads_rank    <= @top_n_value then 1 else 0 end 
             + case when @return_top_avg_logical_reads       = 1 and avg_physical_reads_rank   <= @top_n_value then 1 else 0 end 
             + case when @return_top_total_logical_reads     = 1 and total_physical_reads_rank <= @top_n_value then 1 else 0 end 
             + case when @return_max_logical_reads           = 1 and max_physical_reads_rank   <= @top_n_value then 1 else 0 end 
             + case when @return_min_logical_reads           = 1 and min_physical_reads_rank   <= @top_n_value then 1 else 0 end 
             + case when @return_top_avg_logical_writes      = 1 and avg_logical_writes_rank   <= @top_n_value then 1 else 0 end 
             + case when @return_top_total_logical_writes    = 1 and total_logical_writes_rank <= @top_n_value then 1 else 0 end 
             + case when @return_max_logical_writes          = 1 and max_logical_writes_rank   <= @top_n_value then 1 else 0 end 
             + case when @return_min_logical_writes          = 1 and min_logical_writes_rank   <= @top_n_value then 1 else 0 end 
             + case when @return_top_avg_rows                = 1 and total_rows_rank           <= @top_n_value then 1 else 0 end 
             + case when @return_top_total_rows              = 1 and avg_rows_rank             <= @top_n_value then 1 else 0 end 
             + case when @return_max_rows                    = 1 and min_rows_rank             <= @top_n_value then 1 else 0 end 
             + case when @return_min_rows                    = 1 and max_rows_rank             <= @top_n_value then 1 else 0 end as top_n_ranking_count 
         into #ad_hoc_rank_filtered
         from #all_query_stats_entries
         where 
         @apply_no_top_filters = 1 or
         (
            (@return_top_cached_entries           = 1 and cache_enties_rank              <= @top_n_value)
         or (@return_top_total_execution_count    = 1 and execution_count_rank           <= @top_n_value)
         or (@return_top_avg_execution_time_ms    = 1 and avg_time_rank                  <= @top_n_value)
         or (@return_top_total_execution_time_ms  = 1 and total_time_rank                <= @top_n_value)
         or (@return_max_execution_time_ms        = 1 and min_time_rank                  <= @top_n_value)
         or (@return_min_execution_time_ms        = 1 and max_time_rank                  <= @top_n_value)
         or (@return_top_avg_cpu                  = 1 and avg_cpu_ms_rank                <= @top_n_value)
         or (@return_top_total_cpu                = 1 and total_cpu_ms_rank              <= @top_n_value)
         or (@return_max_cpu                      = 1 and min_cpu_ms_rank                <= @top_n_value)
         or (@return_min_cpu                      = 1 and max_cpu_ms_rank                <= @top_n_value)
         or (@return_top_avg_physical_reads       = 1 and avg_logical_reads_rank         <= @top_n_value)
         or (@return_top_total_physical_reads     = 1 and total_logical_reads_rank       <= @top_n_value)
         or (@return_max_physical_reads           = 1 and min_logical_reads_rank         <= @top_n_value)
         or (@return_min_physical_reads           = 1 and max_logical_reads_rank         <= @top_n_value)
         or (@return_top_avg_logical_reads        = 1 and avg_physical_reads_rank        <= @top_n_value)
         or (@return_top_total_logical_reads      = 1 and total_physical_reads_rank      <= @top_n_value)
         or (@return_max_logical_reads            = 1 and min_physical_reads_rank        <= @top_n_value)
         or (@return_min_logical_reads            = 1 and max_physical_reads_rank        <= @top_n_value)
         or (@return_top_avg_logical_writes       = 1 and avg_logical_writes_rank        <= @top_n_value)
         or (@return_top_total_logical_writes     = 1 and total_logical_writes_rank      <= @top_n_value)
         or (@return_max_logical_writes           = 1 and min_logical_writes_rank        <= @top_n_value)
         or (@return_min_logical_writes           = 1 and max_logical_writes_rank        <= @top_n_value)
         or (@return_top_avg_rows                 = 1 and total_rows_rank                <= @top_n_value)
         or (@return_top_total_rows               = 1 and avg_rows_rank                  <= @top_n_value)
         or (@return_max_rows                     = 1 and min_rows_rank                  <= @top_n_value)
         or (@return_min_rows                     = 1 and max_rows_rank                  <= @top_n_value))


/* get sample query plan and sql_text*/
if @include_ad_hoc_query_plans = 1
   begin  
	select *
	from (
      select
         q.query_hash
       , q.query_plan_hash
       , replace(replace(replace(     substring(qt.text,qs.statement_start_offset/2 +1, 
                     (case when qs.statement_end_offset = -1 
                           then len(convert(nvarchar(max), qt.text)) * 2 
                           else qs.statement_end_offset end -
                                 qs.statement_start_offset
                     )/2
                  ), char(10), ' '), char(13), ' '), char(9) , ' ') as query_text
       , y.query_plan
       , q.cache_enties
       , q.execution_count
       , q.min_cache_datetime
       , q.max_cache_datetime    
       , q.min_last_execution_time  
       , q.max_last_execution_time  
       , q.avg_time_ms
       , q.total_time_ms
       , q.min_time_ms
       , q.max_time_ms
       , q.avg_cpu_ms
       , q.total_cpu_ms
       , q.min_cpu_ms
       , q.max_cpu_ms
       , q.avg_logical_reads
       , q.total_logical_reads
       , q.min_logical_reads
       , q.max_logical_reads
       , q.avg_physical_reads
       , q.total_physical_reads
       , q.min_physical_reads
       , q.max_physical_reads
       , q.avg_logical_writes
       , q.total_logical_writes
       , q.min_logical_writes
       , q.max_logical_writes
       , q.total_rows
       , q.avg_rows
       , q.min_rows
       , q.max_rows
       , q.sample_sql_handle
       , sample_plan_handle
	   , row_number() over(partition by q.query_hash, q.query_plan_hash order by getdate()) as row_num
         from #ad_hoc_rank_filtered q
         left join sys.dm_exec_query_stats qs on qs.plan_handle = q.sample_plan_handle
         and 		 qs.query_hash = q.query_hash
         and qs.query_plan_hash = q.query_plan_hash
		 and qs.plan_generation_num = q.max_plan_generation_num
          outer apply sys.dm_exec_sql_text(q.sample_sql_handle) as qt 
          outer apply sys.dm_exec_text_query_plan(sample_plan_handle, 0, -1) y ) tbl
		  where row_num = 1
         order by query_hash, query_plan_hash
   end -- if @include_ad_hoc_query_plans = 1


 

   if @include_ad_hoc_query_plans = 0
      begin  
	  select * 
	  from (
         select
            q.query_hash
          , q.query_plan_hash
          , replace(replace(replace(     substring(qt.text,qs.statement_start_offset/2 +1, 
                        (case when qs.statement_end_offset = -1 
                              then len(convert(nvarchar(max), qt.text)) * 2 
                              else qs.statement_end_offset end -
                                    qs.statement_start_offset
                        )/2
                     ), char(10), ' '), char(13), ' '), char(9) , ' ') as query_text
          , '' as query_plan
          , q.cache_enties
          , q.execution_count
          , q.min_cache_datetime
          , q.max_cache_datetime  
          , q.min_last_execution_time  
          , q.max_last_execution_time    
          , q.avg_time_ms
          , q.total_time_ms
          , q.min_time_ms
          , q.max_time_ms
          , q.avg_cpu_ms
          , q.total_cpu_ms
          , q.min_cpu_ms
          , q.max_cpu_ms
          , q.avg_logical_reads
          , q.total_logical_reads
          , q.min_logical_reads
          , q.max_logical_reads
          , q.avg_physical_reads
          , q.total_physical_reads
          , q.min_physical_reads
          , q.max_physical_reads
          , q.avg_logical_writes
          , q.total_logical_writes
          , q.min_logical_writes
          , q.max_logical_writes
          , q.total_rows
          , q.avg_rows
          , q.min_rows
          , q.max_rows
          , q.sample_sql_handle
          , sample_plan_handle
		  , row_number() over(partition by q.query_hash, q.query_plan_hash order by getdate()) as row_num
            from #ad_hoc_rank_filtered q
            left join sys.dm_exec_query_stats qs on qs.plan_handle = q.sample_plan_handle
            and qs.query_hash = q.query_hash
            and qs.query_plan_hash = q.query_plan_hash
             outer apply sys.dm_exec_sql_text(q.sample_sql_handle) as qt ) tbl
		  where row_num = 1
         order by query_hash, query_plan_hash
      end --if @include_ad_hoc_query_plans = 0
   end --if @show_ad_hoc = 0  

end
--end
