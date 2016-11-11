begin -- overview section
/* 
script        :  index analysis 
written by    :  john kauffman (The Jolly DBA)
assumes       :  sql 2008 or higher.  2005 does not allow DECLARE...SET logic in the same statement.
limitations   :  collation and case-sensitivity issues have been found when running on some databases.  corrections were made at the time, 
                           but have not been tested as the script has been updated.
                 Less common index types (XML, spatial, In-Memory, and Columnstore) are not expressly handled in this script.
-------------------------------------------------------------------------------------------------------------------------
functionality :  this script pulls information from the following sources:
                    - index metadata (sys.indexes, sys.partitions, sys.partition_stats, etc)
                    - missing indexes
                    - sys.dm_db_index_usage_stats
                    - sys.dm_db_index_operational_stats
                    - sys.dm_db_index_physical_stats
                    - sys.dm_os_buffer_descriptors -- shows amt of index in cache
                    - sp_estimate_data_compression_savings 
            -------------------------------------------------------------------------------------------------------------------------
DISCLAIMER:
      this sample code is provided for the purpose of illustration only and is not intended  to be used in a production environment.  this sample code and any related information are
      provided "as is" without warranty of any kind, either expressed or implied, including but not limited to the implied warranties of merchantability and/or fitness for a particular
      purpose.  we grant you a nonexclusive, royalty-free right to use and modify the sample code and to reproduce and distribute the object code form of the sample code, provided that you agree: 
      (i) to not use our name, logo, or trademarks to market your software product in which the sample code is embedded; 
      (ii) to include a valid copyright notice on your software product in which the sample code is embedded; 
      and (iii) to indemnify, hold harmless, and defend us and our suppliers from and against any claims or lawsuits, including attorneys fees, that arise or result from the use or distribution of the sample code.
*/  
------------------------------------------------------------------------------------------------
/* PARAMETERS OVERVIEW:
This script has a lot of parameters. They break into the following categories:
1.  Filters (for table names, index type, row counts, etc.)
2.  Functionality (Missing indices; index metadata and usage; compression estimation; fragmentation calculation; presence in buffer pool)
3.  Optional reports derived from metadata and usage.  examples include 'hypothetical indices', 'heaps', 'unused indices'.
      All can be derived via sorting or pivoting the data in excel, but this just makes it easier.
4.  "Other" parameters
      one set is used to control the 'alter index' statements generated (e.g., set max_dop, set sort_in_tempdb)
      one set is used to control the compression estimation logic.  e.g., minimum space savings needed to decide to compress.
*/
-------------------------------------------------------------------------------------------------------------------------
set nocount on
------------------------------------------------------------------------------------------------------------------------------------------------                                          
/* FILTER PARAMETERS*/
------------------------------------------------------------------------------------------------------------------------------------------------                                          

declare @table_name nvarchar(max)              = N'*'  -- takes CSV list (table1, table2)
declare @table_name_contains  nvarchar(100)    = N'*'  -- filters tables.  will implement as '%@contains%'.  pass '*' or null to return unfiltered data
declare @ix_name nvarchar(1024)                = N'*'  -- use '*' for all.  does not support csv of index names - can only take 1.
declare @ix_type nvarchar(1024)                = N'*'  -- use '*' for all.  clustered, heap, nonclustered.  does not support csv of index types  
declare @schema_name nvarchar(512)             = N'*'  -- use '*' for all.  does not support csv of schema names - can only take 1.
declare @min_row_count int                     = 0     -- 0 or null for no lower limit
declare @max_row_count int                     = 0     -- 0 or null for no upper limit
declare @min_page_count int                    = 0
declare @max_page_count int                    = 0
declare @min_megabytes int                     = null
declare @max_megabytes int                     = null
declare @show_indexed_views bit                = 1
declare @compressed_only bit                   = 0    
declare @not_compressed_only bit               = 0
declare @only_show_old_stats bit               = 0 -- set age of stats below - @gen_update_if_stat_over_days_old
      , @gen_update_if_stat_over_days_old int  = 5 -- define old by #days prior to today

------------------------------------------------------------------------------------------------------------------------------------------------                                          
/* PARAMETERS TO RUN MAJOR FUNCTIONALITY*/             
------------------------------------------------------------------------------------------------------------------------------------------------                                          
declare @run_missing bit                       = 1   -- look for recommended indices for the table.  ignores any index-specific value entered above.

declare @run_metadata bit                      = 1  /* you'd get a lot of this from sp_help on a table or sp_helpindex, but that sp doesn't list included columns*/
declare @include_idx_defn bit                  = 1 /* the flattened index definitions are generated via loop over sys.index_columns.  turn off if running slowly over very large sets*/
declare @show_index_or_partition varchar(10)   = 'partition' /* options = 'index', 'partition'. sum rows, pages, etc across partitions to get per index output.
                                                              note - usage stats are at index level.  ops stats are at partition level.
                                                              'index' rolls ops stats up, but 'partition' duplicates index stats.*/

declare @run_usage bit                         = 1  /* pull data from index_usage_stats*/
declare @run_ops_stats bit                     = 1  /* pull data from the operational stats dmv */
declare @show_consolidated_usage_info bit      = 1  /* combines metadata, usage stats, operational stats.  sets their parms to 1*/

/* ROW COMPRESSION CAN BE SLOW FOR LARGE NUMBERS OF INDICES*/
declare @calculate_row_compression bit         = 0  /* generally, calculate row and page compression with 'higher' set to 1, 'same', 'lower' set to 0.*/
declare @calculate_page_compression bit        = 0  /* consider 'no compression' when new columns are added to a row- or page-compressed index*/
declare @calculate_no_compression bit          = 0  /* calc higher will do row and page compression for indices with no compression, only page for row_compressed indices.  */

/* CALCULATING FRAGMENTATION CAN BE IMPACTFUL IN A PRODUCTION SYSTEM*/                                               
declare @run_phys_stats bit                    = 0  /* pull data from physical stats dmv.  the query won't allow you to use this for a lot of indices at once.*/
      , @phys_stats_level nvarchar(10)         = N'detailed' -- options are 'limited' and 'detailed'
 
/* THIS CAN TAKE SOME TIME IN A SYSTEM WITH A LARGE AMOUNT OF MEMORY*/                                              
declare @run_cache_usage bit                   = 1  -- look at how much of the index is being held in memory.  the query won't allow you to use this for a lot of indices at once.
----------------------------------------------------------------------------------------
/* DERIVED REPORTS*/
/* All of the reports below can be found by sorting or pivoting the data in excel.  
these are here just to simplify the analysis for those that don't want to go that route.

Feel free to add any additional reports needed.

Note that some of the reports are listed here, but not yet coded.  They're just here to remind me that i want to develop them.
They will give a message to indicate such if you select them.*/
-------------------------------------------------------------------------------------------
      ---------------------------------------------------------------------------------------------
      --/* metadata reports*/
      ---------------------------------------------------------------------------------------------
      --declare @show_hypothetical_indices  bit         = 0
      --declare @show_disabled_indices      bit         = 0
      --declare @show_heaps                 bit         = 0
      --declare @show_duplicate_indices     bit         = 0
      --declare @show_blob_indices          bit         = 0
      --declare @show_row_overflow_indices  bit         = 0
      ----declare @show_online_rebuild_eligible nvarchar(20) = n'eligible' -- 'eligible', 'not eligible', '*' or null for all.
      ----declare @show_reorganize_eligibile    nvarchar(20) = n'eligible' -- 'eligible', 'not eligible', '*' or null for all.

      --declare @show_compression_summary_by_table bit  = 0  /* get table-level output including compression setting for pk and clustered, #row compressed, #page compressed, #not compressed indices.*/
      --declare @show_filegroup_summary_by_table   bit  = 0  /* get table-level output showing number of indices and size in mb for each filegroup*/
      --declare @show_idx_count_by_data_type       bit  = 0

      -------------------------------------------------------------------------------------------
      /* usage reports*/
      -------------------------------------------------------------------------------------------

      declare @show_unused_indices bit               = 0    -- total reads from usage stats = 0 or total reads from ops stats = 0 or reads/updates < @use_percent
            , @use_percent int                       = 0    -- set definition of 'unused'.  'unused' is anything less than the percent you set here. (basically, how many reads on index as percent of writes)
      declare @show_highly_scanned_indices bit       = 0
            , @min_scan_pct decimal(5, 2)            = 85

      --declare @show_categories_usage_by_size bit     = 0
      --      , @size_metric nvarchar(50)              = N'row count'  -- 'row count', 'used mb'
      --      , @usage_metric nvarchar(50)             = N'usage_stats' -- 'usage_stats', 'ops_stats'

-------------------------------------------------------------------------------------------
/* "Nice to know" reports*/  
-------------------------------------------------------------------------------------------                                           
declare @show_server_start_date bit            = 1 -- show start date to see how much trust you can put in the index usage stats

----------------------------------------------------------------------- ------------------------------------------
/* additional supporting parameters, less frequently used*/
----------------------------------------------------------------------- ------------------------------------------
   ----------------------------------------------------------------------- ------------------------------------------
   /* statement generation options*/
   ----------------------------------------------------------------------- ------------------------------------------
   declare @statistics_sampling_pct int     = 50 -- used to build stats update statement. 
   declare @rebuild_index_max_dop   int     = 8         -- dop settings entered here will override server defaults.  don’t go nuts here.
   declare @rebuild_index_online_yn char(3) = 'off'      -- compress online or not?  takes values 'on' and 'off'
   declare @sort_in_tempdb   char(3)        = 'on'

   -----------------------------------------------------------------------------------------------------------------
   /* compression options*/
   -----------------------------------------------------------------------------------------------------------------
   declare @row_over_page_if_pct_diff_less_than      decimal(12, 4) =  5   -- page compression is more resource intensive than row compression.  if savings diff is minimal, row compress. 
   declare @compress_if_space_saved_mb_greater_than  decimal(12, 4) = 50   -- if space saved in megs is greater than @compress_if_space_saved_mb_greater_than 
   declare @compress_if_space_saved_pct_greater_than decimal(12, 4) = 10   -- or is space saved is a percentage greater than @compress_if_space_saved_pct_greater_than
                                                                           -- then set the column 'eligible_to_compress'  = 1
   declare @compression_min_scan_pct   int                          = 20   -- these two settings affect the output of the column 'page_compress_yn'.  you want
   declare @compression_max_update_pct int                          = 10   -- relatively high scan rate and low update rate.  updates are more important, since they impact cpu, so keep the update pct very low (<10), but
                                                                     --       you can be somewhat more flexible with the scan percentage, dropping as low as 20%.  but, it's better if scan pct is over 50%
   declare @calculate_higher_compression bit = 1          /* and no calcs for page compressed.  consider 'same' and 'lower' when columns are changed, or when going from 2008 up */
   declare @calculate_same_compression   bit = 0          /* to check whether unicode compression changes will make a difference. (only matters if index isn't getting rebuilt)*/
   declare @calculate_lower_compression  bit = 0

----------------------------------------------------------------------------------------------------------------------------
end -- overview section region


begin -- prep work

raiserror('|----------------------------------------------------------------', 10, 1) with nowait
raiserror('|- Begin Prep work', 10, 1) with nowait

-----------------------------------------------------------------------------------------------------------------
/* get other required parms - no user input required*/
declare @object_id int                    = object_id(@table_name);  -- will return null if @table_name = '*' or csv.  essential for operational stats
declare @ix_id int                        = null                                           
declare @server_instance  nvarchar(1024)  = cast(serverproperty('servername') as  nvarchar(1024)) collate database_default
declare @db_name  nvarchar(1024)          = db_name()  collate database_default 
declare @db_id int                        = db_id();
-----------------------------------------------------------------------------------------------------------------
/* get index id, if a single index was selected above*/
if @ix_name <> '*'
begin
select  top 1 @ix_id = i.index_id     
from sys.indexes i
      where  (object_name(i.object_id) = @table_name or @table_name = '*')
      and i.name = @ix_name 
end  

-----------------------------------------------------------------------------------------------------------------
/* reset variables */
if @show_consolidated_usage_info = 0
   and 
    (   @calculate_row_compression   = 1   
     or @calculate_page_compression  = 1  
     or @show_unused_indices         = 1
     or @show_highly_scanned_indices = 1)
   begin
      set @show_consolidated_usage_info = 1
      raiserror('  |- @show_consolidated_usage_info being set to 1 to support one or more of the following selected options: ', 10, 1) with nowait
      raiserror('      -- @calculate_row_compression, @calculate_page_compression, @show_unused_indices, @show_highly_scanned_indices.', 10, 1) with nowait

   end

if @show_consolidated_usage_info = 1
   begin
      set @run_metadata = 1
      set @run_usage = 1
      set @run_ops_stats = 1
   end
-----------------------------------------------------------------------------------------------------------------
/* drop temp objects created, if they exist*/

if object_id('tempdb..#table_names_full') <> 0 drop table #table_names_full
if object_id('tempdb..#stats') <> 0 drop table #stats
if object_id('tempdb..#metadata') <> 0 drop table #metadata
if object_id('tempdb..#usage') <> 0 drop table #usage
if object_id('tempdb..#ops_stats') <> 0 drop table #ops_stats
if object_id('tempdb..#phys_stats') <> 0 drop table #phys_stats
if object_id('tempdb..#cache') <> 0 drop table #cache
if object_id('tempdb..#ix') <> 0 drop table #ix
if object_id('tempdb..#ix_flattened') <> 0 drop table #ix_flattened
if object_id('tempdb..#allocations') <> 0 drop table #allocations
if object_id('tempdb..#buffer') <> 0 drop table #buffer
if object_id('tempdb..#consolidated_usage') <> 0 drop table #consolidated_usage
if object_id('tempdb..#index_update_pct') <> 0 drop table #index_update_pct
if object_id('tempdb..#test_row') <> 0 drop table #test_row
if object_id('tempdb..#test_page') <> 0 drop table #test_page
if object_id('tempdb..#output') <> 0 drop table #output
if object_id('tempdb..#output2') <> 0 drop table #output2
if object_id('tempdb..#output3') <> 0 drop table #output3

-----------------------------------------------------------------------------------------------------------------



-------------------------------------------------------------------------------------
/* get server start time.  physical/operational stats are valid as of server start.*/
if @show_server_start_date = 1
begin
   declare @server_start_date as datetime 
   select @server_start_date = sqlserver_start_time from sys.dm_os_sys_info   

   select @server_start_date as instance_start_datetime, 'usage metrics, missing indices, etc. are good - at most - since server up-time.' as notes
end

-------------------------------------------------------------------------------------
/* process table filters*/
if object_id(N'tempdb..#table_names') is not null drop table #table_names

create table #table_names( table_name sysname collate database_default)
-------------------------------------------------------------------------------------
/* convert csv table list to separate rows and filter result sets*/

   set @table_name_contains = upper(ltrim(rtrim(@table_name_contains)))

   if @table_name_contains is null or @table_name_contains = N''  or @table_name_contains = N'all' or @table_name_contains = N'*'  or @table_name_contains = N'null' 
      begin  
         set @table_name_contains = N'*'  
      end 

   set @table_name = upper(ltrim(rtrim(@table_name)))

   if @table_name is null or @table_name = N''  or @table_name = N'all' or @table_name = N'*'  or @table_name = N'null' 
      begin  
         set @table_name = N'*'  
      end 


   if @table_name <> N'*'  
      begin
         insert into #table_names
            select ltrim(rtrim(item ))
            from (
                  select item = y.i.value('(./text())[1]', 'nvarchar(4000)')
                  from 
                  ( 
                    select x = convert(xml, '<i>' 
                      + replace(@table_name, ',', '</i><i>') 
                      + '</i>').query('.')
                  ) as a cross apply x.nodes('i') as y(i) ) x
            where charindex(@table_name_contains, item )<> 0 or @table_name_contains = '*'
      end
   else
      begin
         insert into #table_names
            select name 
            from sys.tables 
            where type = 'u'
            and charindex(@table_name_contains, name) > 0  or @table_name_contains = '*'

         if coalesce(@show_indexed_views, 1)  = 1
            begin
               insert into #table_names
                  select name 
                  from sys.views 
                  where ( charindex(@table_name_contains, name) > 0  or @table_name_contains = '*')
                  and object_id in (select object_id from sys.indexes)
            end
      end

-----------------------------------------------------------------------------------------------------------------
/* need fully qualified name in certain places (e.g., missing index)*/

select distinct
quotename(@db_name)  collate database_default  + '.' + quotename(s.name)  collate database_default + '.' + quotename(t.table_name) as full_table_name
     , table_name collate database_default as table_name
      , object_id
      , s.schema_id
      , s.name as schema_name
into #table_names_full
from #table_names t 
join (select name collate database_default as name
         , object_id
         , schema_id 
         from sys.tables 
          union all 
         select name, object_id, schema_id 
         from sys.views) st on st.name collate database_default   = t.table_name collate database_default                                                                                              
join sys.schemas s on s.schema_id  = st.schema_id
where s.name = @schema_name or @schema_name = N'*'
------------------------------------------------------------------------------------------------------------------/* keep proc from getting out of control*/
declare @table_count int
select @table_count = count(*) from #table_names_full

--if @table_count > 100 and (@run_cache_usage = 1 or @run_phys_stats = 1 or @calculate_page_compression = 1 or @calculate_row_compression = 1 )
--   begin
--      select 'do not run cache_usage or physical_stats, or estimate compression values for all tables, or for large csv list.  it takes forever.'
--      return
--   end

   raiserror('|- End   prep work', 10, 1) with nowait
   raiserror('|----------------------------------------------------------------', 10, 1) with nowait

end -- prep work region
-----------------------------------------------------------------------------------------------------------------
/* begin outputs */
-----------------------------------------------------------------------------------------------------------------
if @run_metadata = 1 
   begin

      raiserror('|- Begin Metadata', 10, 1) with nowait


      /* get statistics information*/
      raiserror('   |--- Begin Get stats dates ', 10, 1) with nowait

      declare @samplepct nvarchar(10) = cast(@statistics_sampling_pct as nvarchar(10))

      select object_name(object_id) collate database_default as table_name
         , object_id
         , name collate database_default as name
         , stats_date(object_id, stats_id) as statistics_update_date
      into #stats
      from sys.stats  
      where object_id in (select object_id 
                          from #table_names_full)

      raiserror('   |--- end   get stats dates ', 10, 1) with nowait
      raiserror('   |---', 10, 1) with nowait


   ------------------------------------------------------------------------------------------------------------------
      raiserror('   |--- begin loading #ix ', 10, 1) with nowait

      if object_id(N'tempdb..#ix') is not null drop table #ix
      if object_id(N'tempdb..#ix_columns') is not null drop table #ix_columns

      create table #ix
      (
           object_id                   int
         , schema_name                 nvarchar(128) collate database_default
         , table_name                  nvarchar(128) collate database_default
         , index_id                    int
         , index_name                  nvarchar(128) collate database_default
         , filegroup                   nvarchar(128) collate database_default
         , statistics_update_date      datetime
         , is_unique                   bit
         , is_primary_key              bit
         , type_desc                   nvarchar(60) collate database_default
         , is_disabled                 bit
         , is_hypothetical             bit
         , allow_row_locks             bit
         , allow_page_locks            bit
         , fill_factor                 tinyint
         , has_filter                  bit
         , filter_definition           nvarchar(max) collate database_default
         , rank_id                     bigint
         , primary_columns             nvarchar(1000) collate database_default
         , included_columns            nvarchar(1000) collate database_default
         , rebuild_stmt_no_crlf        nvarchar(1000) collate database_default
         , reorg_stmt_no_crlf          nvarchar(1000) collate database_default
         , drop_stmt_no_crlf           nvarchar(1000) collate database_default
         , update_stats_stmt_no_crlf   nvarchar(1000) collate database_default
         , create_stmt_no_crlf         nvarchar(4000) collate database_default
         , data_space_id               int
      )
      create   index ix_clustered on #ix (rank_id) 

      insert into #ix
            select i.object_id
            , t.schema_name
            , t.table_name
            , i.index_id
            , coalesce(i.name, 'no name - heap') as index_name
            , fg.name as filegroup
            , st.statistics_update_date
            , i.is_unique
            , i.is_primary_key
            , i.type_desc
            , i.is_disabled
            , i.is_hypothetical
            , i.allow_row_locks
            , i.allow_page_locks
            , i.fill_factor
            , i.has_filter
            , i.filter_definition
            , dense_rank() over( order by  t.schema_id, t.table_name, i.name) as rank_id
            , cast('' as nvarchar(1000)) as primary_columns
            , cast('' as nvarchar(1000)) as included_columns

            , 'alter '  + case when i.index_id = 0 then 'table ' else 'index ' + quotename(i.name) + ' on ' end  + quotename(t.schema_name) + '.' + quotename(t.table_name) + '  rebuild with ( maxdop = ' + cast(@rebuild_index_max_dop as varchar(10)) + ', sort_in_tempdb = ' + @sort_in_tempdb + ', online = ' +  @rebuild_index_online_yn + ')  ' as rebuild_statement
            , 'alter index ' + quotename(i.name) + ' on ' + quotename(t.schema_name) + '.' + quotename(t.table_name) + '  reorganize ' +'      go' as reorganize_statement
            , 'drop index ' + quotename(t.schema_name) + '.' + quotename(t.table_name) + '.' + quotename(i.name) 
            , case when st.statistics_update_date < getdate() - @gen_update_if_stat_over_days_old then 'update statistics ' + quotename(t.schema_name) collate database_default + '.' + quotename(t.table_name)  collate database_default   + ' ' + quotename(i.name  collate database_default  ) + ' with sample ' + @samplepct + ' percent'  else '' end  as update_stats_statement
            , case when is_primary_key = 0 
                   then 'create ' + case when is_unique = 1 then 'unique ' else '' end + i.type_desc +  ' index ' + quotename(i.name) + ' on ' + quotename(t.schema_name) + '.' + quotename(t.table_name) + ' ([index])' + '(include)' + case when filter_definition is null then '' else ' where ' + filter_definition end + ' with (data_compression = (compress)' + case when fill_factor = 0 then '' else ', fillfactor = ' + cast(fill_factor as nvarchar(3)) end + ', allow_row_locks = ' + case when allow_row_locks = 1 then ' on ' else ' off ' end + ', allow_page_locks = ' + case when allow_page_locks = 1 then ' on ' else ' off ' end +  ', pad_index = ' + case when is_padded = 1 then 'on' else 'off' end + ')' 
                   else 'alter table ' + quotename(t.schema_name) + '.' + quotename(t.table_name) + ' with check add constraint ' + quotename(k.name) + ' primary key ' + i.type_desc + ' ([index])' end
            , i.data_space_id
            from (select distinct * from #table_names_full ) t
            left join sys.indexes         i on i.object_id = t.object_id
            left join (select * from sys.key_constraints where type = 'pk') k on k.parent_object_id = t.object_id
            left join sys.data_spaces    fg on fg.data_space_id = i.data_space_id and fg.type = 'fg' -- filegroup
            left join #stats             st on st.object_id = t.object_id and st.name collate database_default = i.name collate database_default
            where   (i.name = @ix_name or @ix_name = '*')
              and (i.type_desc = @ix_type or @ix_type = '*')
            and ( (@only_show_old_stats = 1 and  st.statistics_update_date < getdate() - @gen_update_if_stat_over_days_old) or @only_show_old_stats = 0)

      raiserror('   |--- end   loading #ix ', 10, 1) with nowait
      raiserror('   |---', 10, 1) with nowait

      ---------------------------------------------------------------------------------------------------------------------
      /* get partition and partition stats info, and generate final #metadata table*/

      raiserror('   |--- begin loading #metadata ', 10, 1) with nowait

      select  @server_instance collate database_default as server_instance 
            , @db_name collate database_default as db_name 
            , ix.table_name
            , ix.index_name
            , ix.is_primary_key
            , ix.type_desc
            , ix.is_unique
            , p.data_compression_desc collate database_default as data_compression_desc
            , ix.has_filter
            , ix.primary_columns
            , ix.included_columns
            , coalesce(ix.filter_definition, '') as filter_definition
            , ps.row_count
            , ps.used_page_count * 8.0 / 1024 as used_mb
            , ps.used_page_count
            , case 
                   when row_count =         0 then 'a - no rows'
                   when row_count <      1000 then 'b - under 1 thousand'
                   when row_count <     10000 then 'c - under 10 thousand'
                   when row_count <    100000 then 'd - under 100 thousand'
                   when row_count <    500000 then 'e - under 500 thousand'
                   when row_count <   1000000 then 'f - under 1 million'
                   when row_count <   5000000 then 'g - under 5 million'
                   when row_count <  10000000 then 'h - under 10 million'
                   when row_count <  20000000 then 'i - under 20 million'
                   when row_count <  50000000 then 'j - under 50 million'
                   when row_count < 100000000 then 'k - under 100 million'
                   when row_count < 150000000 then 'l - under 150 million'
                   when row_count < 200000000 then 'm - under 200 million'
                   when row_count < 250000000 then 'n - under 250 million'
                   when row_count < 500000000 then 'o - under 500 million'
                   when row_count > 500000000 then 'p - more than 500 million'
                   else 'other' end collate database_default as row_count_category 
            , cast(ntile(10) over(order by ps.used_page_count * 8.0 / 1024) * 10 as nvarchar(10)) + 'th %ile' as used_mb_ntile
            , ix.statistics_update_date
            , ix.object_id
            , ix.schema_name
            , ix.index_id
            , p.partition_number
            , case when ix.filegroup = N'primary' then N'.primary' else ix.filegroup end as filegroup
            , p.filestream_filegroup_id
            , ix.fill_factor
            , ix.is_disabled
            , ix.is_hypothetical
            , ix.allow_row_locks
            , ix.allow_page_locks 
            , psc.name collate database_default as partition_scheme
            , pf.name collate database_default as partition_function 
            , cast(null as sql_variant) as function_boundary
            , cast(null as varchar(10)) collate database_default as boundary_direction
            , ps.reserved_page_count
            , ps.in_row_data_page_count
            , ps.in_row_reserved_page_count
            , ps.in_row_used_page_count
            , ps.lob_reserved_page_count
            , ps.lob_used_page_count
            , ps.row_overflow_reserved_page_count
            , ps.row_overflow_used_page_count
            , rebuild_stmt_no_crlf        
            , reorg_stmt_no_crlf          
            , drop_stmt_no_crlf           
            , update_stats_stmt_no_crlf  
            , create_stmt_no_crlf 
            , rank_id
            , ix.data_space_id
            , p.partition_id

      into #metadata
      from  #ix                         ix 
      left join  sys.partitions               p (nolock) on p.object_id = ix.object_id and p.index_id = ix.index_id
      left join sys.partition_schemes  psc (nolock) on psc.data_space_id = ix.data_space_id 
      left join sys.partition_functions pf (nolock) on pf.function_id = psc.function_id
      left join sys.dm_db_partition_stats ps on ps.partition_id = p.partition_id
               where ( (@compressed_only = 0 and @not_compressed_only = 0)
                         or (@compressed_only = 1 and data_compression_desc in ('page', 'row'))
                         or (@not_compressed_only = 1 and data_compression_desc not in ('page', 'row'))
                     ) 
                     and ( coalesce(@min_row_count, 0)  = 0  or row_count  >= @min_row_count)
                     and ( coalesce(@max_row_count, 0)  = 0  or row_count  <= @max_row_count)
                     and ( coalesce(@min_page_count, 0) = 0  or used_page_count >= @min_page_count)
                     and ( coalesce(@max_page_count, 0) = 0  or used_page_count <= @max_page_count)
                     and ( coalesce(@min_megabytes, 0)  = 0  or ps.used_page_count * 8.0 / 1024  >= @min_megabytes)
                     and ( coalesce(@max_megabytes, 0)  = 0  or ps.used_page_count * 8.0 / 1024  <= @max_megabytes)

if object_id(N'tempdb..#partition_scheme_function') is not null drop table #partition_scheme_function

select main.tabname, main.partition_id, main.partition_number, main.rows, main.name as file_group, main.data_space_id,
main.partition_scheme_id, main.partid, main.function_id, part.value, boundary_value_on_right
into #partition_scheme_function
from 
(select object_name(a.object_id) tabname, a.partition_id, a.partition_number, a.rows,
c.name, c.data_space_id, d.partition_scheme_id, d.destination_id as partid, e.function_id
from #metadata                         m
join sys.partitions                    a on a.partition_id = m.partition_id
inner join sys.allocation_units        b on a.hobt_id = b.container_id
inner join sys.data_spaces             c on b.data_space_id = c.data_space_id
inner join sys.destination_data_spaces d on c.data_space_id = d.data_space_id
                                            and d.destination_id = m.partition_number

inner join sys.partition_schemes e on d.partition_scheme_id = e.data_space_id
) main
left join 
(select a.function_id, b.value, boundary_value_on_right
, case when a.boundary_value_on_right = 0 then b.boundary_id else b.boundary_id + 1 end partition_id
from sys.partition_functions a 
inner join sys.partition_range_values b on a.function_id = b.function_id) part 
on main.function_id = part.function_id and main.partition_number = part.partition_id

update #metadata
set filegroup = psf.file_group
, function_boundary = value 
, boundary_direction =  case when boundary_value_on_right = 1 then 'right' else 'left' end
from #metadata m 
join #partition_scheme_function psf on psf.partition_id = m.partition_id

      raiserror('   |--- end   loading #metadata ', 10, 1) with nowait
      raiserror('   |---', 10, 1) with nowait

/* apply partition-level filters back to main list of tables*/
delete from #table_names_full where table_name not in (select distinct table_name from #metadata)



   ----------------------------------------------------------------------------------------------------------------
   /* pull index definitions if requested*/

   if @include_idx_defn = 1
      begin
         raiserror('   |--- begin loading #ix_columns ', 10, 1) with nowait

         create table #ix_columns
         (
          object_id int
         ,index_id int
         ,rank_id int
         ,column_name nvarchar(128) collate database_default
         ,index_column_id int
         ,is_included_column bit
         ,is_descending_key bit
         --,user_data_type sysname
         --,system_data_type sysname
         ,row_num bigint
         )
         --create index ix_clustered on #ix_columns (rank_id, index_column_id)
         create index ix_row_num on #ix_columns (row_num) --, is_included_column, column_name) 
         include(rank_id, column_name, is_included_column, is_descending_key)

         create index ix_rank_id on #ix_columns (rank_id, row_num) include ( column_name, is_included_column, is_descending_key)


         insert into #ix_columns
               select i.object_id
               , i.index_id
               , i.rank_id
               , c.name as column_name
               , ic.index_column_id 
               , ic.is_included_column
               , ic.is_descending_key
               --, case when type_desc = 'heap' then 'r.i.d.' else t.name end as user_data_type
               --, case when type_desc = 'heap' then 'r.i.d.' else t2.name end as system_data_type
               , row_number() over(order by rank_id, case when is_included_column = 1 then c.name else '' end, index_column_id  ) as row_num  -- included column order is irrelevant
               from (select distinct object_id, index_id, rank_id from #ix) i
               left join sys.index_columns ic on ic.object_id = i.object_id and ic.index_id = i.index_id
               left join sys.columns        c on c.column_id = ic.column_id and c.object_id = ic.object_id
          --
         declare @counter int = 1
         declare @max_counter int = coalesce((select count(*) from #ix), 0)
         declare @primary_columns nvarchar(1000) = ''
         declare @included_columns nvarchar(1000) = ''

         while (@counter <= @max_counter)
            begin

                  select @primary_columns = @primary_columns + left(coalesce(column_name + case when is_descending_key = 1 then N' desc' else N'' end + N', '  , N'') , 1000) 
                  from #ix_columns 
                  where rank_id  = @counter
                  and is_included_column = 0
                  order by row_num

                  select @included_columns = @included_columns + left(coalesce(column_name + case when is_descending_key = 1 then N' desc' else N'' end + N', '  , N'') , 1000) 
                  from #ix_columns 
                  where rank_id  = @counter
                  and is_included_column = 1
                  order by row_num

                  update #metadata
                  set primary_columns = @primary_columns
                      , included_columns = @included_columns
                  where rank_id = @counter

                  set @primary_columns = ''
                  set @included_columns = ''
                  set @counter = @counter + 1

            end  -- while (@counter <= @max_counter)

         update #metadata set primary_columns = left(primary_columns, len(primary_columns) - 1 )
                             , create_stmt_no_crlf = replace(create_stmt_no_crlf, '[index]', left(primary_columns, len(primary_columns) - 1 ))  
                          where len(primary_columns) > 0
         update #metadata set included_columns = left(included_columns, len(included_columns) - 1 ) 
                             , create_stmt_no_crlf = replace(create_stmt_no_crlf, '(include)', left(included_columns, len(included_columns) - 1 ) ) 
                           where coalesce(len(included_columns), 0) > 0
         update #metadata set  create_stmt_no_crlf = replace(create_stmt_no_crlf , '(include)', '')                          

         update #metadata  set create_stmt_no_crlf = replace(create_stmt_no_crlf, '(compress)', data_compression_desc)

         raiserror('   |--- end   loading #ix_columns ', 10, 1) with nowait
         raiserror('   |---', 10, 1) with nowait

      end --   if @include_idx_defn = 1

         --/* apply partition-level filters back to main list of tables*/
         --delete from #table_names_full where table_name not in (select distinct table_name from #metadata)


/* roll up partition level data to index level - used in several outputs*/

         if @show_index_or_partition = 'index'
            begin

               if object_id(N'tempdb..#index_rollup') is not null drop table #index_rollup

               select 
                  object_id
                , schema_name
                , index_id
                , max(partition_function) as partition_function
                , sum(row_count) as row_count
                , sum(used_mb) as used_mb
                , sum(used_page_count) as used_page_count
                , case 
                   when sum(row_count) =         0 then 'a - no rows'
                   when sum(row_count) <      1000 then 'b - under 1 thousand'
                   when sum(row_count) <     10000 then 'c - under 10 thousand'
                   when sum(row_count) <    100000 then 'd - under 100 thousand'
                   when sum(row_count) <    500000 then 'e - under 500 thousand'
                   when sum(row_count) <   1000000 then 'f - under 1 million'
                   when sum(row_count) <   5000000 then 'g - under 5 million'
                   when sum(row_count) <  10000000 then 'h - under 10 million'
                   when sum(row_count) <  20000000 then 'i - under 20 million'
                   when sum(row_count) <  50000000 then 'j - under 50 million'
                   when sum(row_count) < 100000000 then 'k - under 100 million'
                   when sum(row_count) < 150000000 then 'l - under 150 million'
                   when sum(row_count) < 200000000 then 'm - under 200 million'
                   when sum(row_count) < 250000000 then 'n - under 250 million'
                   when sum(row_count) < 500000000 then 'o - under 500 million'
                   when sum(row_count) > 500000000 then 'p - more than 500 million'
                   else 'other' end collate database_default as row_count_category 
                , cast(ntile(10) over(order by sum(used_page_count) * 8.0 / 1024) * 10 as nvarchar(10)) + 'th %ile' as used_mb_ntile
                , min(statistics_update_date) as statistics_update_date
                , count(partition_number) as partition_count
                , sum(m.reserved_page_count               ) as reserved_page_count  
                , sum(m.in_row_data_page_count            ) as in_row_data_page_count
                , sum(m.in_row_reserved_page_count        ) as in_row_reserved_page_count
                , sum(m.in_row_used_page_count            ) as in_row_used_page_count
                , sum(m.lob_reserved_page_count           ) as lob_reserved_page_count
                , sum(m.lob_used_page_count               ) as lob_used_page_count
                , sum(m.row_overflow_reserved_page_count  ) as row_overflow_reserved_page_count
                , sum(m.row_overflow_used_page_count      ) as row_overflow_used_page_count
               into #index_rollup
               from #metadata m
               group by     
                 object_id
                , schema_name
                , index_id
         end

      ----------------------------------------------------------------------------------------------------------------
      /* return metadata-level outputs*/

         if @show_consolidated_usage_info = 0 and @show_index_or_partition = 'partition'
            begin
               select 'Metadata' as Output_Type
                , server_instance
                , db_name
                , object_id
                , schema_name
                , table_name
                , index_name
                , index_id
                , partition_number
                , type_desc
                , is_primary_key
                , is_unique
                , data_compression_desc
                , has_filter
                , primary_columns
                , included_columns
                , filter_definition
                , row_count
                , used_mb
                , used_page_count
                , row_count_category
                , used_mb_ntile
                , statistics_update_date
                , partition_scheme
                , partition_function
                , function_boundary
                , boundary_direction
                , filegroup
                , filestream_filegroup_id
                , fill_factor
                , is_disabled
                , is_hypothetical
                , allow_row_locks
                , allow_page_locks
                , reserved_page_count
                , in_row_data_page_count
                , in_row_reserved_page_count
                , in_row_used_page_count
                , lob_reserved_page_count
                , lob_used_page_count
                , row_overflow_reserved_page_count
                , row_overflow_used_page_count
                , rebuild_stmt_no_crlf
                , reorg_stmt_no_crlf
                , drop_stmt_no_crlf
                , update_stats_stmt_no_crlf
                , create_stmt_no_crlf
               from #metadata
               order by table_name, index_id, partition_number
            end



         if @show_consolidated_usage_info = 0 and @show_index_or_partition = 'index'
            begin
               select 'metadata' as Output_Type
                , m.server_instance
                , m.db_name
                , m.object_id
                , m.schema_name
                , m.table_name
                , m.index_name
                , m.index_id
                , ir.partition_count
                , m.type_desc
                , m.is_primary_key
                , m.is_unique
                , m.data_compression_desc
                , m.has_filter
                , m.primary_columns
                , m.included_columns
                , m.filter_definition
                , ir.row_count
                , ir.used_mb
                , ir.used_page_count
                , ir.row_count_category 
                , ir.used_mb_ntile
                , ir.statistics_update_date

                , m.partition_function
                , m.filegroup
                , m.filestream_filegroup_id
                , m.fill_factor
                , m.is_disabled
                , m.is_hypothetical
                , m.allow_row_locks
                , m.allow_page_locks
                , m.boundary_direction
                , ir.reserved_page_count
                , ir.in_row_data_page_count
                , ir.in_row_reserved_page_count
                , ir.in_row_used_page_count
                , ir.lob_reserved_page_count
                , ir.lob_used_page_count
                , ir.row_overflow_reserved_page_count
                , ir.row_overflow_used_page_count
                , m.rebuild_stmt_no_crlf
                , m.reorg_stmt_no_crlf
                , m.drop_stmt_no_crlf
                , m.update_stats_stmt_no_crlf
                , m.create_stmt_no_crlf
               from #metadata m
               left join #index_rollup ir on ir.object_id = m.object_id
                                    and ir.index_id = m.index_id
                                    and ir.schema_name = m.schema_name
               where partition_number = 1 or partition_number is null -- for xml
               order by m.table_name, m.index_id, m.partition_number
            end

      raiserror('|- End   metadata', 10, 1) with nowait
      raiserror('|----------------------------------------------------------------', 10, 1) with nowait

end -- if @run_metadata = 1

--if @show_compression_summary_by_table = 1
--   begin
--     raiserror('|- Begin output- Compression summary by table  ', 10, 1) with nowait

--     if @run_metadata = 0
--         begin
--            raiserror('@run_metadata must be set to 1 to produce metadata outputs.  exiting', 16, 1)
--            return
--         end

--      select  db_name, schema_name, table_name, object_id, sum(row_count) as row_count, sum(used_page_count) as used_page_count, sum(reserved_page_count) as reserved_page_count
--      into #table
--      from #metadata
--      group by  db_name, schema_name, table_name, object_id

--      select 'Compression summary by table ' as Output_Type
--      , t.*
--      , coalesce((select c.data_compression_desc from #metadata c where c.object_id = t.object_id and is_primary_key = 1), 'no pk') as primary_key_compression
--      , (select data_compression_desc from #metadata c where c.object_id = t.object_id and type_desc in ( 'clustered', 'heap')) as cluster_heap_compression
--      , (select count(*) from #metadata c where c.object_id = t.object_id and type_desc = 'nonclustered' and data_compression_desc = 'row' ) as nonclustered_row_compression_count
--      , (select count(*) from #metadata c where c.object_id = t.object_id and type_desc = 'nonclustered' and data_compression_desc = 'page') as nonclustered_page_compression_count
--      , (select count(*) from #metadata c where c.object_id = t.object_id and type_desc = 'nonclustered' and coalesce(data_compression_desc, 'none') = 'none') as nonclustered_no_compression_count
--      from #table t
--      order by cluster_heap_compression desc, db_name, table_name

--      drop table #table

--      raiserror('|- End   output- Compression summary by table ', 10, 1) with nowait
--      raiserror('|----------------------------------------------------------------', 10, 1) with nowait

--   end --if @show_compression_summary_by_table = 1

--if @show_filegroup_summary_by_table = 1
--   begin
--      raiserror('|- Begin output- Filegroup summary by table ', 10, 1) with nowait

--      if @run_metadata = 0
--         begin
--            raiserror('@run_metadata must be set to 1 to produce metadata outputs.  exiting', 16, 1)
--            return
--         end


--   /* build csv list.  could have done with fn, but the sample i copied used xml path*/
--      declare @cols nvarchar(4000)
--      declare @sql nvarchar(4000)
--      set @cols = stuff(
--                        (select ', ' + quotename(filegroup) as [text()]
--                        from (select distinct filegroup from #metadata) as y
--                        order by filegroup
--                        for xml path('')), 1, 1, '')

--      if object_id(N'tempdb..#cols') is not null drop table #cols
--      if object_id(N'tempdb..#fg_prep') is not null drop table #fg_prep
--      if object_id(N'tempdb..#fg') is not null drop table #fg
--      if object_id(N'tempdb..#fg_count') is not null drop table #fg_count
--      if object_id(N'tempdb..##pivoted_output') is not null drop table ##pivoted_output
--      if object_id(N'tempdb..##pivoted_output2') is not null drop table ##pivoted_output2

--      select distinct filegroup into #cols from #metadata

--      /* build pivot*/


--      select m.schema_name
--         , m.table_name
--         , m.filegroup
--         , sum(m.used_mb) as used_mb
--         , count(*)  as index_count
--         into #fg_prep
--         from #metadata m
--         group by m.schema_name, m.table_name
--         , m.filegroup

--      select schema_name   
--         , table_name
--         , c.filegroup
--         , case when m.filegroup = c.filegroup then coalesce(m.used_mb, 0) else 0 end  as used_mb
--         , case when m.filegroup = c.filegroup then coalesce(m.index_count, 0) else 0 end  as index_count
--      into #fg
--      from #fg_prep    m    
--         cross join #cols   c 


--      set @sql = N'select * into ##pivoted_output
--                  from  (select  schema_name, table_name , filegroup , used_mb
--                        from #fg) as d
--                  pivot (sum(used_mb) for filegroup in (' collate database_default + @cols +')) as p '  ;
--         --print @sql
--         exec (@sql)

--      set @sql = N'select * into ##pivoted_output2
--                  from  (select schema_name, table_name , filegroup, index_count
--                        from #fg) as d
--                  pivot (sum(index_count) for filegroup in (' + @cols + ')) as p '  ;
--         --print @sql
--         exec (@sql)

--      select schema_name
--      , table_name
--      , count(distinct filegroup) as fg_count 
--      , count(*) as index_count
--      into #fg_count 
--      from #metadata
--      group by schema_name, table_name

--/* renaming columns to make it clearer what each is - and to avoid printing schema and table name twice*/

--      set @cols = stuff(
--                        (select ', p.' + quotename(name) + ' as ''Used MB in ' + quotename(name) + '''' as [text()]
--                        from (select name 
--                               From 
--                              tempdb.sys.columns Where object_id=OBJECT_ID('tempdb.dbo.##pivoted_output')
--                              and name not in ('schema_name', 'table_name')) as y
--                        order by name
--                        for xml path('')), 1, 1, '')

--      declare @cols2 nvarchar(max)

--      set @cols2 = stuff(
--                        (select ', p2.' + quotename(name) + ' as ''Indices in ' + quotename(name) + '''' as [text()]
--                        from (select name 
--                               From 
--                              tempdb.sys.columns Where object_id=OBJECT_ID('tempdb.dbo.##pivoted_output2')
--                              and name not in ('schema_name', 'table_name')) as y
--                        order by name
--                        for xml path('')), 1, 1, '')

--      set @sql = '

--            select ''Filegroup summary by table'' as Output_Type
--            , p.schema_name, p.table_name, ' + @cols + ', ' + @cols2 + '
--            , fg_count 
--            , index_count as total_index_count
--            from ##pivoted_output p
--            join #fg_count        f on f.table_name = p.table_name and f.schema_name = p.schema_name
--            join ##pivoted_output2 p2 on p2.table_name = f.table_name  and f.schema_name = p2.schema_name
--            order by total_index_count desc, fg_count desc'

--      exec (@sql)

--      drop table #cols
--      drop table ##pivoted_output
--      drop table ##pivoted_output2
--      drop table #fg_prep
--      drop table #fg
--      drop table #fg_count

--      raiserror('|- End   output- filegroup summary by table', 10, 1) with nowait
--      raiserror('|----------------------------------------------------------------', 10, 1) with nowait

--   end --if @show_filegroup_summary_by_table = 1

--if @show_hypothetical_indices= 1
--   begin
--      raiserror('|- Begin output- Hypothetical indices', 10, 1) with nowait

--     if @run_metadata = 0
--         begin
--            raiserror('@run_metadata must be set to 1 to produce metadata outputs.  exiting', 16, 1)
--            return
--         end

--      if exists ( select * 
--                  from #metadata
--                  where is_hypothetical = 1)
--         begin
--            select 'hypothetical indices' as Output_Type
--            , * 
--            from #metadata
--            where is_hypothetical = 1       
--            order by table_name, index_name
--         end
--      else
--         begin
--            select 'hypothetical indices' as Output_Type
--               , 'No hypothetical indices based on filter criteria.' as result
--         end

--      raiserror('|- End   output- hypothetical indices', 10, 1) with nowait
--      raiserror('|----------------------------------------------------------------', 10, 1) with nowait

--   end --if @show_hypothetical_indices= 1

--if @show_disabled_indices    = 1
--   begin
--      raiserror('|- Begin output- Disabled indices', 10, 1) with nowait

--     if @run_metadata = 0
--         begin
--            raiserror('@run_metadata must be set to 1 to produce metadata outputs.  exiting', 16, 1)
--            return
--         end

--      if exists ( select * 
--                  from #metadata
--                  where is_disabled = 1)
--         begin
--            select 'disabled indices' as Output_Type
--            , * 
--            from #metadata
--            where is_disabled = 1       
--            order by table_name, index_name
--         end
--      else
--         begin
--            select 'disabled indices' as Output_Type
--               , 'No disabled indices based on filter criteria.' as result
--         end

--      raiserror('|- End   output- disabled indices', 10, 1) with nowait
--      raiserror('|----------------------------------------------------------------', 10, 1) with nowait

--   end --if @show_disabled_indices    = 1

--if @show_heaps    = 1
--   begin
--      raiserror('|- Begin output- Heaps', 10, 1) with nowait

--     if @run_metadata = 0
--         begin
--            raiserror('@run_metadata must be set to 1 to produce metadata outputs.  exiting', 16, 1)
--            return
--         end

--      if exists ( select * 
--                  from #metadata
--                  where type_desc = 'HEAP')
--         begin
--            select 'Heaps' as Output_Type
--            , * 
--            from #metadata
--            where type_desc = 'HEAP'      
--            order by table_name, index_name
--         end
--      else
--         begin
--            select 'Heaps' as Output_Type
--               , 'No heaps based on filter criteria.' as result
--         end

--      raiserror('|- End   output- Heaps', 10, 1) with nowait
--      raiserror('|----------------------------------------------------------------', 10, 1) with nowait

--   end --if @show_heaps    = 1

--if @show_duplicate_indices   = 1
--   begin
--      raiserror('|- Begin output- Duplicated indices', 10, 1) with nowait

--     if @run_metadata = 0
--         begin
--            raiserror('@run_metadata must be set to 1 to produce metadata outputs.  exiting', 16, 1)
--            return
--         end

--      select 'duplicated indices' as Output_Type
--         , 'This has not been coded yet.' as result

--      raiserror('|- End   output- duplicated indices', 10, 1) with nowait
--      raiserror('|----------------------------------------------------------------', 10, 1) with nowait

--   end -- if @show_duplicate_indices   = 1


--if @show_blob_indices   = 1
--   begin
--      raiserror('|- Begin output- Blob indices', 10, 1) with nowait

--     if @run_metadata = 0
--         begin
--            raiserror('@run_metadata must be set to 1 to produce metadata outputs.  exiting', 16, 1)
--            return
--         end

--      select 'Blob indices' as Output_Type
--         , 'This has not been coded yet.' as result

--      raiserror('|- End   output- blob indices', 10, 1) with nowait
--      raiserror('|----------------------------------------------------------------', 10, 1) with nowait

--   end -- if @show_blob_indices   = 1

--if @show_row_overflow_indices   = 1
--   begin
--      raiserror('|- Begin output- Duplicated indices', 10, 1) with nowait

--     if @run_metadata = 0
--         begin
--            raiserror('@run_metadata must be set to 1 to produce metadata outputs.  exiting', 16, 1)
--            return
--         end

--      select 'Row overflow indices' as Output_Type
--         , 'This has not been coded yet.' as result

--      raiserror('|- End   output- row overflow indices', 10, 1) with nowait
--      raiserror('|----------------------------------------------------------------', 10, 1) with nowait

--   end -- if @show_row_overflow_indices   = 1

--if @show_idx_count_by_data_type  = 1
--   begin
--      raiserror('|- Begin output- Index count by data type', 10, 1) with nowait

--     if @run_metadata = 0 or @include_idx_defn = 0
--         begin
--            raiserror('@run_metadata and @include_idx_defn must both be set to 1 to produce index count by data type output.  exiting', 16, 1)
--            return
--         end

--      select 'Index count by data type' as Output_Type
--         , 'This has not been coded yet.' as result

--      --if exists ( select * 
--      --            from #ix_columns
--      --            )
--      --   begin
--            --select 'index count by data type' as Output_Type
--            --, system_data_type   
--            --, user_data_type     
--            --, count(*) as index_count 
--            --from #ix_columns
--            --group by system_data_type   
--            --       , user_data_type
--            --order by system_data_type   
--            --       , user_data_type
--         --end
--      --else
--      --   begin
--      --      select 'disabled indices' as Output_Type
--      --         , 'no disabled indices based on filter criteria' as result
--      --   end

--      raiserror('|- End   output- index count by data type', 10, 1) with nowait
--      raiserror('|----------------------------------------------------------------', 10, 1) with nowait

--   end --if @show_disabled_indices    = 1


-----------------------------------------------------------------------------------------
--/* find missing indices*/
if @run_missing = 1
   begin
      raiserror('|- Begin Missing indices', 10, 1) with nowait

      if object_id(N'tempdb..#missing') is not null drop table #missing

      select  
        'Missing indices' as Output_Type 
      , getdate() as capture_datetime
      , @server_instance as server_instance
      , [total_cost]  = round(s.avg_total_user_cost * s.avg_user_impact * (s.user_seeks + s.user_scans),0) 
      , s.avg_user_impact
      , schema_name
      , table_name =  d.statement collate database_default
      , unique_compiles
      , last_user_seek
      , last_user_scan
      --, last_system_seek
      --, last_system_scan
      , d.equality_columns 
      , d.inequality_columns
      , d.included_columns
      into #missing
      from        sys.dm_db_missing_index_groups      g 
      inner join  sys.dm_db_missing_index_group_stats s on s.group_handle = g.index_group_handle 
      inner join  sys.dm_db_missing_index_details     d on d.index_handle = g.index_handle
      inner join  #table_names_full                   t on t.full_table_name  collate database_default = d.statement  collate database_default
      where  d.database_id = @db_id

      if @@rowcount = 0
         begin
            select 'Missing indices' as Output_Type
               , 'No missing indices based on filter criteria.' as result
         end 
      else
         begin
            select *
            from #missing
            order by total_cost desc;
         end
  


      raiserror('|- End   missing indices', 10, 1) with nowait
      raiserror('|----------------------------------------------------------------', 10, 1) with nowait


   end --if @run_missing = 1
-----------------------------------------------------------------------------------------------------------------

/* find undex usage*/ 
if @run_usage = 1
   begin

      raiserror('|- Begin Index usage stats', 10, 1) with nowait

      select    @server_instance  as server_instance
              , @db_name as db_name
              , s.object_id
              , t.schema_name 
              , t.table_name  collate database_default as table_name
              , indexname = i.name collate database_default
              , i.index_id
              , round(case when user_updates = 0 
                           and (isnull(user_lookups, 0) + isnull(user_seeks, 0) + isnull(user_scans, 0) )> 0 then 100.0
                           when user_updates = 0 then 0.0 else
                           cast(isnull(user_seeks, 0) 
                              + isnull(user_scans, 0) 
                              + isnull(user_lookups, 0) as float)/cast(user_updates as float) * 100 end, 2) as use_percentage

            , case when coalesce(isnull(user_seeks, 0) + isnull(user_scans, 0) + isnull(user_lookups, 0), 0)   = 0 then 'a - unused'
                   when coalesce(isnull(user_seeks, 0)  + isnull(user_scans, 0)  + isnull(user_lookups, 0), 0)   < 100 then 'b - mimimal_use'
                   else 'c - used' end  collate database_default as total_usage_category
         , cast(ntile(10) over(order by isnull(user_seeks, 0) + isnull(user_scans, 0)  + isnull(user_lookups, 0)) * 10 as nvarchar(10)) collate database_default + 'th %ile' collate database_default as total_usage_ntile

              , user_updates    
              , system_updates 
              , user_seeks 
              , user_scans  
              , user_lookups
              , isnull(user_seeks, 0) 
                + isnull(user_scans, 0) 
                + isnull(user_lookups, 0) as total_usage
              , is_unique 
      into #usage
      from   #table_names_full           t
        join sys.dm_db_index_usage_stats s on  t.object_id = s.object_id 
        join sys.indexes                 i on  s.[object_id] = i.[object_id] 
                                               and s.index_id = i.index_id 
      where  s.database_id = db_id()
          and objectproperty(s.[object_id], 'ismsshipped') = 0
      and i.name is not null    -- ignore heap indexes.
      and (i.name collate database_default = @ix_name collate database_default or @ix_name = '*' collate database_default)
      order by table_name, index_id
      --order by user_updates desc

      if @show_consolidated_usage_info = 0 
      begin
         select 'Usage Stats' as Output_Type
         , * 
         from #usage

         if @@rowcount = 0
         select 'Usage Stats - no rows returned' as Output_Type
      end

      raiserror('|- End   Index usage stats', 10, 1) with nowait
      raiserror('|----------------------------------------------------------------', 10, 1) with nowait

   end -- if @run_usage = 1

-----------------------------------------------------------------------------------------------------------------

/* find index operational stats.

this has to be done in a loop for csv list of tables.  */ 
-----------------------------------------------------------------------------------------------------------------

if @run_ops_stats  = 1  -- handy = passing '*' for table name results in null value in object_id, so no query adjustments needed 
     begin
         raiserror('|- Begin Index operational stats', 10, 1) with nowait

         select 'Operational stats' as Output_Type
         , @server_instance as server_instance
         , @db_name as db_name
         , m.object_id
         , m.schema_name
         , m.table_name 
         , m.index_name
         , m.index_id
         , m.partition_number
         , m.type_desc as index_type
         , partition_function
         , leaf_allocation_count
         , case when coalesce(i.range_scan_count, 0) + coalesce(i.singleton_lookup_count, 0)= 0 then 0
           else 
             cast(  coalesce(i.range_scan_count, 0) * 100.0 / 
              (coalesce(i.range_scan_count, 0) + coalesce(i.singleton_lookup_count, 0))as decimal(6, 3))
            end as percent_scan
         , leaf_insert_count + leaf_delete_count +  leaf_update_count as total_dml
         , range_scan_count +    singleton_lookup_count as total_reads  
         , case when (leaf_insert_count + leaf_delete_count +  leaf_update_count+ range_scan_count +    singleton_lookup_count ) = 0 then 0
                 else round(cast( leaf_insert_count + leaf_delete_count +  leaf_update_count as float)/
                   (leaf_insert_count + leaf_delete_count +  leaf_update_count+ range_scan_count +    singleton_lookup_count ), 3) * 100 end as percent_dml
         , case when coalesce( i.range_scan_count + i.leaf_insert_count + i.leaf_delete_count + i.leaf_update_count + i.leaf_page_merge_count + i.singleton_lookup_count, 0)= 0 then 0
            else 
           cast( i.leaf_update_count * 100.0 / 
              (1+ i.range_scan_count + i.leaf_insert_count + i.leaf_delete_count + i.leaf_update_count + i.leaf_page_merge_count + i.singleton_lookup_count) as decimal(6, 3))
            end as  percent_update
         , coalesce(leaf_delete_count      , 0) as leaf_delete_count       
         , coalesce(leaf_ghost_count       , 0) as leaf_ghost_count
         , coalesce(leaf_insert_count      , 0) as leaf_insert_count
         , coalesce(leaf_page_merge_count  , 0) as leaf_page_merge_count
         , coalesce(leaf_update_count      , 0) as leaf_update_count
         , coalesce(range_scan_count       , 0) as range_scan_count
         , coalesce(singleton_lookup_count , 0) as singleton_lookup_count
         , i.range_scan_count + i.leaf_insert_count + i.leaf_delete_count + i.leaf_update_count + i.leaf_page_merge_count + i.singleton_lookup_count as total_activity

         , coalesce(nonleaf_allocation_count          , 0) as nonleaf_allocation_count
         , coalesce(nonleaf_delete_count              , 0) as nonleaf_delete_count
         , coalesce(nonleaf_insert_count              , 0) as nonleaf_insert_count
         , coalesce(nonleaf_page_merge_count          , 0) as nonleaf_page_merge_count
         , coalesce(nonleaf_update_count              , 0) as nonleaf_update_count
         , coalesce(page_compression_attempt_count    , 0) as page_compression_attempt_count
         , coalesce(page_compression_success_count    , 0) as page_compression_success_count
         , coalesce(column_value_pull_in_row_count    , 0) as column_value_pull_in_row_count
         , coalesce(column_value_push_off_row_count   , 0) as column_value_push_off_row_count
         , coalesce(forwarded_fetch_count             , 0) as forwarded_fetch_count
         , coalesce(index_lock_promotion_attempt_count, 0) as index_lock_promotion_attempt_count
         , coalesce(index_lock_promotion_count        , 0) as index_lock_promotion_count
         , coalesce(row_lock_count                    , 0) as row_lock_count
         , coalesce(row_lock_wait_count               , 0) as row_lock_wait_count
         , coalesce(row_lock_wait_in_ms               , 0) as row_lock_wait_in_ms
         , coalesce(page_lock_count                   , 0) as page_lock_count
         , coalesce(page_lock_wait_count              , 0) as page_lock_wait_count
         , coalesce(page_lock_wait_in_ms              , 0) as page_lock_wait_in_ms
         , coalesce(page_io_latch_wait_count          , 0) as page_io_latch_wait_count
         , coalesce(page_io_latch_wait_in_ms          , 0) as page_io_latch_wait_in_ms
         , coalesce(page_latch_wait_count             , 0) as page_latch_wait_count
         , coalesce(page_latch_wait_in_ms             , 0) as page_latch_wait_in_ms
         , coalesce(tree_page_io_latch_wait_count     , 0) as tree_page_io_latch_wait_count
         , coalesce(tree_page_io_latch_wait_in_ms     , 0) as tree_page_io_latch_wait_in_ms
         , coalesce(tree_page_latch_wait_count        , 0) as tree_page_latch_wait_count
         , coalesce(tree_page_latch_wait_in_ms        , 0) as tree_page_latch_wait_in_ms
         , coalesce(lob_fetch_in_bytes                , 0) as lob_fetch_in_bytes
         , coalesce(lob_fetch_in_pages                , 0) as lob_fetch_in_pages
         , coalesce(lob_orphan_create_count           , 0) as lob_orphan_create_count
         , coalesce(lob_orphan_insert_count           , 0) as lob_orphan_insert_count
         , coalesce(row_overflow_fetch_in_bytes       , 0) as row_overflow_fetch_in_bytes
         , coalesce(row_overflow_fetch_in_pages       , 0) as row_overflow_fetch_in_pages
         into #ops_stats
         from sys.dm_db_index_operational_stats (db_id(), @object_id, @ix_id, null) i -- null @object_id when table_list = '*' or csv.  the join to #table_names_full removes unwanted tables
         --join #table_names_full      t on t.object_id = i.object_id
         --join sys.indexes            x on x.object_id = i.object_id and x.index_id = i.index_id
         join #metadata              m on m.object_id = i.object_id and m.index_id = i.index_id and m.partition_number = i.partition_number

          if @show_consolidated_usage_info = 0 and @show_index_or_partition = 'partition'
            begin
                 select *
                 from #ops_stats
            end

         if @show_index_or_partition = 'index'
            begin
               if object_id(N'tempdb..#ops_stats_rollup') is not null drop table #ops_stats_rollup

         select 'Operational stats' as Output_Type
         , server_instance
         , db_name
         , object_id
         , schema_name
         , table_name 
         , index_name
         , index_id
         , index_type
         , sum(partition_number) as partition_count
         , max(partition_function) as partition_function
         , sum(leaf_allocation_count) as leaf_allocation_count
          ,sum(total_dml            ) as total_dml
          ,sum(total_reads          ) as total_reads
          , case when (sum(leaf_insert_count + leaf_delete_count +  leaf_update_count + range_scan_count + singleton_lookup_count )) = 0 then 0
                 else round(cast( sum(leaf_insert_count + leaf_delete_count + leaf_update_count) as float)/
                   sum(leaf_insert_count+ leaf_delete_count + leaf_update_count + range_scan_count + singleton_lookup_count ), 3) * 100 end as percent_dml
         , sum(leaf_delete_count                  ) as leaf_delete_count 
         , sum(leaf_ghost_count                   ) as leaf_ghost_count
         , sum(leaf_insert_count                  ) as leaf_insert_count
         , sum(leaf_page_merge_count              ) as leaf_page_merge_count
         , sum(leaf_update_count                  ) as leaf_update_count
         , sum(range_scan_count                   ) as range_scan_count
         , sum(singleton_lookup_count             ) as singleton_lookup_count
         , sum(range_scan_count + leaf_insert_count + leaf_delete_count + leaf_update_count + leaf_page_merge_count + singleton_lookup_count) as total_activity
         , case when sum(coalesce(range_scan_count, 0) + coalesce(singleton_lookup_count, 0))= 0 then 0
           else 
             cast(  sum(coalesce(range_scan_count, 0)) * 100.0 / 
              sum(coalesce(range_scan_count, 0) + coalesce(singleton_lookup_count, 0))as decimal(6, 3))
            end as percent_scan
         , case when sum(coalesce( range_scan_count + leaf_insert_count + leaf_delete_count + leaf_update_count + leaf_page_merge_count + singleton_lookup_count, 0)) = 0 then 0
            else 
           cast( sum(leaf_update_count) * 100.0 / 
             sum( (1+ range_scan_count + leaf_insert_count + leaf_delete_count + leaf_update_count + leaf_page_merge_count + singleton_lookup_count)) as decimal(6, 3))
            end as  percent_update
         , sum(nonleaf_allocation_count            ) as nonleaf_allocation_count
         , sum(nonleaf_delete_count                ) as nonleaf_delete_count
         , sum(nonleaf_insert_count                ) as nonleaf_insert_count
         , sum(nonleaf_page_merge_count            ) as nonleaf_page_merge_count
         , sum(nonleaf_update_count                ) as nonleaf_update_count
         , sum(page_compression_attempt_count      ) as page_compression_attempt_count
         , sum(page_compression_success_count      ) as page_compression_success_count
         , sum(column_value_pull_in_row_count      ) as column_value_pull_in_row_count
         , sum(column_value_push_off_row_count     ) as column_value_push_off_row_count
         , sum(forwarded_fetch_count               ) as forwarded_fetch_count
         , sum(index_lock_promotion_attempt_count  ) as index_lock_promotion_attempt_count
         , sum(index_lock_promotion_count          ) as index_lock_promotion_count
         , sum(row_lock_count                      ) as row_lock_count
         , sum(row_lock_wait_count                 ) as row_lock_wait_count
         , sum(row_lock_wait_in_ms                 ) as row_lock_wait_in_ms
         , sum(page_lock_count                     ) as page_lock_count
         , sum(page_lock_wait_count                ) as page_lock_wait_count
         , sum(page_lock_wait_in_ms                ) as page_lock_wait_in_ms
         , sum(page_io_latch_wait_count            ) as page_io_latch_wait_count
         , sum(page_io_latch_wait_in_ms            ) as page_io_latch_wait_in_ms
         , sum(page_latch_wait_count               ) as page_latch_wait_count
         , sum(page_latch_wait_in_ms               ) as page_latch_wait_in_ms
         , sum(tree_page_io_latch_wait_count       ) as tree_page_io_latch_wait_count
         , sum(tree_page_io_latch_wait_in_ms       ) as tree_page_io_latch_wait_in_ms
         , sum(tree_page_latch_wait_count          ) as tree_page_latch_wait_count
         , sum(tree_page_latch_wait_in_ms          ) as tree_page_latch_wait_in_ms
         , sum(lob_fetch_in_bytes                  ) as lob_fetch_in_bytes
         , sum(lob_fetch_in_pages                  ) as lob_fetch_in_pages
         , sum(lob_orphan_create_count             ) as lob_orphan_create_count
         , sum(lob_orphan_insert_count             ) as lob_orphan_insert_count
         , sum(row_overflow_fetch_in_bytes         ) as row_overflow_fetch_in_bytes
         , sum(row_overflow_fetch_in_pages         ) as row_overflow_fetch_in_pages
         into #ops_stats_rollup
         from #ops_stats
         group by server_instance
                  , db_name
                  , object_id
                  , schema_name
                  , table_name 
                  , index_id
                  , index_name
                  , index_type
               

            end   
         if @show_consolidated_usage_info = 0 and @show_index_or_partition = 'index'
            begin
               select
                     o.Output_Type
                   , o.server_instance
                   , o.db_name
                   , o.object_id
                   , o.schema_name
                   , o.table_name
                   , o.index_name
                   , o.index_id
                   , o.partition_count
                   , o.index_type
                   , o.partition_function
                   , o.leaf_allocation_count
                   , o.total_dml
                   , o.total_reads
                   , o.percent_dml
                   , o.leaf_delete_count
                   , o.leaf_ghost_count
                   , o.leaf_insert_count
                   , o.leaf_page_merge_count
                   , o.leaf_update_count
                   , o.range_scan_count
                   , o.singleton_lookup_count
                   , o.total_activity
                   , o.percent_scan
                   , o.percent_update
                   , o.nonleaf_allocation_count
                   , o.nonleaf_delete_count
                   , o.nonleaf_insert_count
                   , o.nonleaf_page_merge_count
                   , o.nonleaf_update_count
                   , o.page_compression_attempt_count
                   , o.page_compression_success_count
                   , o.column_value_pull_in_row_count
                   , o.column_value_push_off_row_count
                   , o.forwarded_fetch_count
                   , o.index_lock_promotion_attempt_count
                   , o.index_lock_promotion_count
                   , o.row_lock_count
                   , o.row_lock_wait_count
                   , o.row_lock_wait_in_ms
                   , o.page_lock_count
                   , o.page_lock_wait_count
                   , o.page_lock_wait_in_ms
                   , o.page_io_latch_wait_count
                   , o.page_io_latch_wait_in_ms
                   , o.page_latch_wait_count
                   , o.page_latch_wait_in_ms
                   , o.tree_page_io_latch_wait_count
                   , o.tree_page_io_latch_wait_in_ms
                   , o.tree_page_latch_wait_count
                   , o.tree_page_latch_wait_in_ms
                   , o.lob_fetch_in_bytes
                   , o.lob_fetch_in_pages
                   , o.lob_orphan_create_count
                   , o.lob_orphan_insert_count
                   , o.row_overflow_fetch_in_bytes
                   , o.row_overflow_fetch_in_pages
               from #ops_stats_rollup o
            end

        raiserror('|- End   index operational stats', 10, 1) with nowait
        raiserror('|----------------------------------------------------------------', 10, 1) with nowait

   end

---------------------------------------------------------------------------------------------------
/* get output useful for analyzing compression options*/

if @show_consolidated_usage_info = 1 
   begin
      if object_id(N'tempdb..#consolidated_usage') is not null drop table #consolidated_usage
 
      create table #consolidated_usage
      (
            Output_type                        varchar(31)
          , db_name                            nvarchar(1024)
          , object_id                          int
          , schema_name                        nvarchar(128)
          , table_name                         nvarchar(128)
          , index_name                         nvarchar(128)
          , index_id                           int
          , partition_number                   int
          , type_desc                          nvarchar(60)
          , is_primary_key                     bit
          , data_compression_desc              nvarchar(60)
          , has_filter                         bit
          , primary_columns                    nvarchar(1000)
          , included_columns                   nvarchar(1000)
          , filter_definition                  nvarchar(max)
          , total_usage_category               varchar(15)
          , total_usage_ntile                  nvarchar(17)
          , row_count_category                 varchar(25)
          , used_mb_ntile                      nvarchar(17)
          , row_count                          bigint
          , used_mb                            numeric(27,6)
          , used_page_count                    bigint
          , rows_per_page                      numeric(12,2)
          , rows_per_mb                        numeric(12,2)
          , leaf_ghost_count                   bigint
          , statistics_update_date             datetime
          , is_unique                          bit
          , server_instance                    nvarchar(1024)
          , usage_seeks                        bigint
          , usage_scans                        bigint
          , usage_lookups                      bigint
          , usage_total_reads                  bigint
          , usage_percent_scans                decimal(8,4)
          , usage_updates                      bigint
          , usage_percent_updates              decimal(8,4)
          , ops_range_scans                    bigint
          , ops_singleton_lookups              bigint
          , ops_total_reads                    bigint
          , ops_percent_scans                  decimal(6,3)
          , ops_total_dml                      bigint
          , ops_percent_dml                    float(53)
          , ops_percent_update                 decimal(6,3)
          , filegroup                          nvarchar(128)
          , filestream_filegroup_id            smallint
          , partition_scheme                   nvarchar(128)
          , partition_function                 nvarchar(128)
          , function_boundary                  sql_variant
          , boundary_direction                 varchar(10)
          , fill_factor                        tinyint
          , is_disabled                        bit
          , is_hypothetical                    bit
          , allow_row_locks                    bit
          , allow_page_locks                   bit
          , reserved_page_count                bigint
          , in_row_data_page_count             bigint
          , in_row_reserved_page_count         bigint
          , in_row_used_page_count             bigint
          , lob_reserved_page_count            bigint
          , lob_used_page_count                bigint
          , row_overflow_reserved_page_count   bigint
          , row_overflow_used_page_count       bigint
          , leaf_allocation_count              bigint
          , leaf_delete_count                  bigint
          , leaf_insert_count                  bigint
          , leaf_page_merge_count              bigint
          , leaf_update_count                  bigint
          , range_scan_count                   bigint
          , singleton_lookup_count             bigint
          , nonleaf_allocation_count           bigint
          , nonleaf_delete_count               bigint
          , nonleaf_insert_count               bigint
          , nonleaf_page_merge_count           bigint
          , nonleaf_update_count               bigint
          , page_compression_attempt_count     bigint
          , page_compression_success_count     bigint
          , column_value_pull_in_row_count     bigint
          , column_value_push_off_row_count    bigint
          , forwarded_fetch_count              bigint
          , index_lock_promotion_attempt_count bigint
          , index_lock_promotion_count         bigint
          , row_lock_count                     bigint
          , row_lock_wait_count                bigint
          , row_lock_wait_in_ms                bigint
          , page_lock_count                    bigint
          , page_lock_wait_count               bigint
          , page_lock_wait_in_ms               bigint
          , page_io_latch_wait_count           bigint
          , page_io_latch_wait_in_ms           bigint
          , page_latch_wait_count              bigint
          , page_latch_wait_in_ms              bigint
          , tree_page_io_latch_wait_count      bigint
          , tree_page_io_latch_wait_in_ms      bigint
          , tree_page_latch_wait_count         bigint
          , tree_page_latch_wait_in_ms         bigint
          , lob_fetch_in_bytes                 bigint
          , lob_fetch_in_pages                 bigint
          , lob_orphan_create_count            bigint
          , lob_orphan_insert_count            bigint
          , row_overflow_fetch_in_bytes        bigint
          , row_overflow_fetch_in_pages        bigint
          , rebuild_stmt_no_crlf               nvarchar(1000)
          , reorg_stmt_no_crlf                 nvarchar(1000)
          , drop_stmt_no_crlf                  nvarchar(1000)
          , update_stats_stmt_no_crlf          nvarchar(1000)
          , create_stmt_no_crlf                nvarchar(4000)

      )


      if @show_consolidated_usage_info = 1 and @show_index_or_partition = 'partition'
         begin
            raiserror('|- Begin Consolidated metadata and usage output', 10, 1) with nowait
            insert into #consolidated_usage
            select  'Consolidated metadata and usage' as Output_Type
                  , m.db_name
                  , m.object_id
                  , m.schema_name
                  , m.table_name
                  , m.index_name
                  , m.index_id
                  , m.partition_number
                  , m.type_desc
                  , m.is_primary_key
                  , m.data_compression_desc
                  , m.has_filter
                  , m.primary_columns
                  , m.included_columns
                  , m.filter_definition
                  , coalesce(total_usage_category , '.not in cache') as total_usage_category
                  , coalesce(total_usage_ntile , '.not in cache') as total_usage_ntile
                  , row_count_category
                  , used_mb_ntile
                  , m.row_count
                  , used_mb
                  , m.used_page_count
                  , case when m.used_page_count = 0 then 0.0 else cast(m.row_count* 1.0/m.used_page_count as decimal(12, 2)) end  as rows_per_page
                  , case when m.used_mb = 0         then 0.0 else cast(m.row_count* 1.0/m.used_mb as decimal(12, 2)) end  as rows_per_mb
                  , coalesce(o.leaf_ghost_count, 0)                   as leaf_ghost_count
                  , m.statistics_update_date
                  , m.is_unique
                  , m.server_instance
  
                  , coalesce(u.user_seeks, 0)                         as usage_seeks
                  , coalesce(u.user_scans, 0)                         as usage_scans
                  , coalesce(u.user_lookups, 0)                       as usage_lookups
                  , coalesce(u.total_usage, 0)                        as usage_total_reads
                  , case when coalesce(u.total_usage, 0)  = 0 then 0 else cast(coalesce(u.user_scans, 0) * 100.0/coalesce(u.total_usage, 0) as decimal(8, 4)) end as usage_percent_scans
                  , coalesce(u.user_updates, 0)                       as usage_updates
                  , case when coalesce(u.total_usage, 0)  = 0 then 0 else cast(coalesce(u.user_updates, 0) * 100.0/coalesce(u.total_usage + u.user_updates, 0) as decimal(8, 4)) end as usage_percent_updates

                  , coalesce(range_scan_count, 0)                     as ops_range_scans
                  , coalesce(singleton_lookup_count, 0)               as ops_singleton_lookups
                  , coalesce(o.total_reads, 0)                        as ops_total_reads
                  , coalesce(o.percent_scan, 0)                       as ops_percent_scans
                  , coalesce(o.total_dml, 0)                          as ops_total_dml
                  , coalesce(o.percent_dml, 0)                        as ops_percent_dml
                  , coalesce(o.percent_update, 0)                     as ops_percent_update
                  , m.filegroup
                  , m.filestream_filegroup_id
                  , m.partition_scheme
                  , m.partition_function
                  , m.function_boundary
                  , m.boundary_direction
                  , m.fill_factor
                  , m.is_disabled
                  , m.is_hypothetical
                  , m.allow_row_locks
                  , m.allow_page_locks 
                  , m.reserved_page_count
                  , m.in_row_data_page_count
                  , m.in_row_reserved_page_count
                  , m.in_row_used_page_count
                  , m.lob_reserved_page_count
                  , m.lob_used_page_count
                  , m.row_overflow_reserved_page_count
                  , m.row_overflow_used_page_count
                  , coalesce(o.leaf_allocation_count, 0)              as leaf_allocation_count
                  , coalesce(o.leaf_delete_count, 0)                  as leaf_delete_count
                  , coalesce(o.leaf_insert_count, 0)                  as leaf_insert_count
                  , coalesce(o.leaf_page_merge_count, 0)              as leaf_page_merge_count
                  , coalesce(o.leaf_update_count, 0)                  as leaf_update_count
                  , coalesce(o.range_scan_count, 0)                   as range_scan_count 
                  , coalesce(o.singleton_lookup_count, 0)             as singleton_lookup_count
                  , coalesce(o.nonleaf_allocation_count, 0)           as nonleaf_allocation_count
                  , coalesce(o.nonleaf_delete_count, 0)               as nonleaf_delete_count
                  , coalesce(o.nonleaf_insert_count, 0)               as nonleaf_insert_count
                  , coalesce(o.nonleaf_page_merge_count, 0)           as nonleaf_page_merge_count
                  , coalesce(o.nonleaf_update_count, 0)               as nonleaf_update_count
                  , coalesce(o.page_compression_attempt_count, 0)     as page_compression_attempt_count
                  , coalesce(o.page_compression_success_count, 0)     as page_compression_success_count
                  , coalesce(o.column_value_pull_in_row_count, 0)     as column_value_pull_in_row_count
                  , coalesce(o.column_value_push_off_row_count, 0)    as column_value_push_off_row_count
                  , coalesce(o.forwarded_fetch_count, 0)              as forwarded_fetch_count
                  , coalesce(o.index_lock_promotion_attempt_count, 0) as index_lock_promotion_attempt_count
                  , coalesce(o.index_lock_promotion_count, 0)         as index_lock_promotion_count
                  , coalesce(o.row_lock_count, 0)                     as row_lock_count
                  , coalesce(o.row_lock_wait_count, 0)                as row_lock_wait_count
                  , coalesce(o.row_lock_wait_in_ms, 0)                as row_lock_wait_in_ms
                  , coalesce(o.page_lock_count, 0)                    as page_lock_count
                  , coalesce(o.page_lock_wait_count, 0)               as page_lock_wait_count
                  , coalesce(o.page_lock_wait_in_ms, 0)               as page_lock_wait_in_ms
                  , coalesce(o.page_io_latch_wait_count, 0)           as page_io_latch_wait_count
                  , coalesce(o.page_io_latch_wait_in_ms, 0)           as page_io_latch_wait_in_ms
                  , coalesce(o.page_latch_wait_count, 0)              as page_latch_wait_count
                  , coalesce(o.page_latch_wait_in_ms, 0)              as page_latch_wait_in_ms
                  , coalesce(o.tree_page_io_latch_wait_count, 0)      as tree_page_io_latch_wait_count
                  , coalesce(o.tree_page_io_latch_wait_in_ms, 0)      as tree_page_io_latch_wait_in_ms
                  , coalesce(o.tree_page_latch_wait_count, 0)         as tree_page_latch_wait_count
                  , coalesce(o.tree_page_latch_wait_in_ms, 0)         as tree_page_latch_wait_in_ms
                  , coalesce(o.lob_fetch_in_bytes, 0)                 as lob_fetch_in_bytes
                  , coalesce(o.lob_fetch_in_pages, 0)                 as lob_fetch_in_pages
                  , coalesce(o.lob_orphan_create_count, 0)            as lob_orphan_create_count
                  , coalesce(o.lob_orphan_insert_count, 0)            as lob_orphan_insert_count
                  , coalesce(o.row_overflow_fetch_in_bytes, 0)        as row_overflow_fetch_in_bytes
                  , coalesce(o.row_overflow_fetch_in_pages, 0)        as row_overflow_fetch_in_pages
                  , m.rebuild_stmt_no_crlf
                  , m.reorg_stmt_no_crlf
                  , m.drop_stmt_no_crlf
                  , m.update_stats_stmt_no_crlf
                  , m.create_stmt_no_crlf
               from #metadata        m 
               left join #usage      u on u.object_id = m.object_id and u.index_id = m.index_id 
               left join #ops_stats  o on o.object_id = m.object_id and o.index_id = m.index_id  and o.partition_number = m.partition_number
               where @compressed_only = 0 or (@compressed_only = 1 and data_compression_desc in ('page', 'row'))
               order by schema_name, table_name, case when m.index_id in (0, 1) then 0 else 1 end, is_primary_key desc, index_name

            end  --if @show_consolidated_usage_info = 1 and @show_index_or_partition = 'partition'

      if @show_consolidated_usage_info = 1 and @show_index_or_partition = 'index'
         begin
            raiserror('|- Begin Consolidated metadata and usage output', 10, 1) with nowait
            insert into #consolidated_usage
               select  'Consolidated metadata and usage' as Output_Type
                  , m.db_name
                  , m.object_id
                  , m.schema_name
                  , m.table_name
                  , m.index_name
                  , m.index_id
                  , m.partition_number
                  , m.type_desc
                  , m.is_primary_key
                  , m.data_compression_desc
                  , m.has_filter
                  , m.primary_columns
                  , m.included_columns
                  , m.filter_definition
                  , coalesce(total_usage_category , '.not in cache') as total_usage_category
                  , coalesce(total_usage_ntile , '.not in cache') as total_usage_ntile
                  , ir.row_count_category
                  , ir.used_mb_ntile
                  , ir.row_count
                  , ir.used_mb
                  , ir.used_page_count
                  , case when ir.used_page_count = 0 then 0.0 else cast(ir.row_count* 1.0/ir.used_page_count as decimal(12, 2)) end  as rows_per_page
                  , case when ir.used_mb = 0         then 0.0 else cast(ir.row_count* 1.0/ir.used_mb as decimal(12, 2)) end  as rows_per_mb
                  , coalesce(o.leaf_ghost_count, 0)                   as leaf_ghost_count
                  , m.statistics_update_date
                  , m.is_unique
                  , m.server_instance
  
                  , coalesce(u.user_seeks, 0)                         as usage_seeks
                  , coalesce(u.user_scans, 0)                         as usage_scans
                  , coalesce(u.user_lookups, 0)                       as usage_lookups
                  , coalesce(u.total_usage, 0)                        as usage_total_reads
                  , case when coalesce(u.total_usage, 0)  = 0 then 0 else cast(coalesce(u.user_scans, 0) * 100.0/coalesce(u.total_usage, 0) as decimal(8, 4)) end as usage_percent_scans
                  , coalesce(u.user_updates, 0)                       as usage_updates
                  , case when coalesce(u.total_usage, 0)  = 0 then 0 else cast(coalesce(u.user_updates, 0) * 100.0/coalesce(u.total_usage + u.user_updates, 0) as decimal(8, 4)) end as usage_percent_updates

                  , coalesce(range_scan_count, 0)                     as ops_range_scans
                  , coalesce(singleton_lookup_count, 0)               as ops_singleton_lookups
                  , coalesce(o.total_reads, 0)                        as ops_total_reads
                  , coalesce(o.percent_scan, 0)                       as ops_percent_scans
                  , coalesce(o.total_dml, 0)                          as ops_total_dml
                  , coalesce(o.percent_dml, 0)                        as ops_percent_dml
                  , coalesce(o.percent_update, 0)                     as ops_percent_update
                  , m.filegroup
                  , m.filestream_filegroup_id
                  , m.partition_scheme
                  , m.partition_function
                  , m.function_boundary
                  , m.boundary_direction
                  , m.fill_factor
                  , m.is_disabled
                  , m.is_hypothetical
                  , m.allow_row_locks
                  , m.allow_page_locks 
                  , m.reserved_page_count
                  , m.in_row_data_page_count
                  , m.in_row_reserved_page_count
                  , m.in_row_used_page_count
                  , m.lob_reserved_page_count
                  , m.lob_used_page_count
                  , m.row_overflow_reserved_page_count
                  , m.row_overflow_used_page_count
                  , coalesce(o.leaf_allocation_count, 0)              as leaf_allocation_count
                  , coalesce(o.leaf_delete_count, 0)                  as leaf_delete_count
                  , coalesce(o.leaf_insert_count, 0)                  as leaf_insert_count
                  , coalesce(o.leaf_page_merge_count, 0)              as leaf_page_merge_count
                  , coalesce(o.leaf_update_count, 0)                  as leaf_update_count
                  , coalesce(o.range_scan_count, 0)                   as range_scan_count 
                  , coalesce(o.singleton_lookup_count, 0)             as singleton_lookup_count
                  , coalesce(o.nonleaf_allocation_count, 0)           as nonleaf_allocation_count
                  , coalesce(o.nonleaf_delete_count, 0)               as nonleaf_delete_count
                  , coalesce(o.nonleaf_insert_count, 0)               as nonleaf_insert_count
                  , coalesce(o.nonleaf_page_merge_count, 0)           as nonleaf_page_merge_count
                  , coalesce(o.nonleaf_update_count, 0)               as nonleaf_update_count
                  , coalesce(o.page_compression_attempt_count, 0)     as page_compression_attempt_count
                  , coalesce(o.page_compression_success_count, 0)     as page_compression_success_count
                  , coalesce(o.column_value_pull_in_row_count, 0)     as column_value_pull_in_row_count
                  , coalesce(o.column_value_push_off_row_count, 0)    as column_value_push_off_row_count
                  , coalesce(o.forwarded_fetch_count, 0)              as forwarded_fetch_count
                  , coalesce(o.index_lock_promotion_attempt_count, 0) as index_lock_promotion_attempt_count
                  , coalesce(o.index_lock_promotion_count, 0)         as index_lock_promotion_count
                  , coalesce(o.row_lock_count, 0)                     as row_lock_count
                  , coalesce(o.row_lock_wait_count, 0)                as row_lock_wait_count
                  , coalesce(o.row_lock_wait_in_ms, 0)                as row_lock_wait_in_ms
                  , coalesce(o.page_lock_count, 0)                    as page_lock_count
                  , coalesce(o.page_lock_wait_count, 0)               as page_lock_wait_count
                  , coalesce(o.page_lock_wait_in_ms, 0)               as page_lock_wait_in_ms
                  , coalesce(o.page_io_latch_wait_count, 0)           as page_io_latch_wait_count
                  , coalesce(o.page_io_latch_wait_in_ms, 0)           as page_io_latch_wait_in_ms
                  , coalesce(o.page_latch_wait_count, 0)              as page_latch_wait_count
                  , coalesce(o.page_latch_wait_in_ms, 0)              as page_latch_wait_in_ms
                  , coalesce(o.tree_page_io_latch_wait_count, 0)      as tree_page_io_latch_wait_count
                  , coalesce(o.tree_page_io_latch_wait_in_ms, 0)      as tree_page_io_latch_wait_in_ms
                  , coalesce(o.tree_page_latch_wait_count, 0)         as tree_page_latch_wait_count
                  , coalesce(o.tree_page_latch_wait_in_ms, 0)         as tree_page_latch_wait_in_ms
                  , coalesce(o.lob_fetch_in_bytes, 0)                 as lob_fetch_in_bytes
                  , coalesce(o.lob_fetch_in_pages, 0)                 as lob_fetch_in_pages
                  , coalesce(o.lob_orphan_create_count, 0)            as lob_orphan_create_count
                  , coalesce(o.lob_orphan_insert_count, 0)            as lob_orphan_insert_count
                  , coalesce(o.row_overflow_fetch_in_bytes, 0)        as row_overflow_fetch_in_bytes
                  , coalesce(o.row_overflow_fetch_in_pages, 0)        as row_overflow_fetch_in_pages
                  , m.rebuild_stmt_no_crlf
                  , m.reorg_stmt_no_crlf
                  , m.drop_stmt_no_crlf
                  , m.update_stats_stmt_no_crlf
                  , m.create_stmt_no_crlf
               from #metadata               m 
               left join #index_rollup     ir on ir.object_id = m.object_id and ir.index_id = m.index_id
               left join #usage             u on u.object_id = m.object_id and u.index_id = m.index_id 
               left join #ops_stats_rollup  o on o.object_id = m.object_id and o.index_id = m.index_id 
               where ( @compressed_only = 0 or (@compressed_only = 1 and data_compression_desc in ('page', 'row')))
               and (m.partition_number = 1 or m.partition_number is null)
               order by schema_name, table_name, case when m.index_id in (0, 1) then 0 else 1 end, is_primary_key desc, index_name

         end  -- if @show_consolidated_usage_info = 1 and @show_index_or_partition = 'index'

      /* produce consoidated usage ouptput*/
      select
         cu.* 
      from #consolidated_usage cu
      order by   table_name, type_desc desc, is_primary_key desc, index_name, partition_number -- is_primary_key desc, type_desc, index_name, partition_number--, type_desc, is_primary_key

      raiserror('|- End   Consolidated metadata and usage output', 10, 1) with nowait
      raiserror('|----------------------------------------------------------------', 10, 1) with nowait

   end  -- if @show_consolidated_usage_info = 1 

if @show_unused_indices = 1
   begin
      raiserror('|- Begin Unused indices output', 10, 1) with nowait

     if @show_consolidated_usage_info = 0
         begin
            raiserror('@show_consolidated_usage_info must be set to 1 to produce unused indices report.  exiting', 16, 1)
            return
         end

      if exists ( select * 
                  from #consolidated_usage cu
                  where cu.usage_total_reads = 0
                     or cu.ops_total_reads = 0
                     or cu.usage_percent_updates < @use_percent)
         begin
            select 'Unused indices' as Output_Type
               , cu.usage_total_reads
               , cu.ops_total_reads
               , cu.usage_percent_updates
               , m.*
            from #consolidated_usage cu
            join #metadata           m on m.object_id = cu.object_id
                                      and m.index_id = cu.index_id
            where cu.usage_total_reads = 0
               or cu.ops_total_reads = 0
               or cu.usage_percent_updates < @use_percent
         end
      else
         begin
            select 'Unused indices' as Output_Type
               , 'No unused indices based on filter criteria.' as result
         end

      raiserror('|- End   Unused indices output', 10, 1) with nowait
      raiserror('|----------------------------------------------------------------', 10, 1) with nowait

   end -- @show_unused_indices = 1

if @show_highly_scanned_indices  = 1
   begin
      raiserror('|- Begin Highly scanned indices output', 10, 1) with nowait

     if @show_consolidated_usage_info = 0
         begin
            raiserror('@show_consolidated_usage_info must be set to 1 to produce highly scanned indices report.  exiting', 16, 1)
            return
         end

      if exists ( select * 
                  from #consolidated_usage cu
                  where cu.usage_percent_scans > @min_scan_pct
                     or cu.ops_percent_scans > @min_scan_pct)      
         begin
            select 'Highly scanned indices' as Output_Type
               , cu.usage_percent_scans
               , cu.ops_percent_scans
               , cu.usage_scans
               , cu.usage_total_reads
               , cu.ops_range_scans
               , cu.ops_total_reads
               , m.*
            from #consolidated_usage cu
            join #metadata            m on m.object_id = cu.object_id
                                       and m.index_id = cu.index_id
            where cu.usage_percent_scans > @min_scan_pct
                  or 
                  cu.ops_percent_scans > @min_scan_pct
         end
      else
         begin
            select 'Highly scanned indices' as Output_Type
               , 'No highly scanned indices based on filter criteria.' as result
         end

      raiserror('|- End   Highly scanned indices output', 10, 1) with nowait
      raiserror('|----------------------------------------------------------------', 10, 1) with nowait

   end -- @show_unused_indices = 1


-----------------------------------------------------------------------------------------------------------------------------------------

/* find index physical stats.  takes a while to run (e.g., 20 seconds for trip instance).  
the output includes fragmentation levels.  this is set to run at 'limited' level, which returns data just for leaf level.  this is only to get
the 1 row per index, which allows for better joining with the other data sets produced.  it's also the level used in our optimization process.
it would not be a big deal to use 'detailed' instead.

to get this to work for csv, i need to do a loop.  since i'm lazy and don't want to write out the table definition, i cheat.
*/ 

if @run_phys_stats = 1
   begin;

      raiserror('|- Begin physical stats (fragmentation)', 10, 1) with nowait


      if object_id(N'tempdb..#tables_filtered') is not null drop table #tables_filtered
      select table_name, object_id, schema_id, row_number() over(order by table_name) as row_num
      into #tables_filtered
      from  #table_names_full 

      declare @counter5 int = 1
      declare @max_counter5 int
      select @max_counter5 = max(row_num) from #tables_filtered
      declare @object_id2 int

      create table #phys_stats
      (
            database_id                    smallint
          , object_id                      int
          , index_id                       int
          , partition_number               int
          , index_type_desc                nvarchar(60)
          , alloc_unit_type_desc           nvarchar(60)
          , index_depth                    tinyint
          , index_level                    tinyint
          , avg_fragmentation_in_percent   float(53)
          , fragment_count                 bigint
          , avg_fragment_size_in_pages     float(53)
          , page_count                     bigint
          , avg_page_space_used_in_percent float(53)
          , record_count                   bigint
          , ghost_record_count             bigint
          , version_ghost_record_count     bigint
          , min_record_size_in_bytes       int
          , max_record_size_in_bytes       int
          , avg_record_size_in_bytes       float(53)
          , forwarded_record_count         bigint
          , compressed_page_count          bigint
      )

      raiserror('   |- Begin generate physical stats via sys.dm_db_index_physical_stats', 10, 1) with nowait

      while @counter5 <= @max_counter5
        begin
           select @object_id2 = object_id from #tables_filtered where row_num = @counter5

           insert into #phys_stats
              select * 
              from sys.dm_db_index_physical_stats(@db_id, @object_id2, @ix_id, null, @phys_stats_level);

           raiserror('      |- completed %d of %d', 0, 1, @counter5, @max_counter5)
  
           set @counter5 = @counter5 + 1  
        end --while @counter5 <= @max_counter5
  
      raiserror('   |- End generate physical stats via sys.dm_db_index_physical_stats', 10, 1) with nowait
      raiserror('   |---', 10, 1) with nowait
      raiserror('   |- Begin joining all data into #phys_stats_output', 10, 1) with nowait

      if object_id(N'tempdb..#phys_stats_output') is not null drop table #phys_stats_output 

      select 'Fragmentation/ Physical stats' as Output_Type
      , @db_name as db_name
      , i.object_id
      , i.schema_name
      , i.table_name
      , i.index_name
      , i.index_id
      , i.partition_number
      , index_type_desc as index_type
      , i.statistics_update_date
      , i.row_count_category
      , i.used_mb_ntile
      , coalesce(u.total_usage_category, '.No usage data') as total_usage_category
      , coalesce(u.total_usage_ntile, '.No usage data') as total_usage_ntile
      , case when avg_fragmentation_in_percent <=  1 then 'a - lte  1%'
             when avg_fragmentation_in_percent <=  3 then 'b - lte  3%'
             when avg_fragmentation_in_percent <=  5 then 'c - lte  5%'
             when avg_fragmentation_in_percent <= 10 then 'd - lte 10%'
             when avg_fragmentation_in_percent <= 20 then 'e - lte 20%'
             when avg_fragmentation_in_percent <= 30 then 'e - lte 30%'
             when avg_fragmentation_in_percent <= 50 then 'e - lte 50%'
             when avg_fragmentation_in_percent <= 70 then 'e - lte 70%'
             when avg_fragmentation_in_percent >= 30 then 'f - gt  70%'
         end as frag_pct_category
      , cast(ntile(10) over(order by avg_fragmentation_in_percent)  as nvarchar(10)) + '0th %ile' as frag_pct_ntile
      , alloc_unit_type_desc
      , cast(avg_fragmentation_in_percent as decimal(20, 2)) as avg_frag_pct
      , cast(avg_fragment_size_in_pages   as decimal(38, 2)) as avg_frag_page_size
      --, case when rowcnt = 0 then 0 else cast(round(i2.rowmodctr*1.0/row_count*100, 2) as decimal(12, 2)) end as row_mod_pct
      , cast(avg_page_space_used_in_percent   as decimal(5, 2)) as avg_pct_page_used
      , case when page_count = 0 then 0 else cast(compressed_page_count * 1.0/page_count as decimal(6, 3)) end as compressed_page_pct
      , page_count as leaf_page_count
      , i.row_count
      , i2.rowmodctr as row_mod_count
      , i.used_mb
      , index_level   
      , fragment_count
      , ghost_record_count   
      , version_ghost_record_count   --, min_record_size_in_bytes   --, max_record_size_in_bytes
      , avg_record_size_in_bytes  
      , forwarded_record_count   --, compressed_page_count
      , rebuild_stmt_no_crlf        
      , reorg_stmt_no_crlf          
      , update_stats_stmt_no_crlf   
      from #phys_stats      ps
      join #metadata         i on i.object_id = ps.object_id and i.index_id = ps.index_id and i.partition_number = ps.partition_number
      join sys.sysindexes   i2 on i2.id = i.object_id and i2.indid = i.index_id
      left join #usage       u on u.index_id = i.index_id and u.object_id = i.object_id
      where index_level = 0
      and alloc_unit_type_desc = 'in_row_data'
      order by table_name, index_name, partition_number
      raiserror('   |- End   joining all data into #phys_stats_output', 10, 1) with nowait
      raiserror('   |---', 10, 1) with nowait

     raiserror('|- End physical stats (fragmentation)', 10, 1) with nowait
     raiserror('|----------------------------------------------------------------', 10, 1) with nowait

end;  -- if @run_phys_stats = 1

--------------------------------------------------------------------------------------------------------------

/* look at the ways that the indices for selected table are stored in cache.  
these information are useful for planning data compression.
these data should eventually make it into perfprod, though i don't think that i'd want to collect data too often, or
for all tables.  the sys.os_buffer_descripters aggregation takes 15 seconds just for trip_instance.  it might be worth
collecting the allocation informaiton, and maybe buffer information just at the table level, rather than at the index level, for 
tables of lesser importance.*/

if  @run_cache_usage      = 1
begin
     raiserror('|-begin buffer cache', 10, 1) with nowait

         raiserror('   |--- begin loading allocations ', 10, 1) with nowait


      select o.object_id, t.schema_name,  o.name as table_name,p.index_id, i.name as index_name 
         , au.type_desc as allocation_type, au.allocation_unit_id
         , au.data_pages, used_pages, total_pages, partition_number
      into #allocations
      from sys.allocation_units     as au
         inner join sys.partitions  as p on au.container_id = p.hobt_id 
                                             and (au.type = 1 or au.type = 3)
          join sys.objects          as o on p.object_id = o.object_id
          join #table_names_full       t on t.object_id = o.object_id
          join sys.indexes          as i on p.index_id = i.index_id and i.object_id = p.object_id
      union all
      select o.object_id, t.schema_name, o.name as table_name,p.index_id, i.name as index_name 
         , au.type_desc as allocation_type, au.allocation_unit_id
         , au.data_pages, used_pages, total_pages, partition_number
      from sys.allocation_units    as au
         inner join sys.partitions as  p on au.container_id = p.partition_id 
                                           and au.type = 2
          join sys.objects          as o on p.object_id = o.object_id
          join #table_names_full       t on t.object_id = o.object_id
          join sys.indexes          as i on p.index_id = i.index_id and i.object_id = p.object_id

         raiserror('   |--- end   loading allocations ', 10, 1) with nowait
         raiserror('   |---', 10, 1) with nowait
         raiserror('   |--- begin loading buffer descriptors ', 10, 1) with nowait

      select
           object_id
         , index_id
         ,  count(*)as cached_pages_count 
         , sum(row_count) as row_count_sum
         , avg(row_count) as row_count_avg
         , sum(cast(free_space_in_bytes as bigint))/1024 as free_space_in_kb_sum
         , avg(cast(free_space_in_bytes as float)) as free_space_in_bytes_avg
         , sum(cast(is_modified as int)) as is_modified_sum
         , avg(cast(is_modified as float))*100 as is_modified_percent
      into #buffer
      from sys.dm_os_buffer_descriptors as  bd 
          inner join #allocations          obj  on bd.allocation_unit_id = obj.allocation_unit_id
      where database_id = db_id()
      group by object_id, index_id

      raiserror('   |--- end   loading buffer descriptors', 10, 1) with nowait
      raiserror('   |---', 10, 1) with nowait

      raiserror('   |--- begin cache usage output ', 10, 1) with nowait

      select 
         'cache usage' as output
         , serverproperty('servername') as server_instance
         , db_name() as db_name
         , a.object_id
         , schema_name
         , table_name
         , a.index_id
         , index_name
         , partition_number
         , allocation_type
         , data_pages
         , used_pages
         , total_pages
         , cached_pages_count
         , case when total_pages = 0 then 0 else cast(cast(cached_pages_count as float)/total_pages*100  as decimal(34, 2)) end as pages_cached_percent
         , row_count_sum
         , row_count_avg
         , free_space_in_kb_sum
         , free_space_in_bytes_avg
         , is_modified_sum
         , is_modified_percent
      into #cache
      from #allocations a
      join #buffer      b on b.object_id = a.object_id and b.index_id = a.index_id
      order by cached_pages_count desc;

      select 'cache usage' as Output_Type, * from #cache

      raiserror('   |--- end   cache usage output', 10, 1) with nowait
      raiserror('   |---', 10, 1) with nowait


      drop table #allocations
      drop table #buffer

     raiserror('end   buffer cache', 10, 1) with nowait
     raiserror('|----------------------------------------------------------------', 10, 1) with nowait

end
---------------------------------------------------------------------------------------------------------------------
/* compression estimation*/
---------------------------------------------------------------------------------------------------------------------

if @calculate_row_compression = 1 or @calculate_page_compression = 1 or @calculate_no_compression = 1
   begin
     raiserror('|- Begin Compression', 10, 1) with nowait

      if object_id(N'tempdb..#test_none') is not null drop table #test_none
      create table #test_none (
                             table_name                         sysname collate database_default
                           , schema_name                        sysname collate database_default
                           , index_id                           int
                           , partition_number                   int   
                           , data_compression_desc              nvarchar(60)  collate database_default
                           , row_num                            int
                           )

      create table #test_row (
                             table_name                         sysname collate database_default
                           , schema_name                        sysname collate database_default
                           , index_id                           int
                           , partition_number                   int   
                           , data_compression_desc              nvarchar(60)  collate database_default
                           , row_num                            int
                           )

      create table #test_page (
                             table_name                        sysname collate database_default
                           , schema_name                       sysname collate database_default
                           , index_id                          int
                           , partition_number                  int   
                           , data_compression_desc             nvarchar(60)  collate database_default
                           , row_num                           int
                           )

      create table #output (
                             table_name                         sysname collate database_default
                           , schema_name                        sysname collate database_default
                           , index_id                           int
                           , partition_number                   int
                           , size_with_current_kb               bigint
                           , size_with_requested_compression_kb bigint
                           , sample_size_with_current_kb        bigint
                           , sample_size_with_requested         bigint
                           )

      create table #output2 (
                             table_name                        sysname collate database_default
                           , schema_name                       sysname collate database_default
                           , index_id                           int
                           , partition_number                   int
                           , size_with_current_kb               bigint
                           , size_with_requested_compression_kb bigint
                           , sample_size_with_current_kb        bigint
                           , sample_size_with_requested         bigint
                           , compression_tested                 nvarchar(60) collate database_default
                           )

      declare @compression_setting char(4)  

            if @calculate_no_compression  = 1 
                  begin 
                     raiserror('   |--- begin @calculate_no_compression ', 10, 1) with nowait
               
                     insert into #test_row
                           select    table_name        
                                    , schema_name       
                                    , index_id          
                                    , partition_number  
                                    , data_compression_desc          
                                    , row_number() over(order by getdate() )
                           from #metadata
                           where is_hypothetical = 0
                              and ((data_compression_desc = 'none' collate database_default and @calculate_same_compression = 1)
                                 or (data_compression_desc = 'row' collate database_default  and @calculate_lower_compression = 1)
                                 or (data_compression_desc = 'page' collate database_default and @calculate_lower_compression = 1))

                     declare @counter3 int = 1
                     declare @max_counter3 int = (select count(*) from #test_row)
                     declare @table_name3 sysname
                     declare @schema_name3 sysname
                     declare @ix_id3 int
                     declare @partition_number int
                     declare @current_compression_setting char(4)

                     while @counter3 <= @max_counter3
                        begin
                           select @table_name3 = table_name
                                 , @ix_id3 = index_id 
                                 , @partition_number = partition_number
                                 , @schema_name3 = schema_name
                                 , @current_compression_setting = data_compression_desc
                           from #test_row where row_num = @counter3


                           set @compression_setting = 'none'

                           insert into #output
                              exec sp_estimate_data_compression_savings 
                                   @schema_name3
                                 , @table_name3
                                 , @ix_id3
                                 , @partition_number
                                 , @compression_setting


                           insert into #output2
                              select *, 'none' collate database_default
                              from #output
                              where schema_name = @schema_name3
                                 and table_name = @table_name3
                                 and index_id = @ix_id3
                                 and partition_number = @partition_number

                           delete from #output
                              where schema_name = @schema_name3
                                 and table_name = @table_name3
                                 and index_id = @ix_id3
                                 and partition_number = @partition_number
                     
                           raiserror('      |--- calculated no   compression for  %d of %d', 0, 1, @counter3, @max_counter3) with nowait

                           set @counter3 = @counter3 + 1
                     
                        end -- while @counter3 <= @max_counter3

                        raiserror('   |--- end @calculate_no_compression ', 10, 1) with nowait
                        raiserror('   |---', 10, 1) with nowait

                     end -- if @show_index_or_partition = 'partition'

            if @calculate_row_compression  = 1 
                  begin 
                     raiserror('   |--- begin @calculate_row_compression ', 10, 1) with nowait

                     insert into #test_row
                           select    table_name        
                                   , schema_name       
                                   , index_id          
                                   , partition_number  
                                   , data_compression_desc          
                                   , row_number() over(order by getdate() )
                           from #metadata
                           where is_hypothetical = 0
                              and ((data_compression_desc = 'none' collate database_default and @calculate_higher_compression = 1)
                                or (data_compression_desc = 'row'  collate database_default and @calculate_same_compression = 1)
                                or (data_compression_desc = 'page' collate database_default and @calculate_lower_compression = 1))

                     declare @counter2 int = 1
                     declare @max_counter2 int = (select count(*) from #test_row)
                     declare @table_name2 sysname
                     declare @schema_name2 sysname
                     declare @ix_id2 int


                     while @counter2 <= @max_counter2
                        begin

                           select @table_name2 = table_name
                                 , @ix_id2 = index_id 
                                 , @partition_number = partition_number
                                 , @schema_name2 = schema_name
                                 , @current_compression_setting = data_compression_desc
                           from #test_row where row_num = @counter2


                           set @compression_setting = 'row'

                              insert into #output
                                 exec sp_estimate_data_compression_savings 
                                      @schema_name2
                                    , @table_name2
                                    , @ix_id2
                                    , @partition_number
                                    , @compression_setting


                              insert into #output2
                                 select *, 'row' collate database_default
                                 from #output
                                 where schema_name = @schema_name2
                                   and table_name = @table_name2
                                   and index_id = @ix_id2
                                   and partition_number = @partition_number

                              delete from #output
                                 where schema_name = @schema_name2
                                   and table_name = @table_name2
                                   and index_id = @ix_id2
                                   and partition_number = @partition_number
                        
                              raiserror('      |--- calculated row  compression for  %d of %d', 0, 1, @counter2, @max_counter2) with nowait

                              set @counter2 = @counter2 + 1
                     
                           end --while @counter2 <= @max_counter2

                     raiserror('   |--- end @calculate_row_compression ', 10, 1) with nowait
                     raiserror('   |---', 10, 1) with nowait

                  end -- if row_compression  = 1

            if @calculate_page_compression = 1 
                   begin 
                     raiserror('   |--- begin @calculate_page_compression ', 10, 1) with nowait

                      insert into #test_page
                            select    table_name        
                                    , schema_name       
                                    , index_id          
                                    , partition_number  
                                    , data_compression_desc          
                                    , row_number() over(order by getdate() )
                            from #metadata
                            where is_hypothetical = 0
                              and ((data_compression_desc = 'none' collate database_default and @calculate_higher_compression = 1)
                                or (data_compression_desc = 'row'  collate database_default and @calculate_higher_compression = 1)
                                or (data_compression_desc = 'page' collate database_default and @calculate_same_compression = 1))

                      set @compression_setting = 'page'

                      set @counter2  = 1
                      set @max_counter2  = (select count(*) from #test_page)

                      while @counter2 <= @max_counter2
                      begin
                            select @table_name2 = table_name
                                  , @ix_id2 = index_id 
                                  , @partition_number = partition_number
                                  , @schema_name2 = schema_name
                                  , @current_compression_setting = data_compression_desc
                            from #test_page where row_num = @counter2

                            insert into #output
                               exec sp_estimate_data_compression_savings 
                                     @schema_name2
                                  , @table_name2
                                  , @ix_id2
                                  , @partition_number
                                  , @compression_setting

                               insert into #output2
                                  select *, 'page' collate database_default
                                  from #output
                                  where schema_name = @schema_name2
                                     and table_name = @table_name2
                                     and index_id = @ix_id2
                                     and partition_number = @partition_number
             
                            raiserror('      |--- calculated page compression for  %d of %d', 0, 1, @counter2, @max_counter2) with nowait

                            set @counter2 = @counter2 + 1
                         end --while @counter2 <= @max_counter2

                     raiserror('   |--- end @calculate_page_compression ', 10, 1) with nowait
                     raiserror('   |---', 10, 1) with nowait

               end  --if @calculate_page_compression = 1 

      raiserror('   |--- begin loading #output3 ', 10, 1) with nowait

      select 'compression estimation' as output
         , m.schema_name
         , o.table_name
         , o.index_id
         , m.index_name
         , o.partition_number
         , m.data_compression_desc as current_compression
         , compression_tested
         , case when size_with_current_kb = 0 then 0 
            else cast(((size_with_current_kb- size_with_requested_compression_kb) *1.0/ size_with_current_kb * 100.0)  as decimal(12, 1)) end as compression_pct
         , cast((size_with_current_kb - size_with_requested_compression_kb ) * 1.0/1024 as decimal(12, 2)) as space_savings_mb        
         , case when m.type_desc = 'heap' collate database_default
                then 'alter table ' + quotename(rtrim(o.schema_name)) + '.' + quotename(o.table_name) + ' rebuild with (data_compression = ' + compression_tested + ', maxdop = ' + cast(@rebuild_index_max_dop as varchar(10)) + ', sort_in_tempdb = ' + @sort_in_tempdb + ', online = ' +  @rebuild_index_online_yn + ') ' 
                else 'alter index ' + quotename(m.index_name) + ' on ' + quotename(rtrim(o.schema_name)) + '.' + quotename(o.table_name) + ' rebuild with (data_compression = '+ compression_tested + ', maxdop = ' + cast(@rebuild_index_max_dop as varchar(10)) + ', sort_in_tempdb = ' + @sort_in_tempdb + ', online = ' +  @rebuild_index_online_yn + ') ' 
                end as ddl_statement
         , size_with_current_kb 
         , size_with_requested_compression_kb 
         , case when size_with_current_kb = 0 then 0 
            else cast((sample_size_with_current_kb *1.0/ size_with_current_kb * 100.0)  as decimal(12, 1)) end as sampling_pct
         , sample_size_with_current_kb 
         , sample_size_with_requested 
      into #output3
      from #output2 o
      join #metadata m on m.table_name collate database_default = o.table_name collate database_default
                           and m.index_id = o.index_id
                           and m.partition_number = o.partition_number
                           and m.schema_name collate database_default = o.schema_name collate database_default
      --join sys.objects ob on ob.name collate database_default = o.table_name collate database_default
      --join sys.indexes i on i.object_id = ob.object_id and i.index_id = o.index_id
      where size_with_current_kb is not null

      raiserror('   |--- end   loading #output3 ', 10, 1) with nowait
      raiserror('   |---', 10, 1) with nowait

      /* compression estimation detail output*/

      --if @show_index_or_partition = 'partition'
      --   begin

            raiserror('   |--- begin compression estimation output - partition level ', 10, 1) with nowait

            if object_id(N'tempdb..#compression_results') is not null drop table #compression_results

            select 'compression estimation' as Output_Type
               , cu.db_name
               , cu.table_name
               , cu.index_name
               , cu.partition_number
               , cu.is_primary_key
               , cu.type_desc
               , cu.data_compression_desc as current_compression
               , cu.row_count
               , cu.used_page_count
               --, nocompress.space_savings_mb as no_compression_space_savings_mb
               , row.compression_pct as row_compression_pct
               , page.compression_pct as page_compression_pct
               --, nocompress.compression_pct as no_compression_compression_pct
               , coalesce(page.compression_pct , 0) - coalesce(row.compression_pct, 0) as diff_row_pct_vs_page_pct
               --, nocompress.size_with_requested_compression_kb  as kb_after_no_compression
               , case 
                      when row.compression_pct < @compress_if_space_saved_pct_greater_than 
                           and page.compression_pct < @compress_if_space_saved_pct_greater_than
                      then 'no compression'
                      when coalesce(page.compression_pct , 0) - coalesce(row.compression_pct, 0) < @row_over_page_if_pct_diff_less_than then 'row compression - minimal diff vs page.'
                      else 'page compression'
                 end                        as meets_space_criteria
               , usage_percent_scans            as pct_scans_usage_stats
               , ops_percent_scans          as pct_scans_ops_stats
               , @compression_min_scan_pct  as min_scan_pct_theshold_value
               , case when @compression_min_scan_pct <= usage_percent_scans
                       and @compression_min_scan_pct <= ops_percent_scans
                      then 'ok - high scans' 
                      when @compression_min_scan_pct <= usage_percent_scans
                       or @compression_min_scan_pct <= ops_percent_scans
                      then 'unclear' else 'x - does not meet threshold' end as meets_scan_threshold

               , usage_percent_updates  as pct_updates_usage_stats
               , ops_percent_update          as pct_updates_ops_stats
               , @compression_max_update_pct as max_update_pct_theshold
               , case when @compression_max_update_pct >= usage_percent_updates
                       and @compression_max_update_pct >= ops_percent_update
                      then 'ok - low updates' 
                      when @compression_max_update_pct >= usage_percent_updates
                       or @compression_max_update_pct >= ops_percent_update
                      then 'unclear'else 'x -does not meet threshold' end  as meets_update_threshold
               , usage_total_reads as total_reads_usage_stats
               , ops_total_reads as total_reads_ops_stats
               , total_usage_category
               , row.space_savings_mb as row_space_savings_mb
               , page.space_savings_mb as page_space_savings_mb

               , cast(coalesce(nocompress.size_with_current_kb, row.size_with_current_kb , page.size_with_current_kb )/1024.0 as decimal(38, 4))  as mb_current
               , cast(row.size_with_requested_compression_kb/1024.0 as decimal(38, 4)) as mb_after_row_compression
               , cast(page.size_with_requested_compression_kb/1024.0 as decimal(38, 4))   as mb_after_page_compression
               , coalesce(case when row.compression_pct < @compress_if_space_saved_pct_greater_than 
                           and page.compression_pct < @compress_if_space_saved_pct_greater_than
                      then ''
                      when coalesce(page.compression_pct , 0) - coalesce(row.compression_pct, 0) < @row_over_page_if_pct_diff_less_than then row.ddl_statement
                      else page.ddl_statement
                 end , '')as recommended_script
               , row.ddl_statement as row_ddl_statement
               , page.ddl_statement as page_ddl_statement
               --, nocompress.ddl_statement_with_crlf as no_compression_ddl_statement
            into #compression_results
            from #consolidated_usage cu
            left join (select * from #output3 where compression_tested = 'row') as row on row.table_name = cu.table_name and row.index_id = cu.index_id and row.partition_number = cu.partition_number and row.schema_name = cu.schema_name
            left join (select * from #output3 where compression_tested = 'page') as page on page.table_name = cu.table_name and page.index_id = cu.index_id and page.partition_number = cu.partition_number and page.schema_name = cu.schema_name
            left join (select * from #output3 where compression_tested = 'none') as nocompress on nocompress.table_name = cu.table_name and nocompress.index_id = cu.index_id and nocompress.partition_number = cu.partition_number and nocompress.schema_name = cu.schema_name
            where  coalesce(nocompress.size_with_current_kb, row.size_with_current_kb , page.size_with_current_kb )  is not null

      raiserror('   |--- begin compression estimation summary', 10, 1) with nowait

      if object_id(N'tempdb..#compression_summary') is not null drop table #compression_summary

      select 'estimated compression summary space savings' as Output_Type
         , meets_space_criteria
         , case when meets_space_criteria = 'no compression' then 'n/a' else meets_scan_threshold end as meets_scan_threshold
         , case when meets_space_criteria = 'no compression' then 'n/a' else meets_update_threshold end as meets_update_threshold
         , count(*) as indices_affected
         , sum(mb_current) as mb_current
         , sum(case when meets_space_criteria = 'page compression' then  mb_after_page_compression
                when meets_space_criteria = 'row compression - minimal diff vs page.'  then mb_after_row_compression
                else  mb_current end ) as mb_after_compression
      into #compression_summary
      from #compression_results
      group by meets_space_criteria
         , case when meets_space_criteria = 'no compression' then 'n/a' else meets_scan_threshold end 
         , case when meets_space_criteria = 'no compression' then 'n/a' else meets_update_threshold end
      
      select *
      , mb_current - mb_after_compression as mb_saved
      , case when mb_current = 0 then 0 
             else cast((mb_current - mb_after_compression)/mb_current * 100 as decimal(34, 2))
         end as estimated_compression_pct
      from #compression_summary
      order by meets_space_criteria, meets_scan_threshold, meets_update_threshold

      raiserror('   |--- end compression estimation summary ', 10, 1) with nowait
      raiserror('   |---', 10, 1) with nowait


      select *
      from #compression_results
      order by type_desc, coalesce(page_space_savings_mb, row_space_savings_mb) desc
            -- meets_space_criteria, meets_scan_threshold, meets_update_threshold

      raiserror('   |--- end compression estimation detailed output ', 10, 1) with nowait
      raiserror('   |---', 10, 1) with nowait


     raiserror('|- End compression', 10, 1) with nowait
     raiserror('|----------------------------------------------------------------', 10, 1) with nowait

end --if @calculate_row_compression = 1 or @calculate_page_compression = 1 or @calculate_no_compression = 1



