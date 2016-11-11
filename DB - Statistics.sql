set nocount on

/* script to generate 'update statistics' statements when you need to do them manually.

author - john kauffman
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


sql2012 offers a new dmv.  basic code here for future reference.  leaving as is for backwards compatibility

select 
    obj.name, obj.object_id, stat.name, stat.stats_id, last_updated, modification_counter
from sys.objects as obj 
join sys.stats stat on stat.object_id = obj.object_id
cross apply sys.dm_db_stats_properties(stat.object_id, stat.stats_id) as sp
--where modification_counter > 100
order by modification_counter desc; 

select
    sp.stats_id, name, filter_definition, last_updated, rows, rows_sampled, steps, unfiltered_rows, modification_counter 
from sys.stats as stat 
cross apply sys.dm_db_stats_properties(stat.object_id, stat.stats_id) as sp
where stat.object_id = object_id('dbo.address'); 

*/
/* optional functionality parameters*/
declare @output_density_vector              bit = 0
declare @output_histogram                   bit = 0
declare @output_duplicate_stats_report      bit = 1
declare @statistics_sampling_pct            int = 90     -- used to build stats update statement. 

/* filters to apply.*/
declare @include_system_stats               bit = 1
declare @include_index_stats                bit = 1

declare @show_stats_days_older_than         int = -1     -- define old by # days prior to today.  use -1 for all
declare @show_stats_w_row_mod_pct_over    float = 0.00   -- find entries based on row mod percentage.
declare @show_stats_w_sampling_less_than  float = 11.00 -- find stats with low sampling rates. set to 100% for all 

declare @table_name_csv_list     nvarchar(4000) = 'SalesOrderHeader_ondisk'-- 'schedule_batch, schedule_batch_status, schedule_entity, schedule_entity_comm, schedule_entity_contact, schedule_entity_contact_comm, schedule_entity_reference, schedule_frequency_type, schedule_header, schedule_header_cost, schedule_header_datetime, schedule_header_log, schedule_header_measure, schedule_header_monetary, schedule_header_package, schedule_header_package_detail, schedule_header_package_dim, schedule_header_package_group, schedule_header_package_group_measure, schedule_header_package_group_monetary, schedule_header_package_group_quantity, schedule_header_package_group_ref, schedule_header_package_measure, schedule_header_package_monetary, schedule_header_package_quantity, schedule_header_package_reference, schedule_header_reference, schedule_header_service, schedule_header_shipping_instruction, schedule_header_tax, schedule_history, schedule_jit_quantity, schedule_jit_quantity_reference, schedule_package_detail, schedule_part, schedule_part_datetime, schedule_part_log, schedule_part_measure, schedule_part_monetary, schedule_part_reference, schedule_part_shipping_instruction, schedule_processing_status, schedule_quantity, schedule_quantity_reference, schedule_transit_dock_assn, schedule_transit_offset, schedule_transport_leg, schedule_transport_leg_reference, schedule_transport_segment, schedule_transport_segment_reference, schedule_type, schedule_validation_status, scheduleb_types, scheduling_activity, scheduling_activity_dtm, scheduling_activity_milestone, scheduling_activity_reference, scheduling_activity_style, scheduling_activity_style_type_assn, scheduling_activity_type, '--'fss_shipment_detail_conveyance_assn, fss_power_unit, fss_shipment_group_fss_trip_assn, fss_shipment_conveyance_assn, fss_activity_personnel, fss_trip_personnel_assn, fss_shipment_detail_datetime, fss_trip_addl_service, fss_shipment_package_summary_detail_assn, fss_stop_addl_service, fss_trip_conveyance, fss_shipment_leg_location, fss_shipment_group_quantity, fss_shipment_package_summary, fss_shipment_group_measure, fss_addl_service_reference_nbr, fss_shipment_package_datetime, fss_shipment_activity, fss_trip_distance, fss_shipment_milestone, fss_activity_voucher_assn, fss_shipment_group_datetime, fss_shipment_group_assn, fss_trip_monetary, fss_shipment_monetary, fss_shipment_datetime, fss_activity, fss_stop_reference_nbr, fss_addl_service, fss_conveyance, trip_group_activity, fss_transportation_activity, fss_activity_tariff_assn, trip_group_activity_count, trip_group_financial_detail, fss_shipment_service, fss_trip, fss_shipment_location, fss_shipment_group_reference_nbr, fss_activity_reference_nbr, fss_trip_conveyance_assn, fss_shipment, trip_group_detail, fss_stop, fss_shipment_group, fss_activity_resolution, fss_shipment_leg, fss_trip_group, fss_rating_item, fss_shipment_trip_assn, fss_shipment_package, fss_activity_financial_event, fss_trip_reference_nbr, fss_shipment_package_reference_nbr, fss_shipment_detail, fss_activity_conversion_audit, fss_activity_uom, fss_shipment_package_detail, fss_shipment_reference_nbr, fss_shipment_detail_reference_nbr, '
declare @table_name_contains      nvarchar(100) = '*' -- filters tables.  will implement as '%@table_name_contains%'.  pass '*' or null to return unfiltered data
declare @stats_name              nvarchar(1024) = '*'         -- use '*' for all.  does not support csv of  names
declare @min_row_count                      int = 1 -- 0 or null for no lower limit
declare @max_row_count                      int = null    -- 0 or null for no upper limit
declare @min_page_count                     int = null
declare @max_page_count                     int = null
declare @min_megabytes                      int = null
declare @max_megabytes                      int = null


set nocount on

declare @db_name     nvarchar(1024)          
DECLARE @sqlcmd      NVARCHAR(max)
DECLARE @params      NVARCHAR(500)
DECLARE @sqlmajorver int

set @db_name     = db_name() collate latin1_general_cs_as 
set @sqlmajorver = CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff);

if object_id('tempdb..#table_names')               <> 0 drop table #table_names
if object_id('tempdb..#table_names_full')          <> 0 drop table #table_names_full
if object_id(N'tempdb..#stats_prep')               <> 0 drop table #stats_prep
if object_id(N'tempdb..#stats_prep2')              <> 0 drop table #stats_prep2
if object_id(N'tempdb..#stats')                    <> 0 drop table #stats
if object_id(N'tempdb..#rows')                     <> 0 drop table #rows
IF OBJECT_ID(N'TEMPDB..#stats_columns')            <> 0 DROP TABLE #stats_columns
if object_id(N'tempdb..#stats_header_info')        <> 0 drop table #stats_header_info
if object_id(N'tempdb..#stats_header_info_full')   <> 0 drop table #stats_header_info_full
if object_id(N'tempdb..#stats_density_vector')     <> 0 drop table #stats_density_vector
if object_id(N'tempdb..#stats_density_vector_full')<> 0 drop table #stats_density_vector_full
if object_id(N'tempdb..#stats_histogram')          <> 0 drop table #stats_histogram
if object_id(N'tempdb..#stats_histogram_full')     <> 0 drop table #stats_histogram_full


create table #table_names
      (
        table_name sysname
      )

create table #stats_prep
(
  schema_name nvarchar(128)
, table_name nvarchar(128)
, object_id int
, name nvarchar(128)
, auto_created bit
, stats_id int
, statistics_update_date datetime
, is_memory_optimized bit
, is_incremental bit
, no_recompute bit
)

create table #stats
(
  schema_name nvarchar(128)
, table_name nvarchar(128)
, object_id int
, name nvarchar(128)
, auto_created bit
, stats_id int
, statistics_update_date datetime
, is_memory_optimized bit
, is_incremental bit
, no_recompute bit

, row_num int
)

 
CREATE TABLE #stats_columns
(
      row_num         int
    , stats_column_id int
    , name            nvarchar(128)
)
  

create table #rows
(  schema_name nvarchar(128)
, table_name nvarchar(128)
, object_id int
, stats_id int
, row_count bigint
, used_page_count bigint
, index_id int
, row_logic nvarchar(20)
)

create table #stats_header_info
( name sysname
, updated datetime
, rows bigint
, rows_sampled bigint
, steps smallint
, density float
, average_key_length float
, string_index varchar(10)
, filter_expression nvarchar(1000)
, unfiltered_rows bigint
)

create table #stats_density_vector
( all_density float
  , avg_length float
  , columns nvarchar(4000)
)

create table #stats_histogram
(range_hi_key sql_variant
, range_rows float
, eq_rows float
, distinct_range_rows float
, avg_range_rows float
)

create table #stats_header_info_full
(
  db_name sysname
, schema_name sysname
, table_name sysname
, object_id int
, stats_name sysname
, stats_id int
, column_count smallint
, column_list nvarchar(4000)
, capture_id int
, capture_date date
, capture_hour tinyint
, capture_minute tinyint
, capture_datetime datetime
, updated datetime
, days_old int
, rows bigint
, rows_sampled bigint
, sample_pct decimal(6, 2)
, steps smallint
, density float
, average_key_length float
, string_index varchar(10)
, filter_expression nvarchar(1000)
, unfiltered_rows bigint
, update_cmd nvarchar(500)
)

create table #stats_density_vector_full
( db_name sysname
, schema_name sysname
, table_name sysname
, stats_name sysname
 , all_density float
, avg_length float
, columns nvarchar(4000)
)

create table #stats_histogram_full
( db_name sysname
, schema_name sysname
, table_name sysname
, stats_name sysname
, range_hi_key sql_variant
, range_rows float
, eq_rows float
, distinct_range_rows float
, avg_range_rows float
)

if @table_name_csv_list <>'*'
   begin

      insert into #table_names
         select QUOTENAME(ltrim(rtrim(item )))
         from (
               select item = y.i.value('(./text())[1]', 'nvarchar(4000)')
               from 
               ( 
                 select x = convert(xml, '<i>' 
                   + replace(@table_name_csv_list , ',', '</i><i>') 
                   + '</i>').query('.')
               ) as a cross apply x.nodes('i') as y(i) ) x
         where charindex(@table_name_contains, item )<> 0 or @table_name_contains = '*'

   end
else
   begin
      insert into #table_names
         select quotename(name) 
         from sys.objects
         where type in ( 'u', 'v')
         and charindex(@table_name_contains, name)<> 0  or @table_name_contains = '*'
   end
set nocount on

if @sqlmajorver <=11
   begin
      set @sqlcmd = N'insert into #stats_prep
   select quotename(sc.name) as schema_name
      , quotename(o.name) as table_name
      , o.object_id
      , quotename(s.name) --as stats_name 
      , auto_created bit
      , stats_id
      , stats_date(o.object_id, stats_id) as statistics_update_date
      , 0 as is_memory_optimized
      , 0 as is_incremental
      , no_recompute
   from sys.stats s
   join sys.objects o on o.object_id = s.object_id
   join sys.schemas sc on sc.schema_id = o.schema_id
   left join sys.tables t on t.object_id = o.object_id
   where o.type in (''u'', ''v'') 
    and quotename(o.name) collate database_default in (select table_name collate database_default from #table_names) -- don''t want system tables
    and (s.name  = ''@stats_name''  or ''@stats_name'' = ''*'' )
    and ((@include_system_stats = 1 and  s.auto_created = 1 )
         or
       (@include_index_stats = 1 and s.auto_created = 0))'
   end
else
      set @sqlcmd = N'insert into #stats_prep
   select quotename(sc.name) as schema_name
      , quotename(o.name) as table_name
      , o.object_id
      , quotename(s.name) --as stats_name 
      , auto_created
      , stats_id
      , stats_date(o.object_id, stats_id) as statistics_update_date
      , is_memory_optimized
      , is_incremental
      , no_recompute
   from sys.stats s
   join sys.objects o on o.object_id = s.object_id
   join sys.schemas sc on sc.schema_id = o.schema_id
   left join sys.tables t on t.object_id = o.object_id
   where o.type in (''u'', ''v'') 
    and quotename(o.name) collate database_default in (select table_name collate database_default from #table_names) -- don''t want system tables
    and (s.name  = ''@stats_name'' or ''@stats_name'' = ''*'' )
    and ((@include_system_stats = 1 and  s.auto_created = 1 )
         or
       (@include_index_stats = 1 and s.auto_created = 0))'
set nocount on
set @sqlcmd = replace(@sqlcmd, '@stats_name', @stats_name)
set @sqlcmd = replace(@sqlcmd, '@include_system_stats', @include_system_stats)
set @sqlcmd = replace(@sqlcmd, '@include_index_stats', @include_index_stats)
print @sqlcmd

set nocount on

exec (@sqlcmd)



/* get current rows.
3 use cases.
1.  for index-based stats, get value from partition stats.  this addresses filtered index row counts <> clustered index row counts.
2.  for unfiltered system stats, get row count for clustered index or heap for the table.
3.  for filtered system stats, well.... no good answer.  i could issue queries to get row counts, but that would be extremely costly.
    for now, i just use the unfiltered count.
4.  in-memory tables.  tricky.  will get row counts, since it's really fast.  but, the stats show with row 0 in first pull. 
      so, i exclude them.


* get tables that meet row count or size criteria.  
since filtred indices can have different sizes, take largest value (size of actual table)*/
insert into #rows
   select schema_name
      , table_name
      , s.object_id
      , stats_id 
      , sum(coalesce(row_count, 0)) as row_count
      , sum(coalesce(used_page_count, 0)) as used_page_count
      , p.index_id
      , 'rows insert 1'
   from #stats_prep                     s
   join  sys.partitions            p on p.object_id = s.object_id and p.index_id = s.stats_id
   join sys.dm_db_partition_stats ps on ps.partition_id = p.partition_id
   where is_memory_optimized = 0
   group by schema_name, table_name, s.object_id, p.index_id, s.stats_id

/* get the system stats, and assign clustered index or heap counts to them.*/
if object_id(N'tempdb..#stats_without_rows') is not null drop table #stats_without_rows

select s.*
into #stats_without_rows
from #stats_prep s
left join #rows r on r.object_id = s.object_id and r.stats_id = s.stats_id
where r.object_id is null

--select * from #rows where table_name like '%bigtransactionhistory%'
--select * from #stats_without_rows where table_name like '%bigtransactionhistory%'

insert into #rows
   select schema_name
      , table_name
      , s.object_id
      , stats_id 
      , sum(coalesce(row_count, 0)) as row_count
      , sum(coalesce(used_page_count, 0)) as used_page_count
      , p.index_id
      , 'rows insert 2'
   from #stats_without_rows        s
   join sys.indexes                i on i.object_id = s.object_id
   join sys.partitions             p on p.object_id = s.object_id and p.index_id = i.index_id
   join sys.dm_db_partition_stats ps on ps.partition_id = p.partition_id
   where i.type in (0, 1, 5)  -- heap, clustered, clustered columnstore
   group by schema_name, table_name, s.object_id, stats_id, p.index_id

/* in-memory tables don't have row counts in the partitions table.
   until i figure out how to find the counts programmatically, i need to loop over them*/

IF OBJECT_ID(N'TEMPDB..#stats_without_rows2') IS NOT NULL DROP TABLE #stats_without_rows2

create table #stats_without_rows2 (row_num int identity(1, 1) , schema_name sysname, table_name sysname, object_id int)

insert into #stats_without_rows2
   select distinct s.schema_name, s.table_name, s.object_id
   from #stats_prep s
   left join #rows r on r.object_id = s.object_id and r.stats_id = s.stats_id
   where r.object_id is null

set nocount on
declare @counter2 int = 1
declare @Max_counter2 int = (select count(*) from #stats_without_rows2)

declare @object_id2 int
declare @full_table_name nvarchar(256)
declare @row_count bigint
declare @sql_text2 nvarchar(4000)

IF OBJECT_ID(N'TEMPDB..#row_count') IS NOT NULL DROP TABLE #row_count
create table #row_count (object_id int, row_count bigint)

while @counter2 <= @Max_counter2
   begin
      select @full_table_name = schema_name + N'.' + table_name 
           , @object_id2       = object_id
      from #stats_without_rows2 where row_num = @counter2

      set @sql_text2 = N'insert into #row_count select ' + cast(@object_id2 as varchar) + ', count_big(1) from ' + @full_table_name
         exec (@sql_text2)

      set @counter2 = @counter2 + 1
   end
set nocount on

insert into #rows
   select sp.schema_name
      , sp.table_name
      , rc.object_id
      , sp.stats_id 
      , rc.row_count
      , -1 as used_page_count
      , -1
      , 'rows insert 3'
   from #row_count rc
   join #stats_prep sp on sp.object_id = rc.object_id

--select r.* 
--from #rows r
--join (
--select schema_name
--, table_name
--, stats_id
--from #rows 
--group by schema_name
--, table_name
--, stats_id
--having count(*) > 1) x on x.schema_name = r.schema_name and x.table_name = r.table_name and x.stats_id = r.stats_id




/* remove stats entries that don't meet size criteria*/
delete from #rows
               where not (( coalesce(@min_row_count, 0)  = 0  or coalesce(row_count, 0)  >= @min_row_count)
                      and ( coalesce(@max_row_count, 0)  = 0  or coalesce(row_count, 0)  <= @max_row_count)
                      and ( coalesce(@min_page_count, 0) = 0  or coalesce(used_page_count, 0) >= @min_page_count)
                      and ( coalesce(@max_page_count, 0) = 0  or coalesce(used_page_count, 0) <= @max_page_count)
                      and ( coalesce(@min_megabytes, 0)  = 0  or coalesce(used_page_count, 0) * 8.0 / 1024  >= @min_megabytes)
                      and ( coalesce(@max_megabytes, 0)  = 0  or coalesce(used_page_count, 0) * 8.0 / 1024  <= @max_megabytes))


/*filter by stats date age criteria, and assign row_number used for loop below*/
insert into #stats
select sp.*, row_number() over(order by sp.table_name, sp.stats_id) as row_num
from #stats_prep sp
join #rows r on r.object_id = sp.object_id and sp.stats_id = r.stats_id--where object_id not in (select object_id from #rows)
   where (statistics_update_date < cast(cast(getdate() as date) as datetime) - @show_stats_days_older_than
         or statistics_update_date is null)

/* get columns in stats.  this is available if density vector is run, but that may not always get run*/

insert into #stats_columns
   select row_num, stats_column_id, c.name
   from #stats s
   join sys.stats_columns sc on sc.object_id = s.object_id
                              and sc.stats_id = s.stats_id
   join sys.all_columns c on c.object_id = sc.object_id
                              and c.column_id = sc.column_id

create index ix1 on #stats_columns (row_num, stats_column_id, name)
 

declare @counter int
declare @max_counter int
declare @table_stat nvarchar(1000)
declare @sql_text nvarchar(1000)
declare @schema sysname 
declare @table sysname
declare @stat sysname
declare @object_id int
declare @stats_id int
declare @capture_id int = 1
declare @stats_column_id int = 1
declare @max_stats_column_id int
declare @column_list nvarchar(4000) = ''
declare @column_count smallint 

set @counter = 1
set @max_counter = (select max(row_num) from #stats)
set @table_stat = ''
set @sql_text = ''

print 'rows to process'
print '------------------------------------------------------------------------------------------------------'
print @max_counter
set nocount on
while @counter <= @max_counter
   begin

      select @table_stat = ''''+ schema_name + '.' + table_name + ''', ' + name
         , @table  = table_name
         , @stat = name
         , @schema = schema_name
         , @object_id = object_id
         , @stats_id = stats_id
      from #stats
      where row_num = @counter

print @counter
print @table_stat
print '--------------------'

      /* two tables, because i don't know how to insert extra columns when using an exec @sql syntax.  or rather, with dbcc set outputs*/
      set @sql_text = N'dbcc show_statistics (' + @table_stat + ') with no_infomsgs,  stat_header' 
     -- print @sql_text
      insert into #stats_header_info exec (@sql_text) -- ('dbcc show_statistics ( tab, i1) with  stat_header')

      if @output_density_vector = 1
         begin
            set @sql_text = N'dbcc show_statistics (' + @table_stat + ') with no_infomsgs,  density_vector' 
            insert into #stats_density_vector exec (@sql_text) -- ('dbcc show_statistics ( tab, i1) with  stat_header')
         end

      if @output_histogram = 1
         begin
            set @sql_text = N'dbcc show_statistics (' + @table_stat + ') with no_infomsgs,  histogram' 
            insert into #stats_histogram exec (@sql_text) -- ('dbcc show_statistics ( tab, i1) with  stat_header')
         end

      set @stats_column_id  = 1
      set @max_stats_column_id = (select max(stats_column_id) from #stats_columns where row_num = @counter)
      set @column_list = ''
      set @column_count = 0

      while @stats_column_id <= @max_stats_column_id
         begin
            select @column_list = @column_list + name + ', '
            from #stats_columns 
            where row_num = @counter
               and stats_column_id = @stats_column_id

            set @stats_column_id = @stats_column_id + 1 
            set @column_count = @column_count + 1  
         end

      set @column_list  = case when len(@column_list) > 0 then left(@column_list, len(@column_list) - 1) end

      insert into #stats_header_info_full
         select @db_name
         , @schema
         , @table 
         , @object_id
         , quotename(name)
         , @stats_id
         , @column_count
         , @column_list
         , @capture_id
         , cast(getdate() as date)
         , datepart(hour, getdate())
         , datepart(minute, getdate())
         , getdate()
         , updated 
         , datediff(day, updated, getdate()) as days_old
         , rows 
         , rows_sampled 
         , case when rows = 0 then 0 else cast(rows_sampled * 1.0/rows * 100 as decimal(6, 2)) end as sample_pct
         , steps 
         , density 
         , average_key_length 
         , string_index
         , filter_expression 
         , unfiltered_rows 
         , case when name like '_wa_sys%' 
            then 'update statistics ' + @schema + '.' + @table + '(' + quotename(name) + ') with sample ' + cast(@statistics_sampling_pct as varchar(10)) + ' percent '      
            else 'update statistics ' + @schema + '.' + @table + '.' + quotename(name) + '  with sample ' + cast(@statistics_sampling_pct as varchar) + ' percent '     end as cmd
      from #stats_header_info shi
      where case when coalesce(rows, 0) = 0 then 0 else cast(rows_sampled * 1.0/rows * 100 as decimal(6, 2)) end <= @show_stats_w_sampling_less_than


      insert into #stats_density_vector_full
         select @db_name
         , @schema
         , @table
         , @stat 
         , all_density 
         , avg_length 
         , columns 
         from #stats_density_vector

      insert into #stats_histogram_full
         select @db_name
         , @schema
         , @table
         , @stat 
         , range_hi_key 
         , range_rows 
         , eq_rows 
         , distinct_range_rows 
         , avg_range_rows 
         from #stats_histogram


      truncate table #stats_header_info;
      truncate table #stats_density_vector;
      truncate table #stats_histogram;


      if @counter%10 = 0 print @counter
      --print @sql_text

      set @counter = @counter + 1
   end  --while @counter <= @max_counter
print 'done with loop'


if object_id(N'tempdb..#final') is not null drop table #final

select  
     capture_id 
   , db_name 
   , capture_date 
   , capture_datetime 
   , f.schema_name
   , f.table_name 
   , stats_name 
   , auto_created
   , column_count
   , column_list
   , is_memory_optimized
   , updated 
   , days_old 
   , r.row_count as table_row_count
   , f.rows as stat_row_count -- the number of rows in the table when the stat was calculated.
   , f.rows_sampled 
   , sample_pct  as sample_pct
   , case when r.row_count = 0 then 0 else cast(f.rows * 100.0 /r.row_count as decimal(19, 4)) end as stats_count_pct_of_table_count
   , i.rowmodctr as row_mod_count
   , case when r.row_count = 0 then 0 else cast(i.rowmodctr * 100.0 /r.row_count as decimal(19, 4)) end as row_mod_pct_current_table_count
   , case when f.rows = 0 then 0 else cast(i.rowmodctr * 100.0 /f.rows as decimal(19, 4)) end as row_mod_pct_stat_count
   , case 
       when 
            case when coalesce(r.row_count, 0) = 0 then 0 else cast(i.rowmodctr * 100.0 /r.row_count as decimal(19, 4)) end >= case when coalesce(f.rows, 0) = 0 then 0 else cast(i.rowmodctr * 100.0 /f.rows as decimal(19, 4)) end
        and case when coalesce(r.row_count, 0) = 0 then 0 else cast(i.rowmodctr * 100.0 /r.row_count as decimal(19, 4)) end >= case when coalesce(i.rows, 0) = 0 then 0 else cast(i.rowmodctr * 100.0 /i.rows as decimal(19, 4)) end 
       then case when coalesce(r.row_count, 0) = 0 then 0 else cast(i.rowmodctr * 100.0 /r.row_count as decimal(19, 4)) end
       when         
            case when coalesce(f.rows, 0) = 0 then 0 else cast(i.rowmodctr * 100.0 /f.rows as decimal(19, 4)) end >= case when coalesce(r.row_count, 0) = 0 then 0 else cast(i.rowmodctr * 100.0 /r.row_count as decimal(19, 4)) end
        and case when coalesce(f.rows, 0) = 0 then 0 else cast(i.rowmodctr * 100.0 /f.rows as decimal(19, 4)) end >= case when coalesce(i.rows, 0) = 0 then 0 else cast(i.rowmodctr * 100.0 /i.rows as decimal(19, 4)) end 
       then case when coalesce(f.rows, 0) = 0 then 0 else cast(i.rowmodctr * 100.0 /f.rows as decimal(19, 4)) end
       when           
            case when coalesce(i.rows, 0) = 0 then 0 else cast(i.rowmodctr * 100.0 /i.rows as decimal(19, 4)) end  >= case when coalesce(f.rows, 0) = 0 then 0 else cast(i.rowmodctr * 100.0 /f.rows as decimal(19, 4)) end
        and case when coalesce(i.rows, 0) = 0 then 0 else cast(i.rowmodctr * 100.0 /i.rows as decimal(19, 4)) end  >= case when coalesce(r.row_count, 0) = 0 then 0 else cast(i.rowmodctr * 100.0 /r.row_count as decimal(19, 4)) end 
       then case when coalesce(i.rows, 0) = 0 then 0 else cast(i.rowmodctr * 100.0 /i.rows as decimal(19, 4)) end  
      end as highest_row_mod_pct   , case when r.row_count = 0 then '0 - no rows in table'
      when cast(i.rowmodctr * 100.0 /r.row_count as decimal(19, 4)) =  0 then '0 - no stats updates'
      when cast(i.rowmodctr * 100.0 /r.row_count as decimal(19, 4)) <1 then '1 - lt 1%'
      when cast(i.rowmodctr * 100.0 /r.row_count as decimal(19, 4)) between 1 and 5 then '2 - lt 5%'
      when cast(i.rowmodctr * 100.0 /r.row_count as decimal(19, 4)) between 5 and 10 then '3 - btween 5 and 10 %'
      when cast(i.rowmodctr * 100.0 /r.row_count as decimal(19, 4)) between 10 and 20 then '4 - between 10 and 20 %'
      else '5 - over 10%' end as row_mod_pct_current_count_category
   , case   when f.rows = 0 then '0 - no rows in table'
      when cast(i.rowmodctr * 100.0 /f.rows as decimal(19, 4)) =  0 then '0 - no stats updates'
      when  cast(i.rowmodctr * 100.0 /f.rows as decimal(19, 4)) <1 then '1 - lt 1%'
      when cast(i.rowmodctr * 100.0 /f.rows as decimal(19, 4)) between 1 and 5 then '2 - lt 5%'
      when cast(i.rowmodctr * 100.0 /f.rows as decimal(19, 4)) between 5 and 10 then '3 - between 5 and 10 %'
      when cast(i.rowmodctr * 100.0 /f.rows as decimal(19, 4)) between 10 and 20 then '4 - between 10 and 20 %'
      else '5 - over 20%' end as row_mod_pct_stat_count_category


   , f.steps 
   , f.density 
   , f.average_key_length 
   , f.string_index 
   , f.filter_expression
   , f.unfiltered_rows 
   , f.update_cmd  
into #final
from #stats_header_info_full f
join #rows r on r.object_id= f.object_id
               and r.stats_id = f.stats_id
join #stats s on s.object_id = r.object_id
                  and s.stats_id = f.stats_id
left join sysindexes i on i.id = r.object_id
               and i.indid = f.stats_id



select * 
from #final
where row_mod_count is null or
(@show_stats_w_row_mod_pct_over <= highest_row_mod_pct)
order by db_name, table_name, stats_name

if @output_density_vector  = 1
   begin
      select 'Density Vector' as output_type, dvf.* 
      from #stats_density_vector_full dvf
      join #final f on f.stats_name = dvf.stats_name
                  and f.table_name = dvf.table_name
                  and f.schema_name = dvf.schema_name
      where row_mod_count is null or
      (@show_stats_w_row_mod_pct_over <= highest_row_mod_pct)
      order by db_name, schema_name, table_name, stats_name
   end  --if @output_density_vector  = 1

if @output_histogram  = 1
   begin
      select  'Histogram' as output_type, hf.* 
      from #stats_histogram_full hf
      join #final f on f.stats_name = hf.stats_name
                  and f.table_name = hf.table_name
                  and f.schema_name = hf.schema_name
      where row_mod_count is null or
      (@show_stats_w_row_mod_pct_over <= highest_row_mod_pct)
      order by db_name, schema_name, table_name, stats_name
   end  --if @output_histogram  = 1

if @output_duplicate_stats_report = 1
   begin
      select 'duplicate statistics' as output_type
         , f.*
      from #stats_header_info_full f
      join (
            select schema_name, table_name, column_list
            from #stats_header_info_full
            group by schema_name, table_name, column_list
            having count(1) > 1) x on x.schema_name = f.schema_name 
                                    and f.table_name = x.table_name
                                    and f.column_list = x.column_list
            order by schema_name, table_name, column_list, stats_name
   end  --if @output_duplicate_stats_report = 1

