/* server and instance level*/

/* still in progress, but wanting to capture the following*/
--configurations
--features installed
--trace flags enabled startup/other
--data collections running
--cpu info
--memory info
--server hardware specs
--OS info
--cluster info
--bios info
--sql instance info

declare @show_source_and_definition_details bit = 1 -- typically 0.  set to 1 to see where the values come from, books online definitions, etc.*/


/* store server and instance level properties*/
IF OBJECT_ID(N'TEMPDB..#server_instance') IS NOT NULL DROP TABLE #server_instance

create table #server_instance
(      scope           nvarchar(100)
     , source          nvarchar(2000)
     , property        nvarchar(128)
     , internal_value  sql_variant
     , user_value      sql_variant
     , description     nvarchar(512)
     , minimum         sql_variant 
     , maximum         sql_variant 
     , is_dynamic      bit         
     , is_advanced     bit         
)
-- Declare Global Variables
DECLARE @UpTime VARCHAR(12),@StartDate DATETIME
DECLARE @ErrorSeverity int, @ErrorState int, @ErrorMessage NVARCHAR(4000)
DECLARE @CMD NVARCHAR(4000)
DECLARE @path NVARCHAR(2048)
DECLARE @existout int, @FSO int, @FS int, @OLEResult int, @FileID int

DECLARE @sqlcmd NVARCHAR(max), @params NVARCHAR(500)

--------------------------------------------------------------------------------------------
/* COLLECT SERVER-LEVEL PROPERTIES */
--------------------------------------------------------------------------------------------

DECLARE @sqlmajorver int
, @sqlminorver int
, @sqlbuild int
, @clustered bit
, @winver VARCHAR(5)
, @server VARCHAR(128)
, @instancename NVARCHAR(128)
, @arch smallint
, @winsp VARCHAR(25)
, @SystemManufacturer VARCHAR(128)
, @machinename nvarchar(128)

SELECT @machinename = CONVERT(VARCHAR(128), SERVERPROPERTY('ComputerNamePhysicalNetBIOS'))
SELECT @instancename = CONVERT(VARCHAR(128),SERVERPROPERTY('InstanceName')) 
SELECT @server = RTRIM(CONVERT(VARCHAR(128), SERVERPROPERTY('MachineName')))


IF OBJECT_ID(N'TEMPDB..#machineinfo') IS NOT NULL DROP TABLE #machineinfo

create TABLE #machineinfo ([Value] NVARCHAR(256), [Data] NVARCHAR(256))

INSERT INTO #machineinfo
EXEC xp_instance_regread 'HKEY_LOCAL_MACHINE','HARDWARE\DESCRIPTION\System\BIOS','SystemManufacturer';
INSERT INTO #machineinfo
EXEC xp_instance_regread 'HKEY_LOCAL_MACHINE','HARDWARE\DESCRIPTION\System\BIOS','SystemProductName';
INSERT INTO #machineinfo
EXEC xp_instance_regread 'HKEY_LOCAL_MACHINE','HARDWARE\DESCRIPTION\System\BIOS','SystemFamily';
INSERT INTO #machineinfo
EXEC xp_instance_regread 'HKEY_LOCAL_MACHINE','HARDWARE\DESCRIPTION\System\BIOS','BIOSVendor';
INSERT INTO #machineinfo
EXEC xp_instance_regread 'HKEY_LOCAL_MACHINE','HARDWARE\DESCRIPTION\System\BIOS','BIOSVersion';
INSERT INTO #machineinfo
EXEC xp_instance_regread 'HKEY_LOCAL_MACHINE','HARDWARE\DESCRIPTION\System\BIOS','BIOSReleaseDate';
INSERT INTO #machineinfo
EXEC xp_instance_regread 'HKEY_LOCAL_MACHINE','HARDWARE\DESCRIPTION\System\CentralProcessor\0','ProcessorNameString';
insert into #machineinfo 
EXEC xp_instance_regread 'HKEY_LOCAL_MACHINE','SOFTWARE\MICROSOFT\WINDOWS NT\CURRENTVERSION','CurrentVersion'
insert into #machineinfo 
EXEC xp_instance_regread 'HKEY_LOCAL_MACHINE','SOFTWARE\MICROSOFT\WINDOWS NT\CURRENTVERSION','EditionID'
insert into #machineinfo 
EXEC xp_instance_regread 'HKEY_LOCAL_MACHINE','SOFTWARE\MICROSOFT\WINDOWS NT\CURRENTVERSION','ProductName'

--SELECT * FROM #machineinfo     
insert into #server_instance (scope, source, property, internal_value, user_value, description) SELECT 'SERVER', 'SERVERPROPERTY(''ComputerNamePhysicalNetBIOS'')', 'Server Name', @machinename,@machinename, 'physical server cluster resides on'
insert into #server_instance (scope, source, property, internal_value, user_value, description) SELECT 'SERVER', 'SERVERPROPERTY(''MachineName'')', 'Name', @SERVER, @SERVER, ''
insert into #server_instance (scope, source, property, internal_value, user_value, description) SELECT 'SERVER', 'xp_instance_regread ''HKEY_LOCAL_MACHINE'',''HARDWARE\DESCRIPTION\System\BIOS''              ,''SystemManufacturer''' , VALUE, DATA, DATA, '' FROM #machineinfo WHERE VALUE = 'SystemManufacturer'
insert into #server_instance (scope, source, property, internal_value, user_value, description) SELECT 'SERVER', 'xp_instance_regread ''HKEY_LOCAL_MACHINE'',''HARDWARE\DESCRIPTION\System\BIOS''              ,''SystemProductName'''  , VALUE, DATA, DATA, '' FROM #machineinfo WHERE VALUE = 'SystemProductName'
insert into #server_instance (scope, source, property, internal_value, user_value, description) SELECT 'SERVER', 'xp_instance_regread ''HKEY_LOCAL_MACHINE'',''HARDWARE\DESCRIPTION\System\BIOS''              ,''SystemFamily'''       , VALUE, DATA, DATA, '' FROM #machineinfo WHERE VALUE = 'SystemFamily'
insert into #server_instance (scope, source, property, internal_value, user_value, description) SELECT 'SERVER', 'xp_instance_regread ''HKEY_LOCAL_MACHINE'',''HARDWARE\DESCRIPTION\System\BIOS''              ,''BIOSVendor'''         , VALUE, DATA, DATA, '' FROM #machineinfo WHERE VALUE = 'BIOSVendor'
insert into #server_instance (scope, source, property, internal_value, user_value, description) SELECT 'SERVER', 'xp_instance_regread ''HKEY_LOCAL_MACHINE'',''HARDWARE\DESCRIPTION\System\BIOS''              ,''BIOSVersion'''        , VALUE, DATA, DATA, '' FROM #machineinfo WHERE VALUE = 'BIOSVersion'
insert into #server_instance (scope, source, property, internal_value, user_value, description) SELECT 'SERVER', 'xp_instance_regread ''HKEY_LOCAL_MACHINE'',''HARDWARE\DESCRIPTION\System\BIOS''              ,''BIOSReleaseDate'''    , VALUE, DATA, DATA, '' FROM #machineinfo WHERE VALUE = 'BIOSReleaseDate'
insert into #server_instance (scope, source, property, internal_value, user_value, description) SELECT 'SERVER', 'xp_instance_regread ''HKEY_LOCAL_MACHINE'',''HARDWARE\DESCRIPTION\System\CentralProcessor\0'',''ProcessorNameString''', VALUE, DATA, DATA, '' FROM #machineinfo WHERE VALUE = 'ProcessorNameString'
insert into #server_instance (scope, source, property, internal_value, user_value, description) SELECT 'SERVER', 'xp_instance_regread ''HKEY_LOCAL_MACHINE'',''SOFTWARE\MICROSOFT\WINDOWS NT\CURRENTVERSION''  ,''CurrentVersion'''     , VALUE, DATA, DATA, '' FROM #machineinfo WHERE VALUE = 'CurrentVersion'
insert into #server_instance (scope, source, property, internal_value, user_value, description) SELECT 'SERVER', 'xp_instance_regread ''HKEY_LOCAL_MACHINE'',''SOFTWARE\MICROSOFT\WINDOWS NT\CURRENTVERSION''  ,''EditionID'''          , VALUE, DATA, DATA, '' FROM #machineinfo WHERE VALUE = 'EditionID'
insert into #server_instance (scope, source, property, internal_value, user_value, description) SELECT 'SERVER', 'xp_instance_regread ''HKEY_LOCAL_MACHINE'',''SOFTWARE\MICROSOFT\WINDOWS NT\CURRENTVERSION''  ,''ProductName'''        , VALUE, DATA, DATA, '' FROM #machineinfo WHERE VALUE = 'ProductName'




SELECT @sqlmajorver = CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff);
SELECT @sqlminorver = CONVERT(int, (@@microsoftversion / 0x10000) & 0xff);
SELECT @sqlbuild = CONVERT(int, @@microsoftversion & 0xffff);
DECLARE @CPU_COUNT INT = (SELECT COUNT(cpu_id) FROM sys.dm_os_schedulers WHERE is_online = 1 AND scheduler_id < 255 AND parent_node_id < 64);
DECLARE @numa      INT = (SELECT COUNT(DISTINCT parent_node_id) FROM sys.dm_os_schedulers WHERE scheduler_id < 255 AND parent_node_id < 64);


insert into #server_instance (scope, source, property, internal_value, user_value, description) SELECT 'INSTANCE', 'CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff)', 'SQL Major Version', @sqlmajorver, 	CASE WHEN @sqlmajorver = 9 THEN '2005'
		WHEN @sqlmajorver = 10 AND @sqlminorver = 0 THEN '2008'
		WHEN @sqlmajorver = 10 AND @sqlminorver = 50 THEN '2008R2'
		WHEN @sqlmajorver = 11 THEN '2012'
		WHEN @sqlmajorver = 12 THEN '2014'
      when @sqlmajorver = 13 then '2016'
	END , ''
insert into #server_instance (scope, source, property, internal_value, user_value, description) SELECT 'INSTANCE', 'CONVERT(int, (@@microsoftversion / 0x10000) & 0xff)', 'SQL Minor Version', @sqlminorver,case @sqlminorver when 50 then 'R2' else '' end, ''
insert into #server_instance (scope, source, property, internal_value, user_value, description) SELECT 'INSTANCE', 'CONVERT(int, @@microsoftversion & 0xffff)', 'Build', @sqlbuild,@sqlbuild, ''
insert into #server_instance (scope, source, property, internal_value, user_value, description) SELECT 'INSTANCE', 'SELECT COUNT(cpu_id) FROM sys.dm_os_schedulers WHERE is_online = 1 AND scheduler_id < 255 AND parent_node_id < 64', 'Logical Processor Count', @CPU_COUNT, @CPU_COUNT , ''
insert into #server_instance (scope, source, property, internal_value, user_value, description) SELECT 'INSTANCE', 'SELECT COUNT(DISTINCT parent_node_id) FROM sys.dm_os_schedulers WHERE scheduler_id < 255 AND parent_node_id < 64', 'NUMA Nodes', @numa, @numa , ''


insert into #server_instance  (scope, source, property, internal_value, user_value, description) SELECT 'INSTANCE', 'SELECT SERVERPROPERTY(''ServerName'')'                ,'Server Name'                 , SERVERPROPERTY('ServerName')                  , SERVERPROPERTY('ServerName')                   , 'Both the Windows server and instance information associated with a specified instance of SQL Server.'
insert into #server_instance  (scope, source, property, internal_value, user_value, description) SELECT 'INSTANCE', 'SELECT SERVERPROPERTY(''InstanceName'')'              , 'Instance Name'              , SERVERPROPERTY('InstanceName')                , SERVERPROPERTY('InstanceName')                 , ''
insert into #server_instance  (scope, source, property, internal_value, user_value, description) SELECT 'INSTANCE', 'SELECT SERVERPROPERTY(''Edition'')'                   , 'Edition'                    , SERVERPROPERTY('Edition')                     , SERVERPROPERTY('Edition')                      , 'Installed product edition of the instance of SQL Server. Use the value of this property to determine the features and the limits, such as Compute Capacity Limits by Edition of SQL Server. 64-bit versions of the Database Engine append (64-bit) to the version.'
insert into #server_instance  (scope, source, property, internal_value, user_value, description) SELECT 'INSTANCE', 'SELECT SERVERPROPERTY(''ProductVersion'')'            , 'ProductVersion'             , SERVERPROPERTY('ProductVersion')              , SERVERPROPERTY('ProductVersion')               , 'Version of the instance of SQL Server, in the form of ''major.minor.build.revision''.'
insert into #server_instance  (scope, source, property, internal_value, user_value, description) SELECT 'INSTANCE', 'SELECT SERVERPROPERTY(''ProductLevel'')'              , 'ProductLevel'               , SERVERPROPERTY('ProductLevel')                , SERVERPROPERTY('ProductLevel')                 , 'Level of the version of the instance of SQL Server.'
insert into #server_instance  (scope, source, property, internal_value, user_value, description) SELECT 'INSTANCE', 'SELECT SERVERPROPERTY(''ProcessID'')'                 , 'ProcessID'                  , SERVERPROPERTY('ProcessID')                   , SERVERPROPERTY('ProcessID')                    , 'Process ID of the SQL Server service. ProcessID is useful in identifying which Sqlservr.exe belongs to this instance.'
insert into #server_instance  (scope, source, property, internal_value, user_value, description) SELECT 'INSTANCE', 'SELECT SERVERPROPERTY(''IsClustered'')'               , 'Is Clustered'               , SERVERPROPERTY('IsClustered')                 , case SERVERPROPERTY('IsClustered')  when 0 then 'Not Clustered' when 1 then 'Clustered' end                 , ''
insert into #server_instance  (scope, source, property, internal_value, user_value, description) SELECT 'INSTANCE', 'SELECT SERVERPROPERTY(''ResourceLastUpdateDateTime'')', 'ResourceLastUpdateDateTime' , SERVERPROPERTY('ResourceLastUpdateDateTime')  , SERVERPROPERTY('ResourceLastUpdateDateTime')   , ''
insert into #server_instance  (scope, source, property, internal_value, user_value, description) SELECT 'INSTANCE', 'SELECT SERVERPROPERTY(''ResourceVersion'')'           , 'ResourceVersion'            , SERVERPROPERTY('ResourceVersion')             , SERVERPROPERTY('ResourceVersion')              , ''
insert into #server_instance  (scope, source, property, internal_value, user_value, description) SELECT 'INSTANCE', 'SELECT SERVERPROPERTY(''BuildClrVersion'')'           , 'BuildClrVersion'            , SERVERPROPERTY('BuildClrVersion')             , SERVERPROPERTY('BuildClrVersion')              , 'Version of the Microsoft .NET Framework common language runtime (CLR) that was used while building the instance of SQL Server.'
insert into #server_instance  (scope, source, property, internal_value, user_value, description) SELECT 'INSTANCE', 'SELECT SERVERPROPERTY(''Collation'')'                 , 'Collation'                  , SERVERPROPERTY('Collation')                   , SERVERPROPERTY('Collation')                    , 'Name of the default collation for the server.'
insert into #server_instance  (scope, source, property, internal_value, user_value, description) SELECT 'INSTANCE', 'SELECT SERVERPROPERTY(''LCID'')'                      , 'LCID'                       , SERVERPROPERTY('LCID')                        , SERVERPROPERTY('LCID')                         , 'Windows locale identifier (LCID) of the collation.'
insert into #server_instance  (scope, source, property, internal_value, user_value, description) SELECT 'INSTANCE', 'SELECT SERVERPROPERTY(''ComparisonStyle'')'           , 'Comparison Style'           , SERVERPROPERTY('ComparisonStyle')             , SERVERPROPERTY('ComparisonStyle')              , 'Windows comparison style of the collation. '
insert into #server_instance  (scope, source, property, internal_value, user_value, description) SELECT 'INSTANCE', 'SELECT SERVERPROPERTY(''SqlCharSetName '')'           , 'Sql Character Set'          , SERVERPROPERTY('SqlCharSet')                  , SERVERPROPERTY('SqlCharSetName ')              , 'The SQL character set from the collation.'
insert into #server_instance  (scope, source, property, internal_value, user_value, description) SELECT 'INSTANCE', 'SELECT SERVERPROPERTY(''SqlSortOrderName'')'          , 'Sql Sort Order'             , SERVERPROPERTY('SqlSortOrder')                , SERVERPROPERTY('SqlSortOrderName')             , 'The SQL sort order from the collation.'
insert into #server_instance  (scope, source, property, internal_value, user_value, description) SELECT 'INSTANCE', 'SELECT SERVERPROPERTY(''HadrManagerStatus'')'         , 'Availability Group Status'  , SERVERPROPERTY('HadrManagerStatus')           , CASE SERVERPROPERTY('HadrManagerStatus')        WHEN 0 THEN 'Not started, pending communication.'WHEN 1 THEN 'Started and running.'WHEN 2 THEN 'Not started and failed.' END        , 'Indicates whether the AlwaysOn Availability Groups manager has started.'
insert into #server_instance  (scope, source, property, internal_value, user_value, description) SELECT 'INSTANCE', 'SELECT SERVERPROPERTY(''IsFullTextInstalled'')'       , 'Full Text Enabled'          , SERVERPROPERTY('IsFullTextInstalled')         , case SERVERPROPERTY('IsFullTextInstalled')      when 0 then 'No' when 1 then 'Yes' end         , 'The full-text and semantic indexing components are installed on the current instance of SQL Server.'
insert into #server_instance  (scope, source, property, internal_value, user_value, description) SELECT 'INSTANCE', 'SELECT SERVERPROPERTY(''IsHadrEnabled'')'             , 'Availability Group Enabled' , SERVERPROPERTY('IsHadrEnabled')               , case SERVERPROPERTY('IsHadrEnabled')            when 0 then 'No' when 1 then 'Yes' end          , 'AlwaysOn Availability Groups is enabled on this server instance.'
insert into #server_instance  (scope, source, property, internal_value, user_value, description) SELECT 'INSTANCE', 'SELECT SERVERPROPERTY(''IsIntegratedSecurityOnly'') ' , 'Integrated Security Only'   , SERVERPROPERTY('IsIntegratedSecurityOnly')    , case SERVERPROPERTY('IsIntegratedSecurityOnly') when 0 then 'Both Windows Authentication and SQL Server Authentication.' when 1 then 'Integrated security (Windows Authentication)' end       , 'Server is in integrated security mode.'
insert into #server_instance  (scope, source, property, internal_value, user_value, description) SELECT 'INSTANCE', 'SELECT SERVERPROPERTY(''IsSingleUser'')'              , 'Single Use Mode'            , SERVERPROPERTY('IsSingleUser')                , case SERVERPROPERTY('IsSingleUser')             when 0 then 'No' when 1 then 'Yes' end          , 'Server is in single-user mode.'
insert into #server_instance  (scope, source, property, internal_value, user_value, description) SELECT 'INSTANCE', 'SELECT SERVERPROPERTY(''IsXTPSupported'')'            , 'In Memory OLTP Supported'   , SERVERPROPERTY('IsXTPSupported')              , case SERVERPROPERTY('IsXTPSupported')           when 0 then 'No' when 1 then 'Yes' end          , 'Server supports In-Memory OLTP.'
insert into #server_instance  (scope, source, property, internal_value, user_value, description) SELECT 'INSTANCE', 'SELECT SERVERPROPERTY(''FilestreamShareName'')'       , 'Filestream Share Name'      , SERVERPROPERTY('FilestreamShareName')         , SERVERPROPERTY('FilestreamShareName')          , 'The name of the share used by FILESTREAM.'
insert into #server_instance  (scope, source, property, internal_value, user_value, description) SELECT 'INSTANCE', 'SELECT SERVERPROPERTY(''FilestreamConfiguredLevel'')' , 'Filestream Configured Level', SERVERPROPERTY('FilestreamConfiguredLevel')   , SERVERPROPERTY('FilestreamConfiguredLevel')    , 'The configured level of FILESTREAM access. '
insert into #server_instance  (scope, source, property, internal_value, user_value, description) SELECT 'INSTANCE', 'SELECT SERVERPROPERTY(''FilestreamEffectiveLevel'')'  , 'Filestream Effective Level' , SERVERPROPERTY('FilestreamEffectiveLevel')    , SERVERPROPERTY('FilestreamEffectiveLevel')     , 'The effective level of FILESTREAM access. This value can be different than the FilestreamConfiguredLevel if the level has changed and either an instance restart or a computer restart is pending. For more information, see filestream access level.'
                            
insert into #server_instance  (scope, source, property, internal_value, user_value, description)SELECT 'INSTANCE', 
'SELECT case SERVERPROPERTY(''IsClustered'') 
   when 1 then SERVERPROPERTY(''ComputerNamePhysicalNetBIOS'') 
   else ''Not Clustered'' end' 
, 'Cluster Name'               
, case SERVERPROPERTY('IsClustered') 
      
 when 1 then @machinename 
 else 'Not Clustered' end               
,  case SERVERPROPERTY('IsClustered') when 1 then @machinename else 'Not Clustered' end  
, 'Windows computer name on which the server instance is running.  For a clustered instance, an instance of SQL Server running on a virtual server on Microsoft Cluster Service, it returns the name of the virtual server.'

insert into #server_instance (scope, source, property, internal_value, user_value, description) SELECT 'INSTANCE', 'SELECT SERVERPROPERTY(''EngineEdition'')'             , 'Engine Edition'              , SERVERPROPERTY('EngineEdition')      , CASE SERVERPROPERTY('EngineEdition')  
                                                                                                                                                                        WHEN 1 THEN 'Personal or Desktop Engine (Not available in SQL Server 2005 and later versions.'
                                                                                                                                                                        WHEN 2 THEN 'Standard (This is returned for Standard, Web, and Business Intelligence.)'
                                                                                                                                                                        WHEN 3 THEN 'Enterprise (This is returned for Evaluation, Developer, and both Enterprise editions.)'
                                                                                                                                                                        WHEN 4 THEN 'Express (This is returned for Express, Express with Tools and Express with Advanced Services)'
                                                                                                                                                                        WHEN 5 THEN 'SQL Database' END
                                                                                                                                                                                       , 'Database Engine edition of the instance of SQL Server installed on the server.'


insert into #server_instance SELECT 'INSTANCE - Configuration', 'SELECT * FROM SYS.CONFIGURATIONS WHERE NAME = ''' + NAME + '''', NAME, value, value_in_use, DESCRIPTION, minimum, maximum, is_dynamic, is_advanced 
FROM SYS.CONFIGURATIONS

IF OBJECT_ID(N'TEMPDB..#traces') IS NOT NULL DROP TABLE #traces

create table #traces (traceflag int, status int, global int, session int)
declare @sql_text nvarchar(4000) = 'dbcc tracestatus'
insert into #traces exec(@sql_text)


insert into #server_instance select 'INSTANCE - Trace Flags', 'DBCC TRACESTATUS', 'Trace', traceflag, traceflag, '', '', '', '', ''
from #traces

if @show_source_and_definition_details = 1
   begin
      SELECT
            scope
          , property
          , internal_value
          , user_value
          , description
          , minimum
          , maximum
          , is_dynamic
          , is_advanced 
          , source
      FROM #server_instance
      order by scope, property
   end
if @show_source_and_definition_details = 0
   begin
      SELECT
            scope
          , property
          , internal_value
          , user_value
      FROM #server_instance
      order by scope, property
   end

--------------------------------------------------------------------------------------
--/* HADR */
--------------------------------------------------------------------------------------

--IF OBJECT_ID(N'TEMPDB..#HADR') IS NOT NULL DROP TABLE #HADR
--IF OBJECT_ID(N'TEMPDB..#HADR_SUMMARY') IS NOT NULL DROP TABLE #HADR_SUMMARY

--select name
--   , case when serverproperty('isclustered') = 1    then 1 else 0 end as [Clustered]
--   , case when (select user_value 
--                from #server_instance
--                where property = 'SystemManufacturer') in ('VMware, Inc.', 'Hyper-V') then 1 else 0 end as [Virtualized]
--   , case when d.source_database_id = d.database_id then 1 else 0 end as [Snapshot- Source]
--   , case when d.source_database_id <> d.database_id            
--          and d.source_database_id is not null      then 1 else 0 end as [Snapshot]
--   , case when d.is_published = 1                   then 1 else 0 end as [Replication Publisher- Transactional or Snapshot]
--   , case when d.is_merge_published = 1             then 1 else 0 end as [Replication Publisher- Merge] 
--   , case when d.is_distributor = 1                 then 1 else 0 end as [Replication Distributor]
--   , case when ag.primary_replica is not null       then 1 else 0 end as [Availability Group- Primary]
--   , case when ag.primary_replica is null                       
--            and d.replica_id is not null            then 1 else 0 end as [Availability Group- Secondary]
--   , case when m.mirroring_role = 1                 then 1 else 0 end as [Mirroring- Principal]
--   , case when m.mirroring_role = 2                 then 1 else 0 end as [Mirroring- Mirror]
--   , case when lsp.primary_database is not null     then 1 else 0 end as [Log Shipping- Primary]
--   , case when lss.secondary_database is not null   then 1 else 0 end as [Log Shipping- Secondary]
--into #HADR
--from sys.databases                                    d
--left join sys.dm_hadr_availability_group_states      ag on ag.primary_replica = d.replica_id
--left JOIN sys.database_mirroring                      m ON d.database_id=m.database_id
--left join msdb.dbo.log_shipping_primary_databases   lsp on lsp.primary_database = d.name
--left join msdb.dbo.log_shipping_secondary_databases lss on lss.secondary_database = d.name

--select 'Information - HADR Summary'                             as Output_type
--, max([Clustered]                                             ) as [Clustered]   
--, max([Virtualized]                                           ) as [Virtualized]                                     
--, sum([Snapshot- Source]                                      ) as [Snapshot- Source]                                
--, sum([Snapshot]                                              ) as [Snapshot]                                         
--, sum([Replication Publisher- Transactional or Snapshot]      ) as [Replication Publisher- Transactional or Snapshot] 
--, sum([Replication Publisher- Merge]                          ) as [Replication Publisher- Merge]                     
--, sum([Replication Distributor]                               ) as [Replication Distributor]                          
--, sum([Availability Group- Primary]                           ) as [Availability Group- Primary]                      
--, sum([Availability Group- Secondary]                         ) as [Availability Group- Secondary]                    
--, sum([Mirroring- Principal]                                  ) as [Mirroring- Principal]                             
--, sum([Mirroring- Mirror]                                     ) as [Mirroring- Mirror]                                
--, sum([Log Shipping- Primary]                                 ) as [Log Shipping- Primary]                            
--, sum([Log Shipping- Secondary]                               ) as [Log Shipping- Secondary]  
--into #HADR_SUMMARY                                            
--from #HADR  

--select 'OS' as HADR_Scope, 'Clustered' as HADR_Type, '                 ' as Role, [Clustered]  as DB_Count
--from #HADR_SUMMARY
--union all
--select 'OS' as HADR_Scope, 'Virtualized' as HADR_Type, '                  ' as Role, [Virtualized]  as DB_Count
--from #HADR_SUMMARY
--union all
--select 'DB' as HADR_Scope, 'Availability Group' as HADR_Type, 'Primary' as Role, [Availability Group- Primary] as DB_Count
--from #HADR_SUMMARY
--union all
--select  'DB' as HADR_Scope, 'Availability Group' as HADR_Type, 'Secondary' as Role, [Availability Group- Secondary] as DB_Count
--from #HADR_SUMMARY
--union all
--select  'DB' as HADR_Scope, 'Mirroring' as HADR_Type, 'Principal' as Role, [Mirroring- Principal] as DB_Count
--from #HADR_SUMMARY
--union all
--select  'DB' as HADR_Scope, 'Mirroring' as HADR_Type, 'Principal' as Role, [Mirroring- Principal] as DB_Count
--from #HADR_SUMMARY
--union all
--select  'DB' as HADR_Scope, 'Mirroring' as HADR_Type, 'Mirror' as Role, [Mirroring- Mirror] as DB_Count
--from #HADR_SUMMARY
--union all
--select  'DB' as HADR_Scope, 'Log Shipping' as HADR_Type, 'Primary' as Role, [Log Shipping- Primary] as DB_Count
--from #HADR_SUMMARY
--union all
--select  'DB' as HADR_Scope, 'Log Shipping' as HADR_Type, 'Secondary' as Role, [Log Shipping- Secondary] as DB_Count
--from #HADR_SUMMARY
--union all
--select  'DB' as HADR_Scope, 'Replication' as HADR_Type, 'Publisher - Transactional or Snapshot' as Role, [Replication Publisher- Transactional or Snapshot] as DB_Count
--from #HADR_SUMMARY
--union all
--select  'DB' as HADR_Scope, 'Replication' as HADR_Type, 'Publisher - Merge' as Role, [Replication Publisher- Merge] as DB_Count
--from #HADR_SUMMARY
--union all
--select  'DB' as HADR_Scope, 'Replication' as HADR_Type, 'Publisher - Transactional or Snapshot' as Role, [Replication Publisher- Transactional or Snapshot] as DB_Count
--from #HADR_SUMMARY
--union all
--select  'DB' as HADR_Scope, 'Replication' as HADR_Type, 'Distributor' as Role, [Replication Distributor] as DB_Count
--from #HADR_SUMMARY


--SELECT Output_type
--       , case [Clustered]+[Snapshot- Source]+[Snapshot]+[Replication Publisher- Transactional or Snapshot]+[Replication Publisher- Merge]+[Replication Distributor]
--             +[Availability Group- Primary]+[Availability Group- Secondary]+[Mirroring- Principal]+[Mirroring- Mirror]+[Log Shipping- Primary]+[Log Shipping- Secondary]
--         when 0 then 'No SQL HADR features'
--         else 'HADR features enabled' end as [HADR Summary]
--       , [Clustered]                                        
--       , [Snapshot- Source]                                
--       , [Snapshot]                                         
--       , [Replication Publisher- Transactional or Snapshot] 
--       , [Replication Publisher- Merge]                     
--       , [Replication Distributor]                          
--       , [Availability Group- Primary]                      
--       , [Availability Group- Secondary]                    
--       , [Mirroring- Principal]                             
--       , [Mirroring- Mirror]                                
--       , [Log Shipping- Primary]                            
--       , [Log Shipping- Secondary]  
--from #hadr_summary




--if (select  [Snapshot- Source]+[Snapshot]+[Replication Publisher- Transactional or Snapshot]+[Replication Publisher- Merge]+[Replication Distributor]
--            +[Availability Group- Primary]+[Availability Group- Secondary]+[Mirroring- Principal]+[Mirroring- Mirror]+[Log Shipping- Primary]+[Log Shipping- Secondary] from #HADR_SUMMARY ) > 0
--   begin
--      select 'Information - HADR by Database' as Output_type
--            , *
--      from #hadr
--      where  [Snapshot- Source] = 1
--          or [Snapshot] = 1
--          or [Replication Publisher- Transactional or Snapshot] = 1
--          or [Replication Publisher- Merge] = 1
--          or [Replication Distributor] = 1
--          or [Availability Group- Primary] = 1
--          or [Availability Group- Secondary] = 1
--          or [Mirroring- Principal] = 1 
--          or [Mirroring- Mirror] = 1
--          or [Log Shipping- Primary] = 1
--          or [Log Shipping- Secondary] = 1 
--      order by name
--end
--SELECT * FROM SYS.database_mirroring
--SELECT * FROM SYS.database_mirroring_endpoints
--SELECT * FROM SYS.database_mirroring_witnesses
--SELECT * FROM SYS.dm_db_mirroring_auto_page_repair
--SELECT * FROM SYS.dm_db_mirroring_connections
--SELECT * FROM SYS.dm_db_mirroring_past_actions
--select * from sys.dm_db_script_level
--select * from sys.dm_os_memory_node_access_stats










 
 

