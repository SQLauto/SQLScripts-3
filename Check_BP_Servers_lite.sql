USE [master]
GO

DECLARE @custompath NVARCHAR(500), @allow_xpcmdshell bit, @ptochecks bit, @duration tinyint, @logdetail bit, @diskfrag bit, @ixfrag bit, @ixfragscanmode VARCHAR(8), @bpool_consumer bit, @gen_scripts bit, @dbScope VARCHAR(256)

/* Best Practices Check - pedro.lopes@microsoft.com (http://toolbox/BPCheck; http://aka.ms/ezequiel)

READ ME - Important options for executing BPCheck

Set @duration to the number of seconds between data collection points regarding perf counters, waits and latches. 
	Duration must be between 10s and 255s (4m 15s), with a default of 90s.
Set @ptochecks to OFF if you want to skip more performance tuning and optimization oriented checks.
Uncomment @custompath below and set the custom desired path for .ps1 files. 
	If not, default location for .ps1 files is the Log folder.
Set @allow_xpcmdshell to OFF if you want to skip checks that are dependant on xp_cmdshell. 
	Note that original server setting for xp_cmdshell would be left unchanged if tests were allowed.
Set @diskfrag to ON if you want to check for disk physical fragmentation. 
	Can take some time in large disks. Requires elevated privileges.
Set @ixfrag to ON if you want to check for index fragmentation. 
	Can take some time to collect data depending on number of databases and indexes, as well as the scan mode chosen in @ixfragscanmode.
Set @ixfragscanmode to the scanning mode you prefer. 
	More detail on scanning modes available at http://msdn.microsoft.com/en-us/library/ms188917.aspx
Set @logdetail to OFF if you want to get just the summary info on issues in the Errorlog, rather than the full detail.
Set @bpool_consumer to OFF if you want to list what are the Buffer Pool Consumers from Buffer Descriptors. 
	Mind that it may take some time in servers with large caches.
Set @gen_scripts to ON if you want to generate index related scripts.
	These include drops for Duplicate, Redundant, Hypothetical and Rarely Used indexes, as well as creation statements for FK and Missing Indexes.
Set @dbScope to the appropriate list of database IDs if there's a need to have a specific scope for database specific checks.
	Valid input should be numeric value(s) between single quotes, as follows: '1,6,15,123'
	Leave NULL for all databases
*/

SET @duration = 90
SET @ptochecks = 1 --(1 = ON; 0 = OFF)
--SET @custompath = 'C:\<temp_location>'
SET @allow_xpcmdshell = 1 --(1 = ON; 0 = OFF)
SET @diskfrag = 0 --(1 = ON; 0 = OFF)
SET @ixfrag = 0 --(1 = ON; 0 = OFF)
SET @ixfragscanmode = 'LIMITED' --(Valid inputs are DEFAULT, NULL, LIMITED, SAMPLED, or DETAILED. The default (NULL) is LIMITED)
SET @logdetail = 0 --(1 = ON; 0 = OFF)
SET @bpool_consumer = 0 --(1 = ON; 0 = OFF)
SET @gen_scripts = 0 --(1 = ON; 0 = OFF)
SET @dbScope = NULL --(NULL = All DBs)

/*
DESCRIPTION: This script checks for skews in the most common best practices from SQL Server 2005 onwards.

DISCLAIMER:
This code is not supported under any Microsoft standard support program or service.
This code and information are provided "AS IS" without warranty of any kind, either expressed or implied.
The entire risk arising out of the use or performance of the script and documentation remains with you. 
Furthermore, Microsoft or the author shall not be liable for any damages you may sustain by using this information, whether direct, 
indirect, special, incidental or consequential, including, without limitation, damages for loss of business profits, business interruption, loss of business information 
or other pecuniary loss even if it has been advised of the possibility of such damages.
Read all the implementation and usage notes thoroughly.

v1  	- 28-07-2011 - Initial release
v1.1	- 28-01-2012 - Added some SQL 2012 support.
v1.2	- 08-03-2012 - Added information to Database Information subsection;
						Added I/O stall in excess of 50% in database files subsection.
v1.2.1	- 20-03-2012 - Added redundant index subsection;
						Added more SQL 2012 support;
						Changed database loop method.
v1.2.2	- 22-03-2012 - Added check for Direct Catalog Updates.
v1.2.3	- 05-04-2012 - Added unused index subsection;
						Fixed some collation issues.
v1.2.4 - 07-04-2012 - Split in separate listings the unused indexes from rarely used indexes;
						Split in separate listings the real duplicates from possibly redundant indexes.
v1.2.5 - 08-04-2012 - Added power plan check, by Aaron Bertrand (http://www.mssqltips.com/tip.asp?tip=2225&ctc).
v1.2.6 - 13-04-2012 - Added NTFS block size check;
						Fixed issue in Data and Log locations check.
v1.2.7 - 16-04-2012 - Added Errorlog based checks section.
v1.2.8 - 21-04-2012 - Fixed issue with NTFS block size check;
						Added Replication Components installation check;
						Scan for Startup Procs checks for Replication Components installation.
v1.2.9 - 04-05-2012 - Fixed issues when running on SQL Server 2005;
						COM object creation was revised.
v1.3  - 08-05-2012 - Added support for customizing save location for .ps1 files;
						Default location is the Log folder. Optionally, uncomment line where @custompath is set and insert the desired path.
v1.3.2 - 09-05-2012 - Added descriptive column to Database_Options test, to clear what are the Non-optimal_Settings for each database;
						Corrected bug with powershell files default save location, in case default location has changed since SQL Server install.
v1.3.3 - 31-05-2012 - Fixed issue with DBCC CHECKDB test and offline databases.		
v1.3.4 - 03-06-2012 - Added more system configuration checks.
v1.3.5 - 07-06-2012 - Added suspect pages check;
						Added more permissions checks in pre-requisites section;
						Added search for Errors in Errorlog checks.
v1.3.6 - 08-06-2012 - Revised support for account checks in SQL 2012.
v1.3.7 - 14-06-2012 - Fixed issue with large Object_Names;
						Fixed issue with Powershell 3.0 script;
						Data and log in same volume only lists affected objects now;
						Fixed blocking issue with hypothetical objects drop statements;
						Added duplicate indexes drop statements (all but the 1st of duplicates);
						Added tempDB in same location as user DBs check;
						Added backup checks.
v1.3.8 - 19-06-2012 - Fixed issue with Errorlog based checks;
						Fixed issue with Data and Log locations.
v1.3.9 - 05-07-2012 - Fixed issue with CPUs vs. MaxDOP check.
v1.4.0 - 10-07-2012 - Added LPIM check;
						Added info to trace flags subsection.
v1.4.1 - 18-07-2012 - Added information to CPUs vs. MaxDOP check.
						Added information about all backups since last Full (Database, File or Partial).
v1.4.2 - 02-08-2012 - Added Processor Affinity information to instance info.
v1.4.3 - 19-09-2012 - Fixed issue with large integer in DBs Autogrowth > 1GB in Logs or Data (when IFI is disabled) subsection.
v1.4.4 - 23-09-2012 - Added events to monitor to SQL Agent alerts for severe errors subsection;
						Added comment column for specific errors foun in Errorlog based checks subsection.
v1.4.5 - 24-09-2012 - Fixed issue with Errorlog based checks subsection search.
v1.4.6 - 17-10-2012 - Fixed issue with large percentages in Stall IO check.
v1.4.7 - 24-10-2012 - Fixed issue with Data files and Logs in same physical volume check, where data and fulltext on the same volume raised an issue;
						Fixed issue with Service Accounts and Status check where LocalSystem would not raise issues;
						Widened search for Redundant Indexes.
v1.4.8 - 01-11-2012 - Added generation of drop statements for existing Rarely Used indexes.
v1.5.0 - 11-11-2012 - Fixed cpu affinity shows all zeros when auto affinity was used;
						Fixed issue with duplicate indexes check introduced with v1.4.8;
						Fixed issue with Errorlog based checks subsection search where no comments where shown;
						Added aggregates to CPU information subsection;
						Simplified logic in CPU Affinity in NUMA architecture subsection;
						Added checks to Server Memory information subsection;
						Added checks to System configurations subsection;
						Added a Deprecated features subsection;
						Changed perf counters collection method in Perf counters subsection;
						Added more counters to Perf counters subsection.
v1.5.1 - 11-11-2012 - Fixed perf counters collection collecting from default instances only.
v1.5.2 - 15-11-2012 - Fixed issue with long mountpoint paths;
						Fixed issue with Backup checks subsection;
						Added output from sys.messages to Errorlog based checks.
v1.5.4 - 18-11-2012 - Added sql server process trimming to Errorlog based checks;
						Added more checks to System configurations subsection;
						Added HA information subsection;
						Added optimal nr of VLFs and log size to VLF subsection, based on current findings;
						Added even file number check for tempDB;
						Added minimum file number check for tempDB when < 4;
						Added summary to Errorlog based checks;
						Added column about IFI status in autogrow sections;
						Added further info to VLF section.
v1.5.5 - 19-11-2012 - Added info to Instance Information subsection;
						Added Processor Summary subsection aggregating all CPU information;
						Added RM task output notifications; 
						Format binary affinity mask by node for better reading.
v1.5.6 - 24-11-2012 - Changed collection for several tests that can be supported by DMVs on SQL Server 2008 R2 SP1 and above (was implemented for SQL Server 2012 only);
						Added checks and information to Server Memory subsection.
v1.5.7 - 10-12-2012 - Changed collection for Index checks to cope with large database names;
						Added support for collection through SQLDiag custom collector.
v1.5.9 - 30-12-2012 - Added schema to index script generation;
						Fixed conversion issue with Max Worker Threads option;
						Added extended pagefile checks;
						Added Replication error checks;
						Added check for Indexes with large keys (> 900 bytes).
v1.6.1 - 21-01-2013 - Fixed issue with SQL Server 2005 in Server Memory subsection;
						Fixed conversion issues with large msticks values;
						Fixed issue with Memory reference values.
v1.6.2 - 02-02-2013 - Fixed issues with case sensitive collations;
						Expanded search for redundant indexes;
						Expanded system configuration checks;
						Added Plan use ratio checks;
						Added Hints usage check;
						Added Linked servers information;
						Added AlwaysOn/Mirroring automatic page repairs check;
						Added check for user DBs with Auto_Update_Statistics_Asynch enabled and Auto_Update_Statistics disabled;
						Extended I/O Stall checks to verify I/O latencies.
v1.6.3 - 13-02-2013 - Added option to skip PS based tests where activating xp_cmdshell is strictly forbidden (See Readme at the top).
v1.6.4 - 18-02-2013 - Added Buffer Node to performance counters collection;
						Added information to RM Notifications section;
						Added Cached Query Plans issues checks (Top 25 by CPU, IO and Recompiles with plan warnings);
						Added Object Naming Convention checks
v1.6.5 - 27-02-2013 - Added check for databases that need to run DBCC CHECKDB (...) WITH DATA_PURITY;
						Added Waits and Latches information;
						Added check for statistics that need to be updated;
						Optimized performance counters collection;
						Extended performance counters collection to 90s;
						Removed alternate keys from search for Unused and Rarely used indexes;
						Added parameter to enable/disable performance tuning and optimization oriented checks;
						Added parameter to enable/disable best practices oriented checks;
v1.6.6 - 05-03-2013 - Removed MS Shipped objects from duplicate and redundant indexes check;
						Added search for tables with no indexes, tables with no clustered indexes and tables with more indexes than columns.
v1.6.7 - 01-04-2013 - Fixed issue with AlwaysOn/Mirroring automatic page repair subsection in SQL 2008R2 or below;
						Fixed issue with Cached Query Plans issues subsection in SQL 2008R2 RTM;
						Added more database files information;
						Fixed false positives with DBs Autogrowth > 1GB check;
						Added check for TF834 (Large Page Support for BP) when Column Store Indexes are used;
						Fixed false positives with Power plan subsection.
v1.6.8.1 - 03-04-2013 - Added min server memory setting checks;
						Added information to Errorlog checks;
						Fixed false negatives with "Data files and Logs / tempDB and user Databases in same volume" checks when PS is not available;
						Added check for tempDB Files with different autogrow setting.
v1.6.9 - 11-04-2013 - Added Worker threads exhaustion check;
						Added support for sys.dm_db_stats_properties in Statistics update subsection, if on SQL 2008R2 SP2 or SQL 2012 SP1 or higher.
						Made changes to latch checks;
						Fixed false positives with Indexes with large keys (> 900 bytes) subsection.
v1.7.0.2 - 07-05-2013 - Fixed Statistics update subsection for SQL 2005;
						Changed Errorlog aggregates;
						Added Historical Latches without BUFFER class;
						Added page file size check for WS2003;
						Added information to "Tables with more Indexes than Columns" check.
v1.7.1 - 10-05-2013	- Added "Tables with partition misaligned indexes" check.
v1.7.2 - 18-05-2013	- Added windows service pack level to machine information section;
						Changed information available in "Database_File_Information", namely pages calculated in MB;
						Added Enterprise features usage information per database;
						Added batch performance counters (SQL Server 2012);
						Rewrote all database loops.
v1.7.3 - 24-05-2013 - "Query Plan Warnings" check now ignores workload in master and mssqlsystemresource databases;
						Fixed issue with running BPchecks without PTOChecks.
v1.7.4 - 01-06-2013 - Added FK with no indexes check;
						Fixed issue with Statistics update check in prior to 2008R2 SP2 and 2012 SP1.
v1.7.5 - 25-06-2013 - Added information to Enterprise features usage subsection;
						Split Unused indexes section into Unused_Indexes_With_Updates and Unused_Indexes_No_Updates;
						Extended Objects naming conventions checks;
						Added Disabled indexes subsection;
						Added indexes with low fill factor subsection;
						Added Non-unique clustered indexes subsection;
						Fixed issue with Cached Query Plans issues subsection.
v1.7.6 - 20-07-2013 - Added check for AlwaysOn AG replication status per database (http://support.microsoft.com/kb/2857849);
						Fixed issue with large values in I/O Stall subsection;
						Added search parameters to Objects naming conventions subsection.
v1.7.6.1-24-07-2013 - Fixed issue with Objects naming conventions checks.
v1.7.6.2-30-07-2013 - Fixed issue with Objects naming conventions checks.
v1.7.7 - 05-09-2013 - Added Missing Indexes output (most relevant - score based - use at you own discretion);
						Changed Processor Usage info from 1h to 2h.
v1.7.8 - 21-09-2013 - Changed system database exclusion choices;
						Added information to User DBs with non-default options subsection;
						VLF section shows added information only on databases failing threshold.
v1.7.9 - 09-10-2013 - Fixed issue with false positives in Purity Check;
						Added logic to exclude standard names of MS shipped databases from database design related checks;
						Added performance counters to collection.
v1.8 - 25-10-2013 - Added blocking chains (over 5s);
						Added monitoring duration as parameter;
						Added more file related info to Autogrowth checks;
						Fixed CPU Affinity bit mask issue on 16 CPU+ servers;
						Extended password checks;
						Extended logic to exclude MS shipped databases, based on notable schema objects.
v1.8.0.1 - 28-10-2013 - Fixed issues with logic to exclude MS shipped databases.
v1.8.1 - 03-11-2013 - Added owner info to Database Information subsection;
						Fixed issue where no warning was raised for DBs that never had a Log Backup, but had Full/Diff Backups in Full or Bulk-logged RM;
						Added Statistics sampling < 25 pct check on SQL 2008R2 SP2 / SQL 2012 and above;
						Added Pending I/O Requests check;
						Added info to Windows Version and Architecture subsection;
						Added spinlocks info;
						Added Clustered Indexes with GUIDs in key checks;
						Extended trace flag checks.
v1.8.2.1 - 11-11-2013 - Removed cursor from VLF checks;
						Added checks to System Configurations;
						Added checks to TempDB subsection;
						Fixed index statement generation in Foreign Keys with no Index subsection;
						Fixed issue with Statistics update subsection up to SQL 2008R2 SP2;
						Fixed duplicate key issue in Pending I/O Requests check;
						Fixed duplicate entries in redundant indexes of Missing Indexes checks;
						Fixed syntax issue in Foreign Keys with no Index subsection.
v1.8.2.2 - 14-11-2013 - Added Dynamics GP database exclusions;
						Fixed syntax issue in System Configurations checks.
v1.8.3 - 19-12-2013 - Fixed presentation issue in Pending I/O Requests subsection;
						Fixed conversion issue in Statistics Update subsection.
v1.8.4 - 13-01-2014 - Fixed issue with database exclusions;
						Fixed issue with Service Accounts section in WS2003;
						Removed unwarranted information from DBCC CHECKDB, Direct Catalog Updates and Data Purity subsection.
v1.8.4.1 - 06-02-2014- Added Service Accounts and SPN registration checks;
						Fixed issue with Redundant index checks on servers with CS collations.
v1.8.5	- 21-02-2014 - Added LowMemoryThreshold and OOM information to Server Memory subsection;
						Extended Min Server Memory checks to Server Memory subsection;
						Added DBs with Sparse files checks;
						Added System health error checks;
						Added Resource Governor information;
						Added logdetail parameter to control listing the detail of all relevant Errorlog entries - which can sometimes bloat the output file to several hundred MBs;
						Fixed blocking chains (over 5s) showing on SP_SERVER_DIAGNOSTICS_SLEEP wait;
						Fixed backup size conversion issue;
						Fixed false positives in log backup checks;
						Fixed tempDB file size check and Database file information subsections based in sys.master_files may cause issues not to be reported;
						Fixed arithmetic overflow error in Statistics update check.
v1.8.6 - 14-03-2014 - Duplicate script generation includes logic to account for PK when several IXs are unique;
						Optimized database discovery cycle;
						Fixed execution issue in AG DBs when no connections are allowed to the databases in the secondary replica, and the databases are not available for read access.
v1.8.7 - 01-04-2014 - Fixed issue in Hypothetical objects drop script generation;
						Fixed issue in several subsections when offline DBs exist;
						Fixed issue introduced in v1.8.6 version where some checks would skip user DBs;
						Added information on clock hand notifications to Server Memory subsection;
						Added thresholds to some Performance Counters -> this does NOT replace a longer perf counter data collection and analysis.
v1.8.7.1 - 03-04-2014 - Added Service Pack Supportability check;
						Fixed overflow issue in clock hand notifications;
						Fixed issue with clock hand notifications and SQL 2005.
v1.8.8 - 23-04-2014 - Changed Pagefile free space check;
						Fixed missing index script creation of covering indexes;
						Fixed divide by zero errors in perf counters;
						Fixed issue with statistics checks in DBs with cmpt level 80;
						Fixed tempDB data file size issue - check was ok, but listed information might not be accurate;
						Added category column to all outputs (preparation for future developments).
v1.8.9 - 09-05-2014 - Detailed categorization of memory related waits;
						Detailed MaxMem recommendation output;
						Added recommended MaxDOP value to Parallelism_MaxDOP check output;
						Fixed issue with No_Full_Backups check.
v1.9 - 04-06-2014 - Refined search for duplicate and redundant indexes;
						Added partition misalign test (offset < 64KB);
						Added Buffer Pool Extension info subsection.
v1.9.1 - 09-06-2014 - Fixed issue with duplicate and redundant index script generation.
v1.9.2 - 13-06-2014 - Fixed calculation in page file checks;
						Added process paged out check (besides existing based in errorlog search).
v1.9.3 - 08-07-2014 - Fixed backup checks reporting wrong size;
						Changed backup chain based checks to ignore copy_only backups;
						Updated Service Pack Supportability check;
						Added Cluster Quorum Model check;
						Added Cluster QFE node equality check.
v1.9.4 - 16-07-2014 - Added Backups and Database files in same location check;
						Enhanced Hints Usage check with info from sql modules;
						Fixed Objects naming conventions checks not reporting issues.
v1.9.5 - 17-09-2014 - Changed Hypothetical object search scope to all DBs, including MS shipped;
						Changed missing index check to include all missing by order of score, but still generating script only when score >= 100000;
						Added features to Enterprise features check;
						Expanded TF checks.
v1.9.6.1 - 20-10-2014 - Fixed hypothetical statistics search;
						Fixed Parallelism_MaxDOP check on large NUMA nodes and specific MaxDOP settings;
						Fixed Cluster Quorum Model false positive when PS is not available;
						Changed Powershell availability verification to single step;
						Added user objects in master check;
						Added logon triggers information;
						Added database triggers information;
						Added Database file autogrows last 72h information;
						Added Cluster NIC Binding order check;
						Added Disk Fragmentation Analysis (only if running with elevated privs and enabled with variable @diskfrag);
						Added Index Fragmentation Analysis (only if enabled with variable @ixfrag);
						Fixed trace flag checks recommending trace flags that did not apply to server hardware.
v1.9.6.2 - 23-10-2014 - Fixed false positive on Cluster NIC Binding order check;
v1.9.6.3 - 25-10-2014 - Fixed syntax error on Cluster NIC Binding order check;
v1.9.7 - 12-11-2014 - Changed Cluster Quorum Model check output;
						Fixed conversion issue with Buffer Pool Consumers section;
						Fixed issue on server that has only AG secondary replicas;
						Added model information to Machine Information section;
						Added @bpool_consumer option to skip listing Buffer Pool Consumers;
						Added @gen_scripts option to skip generating scripts (disabled by default);
						Added listing of which duplicate indexes are eligible for deletion;
						Added search for which of the duplicate indexes that are eligible for deletion are hard coded in sql modules.
v1.9.8 - 10-12-2014 - Fixed issue with Cluster NIC Binding order subsection on case sensitive instances;
						Fixed issue with Database file autogrows last 72h subsection on case sensitive instances;
						Fixed issues with PS based checks running in PS v1;
						Fixed illegal characters issue in XML conversion;
						Fixed syntax error in SQL 2014 Memory Consumers from In-Memory OLTP Engine.
v1.9.8.1 - 16-12-2014 - Fixed issues in Enterprise features usage subsection on SQL 2012 and AlwaysOn in use. 
v1.9.9 - 30-01-2015 - Fixed conversion issue in Plan_use_ratio check;
						Added HADR info to Database Information subsection;
						Added NUMA info to Server Memory subsection.
v1.9.9.1 - 06-02-2015 - Fixed NUMA info collection issue up to SQL 2008R2 introduced in v1.9.9.
v1.9.9.2 - 13-02-2015 - Fixed NUMA info collection issue on SQL 2012 and above introduced in v1.9.9;
						Added support to scope only specific databases by ID.
v1.9.9.3 - 15-03-2015 - Fixed some information in "Database Information" section not being collected, introduced in v1.9.9.2.
v1.9.9.5 - 11-04-2015 - Fixed System health error checks conversion error;
						Extended Memory Allocations from Memory Clerks checks;
						Extended logic to exclude MS shipped databases;
						Fixed overflow error in Blocking Chains check if larger than Integer supports;
						Fixed insert error in Pending I/O Requests check.
v2.0.0 - 22-04-2015 - Added Declarative Referential Integrity - Untrusted Constraints checks;
					Added XTP Index Health Analysis;
					Added CCI Index Health Analysis: pseudo-fragmentation for CCI is the ratio of deleted_rows to total_rows;
					Renamed "Index Fragmentation Analysis" to "Index Health Analysis" subsection;
					Added storage analysis for In-Memory OLTP Engine in Database Information subsection;
					Fixed sp_server_diagnostics showing up as blocked session with long blocking time;
					Extended AO cluster information;
					Database filter now always includes sys DBs.
v2.0.1 - 23-04-2015	Fixed issue with SQL 2012 in Index Health analysis.
v2.0.2 - 14-05-2015 Added Default data collections (check for default trace, blackbox trace, SystemHealth xEvent session, sp_server_diagnostics xEvent session);
					Extended Objects naming conventions checks to functions;
					Added info on Inefficient Plans by CPU and Read I/O;
					Improved search for hypothetical objects;
					Added script generation to fix issues from Declarative Referential Integrity - Untrusted Constraints checks.
v2.0.2.1 - 18-05-2015 Fixed Declarative Referential Integrity script generation.
v2.0.3 - 10-09-2015 Added information about current PMO to Global Trace Flags check when 8048 may be missing.
v2.0.3.1 - 11-09-2015 Fixed PMO check up to 2008R2.

PURPOSE: Checks SQL Server in scope for some of most common skewed Best Practices. Valid from SQL Server 2005 onwards.

	- Contains the following information:
	|- Uptime
	|- Windows Version and Architecture
	|- HA Information
	|- Linked servers info
	|- Instance info
	|- Resource Governor info
	|- Logon triggers
	|- Database Information
	|- Database file autogrows last 72h
	|- Database triggers
	|- Enterprise features usage
	|- Backups
	|- System Configuration

	- And performs the following checks (* means only when @ptochecks is ON):
	|- Processor
		|- Number of available Processors for this instance vs. MaxDOP setting
		|- Processor Affinity in NUMA architecture
		|- Additional Processor information
			|- Processor utilization rate in the last 2 hours *
	|- Memory
		|- Server Memory
		|- RM Task *
		|- Clock hands *
		|- Buffer Pool Consumers from Buffer Descriptors *
		|- Memory Allocations from Memory Clerks *
		|- Memory Consumers from In-Memory OLTP Engine *
		|- Memory Allocations from In-Memory OLTP Engine *
		|- OOM
		|- LPIM
	|- Pagefile
		|- Pagefile
	|- I/O
		|- I/O Stall subsection (wait for 5s) *
		|- Pending disk I/O Requests subsection (wait for a max of 5s) *
	|- Server
		|- Power plan
		|- NTFS block size in volumes that hold database files <> 64KB
		|- Disk Fragmentation Analysis (if enabled)
		|- Cluster Quorum Model
		|- Cluster QFE node equality
		|- Cluster NIC Binding order
	|- Service Accounts
		|- Service Accounts Status
		|- Service Accounts and SPN registration
	|- Instance
		|- Recommended build check
		|- Backups
		|- Global trace flags
		|- System configurations
		|- IFI
		|- Full Text Configurations
		|- Deprecated features *
		|- Default data collections (default trace, blackbox trace, SystemHealth xEvent session, sp_server_diagnostics xEvent session)
	|- Database and tempDB
		|- User objects in master
		|- DBs with collation <> master
		|- DBs with skewed compatibility level
		|- User DBs with non-default options
		|- DBs with Sparse files
		|- DBs Autogrow in percentage
		|- DBs Autogrowth > 1GB in Logs or Data (when IFI is disabled)
		|- VLF
		|- Data files and Logs / tempDB and user Databases / Backups and Database files in same volume (Mountpoint aware)
		|- tempDB data file configurations
		|- tempDB Files autogrow of equal size
	|- Performance
		|- Perf counters, Waits and Latches (wait for 90s) *
		|- Worker thread exhaustion *
		|- Blocking Chains *
		|- Plan use ratio *
		|- Hints usage *
		|- Cached Query Plans issues *
		|- Inefficient Query Plans *
		|- Declarative Referential Integrity - Untrusted Constraints *
	|- Indexes and Statistics
		|- Statistics update *
		|- Statistics sampling *
		|- Hypothetical objects *
		|- Row Index Fragmentation Analysis (if enabled) *
		|- CS Index Health Analysis (if enabled) *
		|- XTP Index Health Analysis (if enabled) *
		|- Duplicate or Redundant indexes *
		|- Unused and rarely used indexes *
		|- Indexes with large keys (> 900 bytes) *
		|- Indexes with fill factor < 80 pct *
		|- Disabled indexes *
		|- Non-unique clustered indexes *
		|- Clustered Indexes with GUIDs in key *
		|- Foreign Keys with no Index *
		|- Indexing per Table *
		|- Missing Indexes *
	|- Naming Convention
		|- Objects naming conventions
	|- Security
		|- Password check
	|- Maintenance and Monitoring
		|- SQL Agent alerts for severe errors
		|- DBCC CHECKDB, Direct Catalog Updates and Data Purity
		|- AlwaysOn/Mirroring automatic page repair
		|- Suspect pages
		|- Replication Errors
		|- Errorlog based checks
		|- System health checks

DISCLAIMER:
This code and information are provided "AS IS" without warranty of any kind, either expressed or implied.
Furthermore, the author or Microsoft shall not be liable for any damages you may sustain by using this information, whether direct, indirect, special, incidental or consequential, even if it has been advised of the possibility of such damages.
			
IMPORTANT pre-requisites:
- Only a sysadmin/local host admin will be able to perform all checks.
- If you want to perform all checks under non-sysadmin credentials, then that login must be:
	Member of serveradmin server role or have the ALTER SETTINGS server permission; 
	Member of MSDB SQLAgentOperatorRole role, or have SELECT permission on the sysalerts table in MSDB;
	Granted EXECUTE permissions on the following extended sprocs to run checks: sp_OACreate, sp_OADestroy, sp_OAGetErrorInfo, xp_enumerrorlogs, xp_fileexist and xp_regenumvalues;
	Granted EXECUTE permissions on xp_msver;
	Granted the VIEW SERVER STATE permission;
	Granted the VIEW DATABASE STATE permission;
	Granted EXECUTE permissions on xp_cmdshell or a xp_cmdshell proxy account should exist to run checks that access disk or OS security configurations.
	Member of securityadmin role, or have EXECUTE permissions on sp_readerrorlog. 
 Otherwise some checks will be bypassed and warnings will be shown.
- Powershell must be installed to run checks that access disk configurations, as well as allow execution of remote signed or unsigned scripts.
*/

SET NOCOUNT ON;
SET ANSI_WARNINGS ON;
SET QUOTED_IDENTIFIER ON;
SET DATEFORMAT mdy;

RAISERROR (N'Starting Pre-requisites section', 10, 1) WITH NOWAIT

--------------------------------------------------------------------------------------------------------------------------------
-- Pre-requisites section
--------------------------------------------------------------------------------------------------------------------------------
DECLARE @sqlcmd NVARCHAR(max), @params NVARCHAR(500), @sqlmajorver int

SELECT @sqlmajorver = CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff);


IF (ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) = 0)
BEGIN
	RAISERROR('[WARNING: Only a sysadmin can run ALL the checks]', 16, 1, N'sysadmin')
	--RETURN
END;

IF (ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) = 0)
BEGIN
	DECLARE @pid int, @pname sysname, @msdbpid int, @masterpid int
	DECLARE @permstbl TABLE ([name] sysname);
	DECLARE @permstbl_msdb TABLE ([id] tinyint IDENTITY(1,1), [perm] tinyint)
	
	SET @params = '@msdbpid_in int'

	SELECT @pid = principal_id, @pname=name FROM master.sys.server_principals (NOLOCK) WHERE sid = SUSER_SID()

	SELECT @masterpid = principal_id FROM master.sys.database_principals (NOLOCK) WHERE sid = SUSER_SID()

	SELECT @msdbpid = principal_id FROM msdb.sys.database_principals (NOLOCK) WHERE sid = SUSER_SID()

	-- Perms 1
	IF (ISNULL(IS_SRVROLEMEMBER(N'serveradmin'), 0) <> 1) AND ((SELECT COUNT(l.name)
		FROM master.sys.server_permissions p (NOLOCK) INNER JOIN master.sys.server_principals l (NOLOCK)
		ON p.grantee_principal_id = l.principal_id
			AND p.class = 100 -- Server
			AND p.state IN ('G', 'W') -- Granted or Granted with Grant
			AND l.is_disabled = 0
			AND p.permission_name = 'ALTER SETTINGS'
			AND QUOTENAME(l.name) = QUOTENAME(@pname)) = 0)
	BEGIN
		RAISERROR('[WARNING: If not sysadmin, then you must be a member of serveradmin server role or have the ALTER SETTINGS server permission]', 16, 1, N'serveradmin')
		RETURN
	END
	ELSE IF (ISNULL(IS_SRVROLEMEMBER(N'serveradmin'), 0) <> 1) AND ((SELECT COUNT(l.name)
		FROM master.sys.server_permissions p (NOLOCK) INNER JOIN sys.server_principals l (NOLOCK)
		ON p.grantee_principal_id = l.principal_id
			AND p.class = 100 -- Server
			AND p.state IN ('G', 'W') -- Granted or Granted with Grant
			AND l.is_disabled = 0
			AND p.permission_name = 'VIEW SERVER STATE'
			AND QUOTENAME(l.name) = QUOTENAME(@pname)) = 0)
	BEGIN
		RAISERROR('[WARNING: If not sysadmin, then you must be a member of serveradmin server role or granted the VIEW SERVER STATE permission]', 16, 1, N'serveradmin')
		RETURN
	END

	-- Perms 2
	INSERT INTO @permstbl
	SELECT a.name
	FROM master.sys.all_objects a (NOLOCK) INNER JOIN master.sys.database_permissions b (NOLOCK) ON a.[OBJECT_ID] = b.major_id
	WHERE a.type IN ('P', 'X') AND b.grantee_principal_id <>0 
	AND b.grantee_principal_id <>2
	AND b.grantee_principal_id = @masterpid;

	INSERT INTO @permstbl_msdb ([perm])
	EXECUTE sp_executesql N'USE msdb; SELECT COUNT([name]) 
FROM msdb.sys.sysusers (NOLOCK) WHERE [uid] IN (SELECT [groupuid] 
	FROM msdb.sys.sysmembers (NOLOCK) WHERE [memberuid] = @msdbpid_in) 
AND [name] = ''SQLAgentOperatorRole''', @params, @msdbpid_in = @msdbpid;

	INSERT INTO @permstbl_msdb ([perm])
	EXECUTE sp_executesql N'USE msdb; SELECT COUNT(dp.grantee_principal_id)
FROM msdb.sys.tables AS tbl (NOLOCK)
INNER JOIN msdb.sys.database_permissions AS dp (NOLOCK) ON dp.major_id=tbl.object_id AND dp.class=1
INNER JOIN msdb.sys.database_principals AS grantor_principal (NOLOCK) ON grantor_principal.principal_id = dp.grantor_principal_id
INNER JOIN msdb.sys.database_principals AS grantee_principal (NOLOCK) ON grantee_principal.principal_id = dp.grantee_principal_id
WHERE dp.state = ''G''
	AND dp.grantee_principal_id = @msdbpid_in
	AND dp.type = ''SL''', @params, @msdbpid_in = @msdbpid;

	IF (SELECT [perm] FROM @permstbl_msdb WHERE [id] = 1) = 0 AND (SELECT [perm] FROM @permstbl_msdb WHERE [id] = 2) = 0
	BEGIN
		RAISERROR('[WARNING: If not sysadmin, then you must be a member of MSDB SQLAgentOperatorRole role, or have SELECT permission on the sysalerts table in MSDB to run full scope of checks]', 16, 1, N'msdbperms')
		--RETURN
	END
	ELSE IF (ISNULL(IS_SRVROLEMEMBER(N'securityadmin'), 0) <> 1) AND ((SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_enumerrorlogs') = 0 OR (SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'sp_readerrorlog') = 0 OR (SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_readerrorlog') = 0)
	BEGIN
		RAISERROR('[WARNING: If not sysadmin, then you must be a member of the securityadmin server role, or have EXECUTE permission on the following extended sprocs to run full scope of checks: xp_enumerrorlogs, xp_readerrorlog, sp_readerrorlog]', 16, 1, N'secperms')
		--RETURN
	END
	ELSE IF (SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_cmdshell') = 0 OR (SELECT COUNT(credential_id) FROM master.sys.credentials WHERE name = '##xp_cmdshell_proxy_account##') = 0
	BEGIN
		RAISERROR('[WARNING: If not sysadmin, then you must be granted EXECUTE permissions on xp_cmdshell and a xp_cmdshell proxy account should exist to run full scope of checks]', 16, 1, N'xp_cmdshellproxy')
		--RETURN
	END
	ELSE IF (SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_fileexist') = 0 OR
		(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'sp_OAGetErrorInfo') = 0 OR
		(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'sp_OACreate') = 0 OR
		(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'sp_OADestroy') = 0 OR
		(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_regenumvalues') = 0 OR
		(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_regread') = 0 OR 
		(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_instance_regread') = 0 OR
		(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_servicecontrol') = 0 
	BEGIN
		RAISERROR('[WARNING: Must be a granted EXECUTE permissions on the following extended sprocs to run full scope of checks: sp_OACreate, sp_OADestroy, sp_OAGetErrorInfo, xp_fileexist, xp_regread, xp_instance_regread, xp_servicecontrol and xp_regenumvalues]', 16, 1, N'extended_sprocs')
		--RETURN
	END
	ELSE IF (SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_msver') = 0 AND @sqlmajorver < 11
	BEGIN
		RAISERROR('[WARNING: Must be granted EXECUTE permissions on xp_msver to run full scope of checks]', 16, 1, N'extended_sprocs')
		--RETURN
	END
END;

-- Declare Global Variables
DECLARE @UpTime VARCHAR(12),@StartDate DATETIME
DECLARE @agt smallint, @ole smallint, @sao smallint, @xcmd smallint
DECLARE @ErrorSeverity int, @ErrorState int, @ErrorMessage NVARCHAR(4000)
DECLARE @CMD NVARCHAR(4000)
DECLARE @path NVARCHAR(2048)
DECLARE @sqlminorver int, @sqlbuild int, @clustered bit, @winver VARCHAR(5), @server VARCHAR(128), @instancename NVARCHAR(128), @arch smallint, @winsp VARCHAR(25), @SystemManufacturer VARCHAR(128)
DECLARE @existout int, @FSO int, @FS int, @OLEResult int, @FileID int
DECLARE @FileName VARCHAR(200), @Text1 VARCHAR(2000), @CMD2 VARCHAR(100)
DECLARE @src VARCHAR(255), @desc VARCHAR(255), @psavail VARCHAR(20), @psver tinyint
DECLARE @dbid int, @dbname VARCHAR(1000)

SELECT @instancename = CONVERT(VARCHAR(128),SERVERPROPERTY('InstanceName')) 
SELECT @server = RTRIM(CONVERT(VARCHAR(128), SERVERPROPERTY('MachineName')))
--SELECT @sqlmajorver = CONVERT(int, (@@microsoftversion / 0x1000000) & 0xff);
SELECT @sqlminorver = CONVERT(int, (@@microsoftversion / 0x10000) & 0xff);
SELECT @sqlbuild = CONVERT(int, @@microsoftversion & 0xffff);
SELECT @clustered = CONVERT(bit,ISNULL(SERVERPROPERTY('IsClustered'),0))

-- Test Powershell policy
IF @allow_xpcmdshell = 1
BEGIN
	IF ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) = 1 -- Is sysadmin
		OR ((ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) <> 1 
			AND (SELECT COUNT(credential_id) FROM sys.credentials WHERE name = '##xp_cmdshell_proxy_account##') > 0) -- Is not sysadmin but proxy account exists
			AND (SELECT COUNT(l.name)
			FROM sys.server_permissions p JOIN sys.server_principals l 
			ON p.grantee_principal_id = l.principal_id
				AND p.class = 100 -- Server
				AND p.state IN ('G', 'W') -- Granted or Granted with Grant
				AND l.is_disabled = 0
				AND p.permission_name = 'ALTER SETTINGS'
				AND QUOTENAME(l.name) = QUOTENAME(USER_NAME())) = 0) -- Is not sysadmin but has alter settings permission
		OR ((ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) <> 1 
			AND ((SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_regread') > 0 AND
			(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_cmdshell') > 0)))
	BEGIN
		DECLARE @pstbl_avail TABLE ([KeyExist] int)
		BEGIN TRY
			INSERT INTO @pstbl_avail
			EXEC master.sys.xp_regread N'HKEY_LOCAL_MACHINE', N'SOFTWARE\Microsoft\PowerShell\1' -- check if Powershell is installed
		END TRY
		BEGIN CATCH
			SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
			SELECT @ErrorMessage = 'Could not determine if Powershell is installed - Error raised in TRY block. ' + ERROR_MESSAGE()
			RAISERROR (@ErrorMessage, 16, 1);
		END CATCH

		SELECT @sao = CAST([value] AS smallint) FROM sys.configurations WITH (NOLOCK) WHERE [name] = 'show advanced options'
		SELECT @xcmd = CAST([value] AS smallint) FROM sys.configurations WITH (NOLOCK) WHERE [name] = 'xp_cmdshell'
		SELECT @ole = CAST([value] AS smallint) FROM sys.configurations WITH (NOLOCK) WHERE [name] = 'Ole Automation Procedures'

		RAISERROR ('|-Configuration options set for Powershell enablement verification', 10, 1) WITH NOWAIT
		IF @sao = 0
		BEGIN
			EXEC sp_configure 'show advanced options', 1; RECONFIGURE WITH OVERRIDE;
		END
		IF @xcmd = 0
		BEGIN
			EXEC sp_configure 'xp_cmdshell', 1; RECONFIGURE WITH OVERRIDE;
		END
		IF @ole = 0
		BEGIN
			EXEC sp_configure 'Ole Automation Procedures', 1; RECONFIGURE WITH OVERRIDE;
		END
		
		IF (SELECT [KeyExist] FROM @pstbl_avail) = 1
		BEGIN
			DECLARE @psavail_output TABLE ([PS_OUTPUT] VARCHAR(2048));
			INSERT INTO @psavail_output
			EXEC master.dbo.xp_cmdshell N'%WINDIR%\System32\WindowsPowerShell\v1.0\powershell.exe -Command "Get-ExecutionPolicy"'
		
			SELECT @psavail = [PS_OUTPUT] FROM @psavail_output WHERE [PS_OUTPUT] IS NOT NULL;
		END
		ELSE
		BEGIN
			RAISERROR ('   [WARNING: Powershell is not installed. Install WinRM to proceed with PS based checks]',16,1);
		END
				
		IF (@psavail IS NOT NULL AND @psavail NOT IN ('RemoteSigned','Unrestricted'))
		RAISERROR ('   [WARNING: Execution of Powershell scripts is disabled on this system.
To change the execution policy, type the following command in Powershell console: Set-ExecutionPolicy RemoteSigned
The Set-ExecutionPolicy cmdlet enables you to determine which Windows PowerShell scripts (if any) will be allowed to run on your computer. Windows PowerShell has four different execution policies:
	Restricted - No scripts can be run. Windows PowerShell can be used only in interactive mode.
	AllSigned - Only scripts signed by a trusted publisher can be run.
	RemoteSigned - Downloaded scripts must be signed by a trusted publisher before they can be run.
		|- REQUIRED by BP Check
	Unrestricted - No restrictions; all Windows PowerShell scripts can be run.]',16,1);

		IF (@psavail IS NOT NULL AND @psavail IN ('RemoteSigned','Unrestricted'))
		BEGIN
			RAISERROR ('|- [INFORMATION: Powershell is installed and enabled for script execution]', 10, 1) WITH NOWAIT
			
			DECLARE @psver_output TABLE ([PS_OUTPUT] VARCHAR(1024));
			INSERT INTO @psver_output
			EXEC master.dbo.xp_cmdshell N'%WINDIR%\System32\WindowsPowerShell\v1.0\powershell.exe -Command "Get-Host | Format-Table -Property Version"'
		
			-- Gets PS version, as commands issued to PS v1 do not support -File
			SELECT @psver = ISNULL(LEFT([PS_OUTPUT],1),2) FROM @psver_output WHERE [PS_OUTPUT] IS NOT NULL AND ISNUMERIC(LEFT([PS_OUTPUT],1)) = 1;
			
			SET @ErrorMessage = '|- [INFORMATION: Installed Powershell is version ' + CONVERT(CHAR(1), @psver) + ']'
			RAISERROR (@ErrorMessage, 10, 1) WITH NOWAIT
		END;
		
		IF @xcmd = 0
		BEGIN
			EXEC sp_configure 'xp_cmdshell', 0; RECONFIGURE WITH OVERRIDE;
		END
		IF @ole = 0
		BEGIN
			EXEC sp_configure 'Ole Automation Procedures', 0; RECONFIGURE WITH OVERRIDE;
		END
		IF @sao = 0
		BEGIN
			EXEC sp_configure 'show advanced options', 0; RECONFIGURE WITH OVERRIDE;
		END;
	END
	ELSE
	BEGIN
		RAISERROR('   [WARNING: Missing permissions for Powershell enablement verification]', 16, 1, N'sysadmin')
		--RETURN
	END
END;

--------------------------------------------------------------------------------------------------------------------------------
-- Information section
--------------------------------------------------------------------------------------------------------------------------------

RAISERROR (N'Starting Information section', 10, 1) WITH NOWAIT

--------------------------------------------------------------------------------------------------------------------------------
-- Uptime subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'|-Starting Uptime', 10, 1) WITH NOWAIT
IF @sqlmajorver < 10
BEGIN
	SET @sqlcmd = N'SELECT @UpTimeOUT = DATEDIFF(mi, login_time, GETDATE()), @StartDateOUT = login_time FROM master..sysprocesses (NOLOCK) WHERE spid = 1';
END
ELSE
BEGIN
	SET @sqlcmd = N'SELECT @UpTimeOUT = DATEDIFF(mi,sqlserver_start_time,GETDATE()), @StartDateOUT = sqlserver_start_time FROM sys.dm_os_sys_info (NOLOCK)';
END

SET @params = N'@UpTimeOUT VARCHAR(12) OUTPUT, @StartDateOUT DATETIME OUTPUT';

EXECUTE sp_executesql @sqlcmd, @params, @UpTimeOUT=@UpTime OUTPUT, @StartDateOUT=@StartDate OUTPUT;

SELECT 'Information' AS [Category], 'Uptime' AS [Information], GETDATE() AS [Current_Time], @StartDate AS Last_Startup, CONVERT(VARCHAR(4),@UpTime/60/24) + 'd ' + CONVERT(VARCHAR(4),@UpTime/60%24) + 'hr ' + CONVERT(VARCHAR(4),@UpTime%60) + 'min' AS Uptime

--------------------------------------------------------------------------------------------------------------------------------
-- Windows Version and Architecture subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'|-Starting Windows Version and Architecture', 10, 1) WITH NOWAIT
IF @sqlmajorver >= 11 OR (@sqlmajorver = 10 AND @sqlminorver = 50 AND @sqlbuild >= 2500)
BEGIN
	SET @sqlcmd = N'SELECT @winverOUT = windows_release, @winspOUT = windows_service_pack_level, @archOUT = CASE WHEN @@VERSION LIKE ''%<X64>%'' THEN 64 WHEN @@VERSION LIKE ''%<IA64>%'' THEN 128 ELSE 32 END FROM sys.dm_os_windows_info (NOLOCK)';
	SET @params = N'@winverOUT VARCHAR(5) OUTPUT, @winspOUT VARCHAR(25) OUTPUT, @archOUT smallint OUTPUT';
	EXECUTE sp_executesql @sqlcmd, @params, @winverOUT=@winver OUTPUT, @winspOUT=@winsp OUTPUT, @archOUT=@arch OUTPUT;
END
ELSE
BEGIN
	BEGIN TRY
		DECLARE @str VARCHAR(500), @str2 VARCHAR(500), @str3 VARCHAR(500)
		DECLARE @sysinfo TABLE (id int, 
			[Name] NVARCHAR(256), 
			Internal_Value bigint, 
			Character_Value NVARCHAR(256));
			
		INSERT INTO @sysinfo
		EXEC xp_msver;
		
		SELECT @winver = LEFT(Character_Value, CHARINDEX(' ', Character_Value)-1) -- 5.2 is WS2003; 6.0 is WS2008; 6.1 is WS2008R2; 6.2 is WS2012, 6.3 is WS2012R2
		FROM @sysinfo
		WHERE [Name] LIKE 'WindowsVersion%';
		
		SELECT @arch = CASE WHEN RTRIM(Character_Value) LIKE '%x64%' OR RTRIM(Character_Value) LIKE '%AMD64%' THEN 64
			WHEN RTRIM(Character_Value) LIKE '%x86%' OR RTRIM(Character_Value) LIKE '%32%' THEN 32
			WHEN RTRIM(Character_Value) LIKE '%IA64%' THEN 128 END
		FROM @sysinfo
		WHERE [Name] LIKE 'Platform%';
		
		SET @str = (SELECT @@VERSION)
		SELECT @str2 = RIGHT(@str, LEN(@str)-CHARINDEX('Windows',@str) + 1)
		SELECT @str3 = RIGHT(@str2, LEN(@str2)-CHARINDEX(': ',@str2))
		SELECT @winsp = LTRIM(LEFT(@str3, CHARINDEX(')',@str3) -1))
	END TRY
	BEGIN CATCH
		SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
		SELECT @ErrorMessage = 'Windows Version and Architecture subsection - Error raised in TRY block. ' + ERROR_MESSAGE()
		RAISERROR (@ErrorMessage, 16, 1);
	END CATCH
END;

DECLARE @machineinfo TABLE ([Value] NVARCHAR(256), [Data] NVARCHAR(256))

INSERT INTO @machineinfo
EXEC xp_instance_regread 'HKEY_LOCAL_MACHINE','HARDWARE\DESCRIPTION\System\BIOS','SystemManufacturer';
INSERT INTO @machineinfo
EXEC xp_instance_regread 'HKEY_LOCAL_MACHINE','HARDWARE\DESCRIPTION\System\BIOS','SystemProductName';
INSERT INTO @machineinfo
EXEC xp_instance_regread 'HKEY_LOCAL_MACHINE','HARDWARE\DESCRIPTION\System\BIOS','SystemFamily';
INSERT INTO @machineinfo
EXEC xp_instance_regread 'HKEY_LOCAL_MACHINE','HARDWARE\DESCRIPTION\System\BIOS','BIOSVendor';
INSERT INTO @machineinfo
EXEC xp_instance_regread 'HKEY_LOCAL_MACHINE','HARDWARE\DESCRIPTION\System\BIOS','BIOSVersion';
INSERT INTO @machineinfo
EXEC xp_instance_regread 'HKEY_LOCAL_MACHINE','HARDWARE\DESCRIPTION\System\BIOS','BIOSReleaseDate';
INSERT INTO @machineinfo
EXEC xp_instance_regread 'HKEY_LOCAL_MACHINE','HARDWARE\DESCRIPTION\System\CentralProcessor\0','ProcessorNameString';

SELECT @SystemManufacturer = [Data] FROM @machineinfo WHERE [Value] = 'SystemManufacturer';

SELECT 'Information' AS [Category], 'Machine' AS [Information], 
	CASE @winver WHEN '5.2' THEN 'XP/WS2003'
		WHEN '6.0' THEN 'Vista/WS2008'
		WHEN '6.1' THEN 'W7/WS2008R2'
		WHEN '6.2' THEN 'W8/WS2012'
		WHEN '6.3' THEN 'W8.1/WS2012R2'
	END AS [Windows_Version],
	@winsp AS [Service_Pack_Level],
	@arch AS [Architecture],
	SERVERPROPERTY('MachineName') AS [Machine_Name],
	SERVERPROPERTY('ComputerNamePhysicalNetBIOS') AS [NetBIOS_Name],
	@SystemManufacturer AS [System_Manufacturer],
	(SELECT [Data] FROM @machineinfo WHERE [Value] = 'SystemFamily') AS [System_Family],
	(SELECT [Data] FROM @machineinfo WHERE [Value] = 'SystemProductName') AS [System_ProductName],
	(SELECT [Data] FROM @machineinfo WHERE [Value] = 'BIOSVendor') AS [BIOS_Vendor],
	(SELECT [Data] FROM @machineinfo WHERE [Value] = 'BIOSVersion') AS [BIOS_Version],
	(SELECT [Data] FROM @machineinfo WHERE [Value] = 'BIOSReleaseDate') AS [BIOS_Release_Date],
	(SELECT [Data] FROM @machineinfo WHERE [Value] = 'ProcessorNameString') AS [Processor_Name];
	
--------------------------------------------------------------------------------------------------------------------------------
-- HA Information subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'|-Starting HA Information', 10, 1) WITH NOWAIT
IF @clustered = 1
BEGIN
	IF @sqlmajorver < 11
		BEGIN
			EXEC ('SELECT ''Information'' AS [Category], ''Cluster'' AS [Information], NodeName AS node_name FROM sys.dm_os_cluster_nodes (NOLOCK)')
		END
	ELSE
		BEGIN
			EXEC ('SELECT ''Information'' AS [Category], ''Cluster'' AS [Information], NodeName AS node_name, status_description, is_current_owner FROM sys.dm_os_cluster_nodes (NOLOCK)')
		END
	SELECT 'Information' AS [Category], 'Cluster' AS [Information], DriveName AS cluster_shared_drives FROM sys.dm_io_cluster_shared_drives (NOLOCK)
END
ELSE
BEGIN
	SELECT 'Information' AS [Category], 'Cluster' AS [Information], 'NOT_CLUSTERED' AS [Status]
END;

IF @sqlmajorver > 10
BEGIN
	DECLARE @IsHadrEnabled tinyint, @HadrManagerStatus tinyint
	SELECT @IsHadrEnabled = CONVERT(tinyint, SERVERPROPERTY('IsHadrEnabled'))
	SELECT @HadrManagerStatus = CONVERT(tinyint, SERVERPROPERTY('HadrManagerStatus'))
	
	SELECT 'Information' AS [Category], 'AlwaysOn_AG' AS [Information], 
		CASE @IsHadrEnabled WHEN 0 THEN 'Disabled'
			WHEN 1 THEN 'Enabled' END AS [AlwaysOn_Availability_Groups],
		CASE WHEN @IsHadrEnabled = 1 THEN
			CASE @HadrManagerStatus WHEN 0 THEN '[Not started, pending communication]'
				WHEN 1 THEN '[Started and running]'
				WHEN 2 THEN '[Not started and failed]'
			END
		END AS [Status];
	
	IF @IsHadrEnabled = 1
	BEGIN	
		IF EXISTS (SELECT 1 FROM sys.dm_hadr_cluster) 
		SELECT 'Information' AS [Category], 'AlwaysOn_Cluster' AS [Information], cluster_name, quorum_type_desc, quorum_state_desc 
		FROM sys.dm_hadr_cluster;

		IF EXISTS (SELECT 1 FROM sys.dm_hadr_cluster_members) 
		SELECT 'Information' AS [Category], 'AlwaysOn_Cluster_Members' AS [Information], member_name, member_type_desc, member_state_desc, number_of_quorum_votes 
		FROM sys.dm_hadr_cluster_members;
		
		IF EXISTS (SELECT 1 FROM sys.dm_hadr_cluster_networks) 
		SELECT 'Information' AS [Category], 'AlwaysOn_Cluster_Networks' AS [Information], member_name, network_subnet_ip, network_subnet_ipv4_mask, is_public, is_ipv4 
		FROM sys.dm_hadr_cluster_networks;
	END;
	
	IF @ptochecks = 1 AND @IsHadrEnabled = 1
	BEGIN
		-- Note: If low_water_mark_for_ghosts number is not increasing over time, it implies that ghost cleanup might not happen.
		SELECT 'Information' AS [Category], 'AlwaysOn_Replicas' AS [Information], database_id, group_id, replica_id, group_database_id, is_local, synchronization_state_desc, 
			is_commit_participant, synchronization_health_desc, database_state_desc, is_suspended, suspend_reason_desc, last_sent_time, last_received_time, last_hardened_time, 
			last_redone_time, log_send_queue_size, log_send_rate, redo_queue_size, redo_rate, filestream_send_rate, last_commit_time, low_water_mark_for_ghosts 
		FROM sys.dm_hadr_database_replica_states;

		SELECT 'Information' AS [Category], 'AlwaysOn_Replica_Cluster' AS [Information], replica_id, group_database_id, database_name, is_failover_ready, is_pending_secondary_suspend, 
			is_database_joined, recovery_lsn, truncation_lsn 
		FROM sys.dm_hadr_database_replica_cluster_states;
	END
END;

--------------------------------------------------------------------------------------------------------------------------------
-- Linked servers info subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'|-Starting Linked servers info', 10, 1) WITH NOWAIT
IF (SELECT COUNT(*) FROM sys.servers AS s INNER JOIN sys.linked_logins AS l (NOLOCK) ON s.server_id = l.server_id INNER JOIN sys.server_principals AS p (NOLOCK) ON p.principal_id = l.local_principal_id WHERE s.is_linked = 1) > 0
BEGIN
	IF @sqlmajorver > 9
	BEGIN
		EXEC ('SELECT ''Information'' AS [Category], ''Linked_servers'' AS [Information], s.name, s.product, 
	s.provider, s.data_source, s.location, s.provider_string, s.catalog, s.connect_timeout, 
	s.query_timeout, s.is_linked, s.is_remote_login_enabled, s.is_rpc_out_enabled, 
	s.is_data_access_enabled, s.is_collation_compatible, s.uses_remote_collation, s.collation_name, 
	s.lazy_schema_validation, s.is_system, s.is_publisher, s.is_subscriber, s.is_distributor, 
	s.is_nonsql_subscriber, s.is_remote_proc_transaction_promotion_enabled, 
	s.modify_date, CASE WHEN l.local_principal_id = 0 THEN ''local or wildcard'' ELSE p.name END AS [local_principal], 
	CASE WHEN l.uses_self_credential = 0 THEN ''use own credentials'' ELSE ''use supplied username and pwd'' END AS uses_self_credential, 
	l.remote_name, l.modify_date AS [linked_login_modify_date]
FROM sys.servers AS s (NOLOCK)
INNER JOIN sys.linked_logins AS l (NOLOCK) ON s.server_id = l.server_id
INNER JOIN sys.server_principals AS p (NOLOCK) ON p.principal_id = l.local_principal_id
WHERE s.is_linked = 1')
	END
	ELSE 
	BEGIN
		EXEC ('SELECT ''Information'' AS [Category], ''Linked_servers'' AS [Information], s.name, s.product, 
	s.provider, s.data_source, s.location, s.provider_string, s.catalog, s.connect_timeout, 
	s.query_timeout, s.is_linked, s.is_remote_login_enabled, s.is_rpc_out_enabled, 
	s.is_data_access_enabled, s.is_collation_compatible, s.uses_remote_collation, s.collation_name, 
	s.lazy_schema_validation, s.is_system, s.is_publisher, s.is_subscriber, s.is_distributor, 
	s.is_nonsql_subscriber, s.modify_date, CASE WHEN l.local_principal_id = 0 THEN ''local or wildcard'' ELSE p.name END AS [local_principal], 
	CASE WHEN l.uses_self_credential = 0 THEN ''use own credentials'' ELSE ''use supplied username and pwd'' END AS uses_self_credential, 
	l.remote_name, l.modify_date AS [linked_login_modify_date]
FROM sys.servers AS s (NOLOCK)
INNER JOIN sys.linked_logins AS l (NOLOCK) ON s.server_id = l.server_id
INNER JOIN sys.server_principals AS p (NOLOCK) ON p.principal_id = l.local_principal_id
WHERE s.is_linked = 1')
	END
END
ELSE
BEGIN
	SELECT 'Information' AS [Category], 'Linked_servers' AS [Information], '[None]' AS [Status]
END;

--------------------------------------------------------------------------------------------------------------------------------
-- Instance info subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'|-Starting Instance info', 10, 1) WITH NOWAIT
DECLARE @port VARCHAR(5), @replication int, @RegKey NVARCHAR(255), @cpuaffin VARCHAR(255), @cpucount int, @numa int
DECLARE @i int, @cpuaffin_fixed VARCHAR(300)

IF @sqlmajorver < 11 OR (@sqlmajorver = 10 AND @sqlminorver = 50 AND @sqlbuild >= 2500)
BEGIN
	IF (ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) = 1) OR ((SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_regread') = 1)
	BEGIN
		BEGIN TRY
			SELECT @RegKey = CASE WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('InstanceName')) IS NULL THEN N'Software\Microsoft\MSSQLServer\MSSQLServer\SuperSocketNetLib\Tcp'
				ELSE N'Software\Microsoft\Microsoft SQL Server\' + CAST(SERVERPROPERTY('InstanceName') AS NVARCHAR(128)) + N'\MSSQLServer\SuperSocketNetLib\Tcp' END
			EXEC master.sys.xp_regread N'HKEY_LOCAL_MACHINE', @RegKey, N'TcpPort', @port OUTPUT, NO_OUTPUT
		END TRY
		BEGIN CATCH
			SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
			SELECT @ErrorMessage = 'Instance info subsection - Error raised in TRY block 1. ' + ERROR_MESSAGE()
			RAISERROR (@ErrorMessage, 16, 1);
		END CATCH
	END
	ELSE
	BEGIN
		RAISERROR('[WARNING: Missing permissions for full "Instance info" checks. Bypassing TCP port check]', 16, 1, N'sysadmin')
		--RETURN
	END
END
ELSE
BEGIN
	BEGIN TRY
		SET @sqlcmd = N'SELECT @portOUT = MAX(CONVERT(int,CONVERT(float,value_data))) FROM sys.dm_server_registry WHERE registry_key LIKE ''%MSSQLServer\SuperSocketNetLib\Tcp\%'' AND value_name LIKE N''%TcpPort%'' AND CONVERT(float,value_data) > 0;';
		SET @params = N'@portOUT int OUTPUT';
		EXECUTE sp_executesql @sqlcmd, @params, @portOUT = @port OUTPUT;
		IF @port IS NULL
		BEGIN
			SET @sqlcmd = N'SELECT @portOUT = CONVERT(int,CONVERT(float,value_data)) FROM sys.dm_server_registry WHERE registry_key LIKE ''%MSSQLServer\SuperSocketNetLib\Tcp\%'' AND value_name LIKE N''%TcpDynamicPort%'' AND CONVERT(float,value_data) > 0;';
			SET @params = N'@portOUT int OUTPUT';
			EXECUTE sp_executesql @sqlcmd, @params, @portOUT = @port OUTPUT;
		END
	END TRY
	BEGIN CATCH
		SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
		SELECT @ErrorMessage = 'Instance info subsection - Error raised in TRY block 2. ' + ERROR_MESSAGE()
		RAISERROR (@ErrorMessage, 16, 1);
	END CATCH
END

IF (ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) = 1) OR ((SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_instance_regread') = 1)
BEGIN
	BEGIN TRY
		EXEC master..xp_instance_regread N'HKEY_LOCAL_MACHINE', N'SOFTWARE\Microsoft\MSSQLServer\Replication', N'IsInstalled', @replication OUTPUT, NO_OUTPUT
	END TRY
	BEGIN CATCH
		SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
		SELECT @ErrorMessage = 'Instance info subsection - Error raised in TRY block 3. ' + ERROR_MESSAGE()
		RAISERROR (@ErrorMessage, 16, 1);
	END CATCH
END
ELSE
BEGIN
	RAISERROR('[WARNING: Missing permissions for full "Instance info" checks. Bypassing replication check]', 16, 1, N'sysadmin')
	--RETURN
END

SELECT @cpucount = COUNT(cpu_id) FROM sys.dm_os_schedulers WHERE scheduler_id < 255 AND parent_node_id < 64
SELECT @numa = COUNT(DISTINCT parent_node_id) FROM sys.dm_os_schedulers WHERE scheduler_id < 255 AND parent_node_id < 64;

;WITH bits AS 
(SELECT 7 AS N, 128 AS E UNION ALL SELECT 6, 64 UNION ALL 
SELECT 5, 32 UNION ALL SELECT 4, 16 UNION ALL SELECT 3, 8 UNION ALL 
SELECT 2, 4 UNION ALL SELECT 1, 2 UNION ALL SELECT 0, 1), 
bytes AS 
(SELECT 1 M UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL 
SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL 
SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9)
-- CPU Affinity is shown highest to lowest CPU ID
SELECT @cpuaffin = CASE WHEN [value] = 0 THEN REPLICATE('1', @cpucount)
	ELSE RIGHT((SELECT ((CONVERT(tinyint, SUBSTRING(CONVERT(binary(9), [value]), M, 1)) & E) / E) AS [text()] 
		FROM bits CROSS JOIN bytes
		ORDER BY M, N DESC
		FOR XML PATH('')), @cpucount) END
FROM sys.configurations (NOLOCK)
WHERE name = 'affinity mask';

SET @cpuaffin_fixed = @cpuaffin

IF @numa > 1
BEGIN
	-- format binary mask by node for better reading
	SET @i = @cpucount/@numa + 1
	WHILE @i <= @cpucount
	BEGIN
		SELECT @cpuaffin_fixed = STUFF(@cpuaffin_fixed, @i, 1, '_' + SUBSTRING(@cpuaffin, @i, 1))
		SET @i = @i + @cpucount/@numa + 1
	END
END

SELECT 'Information' AS [Category], 'Instance' AS [Information],
	(CASE WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('InstanceName')) IS NULL THEN 'DEFAULT_INSTANCE'
		ELSE CONVERT(VARCHAR(128), SERVERPROPERTY('InstanceName')) END) AS Instance_Name,
	(CASE WHEN SERVERPROPERTY('IsClustered') = 1 THEN 'CLUSTERED' 
		WHEN SERVERPROPERTY('IsClustered') = 0 THEN 'NOT_CLUSTERED'
		ELSE 'INVALID INPUT/ERROR' END) AS Failover_Clustered,
	/*The version of SQL Server instance in the form: major.minor.build*/	
	CONVERT(VARCHAR(128), SERVERPROPERTY('ProductVersion')) AS Product_Version,
	/*Level of the version of SQL Server Instance*/
	CONVERT(VARCHAR(128), SERVERPROPERTY('ProductLevel')) AS Product_Level,
	CONVERT(VARCHAR(128), SERVERPROPERTY('Edition')) AS Edition,
	CONVERT(VARCHAR(128), SERVERPROPERTY('MachineName')) AS Machine_Name,
	RTRIM(@port) AS TCP_Port,
	@@SERVICENAME AS Service_Name,
	/*To identify which sqlservr.exe belongs to this instance*/
	SERVERPROPERTY('ProcessID') AS Process_ID, 
	CONVERT(VARCHAR(128), SERVERPROPERTY('ServerName')) AS Server_Name,
	@cpuaffin_fixed AS Affinity_Mask_Bitmask,
	CONVERT(VARCHAR(128), SERVERPROPERTY('Collation')) AS [Server_Collation],
	(CASE WHEN @replication = 1 THEN 'Installed' 
		WHEN @replication = 0 THEN 'Not_Installed' 
		ELSE 'INVALID INPUT/ERROR' END) AS Replication_Components_Installation,
	(CASE WHEN SERVERPROPERTY('IsFullTextInstalled') = 1 THEN 'Installed' 
		WHEN SERVERPROPERTY('IsFulltextInstalled') = 0 THEN 'Not_Installed' 
		ELSE 'INVALID INPUT/ERROR' END) AS Full_Text_Installation,
	(CASE WHEN SERVERPROPERTY('IsIntegratedSecurityOnly') = 1 THEN 'Integrated_Security' 
		WHEN SERVERPROPERTY('IsIntegratedSecurityOnly') = 0 THEN 'SQL_Server_Security' 
		ELSE 'INVALID INPUT/ERROR' END) AS [Security],
	(CASE WHEN SERVERPROPERTY('IsSingleUser') = 1 THEN 'Single_User' 
		WHEN SERVERPROPERTY('IsSingleUser') = 0	THEN 'Multi_User' 
		ELSE 'INVALID INPUT/ERROR' END) AS [Single_User],
	(CASE WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('LicenseType')) = 'PER_SEAT' THEN 'Per_Seat_Mode' 
		WHEN CONVERT(VARCHAR(128), SERVERPROPERTY('LicenseType')) = 'PER_PROCESSOR' THEN 'Per_Processor_Mode' 
		ELSE 'Disabled' END) AS License_Type, -- From SQL Server 2008R2 always returns DISABLED.
	CONVERT(NVARCHAR(128), SERVERPROPERTY('BuildClrVersion')) AS CLR_Version,
	CASE WHEN @sqlmajorver >= 10 THEN 
		CASE WHEN SERVERPROPERTY('FilestreamConfiguredLevel') = 0 THEN 'Disabled'
			WHEN SERVERPROPERTY('FilestreamConfiguredLevel') = 1 THEN 'Enabled_for_TSQL'
			ELSE 'Enabled for TSQL and Win32' END
	ELSE 'Not compatible' END AS Filestream_Configured_Level,
	CASE WHEN @sqlmajorver >= 10 THEN 
		CASE WHEN SERVERPROPERTY('FilestreamEffectiveLevel') = 0 THEN 'Disabled'
			WHEN SERVERPROPERTY('FilestreamEffectiveLevel') = 1 THEN 'Enabled_for_TSQL'
			ELSE 'Enabled for TSQL and Win32' END
	ELSE 'Not compatible' END AS Filestream_Effective_Level,
	CASE WHEN @sqlmajorver >= 10 THEN 
		SERVERPROPERTY('FilestreamShareName')
	ELSE 'Not compatible' END AS Filestream_Share_Name;
	
--------------------------------------------------------------------------------------------------------------------------------
-- Buffer Pool Extension info subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'|-Starting Buffer Pool Extension info', 10, 1) WITH NOWAIT

IF @sqlmajorver > 11
BEGIN
	SELECT 'Information' AS [Category], 'BP_Extension' AS [Information], 
		CASE WHEN state = 0 THEN 'BP_Extension_Disabled' 
			WHEN state = 1 THEN 'BP_Extension_is_Disabling'
			WHEN state = 3 THEN 'BP_Extension_is_Enabling'
			WHEN state = 5 THEN 'BP_Extension_Enabled'
		END AS state, 
		[path], current_size_in_kb
	FROM sys.dm_os_buffer_pool_extension_configuration
END
ELSE
BEGIN
	SELECT 'Information' AS [Category], 'BP_Extension' AS [Information], '[NA]' AS state
END;

--------------------------------------------------------------------------------------------------------------------------------
-- Resource Governor info subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'|-Starting Resource Governor info', 10, 1) WITH NOWAIT

IF @sqlmajorver > 9
BEGIN
	SELECT 'Information' AS [Category], 'RG_Classifier_Function' AS [Information], CASE WHEN classifier_function_id = 0 THEN 'Default_Configuration' ELSE OBJECT_SCHEMA_NAME(classifier_function_id) + '.' + OBJECT_NAME(classifier_function_id) END AS classifier_function, is_reconfiguration_pending
	FROM sys.dm_resource_governor_configuration

	SET @sqlcmd = 'SELECT ''Information'' AS [Category], ''RG_Resource_Pool'' AS [Information], rp.pool_id, name, statistics_start_time, total_cpu_usage_ms, cache_memory_kb, compile_memory_kb, 
	used_memgrant_kb, total_memgrant_count, total_memgrant_timeout_count, active_memgrant_count, active_memgrant_kb, memgrant_waiter_count, max_memory_kb, used_memory_kb, target_memory_kb, 
	out_of_memory_count, min_cpu_percent, max_cpu_percent, min_memory_percent, max_memory_percent' + CASE WHEN @sqlmajorver > 10 THEN ', cap_cpu_percent, rpa.processor_group, rpa.scheduler_mask' ELSE '' END + '
FROM sys.dm_resource_governor_resource_pools rp' + CASE WHEN @sqlmajorver > 10 THEN ' LEFT JOIN sys.dm_resource_governor_resource_pool_affinity rpa ON rp.pool_id = rpa.pool_id' ELSE '' END
	EXECUTE sp_executesql @sqlcmd

	SET @sqlcmd = 'SELECT ''Information'' AS [Category], ''RG_Workload_Groups'' AS [Information], group_id, name, pool_id, statistics_start_time, total_request_count, total_queued_request_count, 
	active_request_count, queued_request_count, total_cpu_limit_violation_count, total_cpu_usage_ms, max_request_cpu_time_ms, blocked_task_count, total_lock_wait_count, 
	total_lock_wait_time_ms, total_query_optimization_count, total_suboptimal_plan_generation_count, total_reduced_memgrant_count, max_request_grant_memory_kb, 
	active_parallel_thread_count, importance, request_max_memory_grant_percent, request_max_cpu_time_sec, request_memory_grant_timeout_sec, 
	group_max_requests, max_dop' + CASE WHEN @sqlmajorver > 10 THEN ', effective_max_dop' ELSE '' END + ' 
FROM sys.dm_resource_governor_workload_groups'
	EXECUTE sp_executesql @sqlcmd
END;

--------------------------------------------------------------------------------------------------------------------------------
-- Logon triggers subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'|-Starting Logon triggers', 10, 1) WITH NOWAIT
IF (SELECT COUNT([name]) FROM sys.server_triggers WHERE is_disabled = 0 AND is_ms_shipped = 0) > 0
BEGIN
	SELECT 'Information' AS [Category], 'Logon_Triggers' AS [Information], name AS [Trigger_Name], type_desc AS [Trigger_Type],create_date, modify_date
	FROM sys.server_triggers WHERE is_disabled = 0 AND is_ms_shipped = 0
	ORDER BY name;
END
ELSE
BEGIN
	SELECT 'Information' AS [Category], 'Logon_Triggers' AS [Information], '[NA]' AS [Comment]
END;

--------------------------------------------------------------------------------------------------------------------------------
-- Database Information subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'|-Starting Database Information', 10, 1) WITH NOWAIT
RAISERROR (N'  |-Building DB list', 10, 1) WITH NOWAIT
DECLARE @curdbname VARCHAR(1000), @curdbid int, @currole tinyint, @cursecondary_role_allow_connections tinyint, @state tinyint

IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tmpdbs0'))
DROP TABLE #tmpdbs0;
IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tmpdbs0'))
CREATE TABLE #tmpdbs0 (id int IDENTITY(1,1), [dbid] int, [dbname] VARCHAR(1000), [compatibility_level] int, is_read_only bit, [state] tinyint, is_distributor bit, [role] tinyint, [secondary_role_allow_connections] tinyint, is_database_joined bit, is_failover_ready bit, isdone bit);

IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tmpdbfiledetail'))
DROP TABLE #tmpdbfiledetail;
IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tmpdbfiledetail'))
CREATE TABLE #tmpdbfiledetail([database_id] [int] NOT NULL, [file_id] int, [type_desc] NVARCHAR(60), [data_space_id] int,[name] sysname, [physical_name] NVARCHAR(260), [state_desc] NVARCHAR(60), [size] int, [max_size] int, [is_percent_growth] bit, [growth] int, [is_media_read_only] bit, [is_read_only] bit, [is_sparse] bit, [is_name_reserved] bit)

IF @sqlmajorver < 11
BEGIN
	INSERT INTO #tmpdbs0 ([dbid], [dbname], [compatibility_level], is_read_only, [state], is_distributor, [role], [secondary_role_allow_connections], [isdone])
	SELECT database_id, name, [compatibility_level], is_read_only, [state], is_distributor, 1, 1, 0 FROM master.sys.databases (NOLOCK)
END;

IF @sqlmajorver > 10
BEGIN
	INSERT INTO #tmpdbs0 ([dbid], [dbname], [compatibility_level], is_read_only, [state], is_distributor, [role], [secondary_role_allow_connections], is_database_joined, is_failover_ready, [isdone])
	SELECT sd.database_id, sd.name, sd.[compatibility_level], sd.is_read_only, sd.[state], sd.is_distributor, MIN(COALESCE(ars.[role],1)) AS [role], ar.secondary_role_allow_connections, rcs.is_database_joined, rcs.is_failover_ready, 0 
	FROM master.sys.databases sd (NOLOCK) 
		LEFT JOIN sys.dm_hadr_database_replica_states d ON sd.database_id = d.database_id
		LEFT JOIN sys.availability_replicas ar ON d.group_id = ar.group_id AND d.replica_id = ar.replica_id
		LEFT JOIN sys.dm_hadr_availability_replica_states ars ON d.group_id = ars.group_id AND d.replica_id = ars.replica_id
		LEFT JOIN sys.dm_hadr_database_replica_cluster_states rcs ON rcs.database_name = sd.name AND rcs.replica_id = ar.replica_id
	GROUP BY sd.database_id, sd.name, sd.is_read_only, sd.[state], sd.is_distributor, ar.secondary_role_allow_connections, sd.[compatibility_level], rcs.is_database_joined, rcs.is_failover_ready;
END;

/* Validate if database scope is set */
IF @dbScope IS NOT NULL AND ISNUMERIC(@dbScope) <> 1 AND @dbScope NOT LIKE '%,%'
BEGIN
	RAISERROR('ERROR: Invalid parameter. Valid input consists of database IDs. If more than one ID is specified, the values must be comma separated.', 16, 42) WITH NOWAIT;
	RETURN
END;
	
RAISERROR (N'  |-Applying specific database scope list, if any', 10, 1) WITH NOWAIT
IF @dbScope IS NOT NULL
BEGIN
	SELECT @sqlcmd = 'DELETE FROM #tmpdbs0 WHERE [dbid] > 4 AND [dbid] NOT IN (' + REPLACE(@dbScope,' ','') + ')'
	EXEC sp_executesql @sqlcmd;
END;

IF @sqlmajorver < 11
BEGIN
	SET @sqlcmd = N'SELECT ''Information'' AS [Category], ''Databases'' AS [Information],
	db.[name] AS [Database_Name], SUSER_SNAME(db.owner_sid) AS [Owner_Name], db.[database_id], 
	db.recovery_model_desc AS [Recovery_Model], db.create_date, db.log_reuse_wait_desc AS [Log_Reuse_Wait_Description], 
	ls.cntr_value AS [Log_Size_KB], lu.cntr_value AS [Log_Used_KB],
	CAST(CAST(lu.cntr_value AS FLOAT) / CAST(ls.cntr_value AS FLOAT)AS DECIMAL(18,2)) * 100 AS [Log_Used_pct], 
	db.[compatibility_level] AS [DB_Compatibility_Level], db.collation_name AS [DB_Collation], 
	db.page_verify_option_desc AS [Page_Verify_Option], db.is_auto_create_stats_on, db.is_auto_update_stats_on,
	db.is_auto_update_stats_async_on, db.is_parameterization_forced, 
	db.snapshot_isolation_state_desc, db.is_read_committed_snapshot_on,
	db.is_read_only, db.is_auto_close_on, db.is_auto_shrink_on, ''Not compatible'' AS [Indirect_Checkpoint], 
	db.is_trustworthy_on, db.is_db_chaining_on, db.is_parameterization_forced
FROM master.sys.databases AS db (NOLOCK)
INNER JOIN sys.dm_os_performance_counters AS lu (NOLOCK) ON db.name = lu.instance_name
INNER JOIN sys.dm_os_performance_counters AS ls (NOLOCK) ON db.name = ls.instance_name
WHERE lu.counter_name LIKE N''Log File(s) Used Size (KB)%'' 
	AND ls.counter_name LIKE N''Log File(s) Size (KB)%''
	AND ls.cntr_value > 0 AND ls.cntr_value > 0' + CASE WHEN @dbScope IS NOT NULL THEN CHAR(10) + 'AND db.[database_id] IN (' + REPLACE(@dbScope,' ','') + ')' ELSE '' END + '
ORDER BY [Database_Name]	
OPTION (RECOMPILE)'
END
ELSE 
BEGIN
	SET @sqlcmd = N'SELECT ''Information'' AS [Category], ''Databases'' AS [Information],
	db.[name] AS [Database_Name], SUSER_SNAME(db.owner_sid) AS [Owner_Name], db.[database_id], 
	db.recovery_model_desc AS [Recovery_Model], db.create_date, db.log_reuse_wait_desc AS [Log_Reuse_Wait_Description], 
	ls.cntr_value AS [Log_Size_KB], lu.cntr_value AS [Log_Used_KB],
	CAST(CAST(lu.cntr_value AS FLOAT) / CAST(ls.cntr_value AS FLOAT)AS DECIMAL(18,2)) * 100 AS [Log_Used_pct], 
	db.[compatibility_level] AS [DB_Compatibility_Level], db.collation_name AS [DB_Collation], 
	db.page_verify_option_desc AS [Page_Verify_Option], db.is_auto_create_stats_on, db.is_auto_update_stats_on,
	db.is_auto_update_stats_async_on, db.is_parameterization_forced, 
	db.snapshot_isolation_state_desc, db.is_read_committed_snapshot_on,
	db.is_read_only, db.is_auto_close_on, db.is_auto_shrink_on, db.target_recovery_time_in_seconds AS [Indirect_Checkpoint], 
	db.is_trustworthy_on, db.is_db_chaining_on, db.is_parameterization_forced
FROM master.sys.databases AS db (NOLOCK)
INNER JOIN sys.dm_os_performance_counters AS lu (NOLOCK) ON db.name = lu.instance_name
INNER JOIN sys.dm_os_performance_counters AS ls (NOLOCK) ON db.name = ls.instance_name
WHERE lu.counter_name LIKE N''Log File(s) Used Size (KB)%'' 
	AND ls.counter_name LIKE N''Log File(s) Size (KB)%''
	AND ls.cntr_value > 0 AND ls.cntr_value > 0' + CASE WHEN @dbScope IS NOT NULL THEN CHAR(10) + 'AND db.[database_id] IN (' + REPLACE(@dbScope,' ','') + ')' ELSE '' END + '
ORDER BY [Database_Name]	
OPTION (RECOMPILE)'
END

EXECUTE sp_executesql @sqlcmd;

WHILE (SELECT COUNT(id) FROM #tmpdbs0 WHERE isdone = 0) > 0
BEGIN
	SELECT TOP 1 @curdbname = [dbname], @curdbid = [dbid], @currole = [role], @state = [state], @cursecondary_role_allow_connections = secondary_role_allow_connections FROM #tmpdbs0 WHERE isdone = 0
	IF (@currole = 2 AND @cursecondary_role_allow_connections = 0) OR @state <> 0
	BEGIN
		SET @sqlcmd = 'SELECT [database_id], [file_id], type_desc, data_space_id, name, physical_name, state_desc, size, max_size, is_percent_growth,growth, is_media_read_only, is_read_only, is_sparse, is_name_reserved
FROM sys.master_files (NOLOCK) WHERE [database_id] = ' + CONVERT(VARCHAR(10), @curdbid)
	END
	ELSE
	BEGIN
		SET @sqlcmd = 'USE ' + QUOTENAME(@curdbname) + ';
SELECT ' + CONVERT(VARCHAR(10), @curdbid) + ' AS [database_id], [file_id], type_desc, data_space_id, name, physical_name, state_desc, size, max_size, is_percent_growth,growth, is_media_read_only, is_read_only, is_sparse, is_name_reserved
FROM sys.database_files (NOLOCK)'
	END

	BEGIN TRY
		INSERT INTO #tmpdbfiledetail
		EXECUTE sp_executesql @sqlcmd
	END TRY
	BEGIN CATCH
		SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
		SELECT @ErrorMessage = 'Database Information subsection - Error raised in TRY block. ' + ERROR_MESSAGE()
		RAISERROR (@ErrorMessage, 16, 1);
	END CATCH
	
	UPDATE #tmpdbs0
	SET isdone = 1
	WHERE [dbid] = @curdbid
END;
	
SELECT 'Information' AS [Category], 'Database_Files' AS [Information], DB_NAME(database_id) AS [Database_Name], [file_id], type_desc, data_space_id AS [Filegroup], name, physical_name,
	state_desc, (size * 8) / 1024 AS size_MB, CASE max_size WHEN -1 THEN 'Unlimited' ELSE CONVERT(VARCHAR(10), max_size) END AS max_size,
	CASE WHEN is_percent_growth = 0 THEN CONVERT(VARCHAR(10),((growth * 8) / 1024)) ELSE growth END AS [growth], CASE WHEN is_percent_growth = 1 THEN 'Pct' ELSE 'MB' END AS growth_type,
	is_media_read_only, is_read_only, is_sparse, is_name_reserved
FROM #tmpdbfiledetail
ORDER BY database_id, [file_id];

IF @sqlmajorver >= 12
BEGIN
	/*DECLARE @dbid int, @dbname VARCHAR(1000), @sqlcmd NVARCHAR(4000)*/

	IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblInMemDBs'))
	DROP TABLE #tblInMemDBs;
	IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblInMemDBs'))
	CREATE TABLE #tblInMemDBs ([DBName] sysname, [Has_MemoryOptimizedObjects] bit, [MemoryAllocated_MemoryOptimizedObjects_KB] DECIMAL(18,2), [MemoryUsed_MemoryOptimizedObjects_KB] DECIMAL(18,2));
	
	UPDATE #tmpdbs0
	SET isdone = 0;

	UPDATE #tmpdbs0
	SET isdone = 1
	WHERE [state] <> 0 OR [dbid] < 5;

	UPDATE #tmpdbs0
	SET isdone = 1
	WHERE [role] = 2 AND secondary_role_allow_connections = 0;
	
	IF (SELECT COUNT(id) FROM #tmpdbs0 WHERE isdone = 0) > 0
	BEGIN
		RAISERROR (N'  |-Starting Storage analysis for In-Memory OLTP Engine', 10, 1) WITH NOWAIT
	
		WHILE (SELECT COUNT(id) FROM #tmpdbs0 WHERE isdone = 0) > 0
		BEGIN
			SELECT TOP 1 @dbname = [dbname], @dbid = [dbid] FROM #tmpdbs0 WHERE isdone = 0
			
			SET @sqlcmd = 'USE ' + QUOTENAME(@dbname) + ';
SELECT ''' + @dbname + ''' AS [DBName], ISNULL((SELECT 1 FROM sys.filegroups FG WHERE FG.[type] = ''FX''), 0) AS [Has_MemoryOptimizedObjects],
ISNULL((SELECT CONVERT(DECIMAL(18,2), (SUM(tms.memory_allocated_for_table_kb) + SUM(tms.memory_allocated_for_indexes_kb))) FROM sys.dm_db_xtp_table_memory_stats tms), 0.00) AS [MemoryAllocated_MemoryOptimizedObjects_KB],
ISNULL((SELECT CONVERT(DECIMAL(18,2),(SUM(tms.memory_used_by_table_kb) + SUM(tms.memory_used_by_indexes_kb))) FROM sys.dm_db_xtp_table_memory_stats tms), 0.00) AS [MemoryUsed_MemoryOptimizedObjects_KB];'

			BEGIN TRY
				INSERT INTO #tblInMemDBs
				EXECUTE sp_executesql @sqlcmd
			END TRY
			BEGIN CATCH
				SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
				SELECT @ErrorMessage = 'Storage analysis for In-Memory OLTP Engine subsection - Error raised in TRY block. ' + ERROR_MESSAGE()
				RAISERROR (@ErrorMessage, 16, 1);
			END CATCH
			
			UPDATE #tmpdbs0
			SET isdone = 1
			WHERE [dbid] = @dbid
		END
	END;

	IF (SELECT COUNT([DBName]) FROM #tblInMemDBs WHERE [Has_MemoryOptimizedObjects] = 1) > 0
	BEGIN
		SELECT 'Information' AS [Category], 'InMem_Database_Storage' AS [Information], DBName AS [Database_Name],
			[MemoryAllocated_MemoryOptimizedObjects_KB], [MemoryUsed_MemoryOptimizedObjects_KB]
		FROM #tblInMemDBs WHERE Has_MemoryOptimizedObjects = 1
		ORDER BY DBName;
	END
	ELSE
	BEGIN
		SELECT 'Information' AS [Category], 'InMem_Database_Storage' AS [Information], '[NA]' AS [Comment]
	END
END;

-- http://support.microsoft.com/kb/2857849
IF @sqlmajorver > 10 AND @IsHadrEnabled = 1
BEGIN
	SELECT 'Information' AS [Category], 'AlwaysOn_AG_Databases' AS [Information], dc.database_name AS [Database_Name],
		d.synchronization_health_desc, d.synchronization_state_desc, d.database_state_desc
	FROM sys.dm_hadr_database_replica_states d
	INNER JOIN sys.availability_databases_cluster dc ON d.group_database_id=dc.group_database_id
	WHERE d.is_local=1
END;

--------------------------------------------------------------------------------------------------------------------------------
-- Database file autogrows last 72h subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'  |-Starting database file autogrows last 72h', 10, 1) WITH NOWAIT
IF EXISTS (SELECT TOP 1 id FROM sys.traces WHERE is_default = 1)
BEGIN
	DECLARE @tracefilename VARCHAR(500)
	SELECT @tracefilename = LEFT([path],LEN([path]) - PATINDEX('%\%', REVERSE([path]))) + '\log.trc' FROM sys.traces WHERE is_default = 1;
	WITH AutoGrow_CTE (databaseid, [filename], Growth, Duration, StartTime, EndTime)
	AS
	(
	SELECT databaseid, [filename], SUM(IntegerData*8) AS Growth, Duration, StartTime, EndTime--, CASE WHEN EventClass =
	FROM ::fn_trace_gettable(@tracefilename, default)
	WHERE EventClass >= 92 AND EventClass <= 95 AND DATEDIFF(hh,StartTime,GETDATE()) < 72 -- Last 24h
	GROUP BY databaseid, [filename], IntegerData, Duration, StartTime, EndTime
	)
	SELECT 'Information' AS [Category], 'Recorded_Autogrows_Lst72H' AS [Information], DB_NAME(database_id) AS Database_Name, 
		mf.name AS logical_file_name, mf.size*8 / 1024 AS size_MB, mf.type_desc,
		(ag.Growth * 8) AS [growth_KB], CASE WHEN is_percent_growth = 1 THEN 'Pct' ELSE 'MB' END AS growth_type,
		Duration/1000 AS Growth_Duration_ms, ag.StartTime, ag.EndTime
	FROM sys.master_files mf
	LEFT OUTER JOIN AutoGrow_CTE ag ON mf.database_id=ag.databaseid AND mf.name=ag.[filename]
	WHERE ag.Growth > 0 --Only where growth occurred
	GROUP BY database_id, mf.name, mf.size, ag.Growth, ag.Duration, ag.StartTime, ag.EndTime, is_percent_growth, mf.growth, mf.type_desc
	ORDER BY Database_Name, logical_file_name, ag.StartTime;
END
ELSE
BEGIN
	SELECT 'Information' AS [Category], 'Recorded_Autogrows_Lst72H' AS [Information], '[WARNING: Could not gather information on autogrow times]' AS [Comment]
END;

--------------------------------------------------------------------------------------------------------------------------------
-- Database triggers subsection
--------------------------------------------------------------------------------------------------------------------------------
IF @ptochecks = 1
BEGIN
	RAISERROR (N'  |-Starting database triggers', 10, 1) WITH NOWAIT
	/*DECLARE @dbid int, @dbname VARCHAR(1000), @sqlcmd NVARCHAR(4000)*/

	IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblTriggers'))
	DROP TABLE #tblTriggers;
	IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblTriggers'))
	CREATE TABLE #tblTriggers ([DBName] sysname, [triggerName] sysname, [schemaName] sysname, [tableName] sysname, [type_desc] NVARCHAR(60), [parent_class_desc] NVARCHAR(60), [create_date] DATETIME, [modify_date] DATETIME, [is_disabled] bit, [is_instead_of_trigger] bit, [is_not_for_replication] bit);
	
	UPDATE #tmpdbs0
	SET isdone = 0;

	UPDATE #tmpdbs0
	SET isdone = 1
	WHERE [state] <> 0 OR [dbid] < 5;

	UPDATE #tmpdbs0
	SET isdone = 1
	WHERE [role] = 2 AND secondary_role_allow_connections = 0;
	
	IF (SELECT COUNT(id) FROM #tmpdbs0 WHERE isdone = 0) > 0
	BEGIN
		WHILE (SELECT COUNT(id) FROM #tmpdbs0 WHERE isdone = 0) > 0
		BEGIN
			SELECT TOP 1 @dbname = [dbname], @dbid = [dbid] FROM #tmpdbs0 WHERE isdone = 0
			
			SET @sqlcmd = 'USE ' + QUOTENAME(@dbname) + ';
SELECT ''' + @dbname + ''' AS [DBName], st.name, ss.name, stb.name, st.type_desc, st.parent_class_desc, st.create_date, st.modify_date, st.is_disabled, st.is_instead_of_trigger, st.is_not_for_replication
FROM sys.triggers AS st
INNER JOIN sys.tables stb ON st.parent_id = stb.[object_id]
INNER JOIN sys.schemas ss ON stb.[schema_id] = ss.[schema_id]
WHERE st.is_ms_shipped = 0
ORDER BY stb.name, st.name;'

			BEGIN TRY
				INSERT INTO #tblTriggers
				EXECUTE sp_executesql @sqlcmd
			END TRY
			BEGIN CATCH
				SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
				SELECT @ErrorMessage = 'Database triggers subsection - Error raised in TRY block. ' + ERROR_MESSAGE()
				RAISERROR (@ErrorMessage, 16, 1);
			END CATCH
			
			UPDATE #tmpdbs0
			SET isdone = 1
			WHERE [dbid] = @dbid
		END
	END;
	
	IF (SELECT COUNT([triggerName]) FROM #tblTriggers) > 0
	BEGIN
		SELECT 'Information' AS [Category], 'Database_Triggers' AS [Information], DBName AS [Database_Name],
			triggerName AS [Trigger_Name], schemaName AS [Schema_Name], tableName AS [Table_Name], 
			type_desc AS [Trigger_Type], parent_class_desc AS [Trigger_Parent], 
			CASE is_instead_of_trigger WHEN 1 THEN 'INSTEAD_OF' ELSE 'AFTER' END AS [Trigger_Behavior],
			create_date, modify_date, 
			CASE WHEN is_disabled = 1 THEN 'YES' ELSE 'NO' END AS [is_disabled], 
			CASE WHEN is_not_for_replication = 1 THEN 'YES' ELSE 'NO' END AS [is_not_for_replication]
		FROM #tblTriggers
		ORDER BY DBName, tableName, triggerName;
	END
	ELSE
	BEGIN
		SELECT 'Information' AS [Category], 'Database_Triggers' AS [Information], '[NA]' AS [Comment]
	END
END;

--------------------------------------------------------------------------------------------------------------------------------
-- Enterprise features usage subsection
--------------------------------------------------------------------------------------------------------------------------------
IF @sqlmajorver > 9
BEGIN
	RAISERROR (N'|-Starting Enterprise features usage', 10, 1) WITH NOWAIT
	/*DECLARE @dbid int, @dbname VARCHAR(1000), @sqlcmd NVARCHAR(4000)*/

	IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblPerSku'))
	DROP TABLE #tblPerSku;
	IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblPerSku'))
	CREATE TABLE #tblPerSku ([DBName] sysname NULL, [Feature_Name] VARCHAR(100));
	
	UPDATE #tmpdbs0
	SET isdone = 0;

	UPDATE #tmpdbs0
	SET isdone = 1
	WHERE [state] <> 0 OR [dbid] < 5;

	UPDATE #tmpdbs0
	SET isdone = 1
	WHERE [role] = 2 AND secondary_role_allow_connections = 0;
	
	IF (SELECT COUNT(id) FROM #tmpdbs0 WHERE isdone = 0) > 0
	BEGIN
		WHILE (SELECT COUNT(id) FROM #tmpdbs0 WHERE isdone = 0) > 0
		BEGIN
			SELECT TOP 1 @dbname = [dbname], @dbid = [dbid] FROM #tmpdbs0 WHERE isdone = 0
			
			SET @sqlcmd = 'USE ' + QUOTENAME(@dbname) + ';
SELECT ''' + @dbname + ''' AS [DBName], feature_name FROM sys.dm_db_persisted_sku_features (NOLOCK);'

			BEGIN TRY
				INSERT INTO #tblPerSku
				EXECUTE sp_executesql @sqlcmd
			END TRY
			BEGIN CATCH
				SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
				SELECT @ErrorMessage = 'Enterprise features usage subsection - Error raised in TRY block. ' + ERROR_MESSAGE()
				RAISERROR (@ErrorMessage, 16, 1);
			END CATCH
			
			UPDATE #tmpdbs0
			SET isdone = 1
			WHERE [dbid] = @dbid
		END
	END;
	
	IF @sqlmajorver > 10 AND @IsHadrEnabled = 1
	BEGIN
		INSERT INTO #tblPerSku
		SELECT NULL, 'AlwaysOn' AS feature_name
	END;
	
	IF (SELECT COUNT(DISTINCT d.name) FROM master.sys.databases d (NOLOCK) WHERE database_id NOT IN (2,3) AND source_database_id IS NOT NULL) > 0 -- Snapshot
	BEGIN
		INSERT INTO #tblPerSku
		SELECT DISTINCT d.name, 'DBSnapshot' AS feature_name FROM master.sys.databases d (NOLOCK) WHERE database_id NOT IN (2,3) AND source_database_id IS NOT NULL
	END;
	
	IF (SELECT COUNT([Feature_Name]) FROM #tblPerSku) > 0
	BEGIN
		SELECT 'Information' AS [Category], 'Enterprise_features_usage' AS [Check], '[INFORMATION: Some databases are using Enterprise only features]' AS [Comment]
		SELECT 'Information' AS [Category], 'Enterprise_features_usage' AS [Information], DBName AS [Database_Name], [Feature_Name]
		FROM #tblPerSku
		ORDER BY 2, 3
	END
	ELSE
	BEGIN
		SELECT 'Information' AS [Category], 'Enterprise_features_usage' AS [Check], '[NA]' AS [Comment]
	END
END;

--------------------------------------------------------------------------------------------------------------------------------
-- Backups since last Full Information subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'|-Starting Backups', 10, 1) WITH NOWAIT
IF @sqlmajorver > 10
BEGIN
	SET @sqlcmd = N'SELECT ''Information'' AS [Category], ''Backups_since_last_Full'' AS [Information], 
[database_name] AS [Database_Name], CASE WHEN type = ''D'' THEN ''Database''
	WHEN type = ''I'' THEN ''Diff_Database''
	WHEN type = ''L'' THEN ''Log''
	WHEN type = ''F'' THEN ''File''
	WHEN type = ''G'' THEN ''Diff_file''
	WHEN type = ''P'' THEN ''Partial''
	WHEN type = ''Q'' THEN ''Diff_partial''
	ELSE NULL END AS [bck_type],
[backup_start_date], [backup_finish_date],
CONVERT(decimal(20,2),backup_size/1024.00/1024.00) AS [backup_size_MB],
CONVERT(decimal(20,2),compressed_backup_size/1024.00/1024.00) AS [compressed_backup_size_MB],
[recovery_model], [user_name],
database_backup_lsn AS [full_base_lsn], [differential_base_lsn], [expiration_date], 
[is_password_protected], [has_backup_checksums], [is_readonly], is_copy_only, [has_incomplete_metadata] AS [Tail_log]
FROM msdb.dbo.backupset bck1 (NOLOCK)
WHERE is_copy_only = 0 -- No COPY_ONLY backups
AND backup_start_date >= (SELECT MAX(backup_start_date) FROM msdb.dbo.backupset bck2 (NOLOCK) WHERE bck2.type IN (''D'',''F'',''P'') AND is_copy_only = 0 AND bck1.database_name = bck2.database_name)
ORDER BY database_name, backup_start_date DESC'
END
ELSE 
BEGIN
	SET @sqlcmd = N'SELECT ''Information'' AS [Category], ''Backups_since_last_Full'' AS [Information], 
[database_name] AS [Database_Name], CASE WHEN type = ''D'' THEN ''Database''
	WHEN type = ''I'' THEN ''Diff_Database''
	WHEN type = ''L'' THEN ''Log''
	WHEN type = ''F'' THEN ''File''
	WHEN type = ''G'' THEN ''Diff_file''
	WHEN type = ''P'' THEN ''Partial''
	WHEN type = ''Q'' THEN ''Diff_partial''
	ELSE NULL END AS [bck_type],
[backup_start_date], [backup_finish_date], 
CONVERT(decimal(20,2),backup_size/1024.00/1024.00) AS [backup_size_MB],
''[NA]'' AS [compressed_backup_size_MB], 
[recovery_model], [user_name],
database_backup_lsn AS [full_base_lsn], [differential_base_lsn], [expiration_date], 
[is_password_protected], [has_backup_checksums], [is_readonly], is_copy_only, [has_incomplete_metadata] AS [Tail_log]
FROM msdb.dbo.backupset bck1 (NOLOCK)
WHERE is_copy_only = 0 -- No COPY_ONLY backups
AND backup_start_date >= (SELECT MAX(backup_start_date) FROM msdb.dbo.backupset bck2 (NOLOCK) WHERE bck2.type IN (''D'',''F'',''P'') AND is_copy_only = 0 AND bck1.database_name = bck2.database_name)
ORDER BY database_name, backup_start_date DESC'
END;

EXECUTE sp_executesql @sqlcmd;

--------------------------------------------------------------------------------------------------------------------------------
-- System Configuration subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'|-Starting System Configuration', 10, 1) WITH NOWAIT
SELECT 'Information' AS [Category], 'All_System_Configurations' AS [Information],
	name AS [Name],
	configuration_id AS [Number],
	minimum AS [Minimum],
	maximum AS [Maximum],
	is_dynamic AS [Dynamic],
	is_advanced AS [Advanced],
	value AS [ConfigValue],
	value_in_use AS [RunValue],
	description AS [Description]
FROM sys.configurations (NOLOCK)
ORDER BY name OPTION (RECOMPILE);

--------------------------------------------------------------------------------------------------------------------------------
-- Pre-checks section
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'Starting Pre-Checks - Building DB list excluding MS shipped', 10, 1) WITH NOWAIT
DECLARE @MSdb int

IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tmpdbs1'))
DROP TABLE #tmpdbs1;
IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tmpdbs1'))
CREATE TABLE #tmpdbs1 (id int IDENTITY(1,1), [dbid] int, [dbname] VARCHAR(1000), [role] tinyint, [secondary_role_allow_connections] tinyint, isdone bit)

RAISERROR (N'|-Excluding MS shipped by standard names and databases belonging to non-readable AG secondary replicas (if available)', 10, 1) WITH NOWAIT
-- Ignore MS shipped databases and databases belonging to non-readable AG secondary replicas
INSERT INTO #tmpdbs1 ([dbid], [dbname], [role], [secondary_role_allow_connections], [isdone])
SELECT [dbid], [dbname], [role], [secondary_role_allow_connections], 0 
FROM #tmpdbs0 (NOLOCK) 
WHERE is_read_only = 0 AND [state] = 0 AND [dbid] > 4 AND is_distributor = 0
	AND [role] <> 2 AND (secondary_role_allow_connections <> 0 OR secondary_role_allow_connections IS NULL)
	AND lower([dbname]) NOT IN ('virtualmanagerdb', --Virtual Machine Manager
		'scspfdb', --Service Provider Foundation
		'semanticsdb', --Semantic Search
		'servicemanager','service manager','dwstagingandconfig','dwrepository','dwdatamart','dwasdatabase','omdwdatamart','cmdwdatamart', --SCSM
		'ssodb','bamanalysis','bamarchive','bamalertsapplication','bamalertsnsmain','bamprimaryimport','bamstarschema','biztalkmgmtdb','biztalkmsgboxdb','biztalkdtadb','biztalkruleenginedb','bamprimaryimport','biztalkedidb','biztalkhwsdb','tpm','biztalkanalysisdb','bamprimaryimportsuccessfully', --BizTalk
		'aspstate','aspnet', --ASP.NET
		'mscrm_config', --Dynamics CRM
		'cpsdyn','lcslog','lcscdr','lis','lyss','mgc','qoemetrics','rgsconfig','rgsdyn','rtc','rtcab','rtcab1','rtcdyn','rtcshared','rtcxds','xds', --Lync
		'activitylog','branchdb','clienttracelog','eventlog','listingssettings','servicegroupdb','tservercontroller','vodbackend', --MediaRoom
		'operationsmanager','operationsmanagerdw','operationsmanagerac', --SCOM
		'orchestrator', --Orchestrator
		'sso','wss_search','wss_search_config','sharedservices_db','sharedservices_search_db','wss_content','profiledb', 'social db','sync db',	--Sharepoint
		'susdb', --WSUS
		'projectserver_archive','projectserver_draft','projectserver_published','projectserver_reporting', --Project Server
		'reportserver','reportservertempdb','rsdb','rstempdb', --SSRS
		'fastsearchadmindatabase', --Fast Search
		'ppsmonitoring','ppsplanningservice','ppsplanningsystem', --PerformancePoint Services
		'dynamics', --Dynamics GP
		'microsoftdynamicsax','microsoftdynamicsaxbaseline', --Dynamics AX
		'fimservice','fimsynchronizationservice', --Forefront Identity Manager
		'sbgatewaydatabase','sbmanagementdb', --Service Bus
		'wfinstancemanagementdb','wfmanagementdb','wfresourcemanagementdb' --Workflow Manager
	)
	AND [dbname] NOT LIKE 'repANDtingservice[_]%' --SSRS
	AND [dbname] NOT LIKE 'tfs[_]%' --TFS
	AND [dbname] NOT LIKE 'defaultpowerpivotserviceapplicationdb%' --PowerPivot
	AND [dbname] NOT LIKE 'perfANDmancepoint service[_]%' --PerfANDmancePoint Services
	AND [dbname] NOT LIKE '%database nav%' --Dynamics NAV
	AND [dbname] NOT LIKE '%[_]mscrm' --Dynamics CRM
	AND [dbname] NOT LIKE 'dpmdb[_]%' --DPM
	AND [dbname] NOT LIKE 'sbmessagecontainer%' --Service Bus
	AND [dbname] NOT LIKE 'sma%' --SCSMA
	AND [dbname] NOT LIKE 'releasemanagement%' --TFS Release Management
	AND [dbname] NOT LIKE 'projectwebapp%' --Project Server
	AND [dbname] NOT LIKE 'sms[_]%' AND [dbname] NOT LIKE 'cm[_]%' --SCCM
	AND [dbname] NOT LIKE 'fepdw%' AND [dbname] NOT LIKE 'FEPDB[_]%' --Forefront Endpoint Protection
	--Sharepoint
	AND [dbname] NOT LIKE 'sharepoint[_]admincontent%' AND [dbname] NOT LIKE 'sharepoint[_]config%' AND [dbname] NOT LIKE 'wss[_]content%' AND [dbname] NOT LIKE 'wss[_]search%'
	AND [dbname] NOT LIKE 'sharedservices[_]db%' AND [dbname] NOT LIKE 'sharedservices[_]search[_]db%' AND [dbname] NOT LIKE 'sharedservices[_][_]db%' AND [dbname] NOT LIKE 'sharedservices[_][_]search[_]db%'
	AND [dbname] NOT LIKE 'sharedservicescontent%' AND [dbname] NOT LIKE 'application[_]registry[_]service[_]db%' AND [dbname] NOT LIKE 'search[_]service[_]application[_]propertystANDedb[_]%'
	AND [dbname] NOT LIKE 'subscriptionsettings[_]%' AND [dbname] NOT LIKE 'webanalyticsserviceapplication[_]stagingdb[_]%' AND [dbname] NOT LIKE 'webanalyticsserviceapplication[_]repANDtingdb[_]%'
	AND [dbname] NOT LIKE 'bdc[_]service[_]db[_]%' AND [dbname] NOT LIKE 'managed metadata service[_]%' AND [dbname] NOT LIKE 'perfANDmancepoint service application[_]%' 
	AND [dbname] NOT LIKE 'search[_]service[_]application[_]crawlstANDedb[_]%' AND [dbname] NOT LIKE 'search[_]service[_]application[_]db[_]%' AND [dbname] NOT LIKE 'secure[_]stANDe[_]service[_]db[_]%' AND [dbname] NOT LIKE 'stateservice%' 
	AND [dbname] NOT LIKE 'user profile service application[_]profiledb[_]%' AND [dbname] NOT LIKE 'user profile service application[_]syncdb[_]%' AND [dbname] NOT LIKE 'user profile service application[_]socialdb[_]%' 
	AND [dbname] NOT LIKE 'wANDdautomationservices[_]%' AND [dbname] NOT LIKE 'wss[_]logging%' AND [dbname] NOT LIKE 'wss[_]usageapplication%' AND [dbname] NOT LIKE 'appmng[_]service[_]db%' 
	AND [dbname] NOT LIKE 'search[_]service[_]application[_]analyticsrepANDtingstANDedb[_]%' AND [dbname] NOT LIKE 'search[_]service[_]application[_]linksstANDedb[_]%' AND [dbname] NOT LIKE 'sharepoint[_]logging[_]%' 
	AND [dbname] NOT LIKE 'settingsservicedb%' AND [dbname] NOT LIKE 'sharepoint[_]logging[_]%' AND [dbname] NOT LIKE 'translationservice[_]%' AND [dbname] NOT LIKE 'sharepoint translation services[_]%' AND [dbname] NOT LIKE 'sessionstateservice%' 

IF EXISTS (SELECT name FROM msdb.sys.objects (NOLOCK) WHERE name='MSdistributiondbs' AND is_ms_shipped = 1) 
BEGIN 
	DELETE FROM #tmpdbs1 WHERE [dbid] IN (SELECT DB_ID(name) FROM msdb.dbo.MSdistributiondbs)
END;

RAISERROR (N'|-Excluding MS shipped by notable object names', 10, 1) WITH NOWAIT
-- Removing other noticeable MS shipped DBs
WHILE (SELECT COUNT(id) FROM #tmpdbs1 WHERE isdone = 0) > 0
BEGIN
	SELECT TOP 1 @dbname = [dbname], @dbid = [dbid] FROM #tmpdbs1 WHERE isdone = 0
	SET @sqlcmd = 'USE ' + QUOTENAME(@dbname) + ';
IF (OBJECT_ID(''dbo.AR_Class'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.AR_Entity'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.AR_System'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.proc_ar_CreateEntity'',''P'') IS NOT NULL AND OBJECT_ID(''dbo.proc_ar_CreateMethod'',''P'') IS NOT NULL)
OR (OBJECT_ID(''dbo.Versions'',''U'') IS NOT NULL 
	AND (OBJECT_ID(''dbo.ECMApplicationLog'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.ECMTerm'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.proc_ECM_GetPackage'',''P'') IS NOT NULL AND OBJECT_ID(''dbo.proc_ECM_GetGroups'',''P'') IS NOT NULL)
	OR (OBJECT_ID(''dbo.Configuration'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.MonthlyPartitions'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.Search_GetCrawlPipeline'',''P'') IS NOT NULL AND OBJECT_ID(''dbo.prc_EnumSandboxedRequests'',''P'') IS NOT NULL)
	OR (OBJECT_ID(''dbo.MSSConfiguration'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.MSSOrdinal'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.proc_MSS_GetConfigurationProperty'',''P'') IS NOT NULL)
	OR (OBJECT_ID(''dbo.Tenants'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.proc_Admin_ListPartitionedTables'',''P'') IS NOT NULL AND OBJECT_ID(''dbo.proc_DefragmentIndices'',''P'') IS NOT NULL)
	OR (OBJECT_ID(''dbo.WAScope'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.WASetting'',''U'') IS NOT NULL AND SCHEMA_ID(''Processing'') IS NOT NULL)
	OR (OBJECT_ID(''dbo.Groups'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.Items'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.proc_GetGroups'',''P'') IS NOT NULL AND OBJECT_ID(''dbo.proc_GetVersion'',''P'') IS NOT NULL)
	OR (OBJECT_ID(''dbo.Mapping'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.PropertySet'',''U'') IS NOT NULL AND SCHEMA_ID(''SubscriptionSettingsService_Application_Pool'') IS NOT NULL) 
	OR (OBJECT_ID(''dbo.Sessions'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.proc_AddItem'',''P'') IS NOT NULL AND OBJECT_ID(''dbo.proc_GetItemWithLock'',''P'') IS NOT NULL)
	OR (OBJECT_ID(''dbo.SiteMap'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.SiteCounts'',''U'') IS NOT NULL AND SCHEMA_ID(''WSS_Content_Application_Pools'') IS NOT NULL)
	OR (OBJECT_ID(''dbo.PPSAnnotations'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.PPSParameterValues'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.proc_PPS_GetAnnotation'',''P'') IS NOT NULL)	 
	OR (OBJECT_ID(''dbo.AM_Licenses'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.AM_DeploymentIds'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.proc_AM_GetApps'',''P'') IS NOT NULL AND OBJECT_ID(''dbo.proc_AM_SetDeploymentId'',''P'') IS NOT NULL)	
	OR (OBJECT_ID(''dbo.MSSDefinitions'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.MSSSecurityDescriptors'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.proc_MSS_GetCrawls'',''P'') IS NOT NULL)
)
OR (OBJECT_ID(''dbo.SSSApplication'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.SSSAudit'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.SSSConfig'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.proc_sss_GetConfig'',''P'') IS NOT NULL)
OR (OBJECT_ID(''dbo.Actions'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.VersionInfo'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.mms_extensions'',''U'') IS NOT NULL AND SCHEMA_ID(''persistenceUsers'') IS NOT NULL AND SCHEMA_ID(''state_persistence_users'') IS NOT NULL)
OR (OBJECT_ID(''dbo.AllDocs'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.AllLists'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.NameValuePair'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.proc_GetWorkItems'',''P'') IS NOT NULL AND OBJECT_ID(''dbo.proc_EnumLists'',''P'') IS NOT NULL)	
OR (OBJECT_ID(''dbo.SSO_Application'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.SSO_Ticket'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.SSO_Config'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.sso_InsertAudit'',''P'') IS NOT NULL AND OBJECT_ID(''dbo.sso_RetrieveSSOConfig'',''P'') IS NOT NULL)
-- End Sharepoint
OR ((OBJECT_ID(''dbo.ASPStateTempSessions'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.ASPStateTempApplications'',''U'') IS NOT NULL) OR OBJECT_ID(''dbo.CreateTempTables'',''P'') IS NOT NULL AND OBJECT_ID(''dbo.DeleteExpiredSessions'',''P'') IS NOT NULL)
OR (OBJECT_ID(''dbo.aspnet_Applications'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.aspnet_Profile'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.aspnet_Users'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.aspnet_CheckSchemaVersion'',''P'') IS NOT NULL)
-- End ASP.NET
OR (OBJECT_ID(''DataRefresh.Runs'',''U'') IS NOT NULL AND OBJECT_ID(''GeminiService.Version'',''U'') IS NOT NULL AND OBJECT_ID(''Usage.Requests'',''U'') IS NOT NULL AND SCHEMA_ID(''HealthRule'') IS NOT NULL)
-- End PowerPivot
OR (OBJECT_ID(''dbo.LICENSES'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.VERSION'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.TASK_RUNPROGRAM'',''U'') IS NOT NULL AND SCHEMA_ID(''Microsoft.SystemCenter.Orchestrator'') IS NOT NULL)
-- End Orchestrator
OR (OBJECT_ID(''dbo.tbl_Cloud_Cloud'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.tbl_PXE_PxeServer'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.tbl_VMM_Server'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.prc_VMM_AddVmmServer'',''P'') IS NOT NULL AND OBJECT_ID(''dbo.prc_Cloud_Cloud_GetParent'',''P'') IS NOT NULL)
-- End VMM
OR (OBJECT_ID(''scspf.EventHandlers'',''U'') IS NOT NULL AND OBJECT_ID(''scspf.Servers'',''U'') IS NOT NULL AND OBJECT_ID(''scspf.Tenants'',''U'') IS NOT NULL)
-- End Service Provider Foundation
OR ((OBJECT_ID(''dbo.SSOX_AuditTable'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.SSOX_GlobalInfo'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.SSOX_Servers'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.sp_BackupBizTalkFull'',''P'') IS NOT NULL AND OBJECT_ID(''dbo.ssox_spGetDBVersion'',''P'') IS NOT NULL)
OR (OBJECT_ID(''dbo.BizTalkDBVersion'',''U'') IS NOT NULL AND SCHEMA_ID(''BTS_ADMIN_USERS'') IS NOT NULL AND SCHEMA_ID(''BTS_OPERATORS'') IS NOT NULL))
-- End BizTalk
OR (OBJECT_ID(''dbo.Layer'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.ModelGroup'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.SchemaVersion'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.XI_GetUserName'',''P'') IS NOT NULL AND OBJECT_ID(''dbo.XU_AssignAxId'',''P'') IS NOT NULL)
-- End Dynamics AX
OR (OBJECT_ID(''dbo.Notification'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.SystemUserRoles'',''U'') IS NOT NULL 
	AND (OBJECT_ID(''dbo.p_GetCrmUserId'',''P'') IS NOT NULL AND OBJECT_ID(''dbo.p_GetPrivilegesInRole'',''P'') IS NOT NULL)
	OR (OBJECT_ID(''dbo.p_AccountOVRollup'',''P'') IS NOT NULL AND OBJECT_ID(''dbo.p_GetDbSize'',''P'') IS NOT NULL))
-- End Dynamics CRM
OR (OBJECT_ID(''dbo.User Personalization'',''U'') IS NOT NULL AND EXISTS(SELECT 1 FROM sys.all_objects (NOLOCK) WHERE type=''U'' AND (name like ''%$G[_]L Entry'' OR name LIKE ''%$Item Ledger Entry'')))
-- End Dynamics NAV
OR (OBJECT_ID(''dbo.DBVERSION'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.PATH'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.SY_SQL_Options'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.zDP_ActivitySD'',''P'') IS NOT NULL)
-- End Dynamics GP
OR (OBJECT_ID(''dbo.Agents'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.DistributionPoints'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.SysResList'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.PXE_GetPXECert'',''P'') IS NOT NULL)
-- End SCCM
OR (OBJECT_ID(''dbo.dtFEP_Infra_InstalledJobs'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.dtFEP_Common_User'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.dtAN_Infra_JobLastRun'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.spFEP_Infra_CreateJob'',''P'') IS NOT NULL AND OBJECT_ID(''dbo.spAN_Infra_ScheduleJob'',''P'') IS NOT NULL)
-- End Forefront Endpoint Protection
OR (OBJECT_ID(''admin.categories'',''U'') IS NOT NULL AND OBJECT_ID(''admin.keyword'',''U'') IS NOT NULL AND OBJECT_ID(''admin.storeentry'',''U'') IS NOT NULL)
-- End Fast Search
OR (OBJECT_ID(''fim.Objects'',''U'') IS NOT NULL AND SCHEMA_ID(''debug'') IS NOT NULL)
OR (OBJECT_ID(''dbo.mms_extensions'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.mms_partition'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.mms_addmvlink'',''P'') IS NOT NULL AND OBJECT_ID(''dbo.mms_getcsguidfromanchor'',''P'') IS NOT NULL)
-- End Forefront Identity Manager
OR (OBJECT_ID(''dbo.Annotations'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.BsmUsers'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.FCObjects'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.BsmUserCreate'',''P'') IS NOT NULL AND OBJECT_ID(''dbo.BsmUserDelete'',''P'') IS NOT NULL)
OR (OBJECT_ID(''dbo.DBSchemaVersion'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.QueueStatus'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.ServerStates'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.bsp_UpdateQueueSizeLimit'',''P'') IS NOT NULL)
-- End PerformancePoint Services
OR (OBJECT_ID(''dbo.Versions'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.MSP_DAL_GetDatabaseCacheExceptions'',''P'') IS NOT NULL AND (OBJECT_ID(''dbo.MSP_DAL_GetSprocInfo'',''P'') IS NOT NULL OR OBJECT_ID(''dbo.MSP_DAL_GetSprocList'',''P'') IS NOT NULL))
-- End Project Server
OR (OBJECT_ID(''apm.MESSAGES'',''U'') IS NOT NULL AND SCHEMA_ID(''CS'') IS NOT NULL OR SCHEMA_ID(''CM'') IS NOT NULL)
OR (OBJECT_ID(''dbo.Event_00'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.MT_Database'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.PerformanceData_00'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.p_MPSelectViews'',''P'') IS NOT NULL)
OR (OBJECT_ID(''dbo.AemApplication'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.EventLoggingComputer'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.HealthState'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.p_MOMManagementGroupInfoSelect'',''P'') IS NOT NULL)
OR (OBJECT_ID(''dbo.dtMachine'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.dtPartition'',''U'') IS NOT NULL AND SCHEMA_ID(''AdtServer'') IS NOT NULL)
-- End SCOM
OR (OBJECT_ID(''dbo.version'',''U'') IS NOT NULL AND EXISTS(SELECT 1 FROM sys.internal_tables (NOLOCK) WHERE name LIKE ''language[_]model[_]%''))
-- End Semantic Search
OR (OBJECT_ID(''dbo.tbComputerTarget'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.tbTarget'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.tbUpdate'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.spGetUpdateByID'',''P'') IS NOT NULL AND OBJECT_ID(''dbo.spSearchUpdates'',''P'') IS NOT NULL)
-- End WSUS
OR (OBJECT_ID(''dbo.tbl_DPM_InstalledUpdates'',''U'') IS NOT NULL AND SCHEMA_ID(''MSDPMExecRole'') IS NOT NULL AND SCHEMA_ID(''MSDPMRecoveryRole'') IS NOT NULL)
-- End DPM
OR ((OBJECT_ID(''dbo.DomainTable'',''U'') IS NOT NULL AND OBJECT_ID(''etl.Source'',''U'') IS NOT NULL)
OR (OBJECT_ID(''dbo.Module'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.MT_Computer'',''U'') IS NOT NULL AND OBJECT_ID(''LFXSTG.vex_Collection'',''U'') IS NOT NULL AND SCHEMA_ID(''LFX'') IS NOT NULL)
OR (OBJECT_ID(''dbo.DomainTable'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.State'',''U'') IS NOT NULL AND SCHEMA_ID(''etl'') IS NOT NULL))
-- End SCSM
OR (OBJECT_ID(''dbo.ChunkData'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.SegmentedChunk'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.SnapshotData'',''U'') IS NOT NULL AND SCHEMA_ID(''RSExecRole'') IS NOT NULL)
-- End SSRS
OR (OBJECT_ID(''dbo.prc_ChangeHostId'',''P'') IS NOT NULL AND OBJECT_ID(''dbo.prc_EnablePrefixCompression'',''P'') IS NOT NULL 
AND (OBJECT_ID(''dbo.tbl_RegistryItems'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.tbl_OAuthToken'',''U'') IS NOT NULL AND	OBJECT_ID(''dbo.tbl_Content'',''U'') IS NOT NULL)
OR (OBJECT_ID(''dbo.DimBuild'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.FactCurrentWorkItem'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.FactBuildProject'',''U'') IS NOT NULL))
OR (OBJECT_ID(''dbo.LoadTestCase'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.LoadTestReport'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.LoadTestScenario'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.prc_GetAgents'',''P'') IS NOT NULL	AND OBJECT_ID(''dbo.prc_QueryLoadTestRuns'',''P'') IS NOT NULL)
-- End TFS
OR (OBJECT_ID(''dbo.Release'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.Server'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.TeamProject'',''U'') IS NOT NULL AND SCHEMA_ID(''System.Activities.DurableInstancing'') IS NOT NULL)
-- End TFS Release Management
OR (OBJECT_ID(''dbo.ContainersTable'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.Quotas'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.Tenants'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.GetAllEntities'',''P'') IS NOT NULL AND OBJECT_ID(''dbo.UpdateGatewayEntity'',''P'') IS NOT NULL)
OR (OBJECT_ID(''dbo.LockResourcesTable'',''U'') IS NOT NULL AND OBJECT_ID(''Store.Nodes'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.OperationsTable'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.AcquireLock'',''P'') IS NOT NULL AND OBJECT_ID(''dbo.UpdateOperation'',''P'') IS NOT NULL)
OR (OBJECT_ID(''dbo.CursorsTable'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.LogsTable'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.MessagesTable'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.GetCursorState'',''P'') IS NOT NULL AND OBJECT_ID(''dbo.LockEntity'',''P'') IS NOT NULL)
-- End Service Bus
OR (OBJECT_ID(''dbo.DebugTraces'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.Instances'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.StoreVersionTable'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.GetInstanceCount'',''P'') IS NOT NULL AND OBJECT_ID(''dbo.GetStoreVersion'',''P'') IS NOT NULL)
OR (OBJECT_ID(''dbo.StoreVersionTable'',''U'') IS NOT NULL AND OBJECT_ID(''Store.Clusters'',''U'') IS NOT NULL AND OBJECT_ID(''Store.Services'',''U'') IS NOT NULL AND OBJECT_ID(''Store.GetNode'',''P'') IS NOT NULL AND OBJECT_ID(''Store.UpdateCluster'',''P'') IS NOT NULL)
OR (OBJECT_ID(''dbo.Activities'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.Scopes'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.WorkflowServices'',''U'') IS NOT NULL AND OBJECT_ID(''dbo.GetActivities'',''P'') IS NOT NULL AND OBJECT_ID(''dbo.TenantCheck'',''P'') IS NOT NULL)
-- End Workflow Manager
OR (OBJECT_ID(''Core.Runbooks'',''U'') IS NOT NULL AND OBJECT_ID(''Core.Activities'',''U'') IS NOT NULL AND OBJECT_ID(''Core.Connections'',''U'') IS NOT NULL AND SCHEMA_ID(''Common'') IS NOT NULL)
-- End SCSMA
BEGIN
	SELECT @MSdbOUT = ' + CONVERT(VARCHAR(10), @dbid) + '
END'
	SET @params = N'@MSdbOUT int OUTPUT';
	EXECUTE sp_executesql @sqlcmd, @params, @MSdbOUT=@MSdb OUTPUT
	
	IF @MSdb = @dbid
	BEGIN
		DELETE FROM #tmpdbs1 
		WHERE [dbid] = @dbid;
	END
	ELSE
	BEGIN
		UPDATE #tmpdbs1
		SET isdone = 1
		WHERE [dbid] = @dbid
	END
END;

UPDATE #tmpdbs1
SET isdone = 0;

RAISERROR (N'|-Applying 2nd layer of specific database scope, if any', 10, 1) WITH NOWAIT

IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tmpdbs_userchoice'))
DROP TABLE #tmpdbs_userchoice;
IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tmpdbs_userchoice'))
CREATE TABLE #tmpdbs_userchoice ([dbid] int PRIMARY KEY, [dbname] VARCHAR(1000))
	
IF @dbScope IS NOT NULL
BEGIN
	SELECT @sqlcmd = 'SELECT [dbid], [dbname] 
FROM #tmpdbs0 (NOLOCK) 
WHERE is_read_only = 0 AND [state] = 0 AND [dbid] > 4 AND is_distributor = 0
	AND [role] <> 2 AND (secondary_role_allow_connections <> 0 OR secondary_role_allow_connections IS NULL)
	AND [dbid] IN (' + REPLACE(@dbScope,' ','') + ')'
	
	INSERT INTO #tmpdbs_userchoice ([dbid], [dbname])
	EXEC sp_executesql @sqlcmd;

	SELECT @sqlcmd = 'DELETE FROM #tmpdbs1 WHERE [dbid] NOT IN (' + REPLACE(@dbScope,' ','') + ')'
	EXEC sp_executesql @sqlcmd;
END;

--------------------------------------------------------------------------------------------------------------------------------
-- Checks section
--------------------------------------------------------------------------------------------------------------------------------

RAISERROR (N'Starting Checks section', 10, 1) WITH NOWAIT

RAISERROR (N'|-Starting Processor Checks', 10, 1) WITH NOWAIT

--------------------------------------------------------------------------------------------------------------------------------
-- Number of available Processors for this instance vs. MaxDOP setting subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'  |-Starting Number of available Processors for this instance vs. MaxDOP setting', 10, 1) WITH NOWAIT
DECLARE /*@cpucount int, @numa int, */@affined_cpus int

/*
DECLARE @i int, @cpuaffin_fixed VARCHAR(300)
SET @cpuaffin_fixed = @cpuaffin
SET @i = @cpucount/@numa + 1
WHILE @i <= @cpucount
BEGIN
	SELECT @cpuaffin_fixed = STUFF(@cpuaffin_fixed, @i, 1, '_' + SUBSTRING(@cpuaffin, @i, 1))
	SET @i = @i + @cpucount/@numa + 1
END
*/

SELECT @affined_cpus = COUNT(cpu_id) FROM sys.dm_os_schedulers WHERE is_online = 1 AND scheduler_id < 255 AND parent_node_id < 64;
--SELECT @cpucount = COUNT(cpu_id) FROM sys.dm_os_schedulers WHERE scheduler_id < 255 AND parent_node_id < 64
SELECT 'Processor_checks' AS [Category], 'Parallelism_MaxDOP' AS [Check],
	CASE WHEN [value] > @affined_cpus THEN '[WARNING: MaxDOP setting exceeds available processor count (affinity)]'
		WHEN @numa = 1 AND @affined_cpus > 8 AND ([value] = 0 OR [value] > 8) THEN '[WARNING: MaxDOP setting is not recommended for current processor count (affinity)]'
		WHEN @numa > 1 AND (@cpucount/@numa) < 8 AND ([value] = 0 OR [value] > (@cpucount/@numa)) THEN '[WARNING: MaxDOP setting is not recommended for current NUMA node to processor count (affinity) ratio]'
		WHEN @numa > 1 AND (@cpucount/@numa) >= 8 AND ([value] = 0 OR [value] > 8 OR [value] > (@cpucount/@numa)) THEN '[WARNING: MaxDOP setting is not recommended for current NUMA node to processor count (affinity) ratio]'
		ELSE '[OK]'
	END AS [Deviation]
FROM sys.configurations (NOLOCK) WHERE name = 'max degree of parallelism';

SELECT 'Processor_checks' AS [Category], 'Parallelism_MaxDOP' AS [Information], 
	CASE WHEN [value] > @affined_cpus THEN @affined_cpus
		WHEN @numa = 1 AND @affined_cpus > 8 AND ([value] = 0 OR [value] > 8) THEN 8
		WHEN @numa > 1 AND (@cpucount/@numa) < 8 AND ([value] = 0 OR [value] > (@cpucount/@numa)) THEN @cpucount/@numa
		WHEN @numa > 1 AND (@cpucount/@numa) >= 8 AND ([value] = 0 OR [value] > 8 OR [value] > (@cpucount/@numa)) THEN 8
		ELSE 0
	END AS [Recommended_MaxDOP],
	[value] AS [Current_MaxDOP], @cpucount AS [Available_Processors], @affined_cpus AS [Affined_Processors], 
	-- Processor Affinity is shown highest to lowest CPU ID
	@cpuaffin_fixed AS Affinity_Mask_Bitmask
FROM sys.configurations (NOLOCK) WHERE name = 'max degree of parallelism';

--------------------------------------------------------------------------------------------------------------------------------
-- Processor Affinity in NUMA architecture subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'  |-Starting Processor Affinity in NUMA architecture', 10, 1) WITH NOWAIT
IF @numa > 1
BEGIN
	WITH ncpuCTE (ncpus) AS (SELECT COUNT(cpu_id) AS ncpus from sys.dm_os_schedulers WHERE is_online = 1 AND scheduler_id < 255 AND parent_node_id < 64 GROUP BY parent_node_id, is_online HAVING COUNT(cpu_id) = 1),
	cpuCTE (node, afin) AS (SELECT DISTINCT(parent_node_id), is_online FROM sys.dm_os_schedulers WHERE scheduler_id < 255 AND parent_node_id < 64 GROUP BY parent_node_id, is_online)
	SELECT 'Processor_checks' AS [Category], 'Affinity_NUMA' AS [Check],
		CASE WHEN (SELECT COUNT(*) FROM ncpuCTE) > 0 THEN '[WARNING: Current NUMA configuration is not recommended. At least one node has a single assigned CPU]' 
			WHEN (SELECT COUNT(DISTINCT(node)) FROM cpuCTE WHERE afin = 0 AND node NOT IN (SELECT DISTINCT(node) FROM cpuCTE WHERE afin = 1)) > 0 THEN '[WARNING: Current NUMA configuration is not recommended. At least one node does not have assigned CPUs]' 
			ELSE '[OK]' END AS [Deviation]
	FROM sys.dm_os_sys_info (NOLOCK) 
	OPTION (RECOMPILE);
	
	SELECT 'Processor_checks' AS [Category], 'Affinity_NUMA' AS [Information], cpu_count AS [Logical_CPU_Count], 
		(SELECT COUNT(DISTINCT parent_node_id) FROM sys.dm_os_schedulers WHERE scheduler_id < 255 AND parent_node_id < 64) AS [NUMA_Nodes],
		-- Processor Affinity is shown highest to lowest CPU ID
		@cpuaffin_fixed AS Affinity_Mask_Bitmask
	FROM sys.dm_os_sys_info (NOLOCK) 
	OPTION (RECOMPILE);
END
ELSE
BEGIN
	SELECT 'Processor_checks' AS [Category], 'Affinity_NUMA' AS [Check], '[Not_NUMA]' AS [Deviation]
	FROM sys.dm_os_sys_info (NOLOCK)
	OPTION (RECOMPILE);
END;

--------------------------------------------------------------------------------------------------------------------------------
-- Additional Processor information subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'  |-Starting Additional Processor information', 10, 1) WITH NOWAIT

-- Processor Info
SELECT 'Processor_checks' AS [Category], 'Processor_Summary' AS [Information], cpu_count AS [Logical_CPU_Count], hyperthread_ratio AS [Cores2Socket_Ratio],
	cpu_count/hyperthread_ratio AS [CPU_Sockets], 
	CASE WHEN @numa > 1 THEN (SELECT COUNT(DISTINCT parent_node_id) FROM sys.dm_os_schedulers WHERE scheduler_id < 255 AND parent_node_id < 64) ELSE 0 END AS [NUMA_Nodes],
	@affined_cpus AS [Affined_Processors], 
	-- Processor Affinity is shown highest to lowest Processor ID
	@cpuaffin_fixed AS Affinity_Mask_Bitmask
FROM sys.dm_os_sys_info (NOLOCK)
OPTION (RECOMPILE);

IF @ptochecks = 1
BEGIN
	RAISERROR (N'  |-Starting Processor utilization rate in the last 2 hours', 10, 1) WITH NOWAIT
	-- Processor utilization rate in the last 2 hours
	DECLARE @ts_now bigint
	DECLARE @tblAggCPU TABLE (SQLProc tinyint, SysIdle tinyint, OtherProc tinyint, Minutes tinyint)
	SELECT @ts_now = ms_ticks FROM sys.dm_os_sys_info (NOLOCK);

	WITH cteCPU (record_id, SystemIdle, SQLProcessUtilization, [timestamp]) AS (SELECT 
			record.value('(./Record/@id)[1]', 'int') AS record_id,
			record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int') AS SystemIdle,
			record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') AS SQLProcessUtilization,
			[TIMESTAMP] FROM (SELECT [TIMESTAMP], CONVERT(xml, record) AS record 
				FROM sys.dm_os_ring_buffers (NOLOCK)
				WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
				AND record LIKE '%<SystemHealth>%') AS x
		)
	INSERT INTO @tblAggCPU
		SELECT AVG(SQLProcessUtilization), AVG(SystemIdle), 100 - AVG(SystemIdle) - AVG(SQLProcessUtilization), 10 
		FROM cteCPU 
		WHERE DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) > DATEADD(mi, -10, GETDATE())
	UNION ALL 
		SELECT AVG(SQLProcessUtilization), AVG(SystemIdle), 100 - AVG(SystemIdle) - AVG(SQLProcessUtilization), 20
		FROM cteCPU 
		WHERE DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) <= DATEADD(mi, -10, GETDATE()) AND 
			DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) > DATEADD(mi, -20, GETDATE())
	UNION ALL 
		SELECT AVG(SQLProcessUtilization), AVG(SystemIdle), 100 - AVG(SystemIdle) - AVG(SQLProcessUtilization), 30
		FROM cteCPU 
		WHERE DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) <= DATEADD(mi, -20, GETDATE()) AND 
			DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) > DATEADD(mi, -30, GETDATE())
	UNION ALL 
		SELECT AVG(SQLProcessUtilization), AVG(SystemIdle), 100 - AVG(SystemIdle) - AVG(SQLProcessUtilization), 40
		FROM cteCPU 
		WHERE DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) <= DATEADD(mi, -30, GETDATE()) AND 
			DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) > DATEADD(mi, -40, GETDATE())
	UNION ALL 
		SELECT AVG(SQLProcessUtilization), AVG(SystemIdle), 100 - AVG(SystemIdle) - AVG(SQLProcessUtilization), 50
		FROM cteCPU 
		WHERE DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) <= DATEADD(mi, -40, GETDATE()) AND 
			DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) > DATEADD(mi, -50, GETDATE())
	UNION ALL 
		SELECT AVG(SQLProcessUtilization), AVG(SystemIdle), 100 - AVG(SystemIdle) - AVG(SQLProcessUtilization), 60
		FROM cteCPU 
		WHERE DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) <= DATEADD(mi, -50, GETDATE()) AND 
			DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) > DATEADD(mi, -60, GETDATE())
	UNION ALL 
		SELECT AVG(SQLProcessUtilization), AVG(SystemIdle), 100 - AVG(SystemIdle) - AVG(SQLProcessUtilization), 70
		FROM cteCPU 
		WHERE DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) <= DATEADD(mi, -60, GETDATE()) AND 
			DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) > DATEADD(mi, -70, GETDATE())
	UNION ALL 
		SELECT AVG(SQLProcessUtilization), AVG(SystemIdle), 100 - AVG(SystemIdle) - AVG(SQLProcessUtilization), 80
		FROM cteCPU 
		WHERE DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) <= DATEADD(mi, -70, GETDATE()) AND 
			DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) > DATEADD(mi, -80, GETDATE())
	UNION ALL 
		SELECT AVG(SQLProcessUtilization), AVG(SystemIdle), 100 - AVG(SystemIdle) - AVG(SQLProcessUtilization), 90
		FROM cteCPU 
		WHERE DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) <= DATEADD(mi, -80, GETDATE()) AND 
			DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) > DATEADD(mi, -90, GETDATE())
	UNION ALL 
		SELECT AVG(SQLProcessUtilization), AVG(SystemIdle), 100 - AVG(SystemIdle) - AVG(SQLProcessUtilization), 100
		FROM cteCPU 
		WHERE DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) <= DATEADD(mi, -90, GETDATE()) AND 
			DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) > DATEADD(mi, -100, GETDATE())
	UNION ALL 
		SELECT AVG(SQLProcessUtilization), AVG(SystemIdle), 100 - AVG(SystemIdle) - AVG(SQLProcessUtilization), 110
		FROM cteCPU 
		WHERE DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) <= DATEADD(mi, -100, GETDATE()) AND 
			DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) > DATEADD(mi, -110, GETDATE())
	UNION ALL 
		SELECT AVG(SQLProcessUtilization), AVG(SystemIdle), 100 - AVG(SystemIdle) - AVG(SQLProcessUtilization), 120
		FROM cteCPU 
		WHERE DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) <= DATEADD(mi, -110, GETDATE()) AND 
			DATEADD(ms, -1 * (@ts_now - [timestamp]), GETDATE()) > DATEADD(mi, -120, GETDATE())
	
	IF (SELECT COUNT(SysIdle) FROM @tblAggCPU WHERE SysIdle < 30) > 0
	BEGIN
		SELECT 'Processor_checks' AS [Category], 'Processor_Usage_last_2h' AS [Check], '[WARNING: Detected CPU usage over 70 pct]' AS [Deviation];
	END
	ELSE IF (SELECT COUNT(SysIdle) FROM @tblAggCPU WHERE SysIdle < 10) > 0
	BEGIN
		SELECT 'Processor_checks' AS [Category], 'Processor_Usage_last_2h' AS [Check], '[WARNING: Detected CPU usage over 90 pct]' AS [Deviation];
	END
	ELSE
	BEGIN
		SELECT 'Processor_checks' AS [Category], 'Processor_Usage_last_2h' AS [Check], '[OK]' AS [Deviation];
	END;

	SELECT 'Processor_checks' AS [Category], 'Agg_Processor_Usage_last_2h' AS [Information], SQLProc AS [SQL_Process_Utilization], SysIdle AS [System_Idle], OtherProc AS [Other_Process_Utilization], Minutes AS [Time_Slice_min]
	FROM @tblAggCPU;
END;

RAISERROR (N'|-Starting Memory Checks', 10, 1) WITH NOWAIT

--------------------------------------------------------------------------------------------------------------------------------
-- Server Memory subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'  |-Starting Server Memory', 10, 1) WITH NOWAIT
DECLARE @maxservermem bigint, @minservermem bigint, @systemmem bigint, @systemfreemem bigint, @numa_nodes_afinned tinyint, @LowMemoryThreshold int
DECLARE @commit_target bigint -- Includes stolen and reserved memory in the memory manager
DECLARE @committed bigint -- Does not include reserved memory in the memory manager
DECLARE @mwthreads_count int

IF @sqlmajorver = 9
BEGIN
	SET @sqlcmd = N'SELECT @systemmemOUT = t1.record.value(''(./Record/MemoryRecord/TotalPhysicalMemory)[1]'', ''bigint'')/1024, 
	@systemfreememOUT = t1.record.value(''(./Record/MemoryRecord/AvailablePhysicalMemory)[1]'', ''bigint'')/1024
FROM (SELECT MAX([TIMESTAMP]) AS [TIMESTAMP], CONVERT(xml, record) AS record 
	FROM sys.dm_os_ring_buffers (NOLOCK)
	WHERE ring_buffer_type = N''RING_BUFFER_RESOURCE_MONITOR''
		AND record LIKE ''%RESOURCE_MEMPHYSICAL%''
	GROUP BY record) AS t1';
END
ELSE
BEGIN
	SET @sqlcmd = N'SELECT @systemmemOUT = total_physical_memory_kb/1024, @systemfreememOUT = available_physical_memory_kb/1024 FROM sys.dm_os_sys_memory';
END

SET @params = N'@systemmemOUT bigint OUTPUT, @systemfreememOUT bigint OUTPUT';

EXECUTE sp_executesql @sqlcmd, @params, @systemmemOUT=@systemmem OUTPUT, @systemfreememOUT=@systemfreemem OUTPUT;

IF @sqlmajorver >= 9 AND @sqlmajorver < 11
BEGIN
	SET @sqlcmd = N'SELECT @commit_targetOUT=bpool_commit_target*8, @committedOUT=bpool_committed*8 FROM sys.dm_os_sys_info (NOLOCK)'
END
ELSE IF @sqlmajorver >= 11
BEGIN
	SET @sqlcmd = N'SELECT @commit_targetOUT=committed_target_kb, @committedOUT=committed_kb FROM sys.dm_os_sys_info (NOLOCK)'
END

SET @params = N'@commit_targetOUT bigint OUTPUT, @committedOUT bigint OUTPUT';

EXECUTE sp_executesql @sqlcmd, @params, @commit_targetOUT=@commit_target OUTPUT, @committedOUT=@committed OUTPUT;

SELECT @minservermem = CONVERT(int, [value]) FROM sys.configurations (NOLOCK) WHERE [Name] = 'min server memory (MB)';
SELECT @maxservermem = CONVERT(int, [value]) FROM sys.configurations (NOLOCK) WHERE [Name] = 'max server memory (MB)';
SELECT @mwthreads_count = max_workers_count FROM sys.dm_os_sys_info;
SELECT @numa_nodes_afinned = COUNT (DISTINCT parent_node_id) FROM sys.dm_os_schedulers WHERE scheduler_id < 255 AND parent_node_id < 64 AND is_online = 1

/* 
From Windows Internals book by David Solomon and Mark Russinovich:
"The default level of available memory that signals a low-memory-resource notification event is approximately 32 MB per 4 GB, 
to a maximum of 64 MB. The default level that signals a high-memory-resource notification event is three times the default low-memory value."
*/ 

IF (ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) = 1) OR ((SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_regread') = 1)
BEGIN
	BEGIN TRY
		SELECT @RegKey = N'System\CurrentControlSet\Control\SessionManager\MemoryManagement'
		EXEC master.sys.xp_regread N'HKEY_LOCAL_MACHINE', @RegKey, N'LowMemoryThreshold', @LowMemoryThreshold OUTPUT, NO_OUTPUT
		
		IF @LowMemoryThreshold IS NULL
		SELECT @LowMemoryThreshold = CASE WHEN @systemmem <= 4096 THEN 32 ELSE 64 END
	END TRY
	BEGIN CATCH
		SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
		SELECT @ErrorMessage = 'Server Memory subsection - Error raised in TRY block. ' + ERROR_MESSAGE()
		RAISERROR (@ErrorMessage, 16, 1);
	END CATCH
END
ELSE
BEGIN
	RAISERROR('[WARNING: Missing permissions for full "Instance info" checks. Bypassing LowMemoryThreshold check]', 16, 1, N'sysadmin')
	--RETURN
END;

SELECT 'Memory_checks' AS [Category], 'Memory_issues_MaxServerMem' AS [Check],
	CASE WHEN @maxservermem = 2147483647 THEN '[WARNING: MaxMem setting is default. Please revise memory settings]'
		WHEN @maxservermem > @systemmem THEN '[WARNING: MaxMem setting exceeds available system memory]'
		WHEN @numa > 1 AND (@maxservermem/@numa) * @numa_nodes_afinned > (@systemmem/@numa) * @numa_nodes_afinned THEN '[WARNING: Current MaxMem setting will leverage node foreign memory. 
Maximum value for MaxMem setting on this configuration is ' + CONVERT(NVARCHAR,(@systemmem/@numa) * @numa_nodes_afinned) + ' for a single instance]'
		ELSE '[OK]'
	END AS [Deviation], @maxservermem AS [sql_max_mem_MB];

SELECT 'Memory_checks' AS [Category], 'Memory_issues_MinServerMem' AS [Check],
	CASE WHEN @minservermem = 0 AND (LOWER(@SystemManufacturer) = 'microsoft' OR LOWER(@SystemManufacturer) = 'vmware') THEN '[WARNING: Min Server Mem setting is not set in a VM, allowing memory pressure on the Host to attempt to deallocate memory on a guest SQL Server]'
		WHEN @minservermem = 0 AND @clustered = 1 THEN '[INFORMATION: Min Server Mem setting is default in a clustered instance. Leverage Min Server Mem for the purpose of limiting memory concurrency between instances]'
		WHEN @minservermem = @maxservermem THEN '[WARNING: Min Server Mem setting is equal to Max Server Mem. This will not allow dynamic memory. Please revise memory settings]'
		WHEN @numa > 1 AND (@minservermem/@numa) * @numa_nodes_afinned > (@systemmem/@numa) * @numa_nodes_afinned THEN '[WARNING: Current MinMem setting will leverage node foreign memory]'
		ELSE '[OK]'
	END AS [Deviation], @minservermem AS [sql_min_mem_MB];

SELECT 'Memory_checks' AS [Category], 'Memory_issues_FreeMem' AS [Check],
	CASE WHEN (@systemfreemem*100)/@systemmem <= 5 THEN '[WARNING: Less than 5 percent of Free Memory available. Please revise memory settings]'
		/* 64 is the default LowMemThreshold for windows on a system with 8GB of mem or more*/
		WHEN @systemfreemem <= 64*3 THEN '[WARNING: System Free Memory is dangerously low. Please revise memory settings]'
		ELSE '[OK]'
	END AS [Deviation], @systemmem AS system_total_physical_memory_MB, @systemfreemem AS system_available_physical_memory_MB;

SELECT 'Memory_checks' AS [Category], 'Memory_issues_CommitedMem' AS [Check],
	CASE WHEN @commit_target > @committed AND @sqlmajorver >= 11 THEN '[INFORMATION: Memory manager will try to obtain additional memory]'
		WHEN @commit_target < @committed AND @sqlmajorver >= 11  THEN '[INFORMATION: Memory manager will try to shrink the amount of memory committed]'
		WHEN @commit_target > @committed AND @sqlmajorver < 11 THEN '[INFORMATION: Buffer Pool will try to obtain additional memory]'
		WHEN @commit_target < @committed AND @sqlmajorver < 11  THEN '[INFORMATION: Buffer Pool will try to shrink]'
		ELSE '[OK]'
	END AS [Deviation], @commit_target/1024 AS sql_commit_target_MB, @committed/1024 AS sql_commited_MB;

SELECT 'Memory_checks' AS [Category], 'Memory_reference' AS [Check],
	CASE WHEN @arch IS NULL THEN '[WARNING: Could not determine architecture needed for check]'
		WHEN (@systemmem <= 2048 AND @maxservermem > @systemmem-512-(@mwthreads_count*(CASE WHEN @arch = 64 THEN 2 WHEN @arch = 128 THEN 4 WHEN @arch = 32 THEN 0.5 END))) OR
		(@systemmem BETWEEN 2049 AND 4096 AND @maxservermem > @systemmem-819-(@mwthreads_count*(CASE WHEN @arch = 64 THEN 2 WHEN @arch = 128 THEN 4 WHEN @arch = 32 THEN 0.5 END))) OR
		(@systemmem BETWEEN 4097 AND 8192 AND @maxservermem > @systemmem-1228-(@mwthreads_count*(CASE WHEN @arch = 64 THEN 2 WHEN @arch = 128 THEN 4 WHEN @arch = 32 THEN 0.5 END))) OR
		(@systemmem BETWEEN 8193 AND 12288 AND @maxservermem > @systemmem-2048-(@mwthreads_count*(CASE WHEN @arch = 64 THEN 2 WHEN @arch = 128 THEN 4 WHEN @arch = 32 THEN 0.5 END))) OR
		(@systemmem BETWEEN 12289 AND 24576 AND @maxservermem > @systemmem-2560-(@mwthreads_count*(CASE WHEN @arch = 64 THEN 2 WHEN @arch = 128 THEN 4 WHEN @arch = 32 THEN 0.5 END))) OR
		(@systemmem BETWEEN 24577 AND 32768 AND @maxservermem > @systemmem-3072-(@mwthreads_count*(CASE WHEN @arch = 64 THEN 2 WHEN @arch = 128 THEN 4 WHEN @arch = 32 THEN 0.5 END))) OR
		(@systemmem > 32768 AND @maxservermem > @systemmem-4096-(@mwthreads_count*(CASE WHEN @arch = 64 THEN 2 WHEN @arch = 128 THEN 4 WHEN @arch = 32 THEN 0.5 END))) THEN '[WARNING: Not at the recommended MaxMem setting for this server memory configuration, with a single instance]'
		ELSE '[OK]'
	END AS [Deviation],
	CASE WHEN @systemmem <= 2048 THEN @systemmem-512-(@mwthreads_count*(CASE WHEN @arch = 64 THEN 2 WHEN @arch = 128 THEN 4 WHEN @arch = 32 THEN 0.5 END))
		WHEN @systemmem BETWEEN 2049 AND 4096 THEN @systemmem-819-(@mwthreads_count*(CASE WHEN @arch = 64 THEN 2 WHEN @arch = 128 THEN 4 WHEN @arch = 32 THEN 0.5 END))
		WHEN @systemmem BETWEEN 4097 AND 8192 THEN @systemmem-1228-(@mwthreads_count*(CASE WHEN @arch = 64 THEN 2 WHEN @arch = 128 THEN 4 WHEN @arch = 32 THEN 0.5 END))
		WHEN @systemmem BETWEEN 8193 AND 12288 THEN @systemmem-2048-(@mwthreads_count*(CASE WHEN @arch = 64 THEN 2 WHEN @arch = 128 THEN 4 WHEN @arch = 32 THEN 0.5 END))
		WHEN @systemmem BETWEEN 12289 AND 24576 THEN @systemmem-2560-(@mwthreads_count*(CASE WHEN @arch = 64 THEN 2 WHEN @arch = 128 THEN 4 WHEN @arch = 32 THEN 0.5 END))
		WHEN @systemmem BETWEEN 24577 AND 32768 THEN @systemmem-3072-(@mwthreads_count*(CASE WHEN @arch = 64 THEN 2 WHEN @arch = 128 THEN 4 WHEN @arch = 32 THEN 0.5 END))
		WHEN @systemmem > 32768 THEN @systemmem-4096-(@mwthreads_count*(CASE WHEN @arch = 64 THEN 2 WHEN @arch = 128 THEN 4 WHEN @arch = 32 THEN 0.5 END))
	END AS [Recommended_MaxMem_MB_SingleInstance],
	CASE WHEN @systemmem <= 2048 THEN 512
		WHEN @systemmem BETWEEN 2049 AND 4096 THEN 819
		WHEN @systemmem BETWEEN 4097 AND 8192 THEN 1228
		WHEN @systemmem BETWEEN 8193 AND 12288 THEN 2048
		WHEN @systemmem BETWEEN 12289 AND 24576 THEN 2560
		WHEN @systemmem BETWEEN 24577 AND 32768 THEN 3072
		WHEN @systemmem > 32768 THEN 4096
	END AS [Mem_MB_for_OS],
	CASE WHEN @systemmem <= 2048 THEN @mwthreads_count*(CASE WHEN @arch = 64 THEN 2 WHEN @arch = 128 THEN 4 WHEN @arch = 32 THEN 0.5 END)
		WHEN @systemmem BETWEEN 2049 AND 4096 THEN @mwthreads_count*(CASE WHEN @arch = 64 THEN 2 WHEN @arch = 128 THEN 4 WHEN @arch = 32 THEN 0.5 END)
		WHEN @systemmem BETWEEN 4097 AND 8192 THEN @mwthreads_count*(CASE WHEN @arch = 64 THEN 2 WHEN @arch = 128 THEN 4 WHEN @arch = 32 THEN 0.5 END)
		WHEN @systemmem BETWEEN 8193 AND 12288 THEN @mwthreads_count*(CASE WHEN @arch = 64 THEN 2 WHEN @arch = 128 THEN 4 WHEN @arch = 32 THEN 0.5 END)
		WHEN @systemmem BETWEEN 12289 AND 24576 THEN @mwthreads_count*(CASE WHEN @arch = 64 THEN 2 WHEN @arch = 128 THEN 4 WHEN @arch = 32 THEN 0.5 END)
		WHEN @systemmem BETWEEN 24577 AND 32768 THEN @mwthreads_count*(CASE WHEN @arch = 64 THEN 2 WHEN @arch = 128 THEN 4 WHEN @arch = 32 THEN 0.5 END)
		WHEN @systemmem > 32768 THEN @mwthreads_count*(CASE WHEN @arch = 64 THEN 2 WHEN @arch = 128 THEN 4 WHEN @arch = 32 THEN 0.5 END)
	END AS [Potential_threads_mem_MB],
	@mwthreads_count AS [Configured_workers];

IF @sqlmajorver = 9
BEGIN
	SELECT 'Memory_checks' AS [Category], 'Memory_Summary' AS [Information], 
		@maxservermem AS sql_max_mem_MB, @minservermem AS sql_min_mem_MB,
		@commit_target/1024 AS sql_commit_target_MB, --BPool in SQL 2005 to 2008R2
		@committed/1024 AS sql_commited_MB, --BPool in SQL 2005 to 2008R2
		@systemmem AS system_total_physical_memory_MB, 
		@systemfreemem AS system_available_physical_memory_MB
END
ELSE
BEGIN
	SET @sqlcmd = N'SELECT ''Memory_checks'' AS [Category], ''Memory_Summary'' AS [Information], 
	@maxservermemIN AS sql_max_mem_MB, @minservermemIN AS sql_min_mem_MB, 
	@commit_targetIN/1024 AS sql_commit_target_MB, --BPool in SQL 2005 to 2008R2
	@committedIN/1024 AS sql_commited_MB, --BPool in SQL 2005 to 2008R2
	physical_memory_in_use_kb/1024 AS sql_physical_memory_in_use_MB, 
	large_page_allocations_kb/1024 AS sql_large_page_allocations_MB, 
	locked_page_allocations_kb/1024 AS sql_locked_page_allocations_MB,	
	@systemmemIN AS system_total_physical_memory_MB, 
	@systemfreememIN AS system_available_physical_memory_MB, 
	total_virtual_address_space_kb/1024 AS sql_total_VAS_MB, 
	virtual_address_space_reserved_kb/1024 AS sql_VAS_reserved_MB, 
	virtual_address_space_committed_kb/1024 AS sql_VAS_committed_MB, 
	virtual_address_space_available_kb/1024 AS sql_VAS_available_MB,
	page_fault_count AS sql_page_fault_count,
	memory_utilization_percentage AS sql_memory_utilization_percentage, 
	process_physical_memory_low AS sql_process_physical_memory_low, 
	process_virtual_memory_low AS sql_process_virtual_memory_low	
FROM sys.dm_os_process_memory (NOLOCK)'
	SET @params = N'@maxservermemIN bigint, @minservermemIN bigint, @systemmemIN bigint, @systemfreememIN bigint, @commit_targetIN bigint, @committedIN bigint';
	EXECUTE sp_executesql @sqlcmd, @params, @maxservermemIN=@maxservermem, @minservermemIN=@minservermem,@systemmemIN=@systemmem, @systemfreememIN=@systemfreemem, @commit_targetIN=@commit_target, @committedIN=@committed
END;

IF @numa > 1 AND @sqlmajorver > 10
BEGIN
	EXEC ('SELECT ''Memory_checks'' AS [Category], ''NUMA_Memory_Distribution'' AS [Information], memory_node_id, virtual_address_space_reserved_kb, virtual_address_space_committed_kb, locked_page_allocations_kb, pages_kb, foreign_committed_kb, shared_memory_reserved_kb, shared_memory_committed_kb, processor_group FROM sys.dm_os_memory_nodes;')
END
ELSE IF @numa > 1 AND @sqlmajorver = 10
BEGIN
	EXEC ('SELECT ''Memory_checks'' AS [Category], ''NUMA_Memory_Distribution'' AS [Information], memory_node_id, virtual_address_space_reserved_kb, virtual_address_space_committed_kb, locked_page_allocations_kb, single_pages_kb, multi_pages_kb, shared_memory_reserved_kb, shared_memory_committed_kb, processor_group FROM sys.dm_os_memory_nodes;')
END;

IF @ptochecks = 1
BEGIN
	RAISERROR (N'  |-Starting RM Task', 10, 1) WITH NOWAIT

	IF @LowMemoryThreshold IS NOT NULL
	SELECT 'Memory_checks' AS [Category], 'Memory_RM_Tresholds' AS [Information], @LowMemoryThreshold AS [MEMPHYSICAL_LOW_Threshold], @LowMemoryThreshold * 3 AS [MEMPHYSICAL_HIGH_Threshold]

	SELECT 'Memory_checks' AS [Category], 'Memory_RM_Notifications' AS [Information], 
	CASE WHEN x.[TIMESTAMP] BETWEEN -2147483648 AND 2147483647 AND si.ms_ticks BETWEEN -2147483648 AND 2147483647 THEN DATEADD(ms, x.[TIMESTAMP] - si.ms_ticks, GETDATE()) 
		ELSE DATEADD(s, ([TIMESTAMP]/1000) - (si.ms_ticks/1000), GETDATE()) END AS Event_Time,
		record.value('(./Record/ResourceMonitor/Notification)[1]', 'VARCHAR(max)') AS [Notification],
		record.value('(./Record/MemoryRecord/TotalPhysicalMemory)[1]', 'bigint')/1024 AS [Total_Physical_Mem_MB],
		record.value('(./Record/MemoryRecord/AvailablePhysicalMemory)[1]', 'bigint')/1024 AS [Avail_Physical_Mem_MB],
		record.value('(./Record/MemoryRecord/AvailableVirtualAddressSpace)[1]', 'bigint')/1024 AS [Avail_VAS_MB],
		record.value('(./Record/MemoryRecord/TotalPageFile)[1]', 'bigint')/1024 AS [Total_Pagefile_MB],
		record.value('(./Record/MemoryRecord/AvailablePageFile)[1]', 'bigint')/1024 AS [Avail_Pagefile_MB]
	FROM (SELECT [TIMESTAMP], CONVERT(xml, record) AS record 
				FROM sys.dm_os_ring_buffers (NOLOCK)
				WHERE ring_buffer_type = N'RING_BUFFER_RESOURCE_MONITOR') AS x
	CROSS JOIN sys.dm_os_sys_info si (NOLOCK)
	--WHERE CASE WHEN x.[timestamp] BETWEEN -2147483648 AND 2147483648 THEN DATEADD(ms, x.[timestamp] - si.ms_ticks, GETDATE()) 
	--	ELSE DATEADD(s, (x.[timestamp]/1000) - (si.ms_ticks/1000), GETDATE()) END >= DATEADD(hh, -12, GETDATE())
	ORDER BY 2 DESC;

	RAISERROR (N'  |-Starting Hand Movements from Cache Clock Hands', 10, 1) WITH NOWAIT

	IF (SELECT COUNT(rounds_count) FROM sys.dm_os_memory_cache_clock_hands (NOLOCK) WHERE rounds_count > 0) > 0
	BEGIN
		IF @sqlmajorver >= 11
		BEGIN
			SET @sqlcmd = N'SELECT ''Memory_checks'' AS [Category], ''Clock_Hand_Notifications'' AS [Information], mcch.name, mcch.[type], 
	mcch.clock_hand, mcch.clock_status, SUM(mcch.rounds_count) AS rounds_count,
	SUM(mcch.removed_all_rounds_count) AS cache_entries_removed_all_rounds, 
	SUM(mcch.removed_last_round_count) AS cache_entries_removed_last_round,
	SUM(mcch.updated_last_round_count) AS cache_entries_updated_last_round,
	SUM(mcc.pages_kb) AS cache_pages_kb,
	SUM(mcc.pages_in_use_kb) AS cache_pages_in_use_kb,
	SUM(mcc.entries_count) AS cache_entries_count, 
	SUM(mcc.entries_in_use_count) AS cache_entries_in_use_count, 
	CASE WHEN mcch.last_tick_time BETWEEN -2147483648 AND 2147483647 AND si.ms_ticks BETWEEN -2147483648 AND 2147483647 THEN DATEADD(ms, mcch.last_tick_time - si.ms_ticks, GETDATE()) 
		WHEN mcch.last_tick_time/1000 BETWEEN -2147483648 AND 2147483647 AND si.ms_ticks/1000 BETWEEN -2147483648 AND 2147483647 THEN DATEADD(s, (mcch.last_tick_time/1000) - (si.ms_ticks/1000), GETDATE()) 
		ELSE NULL END AS last_clock_hand_move
FROM sys.dm_os_memory_cache_counters mcc (NOLOCK)
INNER JOIN sys.dm_os_memory_cache_clock_hands mcch (NOLOCK) ON mcc.cache_address = mcch.cache_address
CROSS JOIN sys.dm_os_sys_info si (NOLOCK)
WHERE mcch.rounds_count > 0
GROUP BY mcch.name, mcch.[type], mcch.clock_hand, mcch.clock_status, mcc.pages_kb, mcc.pages_in_use_kb, mcch.last_tick_time, si.ms_ticks, mcc.entries_count, mcc.entries_in_use_count
ORDER BY SUM(mcch.removed_all_rounds_count) DESC, mcch.[type];'
		END
		ELSE
		BEGIN
			SET @sqlcmd = N'SELECT ''Memory_checks'' AS [Category], ''Clock_Hand_Notifications'' AS [Information], mcch.name, mcch.[type], 
	mcch.clock_hand, mcch.clock_status, SUM(mcch.rounds_count) AS rounds_count,
	SUM(mcch.removed_all_rounds_count) AS cache_entries_removed_all_rounds, 
	SUM(mcch.removed_last_round_count) AS cache_entries_removed_last_round,
	SUM(mcch.updated_last_round_count) AS cache_entries_updated_last_round,
	SUM(mcc.single_pages_kb) AS cache_single_pages_kb,
	SUM(mcc.multi_pages_kb) AS cache_multi_pages_kb,
	SUM(mcc.single_pages_in_use_kb) AS cache_single_pages_in_use_kb,
	SUM(mcc.multi_pages_in_use_kb) AS cache_multi_pages_in_use_kb,
	SUM(mcc.entries_count) AS cache_entries_count, 
	SUM(mcc.entries_in_use_count) AS cache_entries_in_use_count, 
	CASE WHEN mcch.last_tick_time BETWEEN -2147483648 AND 2147483647 AND si.ms_ticks BETWEEN -2147483648 AND 2147483647 THEN DATEADD(ms, mcch.last_tick_time - si.ms_ticks, GETDATE()) 
		WHEN mcch.last_tick_time/1000 BETWEEN -2147483648 AND 2147483647 AND si.ms_ticks/1000 BETWEEN -2147483648 AND 2147483647 THEN DATEADD(s, (mcch.last_tick_time/1000) - (si.ms_ticks/1000), GETDATE()) 
		ELSE NULL END AS last_clock_hand_move
FROM sys.dm_os_memory_cache_counters mcc (NOLOCK)
INNER JOIN sys.dm_os_memory_cache_clock_hands mcch (NOLOCK) ON mcc.cache_address = mcch.cache_address
CROSS JOIN sys.dm_os_sys_info si (NOLOCK)
WHERE mcch.rounds_count > 0
GROUP BY mcch.name, mcch.[type], mcch.clock_hand, mcch.clock_status, mcc.single_pages_kb, mcc.multi_pages_kb, mcc.single_pages_in_use_kb, mcc.multi_pages_in_use_kb, mcch.last_tick_time, si.ms_ticks, mcc.entries_count, mcc.entries_in_use_count
ORDER BY SUM(mcch.removed_all_rounds_count) DESC, mcch.[type];'
		END
		EXECUTE sp_executesql @sqlcmd;
	END
	ELSE
	BEGIN
		SELECT 'Memory_checks' AS [Category], 'Clock_Hand_Notifications' AS [Information], '[OK]' AS Comment
	END;
	
	IF @bpool_consumer = 1
	BEGIN
		RAISERROR (N'  |-Starting Buffer Pool Consumers from Buffer Descriptors', 10, 1) WITH NOWAIT
		
		-- Note: in case of NUMA architecture, more than one entry per database is expected

		SET @sqlcmd = 'SELECT ''Memory_checks'' AS [Category], ''Buffer_Pool_Consumers'' AS [Information], 
	COUNT_BIG(DISTINCT page_id)*8/1024 AS total_pages_MB, 
	CASE database_id WHEN 32767 THEN ''ResourceDB'' ELSE DB_NAME(database_id) END AS database_name,
	SUM(row_count)/COUNT_BIG(DISTINCT page_id) AS avg_row_count_per_page, 
	SUM(CONVERT(BIGINT, free_space_in_bytes))/COUNT_BIG(DISTINCT page_id) AS avg_free_space_bytes_per_page
	' + CASE WHEN @sqlmajorver >= 12 THEN ',is_in_bpool_extension' ELSE '' END + '
	' + CASE WHEN @sqlmajorver = 10 THEN ',numa_node' ELSE '' END + '
	' + CASE WHEN @sqlmajorver >= 11 THEN ',AVG(read_microsec) AS avg_read_microsec' ELSE '' END + '
FROM sys.dm_os_buffer_descriptors
--WHERE bd.page_type IN (''DATA_PAGE'', ''INDEX_PAGE'')
GROUP BY database_id' + CASE WHEN @sqlmajorver >= 10 THEN ', numa_node' ELSE '' END + CASE WHEN @sqlmajorver >= 12 THEN ', is_in_bpool_extension' ELSE '' END + '
ORDER BY total_pages_MB DESC;'
		EXECUTE sp_executesql @sqlcmd;
	END

	RAISERROR (N'  |-Starting Memory Allocations from Memory Clerks', 10, 1) WITH NOWAIT
	
	SET @sqlcmd = N'SELECT ''Memory_checks'' AS [Category], [type] AS Alloc_Type, 
	' + CASE WHEN @sqlmajorver < 11 THEN 'SUM(single_pages_kb + multi_pages_kb + virtual_memory_committed_kb + shared_memory_committed_kb + awe_allocated_kb) AS Alloc_Mem_KB'
		ELSE 'SUM(pages_kb + virtual_memory_committed_kb + shared_memory_committed_kb + awe_allocated_kb) AS Alloc_Mem_KB' END + '
FROM sys.dm_os_memory_clerks 
WHERE type IN (''CACHESTORE_COLUMNSTOREOBJECTPOOL'',''CACHESTORE_CLRPROC'',''CACHESTORE_OBJCP'',''CACHESTORE_PHDR'',''CACHESTORE_SQLCP'',''CACHESTORE_TEMPTABLES'',
''MEMORYCLERK_SQLBUFFERPOOL'',''MEMORYCLERK_SQLCLR'',''MEMORYCLERK_SQLGENERAL'',''MEMORYCLERK_SQLLOGPOOL'',''MEMORYCLERK_SQLOPTIMIZER'',
''MEMORYCLERK_SQLQUERYCOMPILE'',''MEMORYCLERK_SQLQUERYEXEC'',''MEMORYCLERK_SQLQUERYPLAN'',''MEMORYCLERK_SQLSTORENG'',''MEMORYCLERK_XTP'',
''OBJECTSTORE_LOCK_MANAGER'',''OBJECTSTORE_SNI_PACKET'',''USERSTORE_DBMETADATA'',''USERSTORE_OBJPERM'')
GROUP BY [type]
UNION ALL
SELECT ''Memory_checks'' AS [Category], ''Others'' AS Alloc_Type, 
	' + CASE WHEN @sqlmajorver < 11 THEN 'SUM(single_pages_kb + multi_pages_kb + virtual_memory_committed_kb + shared_memory_committed_kb) AS Alloc_Mem_KB'
		ELSE 'SUM(pages_kb + virtual_memory_committed_kb + shared_memory_committed_kb) AS Alloc_Mem_KB' END + '
FROM sys.dm_os_memory_clerks 
WHERE type NOT IN (''CACHESTORE_COLUMNSTOREOBJECTPOOL'',''CACHESTORE_CLRPROC'',''CACHESTORE_OBJCP'',''CACHESTORE_PHDR'',''CACHESTORE_SQLCP'',''CACHESTORE_TEMPTABLES'',
''MEMORYCLERK_SQLBUFFERPOOL'',''MEMORYCLERK_SQLCLR'',''MEMORYCLERK_SQLGENERAL'',''MEMORYCLERK_SQLLOGPOOL'',''MEMORYCLERK_SQLOPTIMIZER'',
''MEMORYCLERK_SQLQUERYCOMPILE'',''MEMORYCLERK_SQLQUERYEXEC'',''MEMORYCLERK_SQLQUERYPLAN'',''MEMORYCLERK_SQLSTORENG'',''MEMORYCLERK_XTP'',
''OBJECTSTORE_LOCK_MANAGER'',''OBJECTSTORE_SNI_PACKET'',''USERSTORE_DBMETADATA'',''USERSTORE_OBJPERM'')'
	EXECUTE sp_executesql @sqlcmd;
	
	IF @sqlmajorver >= 12
	BEGIN
		RAISERROR (N'  |-Starting Memory Consumers from In-Memory OLTP Engine', 10, 1) WITH NOWAIT
		SET @sqlcmd = N'SELECT ''Memory_checks'' AS [Category], ''InMemory_Consumers'' AS Alloc_Type, 
	OBJECT_NAME([object_id]) AS [Object_Name], memory_consumer_type_desc, [object_id], index_id, 
	allocated_bytes/(1024*1024) AS Allocated_MB, used_bytes/(1024*1024) AS Used_MB, 
	CASE WHEN used_bytes IS NULL THEN ''used_bytes_is_varheap_only'' ELSE '''' END AS [Comment]
FROM sys.dm_db_xtp_memory_consumers
WHERE [object_id] > 0' -- Only user objects; system objects are negative numbers
		EXECUTE sp_executesql @sqlcmd;

		RAISERROR (N'  |-Starting Memory Allocations from In-Memory OLTP Engine', 10, 1) WITH NOWAIT
		SET @sqlcmd = N'SELECT ''Memory_checks'' AS [Category], ''InMemory_Alloc'' AS Alloc_Type, 
SUM(allocated_bytes)/(1024*1024) AS total_allocated_MB, SUM(used_bytes)/(1024*1024) AS total_used_MB
FROM sys.dm_db_xtp_memory_consumers'
		EXECUTE sp_executesql @sqlcmd;
	END;
END;

RAISERROR (N'  |-Starting OOM', 10, 1) WITH NOWAIT

IF (SELECT COUNT([TIMESTAMP]) FROM sys.dm_os_ring_buffers (NOLOCK) WHERE ring_buffer_type = N'RING_BUFFER_OOM') > 0
BEGIN		
	SELECT 'Memory_checks' AS [Category], 'OOM_Notifications' AS [Information], 
	CASE WHEN x.[TIMESTAMP] BETWEEN -2147483648 AND 2147483647 AND si.ms_ticks BETWEEN -2147483648 AND 2147483647 THEN DATEADD(ms, x.[TIMESTAMP] - si.ms_ticks, GETDATE()) 
		ELSE DATEADD(s, ([TIMESTAMP]/1000) - (si.ms_ticks/1000), GETDATE()) END AS Event_Time,
		record.value('(./Record/OOM/Action)[1]', 'varchar(50)') AS [Action],
		record.value('(./Record/OOM/Resources)[1]', 'int') AS [Resources],
		record.value('(./Record/OOM/Task)[1]', 'varchar(20)') AS [Task],
		record.value('(./Record/OOM/Pool)[1]', 'int') AS [PoolID],
		rgrp.name AS [PoolName],
		record.value('(./Record/MemoryRecord/MemoryUtilization)[1]', 'bigint') AS [MemoryUtilPct],
		record.value('(./Record/MemoryRecord/TotalPhysicalMemory)[1]', 'bigint')/1024 AS [Total_Physical_Mem_MB],
		record.value('(./Record/MemoryRecord/AvailablePhysicalMemory)[1]', 'bigint')/1024 AS [Avail_Physical_Mem_MB],
		record.value('(./Record/MemoryRecord/AvailableVirtualAddressSpace)[1]', 'bigint')/1024 AS [Avail_VAS_MB],
		record.value('(./Record/MemoryRecord/TotalPageFile)[1]', 'bigint')/1024 AS [Total_Pagefile_MB],
		record.value('(./Record/MemoryRecord/AvailablePageFile)[1]', 'bigint')/1024 AS [Avail_Pagefile_MB]
	FROM (SELECT [TIMESTAMP], CONVERT(xml, record) AS record 
				FROM sys.dm_os_ring_buffers (NOLOCK)
				WHERE ring_buffer_type = N'RING_BUFFER_OOM') AS x
	CROSS JOIN sys.dm_os_sys_info si (NOLOCK)
	LEFT JOIN sys.resource_governor_resource_pools rgrp (NOLOCK) ON rgrp.pool_id = record.value('(./Record/OOM/Pool)[1]', 'int')
	--WHERE CASE WHEN x.[timestamp] BETWEEN -2147483648 AND 2147483648 THEN DATEADD(ms, x.[timestamp] - si.ms_ticks, GETDATE()) 
	--	ELSE DATEADD(s, (x.[timestamp]/1000) - (si.ms_ticks/1000), GETDATE()) END >= DATEADD(hh, -12, GETDATE())
	ORDER BY 2 DESC;
END
ELSE
BEGIN
	SELECT 'Memory_checks' AS [Category], 'OOM_Notifications' AS [Information], '[OK]' AS Comment
END;

--------------------------------------------------------------------------------------------------------------------------------
-- LPIM subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'  |-Starting LPIM', 10, 1) WITH NOWAIT
DECLARE @lpim bit, @lognumber int, @logcount int
IF @sqlmajorver > 9
BEGIN
	SET @sqlcmd = N'SELECT @lpimOUT = CASE WHEN locked_page_allocations_kb > 0 THEN 1 ELSE 0 END FROM sys.dm_os_process_memory (NOLOCK)'
	SET @params = N'@lpimOUT bit OUTPUT';
	EXECUTE sp_executesql @sqlcmd, @params, @lpimOUT=@lpim OUTPUT
END
ELSE IF @sqlmajorver = 9
BEGIN
	IF ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) = 1 -- Is sysadmin
		OR ISNULL(IS_SRVROLEMEMBER(N'securityadmin'), 0) = 1 -- Is securityadmin
		OR ((SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'sp_readerrorlog') > 0
			AND (SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_readerrorlog') > 0
			AND (SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_enumerrorlogs') > 0)
	BEGIN
		IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#lpimdbcc'))
		DROP TABLE #lpimdbcc;
		IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#lpimdbcc'))
		CREATE TABLE #lpimdbcc (logdate DATETIME, spid VARCHAR(50), logmsg VARCHAR(4000))

		IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#lpimavail_logs'))
		DROP TABLE #lpimavail_logs;
		IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#lpimavail_logs'))
		CREATE TABLE #lpimavail_logs (lognum int, logdate DATETIME, logsize int) 

		-- Get the number of available logs 
		INSERT INTO #lpimavail_logs 
		EXEC xp_enumerrorlogs 
		
		SELECT MIN(lognum) FROM #lpimavail_logs WHERE DATEADD(dd, DATEDIFF(dd, 0, logdate), 0) >= DATEADD(dd, DATEDIFF(dd, 0, '06/17/2013  11:58'), 0)

		SELECT @logcount = ISNULL(MAX(lognum),@lognumber) FROM #lpimavail_logs WHERE DATEADD(dd, DATEDIFF(dd, 0, logdate), 0) >= DATEADD(dd, DATEDIFF(dd, 0, @StartDate), 0)

		IF @lognumber IS NULL
		BEGIN
			SELECT @ErrorMessage = '[WARNING: Could not retrieve information about Locked pages usage in SQL Server 2005]'
			RAISERROR (@ErrorMessage, 16, 1);
		END
		ELSE
		WHILE @lognumber < @logcount 
		BEGIN
			-- Cycle through sql error logs (Cannot use Large Page Extensions:  lock memory privilege was not granted)
			SELECT @sqlcmd = 'EXEC master..sp_readerrorlog ' + CONVERT(VARCHAR(3),@lognumber) + ', 1, ''Using locked pages for buffer pool'''
			BEGIN TRY
				INSERT INTO #lpimdbcc (logdate, spid, logmsg) 
				EXECUTE (@sqlcmd);
			END TRY
			BEGIN CATCH
				SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
				SELECT @ErrorMessage = 'Errorlog based subsection - Error raised in TRY block 1. ' + ERROR_MESSAGE()
				RAISERROR (@ErrorMessage, 16, 1);
			END CATCH
			-- Next log 
			--SET @lognumber = @lognumber + 1 
			SELECT @lognumber = MIN(lognum) FROM #lpimavail_logs WHERE lognum > @lognumber
		END 

		IF (SELECT COUNT(*) FROM #lpimdbcc) > 0
		BEGIN
			SET @lpim = 1
		END
		ELSE IF (SELECT COUNT(*) FROM #lpimdbcc) = 0 AND @lognumber IS NOT NULL
		BEGIN
			SET @lpim = 0
		END;
		
		DROP TABLE #lpimavail_logs;
		DROP TABLE #lpimdbcc;
	END
	ELSE
	BEGIN
		RAISERROR('[WARNING: Only a sysadmin or securityadmin can run the "Locked_pages" check. Bypassing check]', 16, 1, N'permissions')
		RAISERROR('[WARNING: If not sysadmin or securityadmin, then user must be a granted EXECUTE permissions on the following sprocs to run checks: xp_enumerrorlogs and sp_readerrorlog. Bypassing check]', 16, 1, N'extended_sprocs')
		--RETURN
	END;
END

IF @lpim = 0 AND @winver < 6.0 AND @arch = 64
BEGIN
	SELECT 'Memory_checks' AS [Category], 'Locked_pages' AS [Check], '[WARNING: Locked pages are not in use by SQL Server. In a WS2003 x64 architecture it is recommended to enable LPIM]' AS [Deviation]
END
ELSE IF @lpim = 1 AND @winver < 6.0 AND @arch = 64
BEGIN
	SELECT 'Memory_checks' AS [Category], 'Locked_pages' AS [Check], '[INFORMATION: Locked pages are being used by SQL Server. This is recommended in a WS2003 x64 architecture]' AS [Deviation]
END
ELSE IF @lpim = 1 AND @winver >= 6.0 AND @arch = 64
BEGIN
	SELECT 'Memory_checks' AS [Category], 'Locked_pages' AS [Check], '[INFORMATION: Locked pages are being used by SQL Server. This is recommended in WS2008 or above only when there are signs of paging]' AS [Deviation]
END
ELSE IF @lpim IS NULL
BEGIN
	SELECT 'Memory_checks' AS [Category], 'Locked_pages' AS [Check], '[Could_not_retrieve_information]' AS [Deviation]
END
ELSE
BEGIN
	SELECT 'Memory_checks' AS [Category], 'Locked_pages' AS [Check], '[Not_used]' AS [Deviation]
END;

--------------------------------------------------------------------------------------------------------------------------------
-- Pagefile subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'|-Starting Pagefile Checks', 10, 1) WITH NOWAIT
DECLARE @pf_value tinyint--, @RegKey NVARCHAR(255)
DECLARE @pagefile bigint, @freepagefile bigint, @paged bigint
DECLARE @tbl_pf_value TABLE (Value VARCHAR(25), Data VARCHAR(50))

IF @sqlmajorver = 9
BEGIN
	SET @sqlcmd = N'SELECT @pagefileOUT = (t1.record.value(''(./Record/MemoryRecord/TotalPageFile)[1]'', ''bigint'')-t1.record.value(''(./Record/MemoryRecord/TotalPhysicalMemory)[1]'', ''bigint''))/1024,
	@freepagefileOUT = (t1.record.value(''(./Record/MemoryRecord/AvailablePageFile)[1]'', ''bigint'')-t1.record.value(''(./Record/MemoryRecord/AvailablePhysicalMemory)[1]'', ''bigint''))/1024,
	@pagedOUT = ((t1.record.value(''(./Record/MemoryRecord/TotalPageFile)[1]'', ''bigint'')-t1.record.value(''(./Record/MemoryRecord/AvailablePageFile)[1]'', ''bigint''))/t1.record.value(''(./Record/MemoryRecord/TotalPageFile)[1]'', ''bigint''))/1024
FROM (SELECT MAX([TIMESTAMP]) AS [TIMESTAMP], CONVERT(xml, record) AS record 
	FROM sys.dm_os_ring_buffers (NOLOCK)
	WHERE ring_buffer_type = N''RING_BUFFER_RESOURCE_MONITOR''
		AND record LIKE ''%RESOURCE_MEMPHYSICAL%''
	GROUP BY record) AS t1';
END
ELSE
BEGIN
	SET @sqlcmd = N'SELECT @pagefileOUT = (total_page_file_kb-total_physical_memory_kb)/1024, 
	@freepagefileOUT = (available_page_file_kb-available_physical_memory_kb)/1024, 
	@pagedOUT = ((total_page_file_kb-available_page_file_kb)/total_page_file_kb) 
FROM sys.dm_os_sys_memory (NOLOCK)';
END

SET @params = N'@pagefileOUT bigint OUTPUT, @freepagefileOUT bigint OUTPUT, @pagedOUT bigint OUTPUT';

EXECUTE sp_executesql @sqlcmd, @params, @pagefileOUT=@pagefile OUTPUT, @freepagefileOUT=@freepagefile OUTPUT, @pagedOUT=@paged OUTPUT;

IF (ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) = 1) OR ((SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_regread') = 1)
BEGIN
	BEGIN TRY
		SELECT @RegKey = N'System\CurrentControlSet\Control\Session Manager\Memory Management'
		INSERT INTO @tbl_pf_value
		EXEC master.sys.xp_regread N'HKEY_LOCAL_MACHINE', @RegKey, N'PagingFiles', NO_OUTPUT
	END TRY
	BEGIN CATCH
		SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
		SELECT @ErrorMessage = 'Pagefile subsection - Error raised in TRY block 1. ' + ERROR_MESSAGE()
		RAISERROR (@ErrorMessage, 16, 1);
	END CATCH
END
ELSE
BEGIN
	RAISERROR('[WARNING: Missing permissions for full "Pagefile" checks. Bypassing System managed pagefile check]', 16, 1, N'sysadmin')
	--RETURN
END;

IF (SELECT COUNT(*) FROM @tbl_pf_value) > 0 
BEGIN
	SELECT @pf_value = CASE WHEN (SELECT COUNT(*) FROM @tbl_pf_value WHERE Data = '') > 0 THEN 1
			WHEN (SELECT COUNT(*) FROM @tbl_pf_value WHERE Data = '?:\pagefile.sys') > 0 THEN 2
			WHEN (SELECT COUNT(*) FROM @tbl_pf_value WHERE Data LIKE '%:\pagefile.sys 0 0%') > 0 THEN 3
		ELSE 0 END
	FROM @tbl_pf_value

	SELECT 'Pagefile_checks' AS [Category], 'Pagefile_management' AS [Check], 
		CASE WHEN @pf_value = 1 THEN '[WARNING: No pagefile is configured]'
			WHEN @pf_value = 2 THEN '[WARNING: Pagefile is managed automatically on ALL drives]'
			WHEN @pf_value = 3 THEN '[WARNING: Pagefile is managed automatically]'
		ELSE '[OK]' END AS [Deviation]
END

SELECT 'Pagefile_checks' AS [Category], 'Pagefile_free_space' AS [Check],
	CASE WHEN @freepagefile <= 150 THEN '[WARNING: Pagefile free space is dangerously low. Please revise Pagefile settings]'
		WHEN (@freepagefile*100)/@pagefile <= 10 THEN '[WARNING: Less than 10 percent of Pagefile is available. Please revise Pagefile settings]'
		WHEN (@freepagefile*100)/@pagefile <= 30 THEN '[INFORMATION: Less than 30 percent of Pagefile is available]'
		ELSE '[OK]' END AS [Deviation], 
	@pagefile AS total_pagefile_MB, @freepagefile AS available_pagefile_MB;

SELECT 'Pagefile_checks' AS [Category], 'Pagefile_minimum_size' AS [Check],
	CASE WHEN @winver = '5.2' AND @arch = 64 AND @pagefile < 8192 THEN '[WARNING: Pagefile is smaller than 8GB on a WS2003 x64 system. Please revise Pagefile settings]'
		WHEN @winver = '5.2' AND @arch = 32 AND @pagefile < 2048 THEN '[WARNING: Pagefile is smaller than 2GB on a WS2003 x86 system. Please revise Pagefile settings]'
		WHEN @winver <> '5.2' THEN '[NA]'
		ELSE '[OK]' END AS [Deviation], 
	@pagefile AS total_pagefile_MB;
	
SELECT 'Pagefile_checks' AS [Category], 'Process_paged_out' AS [Check],
	CASE WHEN @paged > 0 THEN '[WARNING: Part of SQL Server process memory has been paged out. Please revise LPIM settings]'
		ELSE '[OK]' END AS [Deviation], 
	@paged AS paged_out_MB;

IF @ptochecks = 1
RAISERROR (N'|-Starting I/O Checks', 10, 1) WITH NOWAIT

--------------------------------------------------------------------------------------------------------------------------------
-- I/O stall in database files over 50% of cumulative sampled time or I/O latencies over 20ms in the last 5s subsection
-- io_stall refers to user processes waited for I/O. This number can be much greater than the sample_ms.
-- Might indicate that your I/O has insufficient service capabilities (HBA queue depths, reduced throughput, etc). 
--------------------------------------------------------------------------------------------------------------------------------
IF @ptochecks = 1
BEGIN
	RAISERROR (N'  |-Starting I/O Stall subsection (wait for 5s)', 10, 1) WITH NOWAIT

	DECLARE @mincol DATETIME, @maxcol DATETIME

	IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tmp_dm_io_virtual_file_stats'))
	DROP TABLE #tmp_dm_io_virtual_file_stats;
	IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tmp_dm_io_virtual_file_stats'))	
	CREATE TABLE [dbo].[#tmp_dm_io_virtual_file_stats]([retrieval_time] [datetime],database_id int, [file_id] int, [DBName] sysname, [logical_file_name] NVARCHAR(255), [type_desc] NVARCHAR(60), 
		[physical_location] NVARCHAR(260),[sample_ms] int,[num_of_reads] bigint,[num_of_bytes_read] bigint,[io_stall_read_ms] bigint,[num_of_writes] bigint,
		[num_of_bytes_written] bigint,[io_stall_write_ms] bigint,[io_stall] bigint,[size_on_disk_bytes] bigint,
		CONSTRAINT PK_dm_io_virtual_file_stats PRIMARY KEY CLUSTERED(database_id, [file_id], [retrieval_time]));

	IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblIOStall'))
	DROP TABLE #tblIOStall;
	IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblIOStall'))
	CREATE TABLE #tblIOStall (database_id int, [file_id] int, [DBName] sysname, [logical_file_name] NVARCHAR(255), [type_desc] NVARCHAR(60),
		[physical_location] NVARCHAR(260), size_on_disk_Mbytes int, num_of_reads bigint, num_of_writes bigint, num_of_KBytes_read bigint, num_of_KBytes_written bigint,
		io_stall_ms int, io_stall_read_ms int, io_stall_write_ms int, avg_read_latency_ms int, avg_write_latency_ms int, cumulative_io_stall_read_pct int, 
		cumulative_io_stall_write_pct int, cumulative_sample_HH int, io_stall_pct_of_cumulative_sample int, 		
		CONSTRAINT PK_IOStall PRIMARY KEY CLUSTERED(database_id, [file_id]));

	SELECT @mincol = GETDATE()

	INSERT INTO #tmp_dm_io_virtual_file_stats
	SELECT @mincol, f.database_id, f.[file_id], DB_NAME(f.database_id), f.name AS logical_file_name, f.type_desc, 
		CAST (CASE 
			-- Handle UNC paths (e.g. '\\fileserver\readonlydbs\dept_dw.ndf')
			WHEN LEFT (LTRIM (f.physical_name), 2) = '\\' 
				THEN LEFT (LTRIM (f.physical_name),CHARINDEX('\',LTRIM(f.physical_name),CHARINDEX('\',LTRIM(f.physical_name), 3) + 1) - 1)
				-- Handle local paths (e.g. 'C:\Program Files\...\master.mdf') 
				WHEN CHARINDEX('\', LTRIM(f.physical_name), 3) > 0 
				THEN UPPER(LEFT(LTRIM(f.physical_name), CHARINDEX ('\', LTRIM(f.physical_name), 3) - 1))
			ELSE f.physical_name
		END AS NVARCHAR(255)) AS physical_location,
		fs.[sample_ms],fs.[num_of_reads],fs.[num_of_bytes_read],fs.[io_stall_read_ms],fs.[num_of_writes],
		fs.[num_of_bytes_written],fs.[io_stall_write_ms],fs.[io_stall],fs.[size_on_disk_bytes]
	FROM sys.dm_io_virtual_file_stats (default, default) AS fs
	INNER JOIN sys.master_files AS f ON fs.database_id = f.database_id AND fs.[file_id] = f.[file_id]
	
	WAITFOR DELAY '00:00:05' -- wait 5s between pooling
	
	SELECT @maxcol = GETDATE()

	INSERT INTO #tmp_dm_io_virtual_file_stats
	SELECT @maxcol, f.database_id, f.[file_id], DB_NAME(f.database_id), f.name AS logical_file_name, f.type_desc, 
		CAST (CASE 
			-- Handle UNC paths (e.g. '\\fileserver\readonlydbs\dept_dw.ndf')
			WHEN LEFT (LTRIM (f.physical_name), 2) = '\\' 
				THEN LEFT (LTRIM (f.physical_name),CHARINDEX('\',LTRIM(f.physical_name),CHARINDEX('\',LTRIM(f.physical_name), 3) + 1) - 1)
				-- Handle local paths (e.g. 'C:\Program Files\...\master.mdf') 
				WHEN CHARINDEX('\', LTRIM(f.physical_name), 3) > 0 
				THEN UPPER(LEFT(LTRIM(f.physical_name), CHARINDEX ('\', LTRIM(f.physical_name), 3) - 1))
			ELSE f.physical_name
		END AS NVARCHAR(255)) AS physical_location,
		fs.[sample_ms],fs.[num_of_reads],fs.[num_of_bytes_read],fs.[io_stall_read_ms],fs.[num_of_writes],
		fs.[num_of_bytes_written],fs.[io_stall_write_ms],fs.[io_stall],fs.[size_on_disk_bytes]
	FROM sys.dm_io_virtual_file_stats (default, default) AS fs
	INNER JOIN sys.master_files AS f ON fs.database_id = f.database_id AND fs.[file_id] = f.[file_id]
	
	;WITH cteFileStats1 AS (SELECT database_id,[file_id],[DBName],[logical_file_name],[type_desc], 
			[physical_location],[sample_ms],[num_of_reads],[num_of_bytes_read],[io_stall_read_ms],[num_of_writes],
			[num_of_bytes_written],[io_stall_write_ms],[io_stall],[size_on_disk_bytes]
		FROM #tmp_dm_io_virtual_file_stats WHERE [retrieval_time] = @mincol),
		cteFileStats2 AS (SELECT database_id,[file_id],[DBName],[logical_file_name],[type_desc], 
			[physical_location],[sample_ms],[num_of_reads],[num_of_bytes_read],[io_stall_read_ms],[num_of_writes],
			[num_of_bytes_written],[io_stall_write_ms],[io_stall],[size_on_disk_bytes]
		FROM #tmp_dm_io_virtual_file_stats WHERE [retrieval_time] = @maxcol)
	INSERT INTO #tblIOStall
	SELECT t1.database_id, t1.[file_id], t1.[DBName], t1.logical_file_name, t1.type_desc, t1.physical_location,
		t1.size_on_disk_bytes/1024/1024 AS size_on_disk_Mbytes,
		(t2.num_of_reads-t1.num_of_reads) AS num_of_reads, 
		(t2.num_of_writes-t1.num_of_writes) AS num_of_writes,
		(t2.num_of_bytes_read-t1.num_of_bytes_read)/1024 AS num_of_KBytes_read,
		(t2.num_of_bytes_written-t1.num_of_bytes_written)/1024 AS num_of_KBytes_written,
		(t2.io_stall-t1.io_stall) AS io_stall_ms, 
		(t2.io_stall_read_ms-t1.io_stall_read_ms) AS io_stall_read_ms, 
		(t2.io_stall_write_ms-t1.io_stall_write_ms) AS io_stall_write_ms,
		((t2.io_stall_read_ms-t1.io_stall_read_ms) / (1.0 + (t2.num_of_reads-t1.num_of_reads))) AS avg_read_latency_ms,
		((t2.io_stall_write_ms-t1.io_stall_write_ms) / (1.0 + (t2.num_of_writes-t1.num_of_writes))) AS avg_write_latency_ms,
		((t2.io_stall_read_ms)*100)/(CASE WHEN t2.io_stall = 0 THEN 1 ELSE t2.io_stall END) AS cumulative_io_stall_read_pct, 
		((t2.io_stall_write_ms)*100)/(CASE WHEN t2.io_stall = 0 THEN 1 ELSE t2.io_stall END) AS cumulative_io_stall_write_pct,
		ABS((t2.sample_ms/1000)/60/60) AS cumulative_sample_HH, 
		((t2.io_stall/1000/60)*100)/(ABS((t2.sample_ms/1000)/60)) AS io_stall_pct_of_cumulative_sample
	FROM cteFileStats1 t1 INNER JOIN cteFileStats2 t2 ON t1.database_id = t2.database_id AND t1.[file_id] = t2.[file_id]
		
	IF (SELECT COUNT([logical_file_name]) FROM #tblIOStall WHERE io_stall_pct_of_cumulative_sample > 50) > 0
		OR (SELECT COUNT([logical_file_name]) FROM #tblIOStall WHERE avg_read_latency_ms >= 20) > 0
		OR (SELECT COUNT([logical_file_name]) FROM #tblIOStall WHERE avg_write_latency_ms >= 20) > 0
	BEGIN
		SELECT 'IO_checks' AS [Category], 'Stalled_IO' AS [Check], '[WARNING: Some database files have latencies >= 20ms in the last 5s or stall I/O exceeding 50 pct of cumulative sampled time. Review I/O related performance counters and storage-related configurations.]' AS [Deviation]
		SELECT 'IO_checks' AS [Category], 'Stalled_IO' AS [Information], [DBName] AS [Database_Name], [logical_file_name], [type_desc], avg_read_latency_ms, avg_write_latency_ms, 
		[physical_location], size_on_disk_Mbytes, num_of_reads AS physical_reads, num_of_writes AS physical_writes, 
		num_of_KBytes_read, num_of_KBytes_written, io_stall_ms, io_stall_read_ms, io_stall_write_ms,
		cumulative_io_stall_read_pct, cumulative_io_stall_write_pct, cumulative_sample_HH, io_stall_pct_of_cumulative_sample
		FROM #tblIOStall
		ORDER BY io_stall_pct_of_cumulative_sample DESC, avg_write_latency_ms DESC, avg_read_latency_ms DESC, [DBName], [type_desc], [logical_file_name]
	END
	ELSE
	BEGIN
		SELECT 'IO_checks' AS [Category], 'Stalled_IO' AS [Check], '[OK]' AS [Deviation]
		/*SELECT 'IO_checks' AS [Category], 'Stalled_IO' AS [Information], [DBName] AS [Database_Name], [logical_file_name], [type_desc], avg_read_latency_ms, avg_write_latency_ms, 
		[physical_location], size_on_disk_Mbytes, num_of_reads AS physical_reads, num_of_writes AS physical_writes, 
		num_of_KBytes_read, num_of_KBytes_written, io_stall_ms, io_stall_read_ms, io_stall_write_ms,
		cumulative_io_stall_read_pct, cumulative_io_stall_write_pct, cumulative_sample_HH, io_stall_pct_of_cumulative_sample
		FROM #tblIOStall
		ORDER BY [DBName], [type_desc], [logical_file_name]*/
	END;
END;

--------------------------------------------------------------------------------------------------------------------------------
-- Pending disk I/O Requests subsection
-- Indicate that your I/O has insufficient service capabilities (HBA queue depths, reduced throughput, etc). 
--------------------------------------------------------------------------------------------------------------------------------
IF @ptochecks = 1
BEGIN
	RAISERROR (N'  |-Starting Pending disk I/O Requests subsection (wait for a max of 5s)', 10, 1) WITH NOWAIT
	DECLARE @IOCnt tinyint
	IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblPendingIOReq'))
	DROP TABLE #tblPendingIOReq;
	IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblPendingIOReq'))
	CREATE TABLE #tblPendingIOReq (io_completion_request_address varbinary(8), io_handle varbinary(8), io_type VARCHAR(7), io_pending bigint, io_pending_ms_ticks bigint, scheduler_address varbinary(8));

	IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblPendingIO'))
	DROP TABLE #tblPendingIO;
	IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblPendingIO'))
	CREATE TABLE #tblPendingIO (database_id int, [file_id] int, [DBName] sysname, [logical_file_name] NVARCHAR(255), [type_desc] NVARCHAR(60),
		[physical_location] NVARCHAR(260), io_stall_min int, io_stall_read_min int, io_stall_write_min int, avg_read_latency_ms int,
		avg_write_latency_ms int, io_stall_read_pct int, io_stall_write_pct int, sampled_HH int, 
		io_stall_pct_of_overall_sample int, io_completion_request_address varbinary(8), io_handle varbinary(8), io_type VARCHAR(7), io_pending bigint, io_pending_ms_ticks bigint, scheduler_address varbinary(8),
		scheduler_id int, pending_disk_io_count int, work_queue_count bigint);

	SET @IOCnt = 1
	WHILE @IOCnt < 5
	BEGIN
		INSERT INTO #tblPendingIOReq
		SELECT io_completion_request_address, io_handle, io_type, io_pending, io_pending_ms_ticks, scheduler_address
		FROM sys.dm_io_pending_io_requests;

		IF (SELECT COUNT(io_pending) FROM #tblPendingIOReq WHERE io_type = 'disk') > 1
		BREAK

		WAITFOR DELAY '00:00:01' -- wait 1s between pooling

		SET @IOCnt = @IOCnt + 1
	END;

	IF (SELECT COUNT(io_pending) FROM #tblPendingIOReq WHERE io_type = 'disk') > 0
	BEGIN
		INSERT INTO #tblPendingIO
		SELECT DISTINCT f.database_id, f.[file_id], DB_NAME(f.database_id) AS database_name, f.name AS logical_file_name, f.type_desc, 
			CAST (CASE 
				-- Handle UNC paths (e.g. '\\fileserver\readonlydbs\dept_dw.ndf')
				WHEN LEFT (LTRIM (f.physical_name), 2) = '\\' 
					THEN LEFT (LTRIM (f.physical_name),CHARINDEX('\',LTRIM(f.physical_name),CHARINDEX('\',LTRIM(f.physical_name), 3) + 1) - 1)
					-- Handle local paths (e.g. 'C:\Program Files\...\master.mdf') 
					WHEN CHARINDEX('\', LTRIM(f.physical_name), 3) > 0 
					THEN UPPER(LEFT(LTRIM(f.physical_name), CHARINDEX ('\', LTRIM(f.physical_name), 3) - 1))
				ELSE f.physical_name
			END AS NVARCHAR(255)) AS physical_location,
			fs.io_stall/1000/60 AS io_stall_min, 
			fs.io_stall_read_ms/1000/60 AS io_stall_read_min, 
			fs.io_stall_write_ms/1000/60 AS io_stall_write_min,
			(fs.io_stall_read_ms / (1.0 + fs.num_of_reads)) AS avg_read_latency_ms,
			(fs.io_stall_write_ms / (1.0 + fs.num_of_writes)) AS avg_write_latency_ms,
			((fs.io_stall_read_ms/1000/60)*100)/(CASE WHEN fs.io_stall/1000/60 = 0 THEN 1 ELSE fs.io_stall/1000/60 END) AS io_stall_read_pct, 
			((fs.io_stall_write_ms/1000/60)*100)/(CASE WHEN fs.io_stall/1000/60 = 0 THEN 1 ELSE fs.io_stall/1000/60 END) AS io_stall_write_pct,
			ABS((fs.sample_ms/1000)/60/60) AS 'sample_HH', 
			((fs.io_stall/1000/60)*100)/(ABS((fs.sample_ms/1000)/60))AS 'io_stall_pct_of_overall_sample',
			pio.io_completion_request_address, pio.io_handle, pio.io_type, pio.io_pending,
			pio.io_pending_ms_ticks, pio.scheduler_address, os.scheduler_id, os.pending_disk_io_count, os.work_queue_count
		FROM #tblPendingIOReq AS pio 
		INNER JOIN sys.dm_io_virtual_file_stats (NULL,NULL) AS fs ON fs.file_handle = pio.io_handle
		INNER JOIN sys.dm_os_schedulers AS os ON pio.scheduler_address = os.scheduler_address
		INNER JOIN sys.master_files AS f ON fs.database_id = f.database_id AND fs.[file_id] = f.[file_id];
	END;

	IF (SELECT COUNT(io_pending) FROM #tblPendingIOReq WHERE io_type = 'disk') > 0
	BEGIN
		SELECT 'IO_checks' AS [Category], 'Pending_IO' AS [Check], '[WARNING: Pending disk I/O requests were found. Review I/O related performance counters and storage-related configurations]' AS [Deviation]
		SELECT 'IO_checks' AS [Category], 'Pending_IO' AS [Information], [DBName] AS [Database_Name], [logical_file_name], [type_desc], avg_read_latency_ms, avg_write_latency_ms, 
		io_stall_read_pct, io_stall_write_pct, sampled_HH, io_stall_pct_of_overall_sample, [physical_location], io_stall_min, io_stall_read_min, io_stall_write_min,
		io_completion_request_address, io_type, CASE WHEN io_pending = 1 THEN 'Pending_Context_Switching' ELSE 'Pending_WindowsOS' END AS io_pending_type,
		io_pending_ms_ticks, scheduler_address, scheduler_id, pending_disk_io_count, work_queue_count
		FROM #tblPendingIO
		ORDER BY scheduler_address, [DBName], [type_desc], [logical_file_name]
	END
	ELSE
	BEGIN
		SELECT 'IO_checks' AS [Category], 'Pending_IO' AS [Check], '[OK]' AS [Deviation]
	END;
END;

RAISERROR (N'|-Starting Server Checks', 10, 1) WITH NOWAIT

--------------------------------------------------------------------------------------------------------------------------------
-- Power plan subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'  |-Starting Power plan', 10, 1) WITH NOWAIT
DECLARE @planguid NVARCHAR(64), @powerkey NVARCHAR(255) 
--SELECT @powerkey = 'SOFTWARE\Microsoft\Windows\CurrentVersion\Explorer\ControlPanel\NameSpace\{025A5937-A6BE-4686-A844-36FE4BEC8B6D}'
--SELECT @powerkey = 'SYSTEM\CurrentControlSet\Control\Power\User\Default\PowerSchemes'
SELECT @powerkey = 'SYSTEM\CurrentControlSet\Control\Power\User\PowerSchemes'

IF @winver >= 6.0
BEGIN
	BEGIN TRY
		--EXEC master.sys.xp_regread N'HKEY_LOCAL_MACHINE', @powerkey, 'PreferredPlan', @planguid OUTPUT, NO_OUTPUT
		EXEC master.sys.xp_regread N'HKEY_LOCAL_MACHINE', @powerkey, 'ActivePowerScheme', @planguid OUTPUT, NO_OUTPUT
	END TRY
	BEGIN CATCH
		SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
		SELECT @ErrorMessage = 'Power plan subsection - Error raised in TRY block. ' + ERROR_MESSAGE()
		RAISERROR (@ErrorMessage, 16, 1);
	END CATCH
END

-- http://support.microsoft.com/kb/935799/en-us

IF @winver IS NULL 
BEGIN
	SELECT 'Server_checks' AS [Category], 'Current_Power_Plan' AS [Check], '[WARNING: Could not determine Windows version for check]' AS [Deviation]
END
ELSE IF @planguid IS NOT NULL AND @planguid <> '8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c'
BEGIN
	SELECT 'Server_checks' AS [Category], 'Current_Power_Plan' AS [Check], '[WARNING: The current power plan scheme is not recommended for database servers. Please reconfigure for High Performance mode]' AS [Deviation]
	SELECT 'Server_checks' AS [Category], 'Current_Power_Plan' AS [Information], CASE WHEN @planguid = '381b4222-f694-41f0-9685-ff5bb260df2e' THEN 'Balanced'
		WHEN @planguid = '8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c' THEN 'High Performance'
		WHEN @planguid = 'a1841308-3541-4fab-bc81-f71556f20b4a' THEN 'Power Saver'
		ELSE 'Other' END AS [Power_Plan]
END
ELSE IF @planguid IS NOT NULL AND @planguid = '8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c'
BEGIN
	SELECT 'Server_checks' AS [Category], 'Current_Power_Plan' AS [Check], '[OK]' AS [Deviation]
END;

--------------------------------------------------------------------------------------------------------------------------------
-- Disk Partition alignment offset < 64KB subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'  |-Starting Disk Partition alignment offset < 64KB', 10, 1) WITH NOWAIT
IF @allow_xpcmdshell = 1 AND (@psavail IS NOT NULL AND @psavail IN ('RemoteSigned','Unrestricted'))
BEGIN
	IF ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) = 1 -- Is sysadmin
		OR ((ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) <> 1 
			AND (SELECT COUNT(credential_id) FROM sys.credentials WHERE name = '##xp_cmdshell_proxy_account##') > 0) -- Is not sysadmin but proxy account exists
			AND (SELECT COUNT(l.name)
			FROM sys.server_permissions p JOIN sys.server_principals l 
			ON p.grantee_principal_id = l.principal_id
				AND p.class = 100 -- Server
				AND p.state IN ('G', 'W') -- Granted or Granted with Grant
				AND l.is_disabled = 0
				AND p.permission_name = 'ALTER SETTINGS'
				AND QUOTENAME(l.name) = QUOTENAME(USER_NAME())) = 0) -- Is not sysadmin but has alter settings permission
		OR ((ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) <> 1 
			AND ((SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_fileexist') > 0 AND
			(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_instance_regread') > 0 AND
			(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_regread') > 0 AND
			(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'sp_OAGetErrorInfo') > 0 AND
			(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'sp_OACreate') > 0 AND
			(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'sp_OADestroy') > 0 AND
			(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_cmdshell') > 0 AND
			(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_regenumvalues') > 0)))
	BEGIN
		DECLARE @diskpart int

		SELECT @sao = CAST([value] AS smallint) FROM sys.configurations WITH (NOLOCK) WHERE [name] = 'show advanced options'
		SELECT @xcmd = CAST([value] AS smallint) FROM sys.configurations WITH (NOLOCK) WHERE [name] = 'xp_cmdshell'
		SELECT @ole = CAST([value] AS smallint) FROM sys.configurations WITH (NOLOCK) WHERE [name] = 'Ole Automation Procedures'

		RAISERROR ('    |-Configuration options set for Disk partition alignment offset check', 10, 1) WITH NOWAIT
		IF @sao = 0
		BEGIN
			EXEC sp_configure 'show advanced options', 1; RECONFIGURE WITH OVERRIDE;
		END
		IF @xcmd = 0
		BEGIN
			EXEC sp_configure 'xp_cmdshell', 1; RECONFIGURE WITH OVERRIDE;
		END
		IF @ole = 0
		BEGIN
			EXEC sp_configure 'Ole Automation Procedures', 1; RECONFIGURE WITH OVERRIDE;
		END
		
		DECLARE @output_hw_tot_diskpart TABLE ([PS_OUTPUT] VARCHAR(2048));
		DECLARE @output_hw_format_diskpart TABLE ([volid] smallint IDENTITY(1,1), [HD_Partition] VARCHAR(50) NULL, StartingOffset bigint NULL)

		IF @custompath IS NULL
		BEGIN
			IF @sqlmajorver < 11
			BEGIN
				EXEC master..xp_instance_regread N'HKEY_LOCAL_MACHINE',N'Software\Microsoft\MSSQLServer\Setup',N'SQLPath', @path OUTPUT
				SET @path = @path + '\LOG'
			END
			ELSE
			BEGIN
				SET @sqlcmd = N'SELECT @pathOUT = LEFT([path], LEN([path])-1) FROM sys.dm_os_server_diagnostics_log_configurations';
				SET @params = N'@pathOUT NVARCHAR(2048) OUTPUT';
				EXECUTE sp_executesql @sqlcmd, @params, @pathOUT=@path OUTPUT;
			END

			-- Create COM object with FSO
			EXEC @OLEResult = master.dbo.sp_OACreate 'Scripting.FileSystemObject', @FSO OUT
			IF @OLEResult <> 0
			BEGIN
				EXEC sp_OAGetErrorInfo @FSO, @src OUT, @desc OUT
				SELECT @ErrorMessage = 'Error Creating COM Component 0x%x, %s, %s'
				RAISERROR (@ErrorMessage, 16, 1, @OLEResult, @src, @desc);
			END
			ELSE
			BEGIN
				EXEC @OLEResult = master.dbo.sp_OAMethod @FSO, 'FolderExists', @existout OUT, @path
				IF @OLEResult <> 0
				BEGIN
					EXEC sp_OAGetErrorInfo @FSO, @src OUT, @desc OUT
					SELECT @ErrorMessage = 'Error Calling FolderExists Method 0x%x, %s, %s'
					RAISERROR (@ErrorMessage, 16, 1, @OLEResult, @src, @desc);
				END
				ELSE
				BEGIN
					IF @existout <> 1
					BEGIN
						SET @path = CONVERT(NVARCHAR(500), SERVERPROPERTY('ErrorLogFileName'))
						SET @path = LEFT(@path,LEN(@path)-CHARINDEX('\', REVERSE(@path)))
					END 
				END
				EXEC @OLEResult = sp_OADestroy @FSO
			END
		END
		ELSE
		BEGIN
			SELECT @path = CASE WHEN @custompath LIKE '%\' THEN LEFT(@custompath, LEN(@custompath)-1) ELSE @custompath END
		END
			
		SET @FileName = @path + '\checkbp_diskpart_' + RTRIM(@server) + '.ps1'
				
		EXEC master.dbo.xp_fileexist @FileName, @existout out
		IF @existout = 0
		BEGIN -- Scan for local disks
			SET @Text1 = '[string] $serverName = ''localhost''
$partitions = Get-WmiObject -computername $serverName -query "SELECT * FROM Win32_DiskPartition"
foreach ($partition in $partitions)
{
[string] $diskpart = "{0}_{1};{2}" -f $partition.DiskIndex,$partition.Index,$partition.StartingOffset
Write-Output $diskpart
}
'
			EXEC @OLEResult = master.dbo.sp_OACreate 'Scripting.FileSystemObject', @FS OUT
			IF @OLEResult <> 0
			BEGIN
				EXEC sp_OAGetErrorInfo @FS, @src OUT, @desc OUT
				SELECT @ErrorMessage = 'Error Creating COM Component 0x%x, %s, %s'
				RAISERROR (@ErrorMessage, 16, 1, @OLEResult, @src, @desc);
			END

			--Open file
			EXEC @OLEResult = master.dbo.sp_OAMethod @FS, 'OpenTextFile', @FileID OUT, @FileName, 2, 1
			IF @OLEResult <> 0
			BEGIN
				EXEC sp_OAGetErrorInfo @FS, @src OUT, @desc OUT
				SELECT @ErrorMessage = 'Error Calling OpenTextFile Method 0x%x, %s, %s' + CHAR(10) + 'Could not create file ' + @FileName
				RAISERROR (@ErrorMessage, 16, 1, @OLEResult, @src, @desc);
			END
			ELSE
			BEGIN
				SELECT @ErrorMessage = '    |-Created file ' + @FileName
				RAISERROR (@ErrorMessage, 10, 1) WITH NOWAIT
			END

			--Write Text1
			EXEC @OLEResult = master.dbo.sp_OAMethod @FileID, 'WriteLine', NULL, @Text1
			IF @OLEResult <> 0
			BEGIN
				EXEC sp_OAGetErrorInfo @FS, @src OUT, @desc OUT
				SELECT @ErrorMessage = 'Error Calling WriteLine Method 0x%x, %s, %s' + CHAR(10) + 'Could not write to file ' + @FileName
				RAISERROR (@ErrorMessage, 16, 1, @OLEResult, @src, @desc);
			END

			EXEC @OLEResult = sp_OADestroy @FileID
			EXEC @OLEResult = sp_OADestroy @FS
		END
		ELSE
		BEGIN
			SELECT @ErrorMessage = '    |-Reusing file ' + @FileName
			RAISERROR (@ErrorMessage, 10, 1) WITH NOWAIT
		END
			
		IF @psver = 1
		BEGIN
			SET @CMD = 'powershell -NoLogo -NoProfile "' + @FileName + '" -ExecutionPolicy RemoteSigned'
		END
		ELSE
		BEGIN
			SET @CMD = 'powershell -NoLogo -NoProfile -File "' + @FileName + '" -ExecutionPolicy RemoteSigned'
		END;
		
		INSERT INTO @output_hw_tot_diskpart
		EXEC master.dbo.xp_cmdshell @CMD
			
		SET @CMD = 'del /Q "' + @FileName + '"'
		EXEC master.dbo.xp_cmdshell @CMD, NO_OUTPUT
		
		INSERT INTO @output_hw_format_diskpart ([HD_Partition],StartingOffset)
		SELECT LEFT(RTRIM([PS_OUTPUT]), CASE WHEN CHARINDEX(';', RTRIM([PS_OUTPUT])) = 0 THEN LEN(RTRIM([PS_OUTPUT])) ELSE CHARINDEX(';', RTRIM([PS_OUTPUT]))-1 END),
				RIGHT(RTRIM([PS_OUTPUT]), LEN(RTRIM([PS_OUTPUT]))-CASE WHEN CHARINDEX(';', RTRIM([PS_OUTPUT])) = 0 THEN LEN(RTRIM([PS_OUTPUT])) ELSE CHARINDEX(';', RTRIM([PS_OUTPUT])) END)
		FROM @output_hw_tot_diskpart
		WHERE [PS_OUTPUT] IS NOT NULL;
		
		SET @CMD2 = 'del ' + @FileName
		EXEC master.dbo.xp_cmdshell @CMD2, NO_OUTPUT;
					
		IF @xcmd = 0
		BEGIN
			EXEC sp_configure 'xp_cmdshell', 0; RECONFIGURE WITH OVERRIDE;
		END
		IF @ole = 0
		BEGIN
			EXEC sp_configure 'Ole Automation Procedures', 0; RECONFIGURE WITH OVERRIDE;
		END
		IF @sao = 0
		BEGIN
			EXEC sp_configure 'show advanced options', 0; RECONFIGURE WITH OVERRIDE;
		END;
					
		;WITH diskpartcte (StartingOffset) AS (
			SELECT StartingOffset
			FROM @output_hw_format_diskpart
			WHERE StartingOffset IS NOT NULL OR LEN(StartingOffset) > 0)
		SELECT @diskpart = CASE WHEN (SELECT COUNT(*) FROM diskpartcte) = 0 THEN NULL ELSE COUNT(cte1.[StartingOffset]) END
		FROM diskpartcte cte1
		WHERE cte1.[StartingOffset] < 65536;
		
		IF @diskpart > 0 AND @diskpart IS NOT NULL
		BEGIN
			SELECT 'Server_checks' AS [Category], 'Partition_Alignment' AS [Check], '[WARNING: Some disk partitions are not using a minimum recommended alignment offset of 64KB]' AS [Deviation]
			SELECT 'Server_checks' AS [Category], 'Partition_Alignment' AS [Information], LEFT(t1.[HD_Partition],LEN(t1.[HD_Partition])-CHARINDEX('_',t1.[HD_Partition])) AS HD_Volume, 
				RIGHT(t1.[HD_Partition],LEN(t1.[HD_Partition])-CHARINDEX('_',t1.[HD_Partition])) AS [HD_Partition], 
				(t1.StartingOffset/1024) AS [StartingOffset_KB]
			FROM @output_hw_format_diskpart t1
			WHERE t1.StartingOffset IS NOT NULL OR LEN(t1.StartingOffset) > 0
			ORDER BY t1.[HD_Partition]
			OPTION (RECOMPILE);
		END
		ELSE IF @diskpart IS NULL
		BEGIN
			SELECT 'Server_checks' AS [Category], 'Partition_Alignment' AS [Check], '[WARNING: Could not gather information on disk partition offset size]' AS [Deviation]
		END
		ELSE
		BEGIN
			SELECT 'Server_checks' AS [Category], 'Partition_Alignment' AS [Check], '[OK]' AS [Deviation]
		END;
	END
	ELSE
	BEGIN
		RAISERROR('[WARNING: Only a sysadmin can run the "partition alignment offset" checks. A regular user can also run this check if a xp_cmdshell proxy account exists. Bypassing check]', 16, 1, N'xp_cmdshellproxy')
		RAISERROR('[WARNING: If not sysadmin, then must be a granted EXECUTE permissions on the following extended sprocs to run checks: sp_OACreate, sp_OADestroy, sp_OAGetErrorInfo, xp_cmdshell, xp_instance_regread, xp_regread, xp_fileexist and xp_regenumvalues. Bypassing check]', 16, 1, N'extended_sprocs')
		--RETURN
	END
END
ELSE
BEGIN
	RAISERROR('    |- [INFORMATION: "partition alignment offset" check was skipped: either xp_cmdshell or execution of PS scripts was not allowed.]', 10, 1, N'disallow_xp_cmdshell')
	--RETURN
END;

--------------------------------------------------------------------------------------------------------------------------------
-- NTFS block size in volumes that hold database files <> 64KB subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'  |-Starting NTFS block size in volumes that hold database files <> 64KB', 10, 1) WITH NOWAIT
IF @allow_xpcmdshell = 1 AND (@psavail IS NOT NULL AND @psavail IN ('RemoteSigned','Unrestricted'))
BEGIN
	IF ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) = 1 -- Is sysadmin
		OR ((ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) <> 1 
			AND (SELECT COUNT(credential_id) FROM sys.credentials WHERE name = '##xp_cmdshell_proxy_account##') > 0) -- Is not sysadmin but proxy account exists
			AND (SELECT COUNT(l.name)
			FROM sys.server_permissions p JOIN sys.server_principals l 
			ON p.grantee_principal_id = l.principal_id
				AND p.class = 100 -- Server
				AND p.state IN ('G', 'W') -- Granted or Granted with Grant
				AND l.is_disabled = 0
				AND p.permission_name = 'ALTER SETTINGS'
				AND QUOTENAME(l.name) = QUOTENAME(USER_NAME())) = 0) -- Is not sysadmin but has alter settings permission
		OR ((ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) <> 1 
			AND ((SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_fileexist') > 0 AND
			(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_instance_regread') > 0 AND
			(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_regread') > 0 AND
			(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'sp_OAGetErrorInfo') > 0 AND
			(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'sp_OACreate') > 0 AND
			(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'sp_OADestroy') > 0 AND
			(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_cmdshell') > 0 AND
			(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_regenumvalues') > 0)))
	BEGIN
		DECLARE @ntfs int

		SELECT @sao = CAST([value] AS smallint) FROM sys.configurations WITH (NOLOCK) WHERE [name] = 'show advanced options'
		SELECT @xcmd = CAST([value] AS smallint) FROM sys.configurations WITH (NOLOCK) WHERE [name] = 'xp_cmdshell'
		SELECT @ole = CAST([value] AS smallint) FROM sys.configurations WITH (NOLOCK) WHERE [name] = 'Ole Automation Procedures'

		RAISERROR ('    |-Configuration options set for NTFS Block size check', 10, 1) WITH NOWAIT
		IF @sao = 0
		BEGIN
			EXEC sp_configure 'show advanced options', 1; RECONFIGURE WITH OVERRIDE;
		END
		IF @xcmd = 0
		BEGIN
			EXEC sp_configure 'xp_cmdshell', 1; RECONFIGURE WITH OVERRIDE;
		END
		IF @ole = 0
		BEGIN
			EXEC sp_configure 'Ole Automation Procedures', 1; RECONFIGURE WITH OVERRIDE;
		END

		DECLARE @output_hw_tot_ntfs TABLE ([PS_OUTPUT] VARCHAR(2048));
		DECLARE @output_hw_format_ntfs TABLE ([volid] smallint IDENTITY(1,1), [HD_Volume] NVARCHAR(2048) NULL, [NTFS_Block] NVARCHAR(8) NULL)

		IF @custompath IS NULL
		BEGIN
			IF @sqlmajorver < 11
			BEGIN
				EXEC master..xp_instance_regread N'HKEY_LOCAL_MACHINE',N'Software\Microsoft\MSSQLServer\Setup',N'SQLPath', @path OUTPUT
				SET @path = @path + '\LOG'
			END
			ELSE
			BEGIN
				SET @sqlcmd = N'SELECT @pathOUT = LEFT([path], LEN([path])-1) FROM sys.dm_os_server_diagnostics_log_configurations';
				SET @params = N'@pathOUT NVARCHAR(2048) OUTPUT';
				EXECUTE sp_executesql @sqlcmd, @params, @pathOUT=@path OUTPUT;
			END

			-- Create COM object with FSO
			EXEC @OLEResult = master.dbo.sp_OACreate 'Scripting.FileSystemObject', @FSO OUT
			IF @OLEResult <> 0
			BEGIN
				EXEC sp_OAGetErrorInfo @FSO, @src OUT, @desc OUT
				SELECT @ErrorMessage = 'Error Creating COM Component 0x%x, %s, %s'
				RAISERROR (@ErrorMessage, 16, 1, @OLEResult, @src, @desc);
			END
			ELSE
			BEGIN
				EXEC @OLEResult = master.dbo.sp_OAMethod @FSO, 'FolderExists', @existout OUT, @path
				IF @OLEResult <> 0
				BEGIN
					EXEC sp_OAGetErrorInfo @FSO, @src OUT, @desc OUT
					SELECT @ErrorMessage = 'Error Calling FolderExists Method 0x%x, %s, %s'
					RAISERROR (@ErrorMessage, 16, 1, @OLEResult, @src, @desc);
				END
				ELSE
				BEGIN
					IF @existout <> 1
					BEGIN
						SET @path = CONVERT(NVARCHAR(500), SERVERPROPERTY('ErrorLogFileName'))
						SET @path = LEFT(@path,LEN(@path)-CHARINDEX('\', REVERSE(@path)))
					END 
				END
				EXEC @OLEResult = sp_OADestroy @FSO
			END
		END
		ELSE
		BEGIN
			SELECT @path = CASE WHEN @custompath LIKE '%\' THEN LEFT(@custompath, LEN(@custompath)-1) ELSE @custompath END
		END
			
		SET @FileName = @path + '\checkbp_ntfs_' + RTRIM(@server) + '.ps1'
				
		EXEC master.dbo.xp_fileexist @FileName, @existout out
		IF @existout = 0
		BEGIN -- Scan for local disks
			SET @Text1 = '[string] $serverName = ''localhost''
$vols = Get-WmiObject -computername $serverName -query "select name, blocksize from Win32_Volume where Capacity <> NULL and DriveType = 3"
foreach($vol in $vols)
{
[string] $drive = "{0};{1}" -f $vol.name,$vol.blocksize
Write-Output $drive
} '
			EXEC @OLEResult = master.dbo.sp_OACreate 'Scripting.FileSystemObject', @FS OUT
			IF @OLEResult <> 0
			BEGIN
				EXEC sp_OAGetErrorInfo @FS, @src OUT, @desc OUT
				SELECT @ErrorMessage = 'Error Creating COM Component 0x%x, %s, %s'
				RAISERROR (@ErrorMessage, 16, 1, @OLEResult, @src, @desc);
			END

			--Open file
			EXEC @OLEResult = master.dbo.sp_OAMethod @FS, 'OpenTextFile', @FileID OUT, @FileName, 2, 1
			IF @OLEResult <> 0
			BEGIN
				EXEC sp_OAGetErrorInfo @FS, @src OUT, @desc OUT
				SELECT @ErrorMessage = 'Error Calling OpenTextFile Method 0x%x, %s, %s' + CHAR(10) + 'Could not create file ' + @FileName
				RAISERROR (@ErrorMessage, 16, 1, @OLEResult, @src, @desc);
			END
			ELSE
			BEGIN
				SELECT @ErrorMessage = '    |-Created file ' + @FileName
				RAISERROR (@ErrorMessage, 10, 1) WITH NOWAIT
			END

			--Write Text1
			EXEC @OLEResult = master.dbo.sp_OAMethod @FileID, 'WriteLine', NULL, @Text1
			IF @OLEResult <> 0
			BEGIN
				EXEC sp_OAGetErrorInfo @FS, @src OUT, @desc OUT
				SELECT @ErrorMessage = 'Error Calling WriteLine Method 0x%x, %s, %s' + CHAR(10) + 'Could not write to file ' + @FileName
				RAISERROR (@ErrorMessage, 16, 1, @OLEResult, @src, @desc);
			END

			EXEC @OLEResult = sp_OADestroy @FileID
			EXEC @OLEResult = sp_OADestroy @FS
		END
		ELSE
		BEGIN
			SELECT @ErrorMessage = '    |-Reusing file ' + @FileName
			RAISERROR (@ErrorMessage, 10, 1) WITH NOWAIT
		END

		IF @psver = 1
		BEGIN
			SET @CMD = 'powershell -NoLogo -NoProfile "' + @FileName + '" -ExecutionPolicy RemoteSigned'
		END
		ELSE
		BEGIN
			SET @CMD = 'powershell -NoLogo -NoProfile -File "' + @FileName + '" -ExecutionPolicy RemoteSigned'
		END;

		INSERT INTO @output_hw_tot_ntfs
		EXEC master.dbo.xp_cmdshell @CMD

		SET @CMD = 'del /Q "' + @FileName + '"'
		EXEC master.dbo.xp_cmdshell @CMD, NO_OUTPUT
		
		INSERT INTO @output_hw_format_ntfs ([HD_Volume],[NTFS_Block])
		SELECT LEFT(RTRIM([PS_OUTPUT]), CASE WHEN CHARINDEX(';', RTRIM([PS_OUTPUT])) = 0 THEN LEN(RTRIM([PS_OUTPUT])) ELSE CHARINDEX(';', RTRIM([PS_OUTPUT]))-1 END),
				RIGHT(RTRIM([PS_OUTPUT]), LEN(RTRIM([PS_OUTPUT]))-CASE WHEN CHARINDEX(';', RTRIM([PS_OUTPUT])) = 0 THEN LEN(RTRIM([PS_OUTPUT])) ELSE CHARINDEX(';', RTRIM([PS_OUTPUT])) END)
		FROM @output_hw_tot_ntfs
		WHERE [PS_OUTPUT] IS NOT NULL;
		
		SET @CMD2 = 'del ' + @FileName
		EXEC master.dbo.xp_cmdshell @CMD2, NO_OUTPUT;
			
		IF @xcmd = 0
		BEGIN
			EXEC sp_configure 'xp_cmdshell', 0; RECONFIGURE WITH OVERRIDE;
		END
		IF @ole = 0
		BEGIN
			EXEC sp_configure 'Ole Automation Procedures', 0; RECONFIGURE WITH OVERRIDE;
		END
		IF @sao = 0
		BEGIN
			EXEC sp_configure 'show advanced options', 0; RECONFIGURE WITH OVERRIDE;
		END;
			
		WITH ntfscte (physical_name, ntfsblock) AS (
			SELECT DISTINCT(LEFT(physical_name, LEN(t2.HD_Volume))), [NTFS_Block]
			FROM sys.master_files t1 INNER JOIN @output_hw_format_ntfs t2
			ON LEFT(physical_name, LEN(t2.HD_Volume)) = t2.HD_Volume
			WHERE [database_id] <> 32767 AND (t2.[NTFS_Block] IS NOT NULL OR LEN(t2.[NTFS_Block]) > 0)
		)
		SELECT @ntfs = CASE WHEN (SELECT COUNT(*) FROM ntfscte) = 0 THEN NULL ELSE COUNT(cte1.[ntfsblock]) END
		FROM ntfscte cte1
		WHERE cte1.[ntfsblock] <> 65536;
		
		IF @ntfs > 0 AND @ntfs IS NOT NULL
		BEGIN
			SELECT 'Server_checks' AS [Category], 'NTFS_Block_Size' AS [Check], '[WARNING: Some volumes that hold database files are not formatted using the recommended NTFS block size of 64KB]' AS [Deviation]
			SELECT 'Server_checks' AS [Category], 'NTFS_Block_Size' AS [Information], t1.HD_Volume, (t1.[NTFS_Block]/1024) AS [NTFS_Block_Size_KB]
			FROM (SELECT DISTINCT(LEFT(physical_name, LEN(t2.HD_Volume))) AS [HD_Volume], [NTFS_Block]
				FROM sys.master_files t1 (NOLOCK) INNER JOIN @output_hw_format_ntfs t2
					ON LEFT(physical_name, LEN(t2.HD_Volume)) = t2.HD_Volume
					WHERE [database_id] <> 32767 AND (t2.[NTFS_Block] IS NOT NULL OR LEN(t2.[NTFS_Block]) > 0)) t1
			ORDER BY t1.HD_Volume OPTION (RECOMPILE);
		END
		ELSE IF @ntfs IS NULL
		BEGIN
			SELECT 'Server_checks' AS [Category], 'NTFS_Block_Size' AS [Check], '[WARNING: Could not gather information on NTFS block size]' AS [Deviation]
		END
		ELSE
		BEGIN
			SELECT 'Server_checks' AS [Category], 'NTFS_Block_Size' AS [Check], '[OK]' AS [Deviation]
		END;
	END
	ELSE
	BEGIN
		RAISERROR('[WARNING: Only a sysadmin can run the "NTFS block size" checks. A regular user can also run this check if a xp_cmdshell proxy account exists. Bypassing check]', 16, 1, N'xp_cmdshellproxy')
		RAISERROR('[WARNING: If not sysadmin, then must be a granted EXECUTE permissions on the following extended sprocs to run checks: sp_OACreate, sp_OADestroy, sp_OAGetErrorInfo, xp_cmdshell, xp_instance_regread, xp_regread, xp_fileexist and xp_regenumvalues. Bypassing check]', 16, 1, N'extended_sprocs')
		--RETURN
	END
	END
ELSE
BEGIN
	RAISERROR('    |- [INFORMATION: "NTFS block size" check was skipped: either xp_cmdshell or execution of PS scripts was not allowed.]', 10, 1, N'disallow_xp_cmdshell')
	--RETURN
END;

--------------------------------------------------------------------------------------------------------------------------------
-- Disk Fragmentation Analysis subsection
--------------------------------------------------------------------------------------------------------------------------------
IF @diskfrag = 1
BEGIN
	RAISERROR (N'  |-Starting Disk Fragmentation Analysis', 10, 1) WITH NOWAIT
	IF @allow_xpcmdshell = 1 AND (@psavail IS NOT NULL AND @psavail IN ('RemoteSigned','Unrestricted'))
	BEGIN
		IF ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) = 1 -- Is sysadmin
			OR ((ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) <> 1 
				AND (SELECT COUNT(credential_id) FROM sys.credentials WHERE name = '##xp_cmdshell_proxy_account##') > 0) -- Is not sysadmin but proxy account exists
				AND (SELECT COUNT(l.name)
				FROM sys.server_permissions p JOIN sys.server_principals l 
				ON p.grantee_principal_id = l.principal_id
					AND p.class = 100 -- Server
					AND p.state IN ('G', 'W') -- Granted or Granted with Grant
					AND l.is_disabled = 0
					AND p.permission_name = 'ALTER SETTINGS'
					AND QUOTENAME(l.name) = QUOTENAME(USER_NAME())) = 0) -- Is not sysadmin but has alter settings permission
			OR ((ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) <> 1 
				AND ((SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_fileexist') > 0 AND
				(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_instance_regread') > 0 AND
				(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_regread') > 0 AND
				(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'sp_OAGetErrorInfo') > 0 AND
				(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'sp_OACreate') > 0 AND
				(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'sp_OADestroy') > 0 AND
				(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_cmdshell') > 0 AND
				(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_regenumvalues') > 0)))
		BEGIN
			DECLARE @frag int
		
			SELECT @sao = CAST([value] AS smallint) FROM sys.configurations WITH (NOLOCK) WHERE [name] = 'show advanced options'
			SELECT @xcmd = CAST([value] AS smallint) FROM sys.configurations WITH (NOLOCK) WHERE [name] = 'xp_cmdshell'
			SELECT @ole = CAST([value] AS smallint) FROM sys.configurations WITH (NOLOCK) WHERE [name] = 'Ole Automation Procedures'

			RAISERROR ('    |-Configuration options set for Disk Fragmentation Analysis', 10, 1) WITH NOWAIT

			IF @sao = 0
			BEGIN
				EXEC sp_configure 'show advanced options', 1; RECONFIGURE WITH OVERRIDE;
			END
			IF @xcmd = 0
			BEGIN
				EXEC sp_configure 'xp_cmdshell', 1; RECONFIGURE WITH OVERRIDE;
			END
			IF @ole = 0
			BEGIN
				EXEC sp_configure 'Ole Automation Procedures', 1; RECONFIGURE WITH OVERRIDE;
			END
		
			DECLARE @output_hw_frag TABLE ([PS_OUTPUT] VARCHAR(2048));
			DECLARE @output_hw_format_frag TABLE ([volid] smallint IDENTITY(1,1), [volfrag] VARCHAR(255), [fragrec] VARCHAR(10) NULL)

			IF @custompath IS NULL
			BEGIN
				IF @sqlmajorver < 11
				BEGIN
					EXEC master..xp_instance_regread N'HKEY_LOCAL_MACHINE',N'Software\Microsoft\MSSQLServer\Setup',N'SQLPath', @path OUTPUT
					SET @path = @path + '\LOG'
				END
				ELSE
				BEGIN
					SET @sqlcmd = N'SELECT @pathOUT = LEFT([path], LEN([path])-1) FROM sys.dm_os_server_diagnostics_log_configurations';
					SET @params = N'@pathOUT NVARCHAR(2048) OUTPUT';
					EXECUTE sp_executesql @sqlcmd, @params, @pathOUT=@path OUTPUT;
				END

				-- Create COM object with FSO
				EXEC @OLEResult = master.dbo.sp_OACreate 'Scripting.FileSystemObject', @FSO OUT
				IF @OLEResult <> 0
				BEGIN
					EXEC sp_OAGetErrorInfo @FSO, @src OUT, @desc OUT
					SELECT @ErrorMessage = 'Error Creating COM Component 0x%x, %s, %s'
					RAISERROR (@ErrorMessage, 16, 1, @OLEResult, @src, @desc);
				END
				ELSE
				BEGIN
					EXEC @OLEResult = master.dbo.sp_OAMethod @FSO, 'FolderExists', @existout OUT, @path
					IF @OLEResult <> 0
					BEGIN
						EXEC sp_OAGetErrorInfo @FSO, @src OUT, @desc OUT
						SELECT @ErrorMessage = 'Error Calling FolderExists Method 0x%x, %s, %s'
						RAISERROR (@ErrorMessage, 16, 1, @OLEResult, @src, @desc);
					END
					ELSE
					BEGIN
						IF @existout <> 1
						BEGIN
							SET @path = CONVERT(NVARCHAR(500), SERVERPROPERTY('ErrorLogFileName'))
							SET @path = LEFT(@path,LEN(@path)-CHARINDEX('\', REVERSE(@path)))
						END 
					END
					EXEC @OLEResult = sp_OADestroy @FSO
				END
			END
			ELSE
			BEGIN
				SELECT @path = CASE WHEN @custompath LIKE '%\' THEN LEFT(@custompath, LEN(@custompath)-1) ELSE @custompath END
			END
			
			SET @FileName = @path + '\checkbp_frag_' + RTRIM(@server) + '.ps1'
				
			EXEC master.dbo.xp_fileexist @FileName, @existout out
			IF @existout = 0
			BEGIN -- Scan for frag
				SET @Text1 = '$myWindowsID=[System.Security.Principal.WindowsIdentity]::GetCurrent()
$myWindowsPrincipal=new-object System.Security.Principal.WindowsPrincipal($myWindowsID)
$adminRole=[System.Security.Principal.WindowsBuiltInRole]::Administrator
if ($myWindowsPrincipal.IsInRole($adminRole))
{
	[string] $serverName = ''localhost''
	$DiskResults = @()
	$objDisks = Get-WmiObject -Computername $serverName -Class Win32_Volume | Where-Object { $_.DriveType -eq 3 -and $_.Name -like "*:\"}
	ForEach( $disk in $objDisks)
	{
		$objDefrag = $disk.DefragAnalysis()
		$rec = $objDefrag.DefragRecommended
		$objDefragDetail = $objDefrag.DefragAnalysis
		$diskFragmentation = $objDefragDetail.TotalPercentFragmentation
		$FreeFragmentation = $objDefragDetail.FreeSpacePercentFragmentation
		$FileFragmentation = $objDefragDetail.FilePercentFragmentation

		[string] $ThisVolume = "{0}TotalFragPct {1} :: FreeSpaceFragPct {2} :: FileFragPct {3};{4}" -f $($disk.Name),$diskFragmentation,$FreeFragmentation,$FileFragmentation,$rec
		$DiskResults += $ThisVolume
	}
	$DiskResults
}
else
{
	Write-Host "NotAdmin"
}
'
				EXEC @OLEResult = master.dbo.sp_OACreate 'Scripting.FileSystemObject', @FS OUT
				IF @OLEResult <> 0
				BEGIN
					EXEC sp_OAGetErrorInfo @FS, @src OUT, @desc OUT
					SELECT @ErrorMessage = 'Error Creating COM Component 0x%x, %s, %s'
					RAISERROR (@ErrorMessage, 16, 1, @OLEResult, @src, @desc);
				END

				--Open file
				EXEC @OLEResult = master.dbo.sp_OAMethod @FS, 'OpenTextFile', @FileID OUT, @FileName, 2, 1
				IF @OLEResult <> 0
				BEGIN
					EXEC sp_OAGetErrorInfo @FS, @src OUT, @desc OUT
					SELECT @ErrorMessage = 'Error Calling OpenTextFile Method 0x%x, %s, %s' + CHAR(10) + 'Could not create file ' + @FileName
					RAISERROR (@ErrorMessage, 16, 1, @OLEResult, @src, @desc);
				END
				ELSE
				BEGIN
					SELECT @ErrorMessage = '    |-Created file ' + @FileName
					RAISERROR (@ErrorMessage, 10, 1) WITH NOWAIT
				END

				--Write Text1
				EXEC @OLEResult = master.dbo.sp_OAMethod @FileID, 'WriteLine', NULL, @Text1
				IF @OLEResult <> 0
				BEGIN
					EXEC sp_OAGetErrorInfo @FS, @src OUT, @desc OUT
					SELECT @ErrorMessage = 'Error Calling WriteLine Method 0x%x, %s, %s' + CHAR(10) + 'Could not write to file ' + @FileName
					RAISERROR (@ErrorMessage, 16, 1, @OLEResult, @src, @desc);
				END

				EXEC @OLEResult = sp_OADestroy @FileID
				EXEC @OLEResult = sp_OADestroy @FS
			END
			ELSE
			BEGIN
				SELECT @ErrorMessage = '    |-Reusing file ' + @FileName
				RAISERROR (@ErrorMessage, 10, 1) WITH NOWAIT
			END
			
			RAISERROR ('    |-Getting Disk(s) Fragmentation. This may take some time...', 10, 1) WITH NOWAIT

			IF @psver = 1
			BEGIN
				SET @CMD = 'powershell -NoLogo -NoProfile "' + @FileName + '" -ExecutionPolicy RemoteSigned'
			END
			ELSE
			BEGIN
				SET @CMD = 'powershell -NoLogo -NoProfile -File "' + @FileName + '" -ExecutionPolicy RemoteSigned'
			END;

			INSERT INTO @output_hw_frag
			EXEC master.dbo.xp_cmdshell @CMD
			
			SET @CMD = 'del /Q "' + @FileName + '"'
			EXEC master.dbo.xp_cmdshell @CMD, NO_OUTPUT

			IF (SELECT COUNT([PS_OUTPUT]) FROM @output_hw_frag WHERE [PS_OUTPUT] LIKE '%NotAdmin%') = 1
			BEGIN
				RAISERROR ('[WARNING: Powershell not running under Elevated Privileges. Bypassing Disk Fragmentation Analysis]',16,1);
			END
			ELSE
			BEGIN
				INSERT INTO @output_hw_format_frag ([volfrag],fragrec)
				SELECT LEFT(RTRIM([PS_OUTPUT]), CASE WHEN CHARINDEX(';', RTRIM([PS_OUTPUT])) = 0 THEN LEN(RTRIM([PS_OUTPUT])) ELSE CHARINDEX(';', RTRIM([PS_OUTPUT]))-1 END),
						RIGHT(RTRIM([PS_OUTPUT]), LEN(RTRIM([PS_OUTPUT]))-CASE WHEN CHARINDEX(';', RTRIM([PS_OUTPUT])) = 0 THEN LEN(RTRIM([PS_OUTPUT])) ELSE CHARINDEX(';', RTRIM([PS_OUTPUT])) END)
				FROM @output_hw_frag
				WHERE [PS_OUTPUT] IS NOT NULL
			END
		
			SET @CMD2 = 'del ' + @FileName
			EXEC master.dbo.xp_cmdshell @CMD2, NO_OUTPUT;
			
			IF @xcmd = 0
			BEGIN
				EXEC sp_configure 'xp_cmdshell', 0; RECONFIGURE WITH OVERRIDE;
			END
			IF @ole = 0
			BEGIN
				EXEC sp_configure 'Ole Automation Procedures', 0; RECONFIGURE WITH OVERRIDE;
			END
			IF @sao = 0
			BEGIN
				EXEC sp_configure 'show advanced options', 0; RECONFIGURE WITH OVERRIDE;
			END;

			;WITH fragcte (fragrec) AS (
				SELECT fragrec
				FROM @output_hw_format_frag
				WHERE fragrec IS NOT NULL OR LEN(fragrec) > 0)
			SELECT @frag = CASE WHEN (SELECT COUNT(*) FROM fragcte) = 0 THEN NULL ELSE COUNT(cte1.[fragrec]) END
			FROM fragcte cte1
			WHERE cte1.[fragrec] = 'True';
		
			IF @frag > 0 AND @frag IS NOT NULL
			BEGIN
				SELECT 'Server_checks' AS [Category], 'Disk_Fragmentation' AS [Check], '[WARNING: Found volumes with physical fragmentation. Determine how and when these can be defragmented]' AS [Deviation]
				SELECT 'Server_checks' AS [Category], 'Disk_Fragmentation' AS [Information], 
					LEFT(t1.[volfrag],1) AS HD_Volume, 
					RIGHT(t1.[volfrag],(LEN(t1.[volfrag])-3)) AS [Fragmentation_Percent], 
					t1.fragrec AS [Defragmentation_Recommended]
				FROM @output_hw_format_frag t1
				WHERE t1.fragrec = 'True'
				ORDER BY t1.[volfrag]
				OPTION (RECOMPILE);
			END
			ELSE IF @frag IS NULL
			BEGIN
				SELECT 'Server_checks' AS [Category], 'Disk_Fragmentation' AS [Check], '[WARNING: Could not gather information on Disk Fragmentation Analysis]' AS [Deviation]
			END
			ELSE
			BEGIN
				SELECT 'Server_checks' AS [Category], 'Disk_Fragmentation' AS [Check], '[OK]' AS [Deviation]
				SELECT 'Server_checks' AS [Category], 'Disk_Fragmentation' AS [Information], 
					LEFT(t1.[volfrag],1) AS HD_Volume, 
					RIGHT(t1.[volfrag],(LEN(t1.[volfrag])-3)) AS [Fragmentation_Percent],
					t1.fragrec AS [Defragmentation_Recommended]
				FROM @output_hw_format_frag t1
				ORDER BY t1.[volfrag]
				OPTION (RECOMPILE);
			END;
		END
		ELSE
		BEGIN
			RAISERROR('[WARNING: Only a sysadmin can run the "Disk Fragmentation Analysis" checks. A regular user can also run this check if a xp_cmdshell proxy account exists. Bypassing check]', 16, 1, N'xp_cmdshellproxy')
			RAISERROR('[WARNING: If not sysadmin, then must be a granted EXECUTE permissions on the following extended sprocs to run checks: sp_OACreate, sp_OADestroy, sp_OAGetErrorInfo, xp_cmdshell, xp_instance_regread, xp_regread, xp_fileexist and xp_regenumvalues. Bypassing check]', 16, 1, N'extended_sprocs')
			--RETURN
		END
		END
	ELSE
	BEGIN
		RAISERROR('    |- [INFORMATION: "Disk Fragmentation Analysis" check was skipped: either xp_cmdshell or execution of PS scripts was not allowed]', 10, 1, N'disallow_xp_cmdshell')
		--RETURN
	END
END
ELSE
BEGIN
	RAISERROR('  |- [INFORMATION: "Disk Fragmentation Analysis" check is disabled]', 10, 1, N'disallow_diskfrag')
	--RETURN
END;

--------------------------------------------------------------------------------------------------------------------------------
-- Cluster Quorum Model subsection
--------------------------------------------------------------------------------------------------------------------------------
IF @clustered = 1 AND @winver <> '5.2'
BEGIN
	RAISERROR (N'  |-Starting Cluster Quorum Model', 10, 1) WITH NOWAIT
	IF @allow_xpcmdshell = 1 AND (@psavail IS NOT NULL AND @psavail IN ('RemoteSigned','Unrestricted')) AND @psver > 1
	BEGIN
		IF ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) = 1 -- Is sysadmin
			OR ((ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) <> 1 
				AND (SELECT COUNT(credential_id) FROM sys.credentials WHERE name = '##xp_cmdshell_proxy_account##') > 0)) -- Is not sysadmin but proxy account exists
			OR ((ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) <> 1 
				AND (SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_cmdshell') > 0))
		BEGIN
			SELECT @sao = CAST([value] AS smallint) FROM sys.configurations WITH (NOLOCK) WHERE [name] = 'show advanced options'
			SELECT @xcmd = CAST([value] AS smallint) FROM sys.configurations WITH (NOLOCK) WHERE [name] = 'xp_cmdshell'

			RAISERROR ('    |-Configuration options set for Cluster Quorum Model check', 10, 1) WITH NOWAIT
			IF @sao = 0
			BEGIN
				EXEC sp_configure 'show advanced options', 1; RECONFIGURE WITH OVERRIDE;
			END
			IF @xcmd = 0
			BEGIN
				EXEC sp_configure 'xp_cmdshell', 1; RECONFIGURE WITH OVERRIDE;
			END
			
			DECLARE /*@CMD NVARCHAR(4000), @line int, @linemax int, */ @CntNodes tinyint, @CntVotes tinyint
				
			IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#xp_cmdshell_CluNodesOutput'))
			DROP TABLE #xp_cmdshell_CluNodesOutput;
			IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#xp_cmdshell_CluNodesOutput'))
			CREATE TABLE #xp_cmdshell_CluNodesOutput (line int IDENTITY(1,1) PRIMARY KEY, [Output] VARCHAR(50));
				
			IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#xp_cmdshell_CluOutput'))
			DROP TABLE #xp_cmdshell_CluOutput;
			IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#xp_cmdshell_CluOutput'))
			CREATE TABLE #xp_cmdshell_CluOutput (line int IDENTITY(1,1) PRIMARY KEY, [Output] VARCHAR(50));

			IF @winver <> '5.2'
			BEGIN
				SELECT @CMD = N'powershell -NoLogo -NoProfile "Import-Module FailoverClusters"; "Get-ClusterNode | Format-Table -Autosize -HideTableHeaders NodeWeight"' 
				INSERT INTO #xp_cmdshell_CluNodesOutput ([Output])
				EXEC master.dbo.xp_cmdshell @CMD;
			END
				
			SELECT @CMD = N'powershell -NoLogo -NoProfile "Import-Module FailoverClusters"; "Get-ClusterQuorum | Format-Table -Autosize -HideTableHeaders QuorumType"' 
			INSERT INTO #xp_cmdshell_CluOutput ([Output])
			EXEC master.dbo.xp_cmdshell @CMD;
				
			IF (SELECT COUNT([Output]) FROM #xp_cmdshell_CluNodesOutput WHERE [Output] = '') > 0
			BEGIN				
				SELECT @CntNodes = COUNT(NodeName) FROM sys.dm_os_cluster_nodes (NOLOCK)
				
				SELECT 'Server_checks' AS [Category], 'Cluster_Quorum' AS [Check], 
					CASE WHEN REPLACE([Output], CHAR(9), '') = 'DiskOnly' AND @winver <> '5.2' THEN '[WARNING: The current quorum model is not recommended since WS2003]'
						WHEN REPLACE([Output], CHAR(9), '') = 'NodeAndDiskMajority' AND @CntNodes % 2 = 1 THEN '[WARNING: The current quorum model is not recommended for a cluster with ODD number of nodes]'
						WHEN REPLACE([Output], CHAR(9), '') = 'NodeMajority' AND @CntNodes % 2 = 0 THEN '[WARNING: The current quorum model is not recommended for a cluster with EVEN number of nodes]'
						WHEN REPLACE([Output], CHAR(9), '') = 'NodeAndFileShareMajority' THEN '[INFORMATION: The current quorum model is recommended for clusters with special configurations]'
						ELSE '[OK]' END AS [Deviation], 
					QUOTENAME(REPLACE([Output], CHAR(9), '')) AS QuorumModel,
					'[WARNING: No count of votes available, using count of nodes instead. Check if KB2494036 applies and is installed]' AS [Comment] -- http://support.microsoft.com/kb/2494036
				FROM #xp_cmdshell_CluOutput WHERE [Output] IS NOT NULL
			END
			ELSE
			BEGIN
				SELECT @CntVotes = SUM(CONVERT(int, [Output])) FROM #xp_cmdshell_CluNodesOutput WHERE [Output] IS NOT NULL

				IF EXISTS (SELECT TOP 1 [Output] FROM #xp_cmdshell_CluOutput WHERE [Output] LIKE '%Majority%' OR [Output] LIKE '%Disk%')
				BEGIN
					SELECT 'Server_checks' AS [Category], 'Cluster_Quorum' AS [Check], 
						CASE WHEN REPLACE([Output], CHAR(9), '') = 'DiskOnly' AND @winver <> '5.2' THEN '[WARNING: The current quorum model is not recommended since WS2003]'
							WHEN REPLACE([Output], CHAR(9), '') = 'NodeAndDiskMajority' AND @CntVotes % 2 = 1 THEN '[WARNING: The current quorum model is not recommended for a cluster with ODD number of node votes]'
							WHEN REPLACE([Output], CHAR(9), '') = 'NodeMajority' AND @CntVotes % 2 = 0 THEN '[WARNING: The current quorum model is not recommended for a cluster with EVEN number of node votes]'
							WHEN REPLACE([Output], CHAR(9), '') = 'NodeAndFileShareMajority' THEN '[INFORMATION: The current quorum model is recommended for clusters with special configurations]'
							ELSE '[OK]' END AS [Deviation], 
						QUOTENAME(REPLACE([Output], CHAR(9), '')) AS QuorumModel 
					FROM #xp_cmdshell_CluOutput WHERE [Output] IS NOT NULL 
				END
			END
			
			IF @xcmd = 0
			BEGIN
				EXEC sp_configure 'xp_cmdshell', 0; RECONFIGURE WITH OVERRIDE;
			END
			IF @sao = 0
			BEGIN
				EXEC sp_configure 'show advanced options', 0; RECONFIGURE WITH OVERRIDE;
			END
			
			
		END
		ELSE
		BEGIN
			RAISERROR('[WARNING: Only a sysadmin can run the "Cluster Quorum Model" check. A regular user can also run this check if a xp_cmdshell proxy account exists. Bypassing check]', 16, 1, N'xp_cmdshellproxy')
			RAISERROR('[WARNING: If not sysadmin, then must be a granted EXECUTE permissions on the following extended sprocs to run checks: xp_cmdshell. Bypassing check]', 16, 1, N'extended_sprocs')
			--RETURN
		END
	END
	ELSE IF @allow_xpcmdshell = 1 AND (@psavail IS NOT NULL AND @psavail IN ('RemoteSigned','Unrestricted')) AND @psver = 1
	BEGIN
		RAISERROR('    |- [INFORMATION: "Cluster Quorum Model" check was skipped: cannot execute with PS v1]', 10, 1, N'disallow_ps')
		--RETURN
	END
	ELSE
	BEGIN
		RAISERROR('    |- [INFORMATION: "Cluster Quorum Model" check was skipped: either xp_cmdshell or execution of PS scripts was not allowed]', 10, 1, N'disallow_xp_cmdshell')
		--RETURN
	END
END
ELSE
BEGIN
	SELECT 'Server_checks' AS [Category], 'Cluster_Quorum' AS [Check], 'NOT_CLUSTERED' AS [Deviation]
END;

IF @IsHadrEnabled = 1
BEGIN
	SET @sqlcmd	= N'DECLARE @winver VARCHAR(5), @CntNodes tinyint
SELECT @winver = windows_release FROM sys.dm_os_windows_info (NOLOCK)	
SELECT @CntNodes = SUM(number_of_quorum_votes) FROM sys.dm_hadr_cluster_members (NOLOCK)

SELECT ''Server_checks'' AS [Category], ''AlwaysOn_Cluster_Quorum'' AS [Check], cluster_name,
	CASE WHEN quorum_type = 3 AND @winver <> ''5.2'' THEN ''[WARNING: The current quorum model is not recommended since WS2003]''
		WHEN quorum_type = 1 AND @CntNodes % 2 = 1 THEN ''[WARNING: The current quorum model is not recommended for a cluster with ODD number of nodes]''
		WHEN quorum_type = 0 AND @CntNodes % 2 = 0 THEN ''[WARNING: The current quorum model is not recommended for a cluster with EVEN number of nodes]''
		WHEN quorum_type = 2 THEN ''[INFORMATION: The current quorum model is recommended for clusters with special configurations]''
		ELSE ''[OK]'' END AS [Deviation], 
	QUOTENAME(quorum_type_desc) AS QuorumModel
FROM sys.dm_hadr_cluster;'

	EXECUTE sp_executesql @sqlcmd
END;

--------------------------------------------------------------------------------------------------------------------------------
-- Cluster NIC Binding order subsection
--------------------------------------------------------------------------------------------------------------------------------
IF @allow_xpcmdshell = 1 and @clustered = 1
BEGIN
	RAISERROR (N'  |-Starting Cluster NIC Binding order', 10, 1) WITH NOWAIT
	IF @allow_xpcmdshell = 1 AND (@psavail IS NOT NULL AND @psavail IN ('RemoteSigned','Unrestricted'))
	BEGIN
		IF ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) = 1 -- Is sysadmin
			OR ((ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) <> 1 
				AND (SELECT COUNT(credential_id) FROM sys.credentials WHERE name = '##xp_cmdshell_proxy_account##') > 0) -- Is not sysadmin but proxy account exists
				AND (SELECT COUNT(l.name)
				FROM sys.server_permissions p JOIN sys.server_principals l 
				ON p.grantee_principal_id = l.principal_id
					AND p.class = 100 -- Server
					AND p.state IN ('G', 'W') -- Granted or Granted with Grant
					AND l.is_disabled = 0
					AND p.permission_name = 'ALTER SETTINGS'
					AND QUOTENAME(l.name) = QUOTENAME(USER_NAME())) = 0) -- Is not sysadmin but has alter settings permission
			OR ((ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) <> 1 
				AND ((SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_fileexist') > 0 AND
				(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_instance_regread') > 0 AND
				(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_regread') > 0 AND
				(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'sp_OAGetErrorInfo') > 0 AND
				(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'sp_OACreate') > 0 AND
				(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'sp_OADestroy') > 0 AND
				(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_cmdshell') > 0 AND
				(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_regenumvalues') > 0)))
		BEGIN
			DECLARE @clunic int, @maxnic int

			SELECT @sao = CAST([value] AS smallint) FROM sys.configurations WITH (NOLOCK) WHERE [name] = 'show advanced options'
			SELECT @xcmd = CAST([value] AS smallint) FROM sys.configurations WITH (NOLOCK) WHERE [name] = 'xp_cmdshell'
			SELECT @ole = CAST([value] AS smallint) FROM sys.configurations WITH (NOLOCK) WHERE [name] = 'Ole Automation Procedures'

			RAISERROR ('    |-Configuration options set for Cluster NIC Binding Order check', 10, 1) WITH NOWAIT
			IF @sao = 0
			BEGIN
				EXEC sp_configure 'show advanced options', 1; RECONFIGURE WITH OVERRIDE;
			END
			IF @xcmd = 0
			BEGIN
				EXEC sp_configure 'xp_cmdshell', 1; RECONFIGURE WITH OVERRIDE;
			END
			IF @ole = 0
			BEGIN
				EXEC sp_configure 'Ole Automation Procedures', 1; RECONFIGURE WITH OVERRIDE;
			END
		
			DECLARE @output_hw_nics TABLE ([PS_OUTPUT] VARCHAR(2048));
			DECLARE @output_hw_format_nics TABLE ([nicid] smallint, [nicname] VARCHAR(255) NULL)

			IF @custompath IS NULL
			BEGIN
				IF @sqlmajorver < 11
				BEGIN
					EXEC master..xp_instance_regread N'HKEY_LOCAL_MACHINE',N'Software\Microsoft\MSSQLServer\Setup',N'SQLPath', @path OUTPUT
					SET @path = @path + '\LOG'
				END
				ELSE
				BEGIN
					SET @sqlcmd = N'SELECT @pathOUT = LEFT([path], LEN([path])-1) FROM sys.dm_os_server_diagnostics_log_configurations';
					SET @params = N'@pathOUT NVARCHAR(2048) OUTPUT';
					EXECUTE sp_executesql @sqlcmd, @params, @pathOUT=@path OUTPUT;
				END

				-- Create COM object with FSO
				EXEC @OLEResult = master.dbo.sp_OACreate 'Scripting.FileSystemObject', @FSO OUT
				IF @OLEResult <> 0
				BEGIN
					EXEC sp_OAGetErrorInfo @FSO, @src OUT, @desc OUT
					SELECT @ErrorMessage = 'Error Creating COM Component 0x%x, %s, %s'
					RAISERROR (@ErrorMessage, 16, 1, @OLEResult, @src, @desc);
				END
				ELSE
				BEGIN
					EXEC @OLEResult = master.dbo.sp_OAMethod @FSO, 'FolderExists', @existout OUT, @path
					IF @OLEResult <> 0
					BEGIN
						EXEC sp_OAGetErrorInfo @FSO, @src OUT, @desc OUT
						SELECT @ErrorMessage = 'Error Calling FolderExists Method 0x%x, %s, %s'
						RAISERROR (@ErrorMessage, 16, 1, @OLEResult, @src, @desc);
					END
					ELSE
					BEGIN
						IF @existout <> 1
						BEGIN
							SET @path = CONVERT(NVARCHAR(500), SERVERPROPERTY('ErrorLogFileName'))
							SET @path = LEFT(@path,LEN(@path)-CHARINDEX('\', REVERSE(@path)))
						END 
					END
					EXEC @OLEResult = sp_OADestroy @FSO
				END
			END
			ELSE
			BEGIN
				SELECT @path = CASE WHEN @custompath LIKE '%\' THEN LEFT(@custompath, LEN(@custompath)-1) ELSE @custompath END
			END
			
			SET @FileName = @path + '\checkbp_nics_' + RTRIM(@server) + '.ps1'
				
			EXEC master.dbo.xp_fileexist @FileName, @existout out
			IF @existout = 0
			BEGIN -- Scan for nics
				SET @Text1 = '[string] $serverName = ''localhost''
$nics = Get-WmiObject -Computername $serverName -query "SELECT Description, Index FROM Win32_NetworkAdapterConfiguration"
foreach ($nic in $nics)
{
[string] $allnics = "{0};{1}" -f $nic.Index,$nic.Description
Write-Output $allnics
}
'
				EXEC @OLEResult = master.dbo.sp_OACreate 'Scripting.FileSystemObject', @FS OUT
				IF @OLEResult <> 0
				BEGIN
					EXEC sp_OAGetErrorInfo @FS, @src OUT, @desc OUT
					SELECT @ErrorMessage = 'Error Creating COM Component 0x%x, %s, %s'
					RAISERROR (@ErrorMessage, 16, 1, @OLEResult, @src, @desc);
				END

				--Open file
				EXEC @OLEResult = master.dbo.sp_OAMethod @FS, 'OpenTextFile', @FileID OUT, @FileName, 2, 1
				IF @OLEResult <> 0
				BEGIN
					EXEC sp_OAGetErrorInfo @FS, @src OUT, @desc OUT
					SELECT @ErrorMessage = 'Error Calling OpenTextFile Method 0x%x, %s, %s' + CHAR(10) + 'Could not create file ' + @FileName
					RAISERROR (@ErrorMessage, 16, 1, @OLEResult, @src, @desc);
				END
				ELSE
				BEGIN
					SELECT @ErrorMessage = '    |-Created file ' + @FileName
					RAISERROR (@ErrorMessage, 10, 1) WITH NOWAIT
				END

				--Write Text1
				EXEC @OLEResult = master.dbo.sp_OAMethod @FileID, 'WriteLine', NULL, @Text1
				IF @OLEResult <> 0
				BEGIN
					EXEC sp_OAGetErrorInfo @FS, @src OUT, @desc OUT
					SELECT @ErrorMessage = 'Error Calling WriteLine Method 0x%x, %s, %s' + CHAR(10) + 'Could not write to file ' + @FileName
					RAISERROR (@ErrorMessage, 16, 1, @OLEResult, @src, @desc);
				END

				EXEC @OLEResult = sp_OADestroy @FileID
				EXEC @OLEResult = sp_OADestroy @FS
			END
			ELSE
			BEGIN
				SELECT @ErrorMessage = '    |-Reusing file ' + @FileName
				RAISERROR (@ErrorMessage, 10, 1) WITH NOWAIT
			END
			
			IF @psver = 1
			BEGIN
				SET @CMD = 'powershell -NoLogo -NoProfile "' + @FileName + '" -ExecutionPolicy RemoteSigned'
			END
			ELSE
			BEGIN
				SET @CMD = 'powershell -NoLogo -NoProfile -File "' + @FileName + '" -ExecutionPolicy RemoteSigned'
			END;

			INSERT INTO @output_hw_nics
			EXEC master.dbo.xp_cmdshell @CMD
			
			SET @CMD = 'del /Q "' + @FileName + '"'
			EXEC master.dbo.xp_cmdshell @CMD, NO_OUTPUT
						
			INSERT INTO @output_hw_format_nics ([nicid],nicname)
			SELECT LEFT(RTRIM([PS_OUTPUT]), CASE WHEN CHARINDEX(';', RTRIM([PS_OUTPUT])) = 0 THEN LEN(RTRIM([PS_OUTPUT])) ELSE CHARINDEX(';', RTRIM([PS_OUTPUT]))-1 END),
					RIGHT(RTRIM([PS_OUTPUT]), LEN(RTRIM([PS_OUTPUT]))-CASE WHEN CHARINDEX(';', RTRIM([PS_OUTPUT])) = 0 THEN LEN(RTRIM([PS_OUTPUT])) ELSE CHARINDEX(';', RTRIM([PS_OUTPUT])) END)
			FROM @output_hw_nics
			WHERE [PS_OUTPUT] IS NOT NULL;
		
			SET @CMD2 = 'del ' + @FileName
			EXEC master.dbo.xp_cmdshell @CMD2, NO_OUTPUT;
			
			IF @xcmd = 0
			BEGIN
				EXEC sp_configure 'xp_cmdshell', 0; RECONFIGURE WITH OVERRIDE;
			END
			IF @ole = 0
			BEGIN
				EXEC sp_configure 'Ole Automation Procedures', 0; RECONFIGURE WITH OVERRIDE;
			END
			IF @sao = 0
			BEGIN
				EXEC sp_configure 'show advanced options', 0; RECONFIGURE WITH OVERRIDE;
			END;
			
			SELECT @maxnic = MAX(nicid) FROM @output_hw_format_nics;
			SELECT TOP 1 @clunic = nicid FROM @output_hw_format_nics WHERE nicname LIKE '%Cluster Virtual Adapter%';
		
			IF @clunic < @maxnic OR @clunic IS NULL --http://support2.microsoft.com/kb/955963
			BEGIN
				SELECT 'Server_checks' AS [Category], 'Cluster_NIC_Binding' AS [Check], '[WARNING: The Microsoft Failover Cluster Virtual Adapter is not in the correct binding order. Should be the lowest of all present NICs]' AS [Deviation]
				SELECT 'Server_checks' AS [Category], 'Cluster_NIC_Binding' AS [Information], nicid AS NIC_ID, nicname AS NIC_Name
				FROM @output_hw_format_nics t1
				ORDER BY t1.[nicid]
				OPTION (RECOMPILE);
			END
			ELSE IF @clunic = @maxnic
			BEGIN
				SELECT 'Server_checks' AS [Category], 'Cluster_NIC_Binding' AS [Check], '[OK]' AS [Deviation]
			END
			ELSE
			BEGIN
				SELECT 'Server_checks' AS [Category], 'Cluster_NIC_Binding' AS [Check], '[WARNING: Could not gather information on NIC binding order]' AS [Deviation]
			END;
		END
		ELSE
		BEGIN
			RAISERROR('[WARNING: Only a sysadmin can run the "Cluster NIC Binding Order" checks. A regular user can also run this check if a xp_cmdshell proxy account exists. Bypassing check]', 16, 1, N'xp_cmdshellproxy')
			RAISERROR('[WARNING: If not sysadmin, then must be a granted EXECUTE permissions on the following extended sprocs to run checks: sp_OACreate, sp_OADestroy, sp_OAGetErrorInfo, xp_cmdshell, xp_instance_regread, xp_regread, xp_fileexist and xp_regenumvalues. Bypassing check]', 16, 1, N'extended_sprocs')
			--RETURN
		END
		END
	ELSE
	BEGIN
		RAISERROR('    |- [INFORMATION: "Cluster NIC Binding Order" check was skipped: either xp_cmdshell or execution of PS scripts was not allowed.]', 10, 1, N'disallow_xp_cmdshell')
		--RETURN
	END
END
ELSE
BEGIN
	SELECT 'Server_checks' AS [Category], 'Cluster_NIC_Binding' AS [Check], 'NOT_CLUSTERED' AS [Deviation]
END;

--------------------------------------------------------------------------------------------------------------------------------
-- Cluster QFE node equality subsection
--------------------------------------------------------------------------------------------------------------------------------
IF @clustered = 1
BEGIN
	RAISERROR (N'  |-Starting QFE node equality', 10, 1) WITH NOWAIT
	IF @allow_xpcmdshell = 1
	BEGIN
		IF ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) = 1 -- Is sysadmin
			OR ((ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) <> 1 
				AND (SELECT COUNT(credential_id) FROM sys.credentials WHERE name = '##xp_cmdshell_proxy_account##') > 0)) -- Is not sysadmin but proxy account exists
			OR ((ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) <> 1 
				AND (SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_cmdshell') > 0))
		BEGIN
			SELECT @sao = CAST([value] AS smallint) FROM sys.configurations WITH (NOLOCK) WHERE [name] = 'show advanced options'
			SELECT @xcmd = CAST([value] AS smallint) FROM sys.configurations WITH (NOLOCK) WHERE [name] = 'xp_cmdshell'

			RAISERROR ('    |-Configuration options set for QFE node equality check', 10, 1) WITH NOWAIT
			IF @sao = 0
			BEGIN
				EXEC sp_configure 'show advanced options', 1; RECONFIGURE WITH OVERRIDE;
			END
			IF @xcmd = 0
			BEGIN
				EXEC sp_configure 'xp_cmdshell', 1; RECONFIGURE WITH OVERRIDE;
			END
			
			DECLARE /* @CMD NVARCHAR(4000), @line int, @linemax int, */ @Node VARCHAR(50)
				
			IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#xp_cmdshell_Nodes'))
			DROP TABLE #xp_cmdshell_Nodes;
			IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#xp_cmdshell_Nodes'))
			CREATE TABLE #xp_cmdshell_Nodes (NodeName VARCHAR(50), isdone bit NOT NULL);
				
			IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#xp_cmdshell_QFEOutput'))
			DROP TABLE #xp_cmdshell_QFEOutput;
			IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#xp_cmdshell_QFEOutput'))
			CREATE TABLE #xp_cmdshell_QFEOutput (line int IDENTITY(1,1) PRIMARY KEY, [Output] VARCHAR(150));
				
			IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#xp_cmdshell_QFEFinal'))
			DROP TABLE #xp_cmdshell_QFEFinal;
			IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#xp_cmdshell_QFEFinal'))
			CREATE TABLE #xp_cmdshell_QFEFinal (NodeName VARCHAR(50), [QFE] VARCHAR(150));
				
			INSERT INTO #xp_cmdshell_Nodes
			SELECT NodeName, 0 FROM sys.dm_os_cluster_nodes (NOLOCK);
				
			WHILE (SELECT COUNT(NodeName) FROM #xp_cmdshell_Nodes WHERE isdone = 0) > 0
			BEGIN
				SELECT TOP 1 @Node = NodeName FROM #xp_cmdshell_Nodes WHERE isdone = 0;
					
				SET @CMD = 'wmic /node:"' + @Node + '" qfe get hotfixid' 
				INSERT INTO #xp_cmdshell_QFEOutput ([Output])
				EXEC master.dbo.xp_cmdshell @CMD;
					
				IF (SELECT COUNT([Output]) FROM #xp_cmdshell_QFEOutput WHERE [Output] LIKE '%Access is denied%') = 0
				BEGIN
					INSERT INTO #xp_cmdshell_QFEFinal
					SELECT @Node, RTRIM(REPLACE([Output],CHAR(13),'')) FROM #xp_cmdshell_QFEOutput WHERE RTRIM(REPLACE([Output],CHAR(13),'')) NOT IN ('','HotFixID');
				END
				ELSE
				BEGIN
					SET @ErrorMessage = '[WARNING: Access Denied error while trying to get updates from node ' + @Node + ']'
					RAISERROR (@ErrorMessage,16,1);
				END;
					
				TRUNCATE TABLE #xp_cmdshell_QFEOutput;

				UPDATE #xp_cmdshell_Nodes 
				SET isdone = 1
				WHERE NodeName = @Node;
			END;
				
			IF (SELECT COUNT(DISTINCT NodeName) FROM #xp_cmdshell_QFEFinal) = (SELECT COUNT(DISTINCT NodeName) FROM #xp_cmdshell_Nodes)
			BEGIN
				IF (SELECT COUNT(*) FROM #xp_cmdshell_QFEFinal t1 WHERE t1.[QFE] NOT IN (SELECT DISTINCT t2.[QFE] FROM #xp_cmdshell_QFEFinal t2 WHERE t2.NodeName <> t1.NodeName)) > 0
				BEGIN
					SELECT 'Server_checks' AS [Category], 'Cluster_QFE_Equality' AS [Check], '[WARNING: Missing updates found in some of the nodes]' AS [Deviation]
					SELECT t1.NodeName, t1.[QFE] AS MissingUpdates FROM #xp_cmdshell_QFEFinal t1
					WHERE t1.[QFE] NOT IN (SELECT DISTINCT t2.[QFE] FROM #xp_cmdshell_QFEFinal t2 WHERE t2.NodeName <> t1.NodeName);
				END
				ELSE
				BEGIN
					SELECT 'Server_checks' AS [Category], 'Cluster_QFE_Equality' AS [Check], '[OK]' AS [Deviation];
					SELECT DISTINCT t1.[QFE] AS InstalledUpdates FROM #xp_cmdshell_QFEFinal t1;
				END
			END
			ELSE
			BEGIN
				RAISERROR ('[WARNING: Could not collect data from all cluster nodes. Bypassing QFE node equality check]',16,1);
			END
			
			IF @xcmd = 0
			BEGIN
				EXEC sp_configure 'xp_cmdshell', 0; RECONFIGURE WITH OVERRIDE;
			END
			IF @sao = 0
			BEGIN
				EXEC sp_configure 'show advanced options', 0; RECONFIGURE WITH OVERRIDE;
			END
		END
		ELSE
		BEGIN
			RAISERROR('[WARNING: Only a sysadmin can run the "QFE node equality" check. A regular user can also run this check if a xp_cmdshell proxy account exists. Bypassing check]', 16, 1, N'xp_cmdshellproxy')
			RAISERROR('[WARNING: If not sysadmin, then must be a granted EXECUTE permissions on the following extended sprocs to run checks: xp_cmdshell. Bypassing check]', 16, 1, N'extended_sprocs')
			--RETURN
		END
	END
	ELSE
	BEGIN
		RAISERROR('  |- [INFORMATION: "QFE node equality" check was skipped because xp_cmdshell was not allowed.]', 10, 1, N'disallow_xp_cmdshell')
		--RETURN
	END
END
ELSE
BEGIN
	SELECT 'Server_checks' AS [Category], 'Cluster_QFE_Equality' AS [Check], 'NOT_CLUSTERED' AS [Deviation]
END;

RAISERROR (N'|-Starting Service Accounts Checks', 10, 1) WITH NOWAIT
--------------------------------------------------------------------------------------------------------------------------------
-- Service Accounts Status subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'  |-Starting Service Accounts Status', 10, 1) WITH NOWAIT
IF (ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) = 1) 
	OR ((SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_regread') = 1 AND
		(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_servicecontrol') = 1)
BEGIN
	DECLARE @rc int, @profile NVARCHAR(128)
	DECLARE @sqlservice NVARCHAR(128), @sqlagentservice NVARCHAR(128), @dtsservice NVARCHAR(128), @ftservice NVARCHAR(128)
	DECLARE @browservice NVARCHAR(128), @olapservice NVARCHAR(128), @rsservice NVARCHAR(128)
	DECLARE @statussqlservice NVARCHAR(20), @statussqlagentservice NVARCHAR(20), @statusdtsservice NVARCHAR(20), @statusftservice NVARCHAR(20)
	DECLARE @statusbrowservice NVARCHAR(20), @statusolapservice NVARCHAR(20), @statusrsservice NVARCHAR(20)
	DECLARE @regkeysqlservice NVARCHAR(256), @regkeysqlagentservice NVARCHAR(256), @regkeydtsservice NVARCHAR(256), @regkeyftservice NVARCHAR(256)
	DECLARE @regkeybrowservice NVARCHAR(256), @regkeyolapservice NVARCHAR(256), @regkeyrsservice NVARCHAR(256)
	DECLARE @accntsqlservice NVARCHAR(128), @accntsqlagentservice NVARCHAR(128), @accntdtsservice NVARCHAR(128), @accntftservice NVARCHAR(128)
	DECLARE @accntbrowservice NVARCHAR(128), @accntolapservice NVARCHAR(128), @accntrsservice NVARCHAR(128)

	-- Get service names
	IF (@instancename IS NULL) 
	BEGIN
		IF @sqlmajorver < 11
		BEGIN
			SELECT @sqlservice = N'MSSQLServer' 
			SELECT @sqlagentservice = N'SQLServerAgent'
		END
		SELECT @olapservice = N'MSSQLServerOLAPService' 
		SELECT @rsservice = N'ReportServer' 
	END 
	ELSE 
	BEGIN
		IF @sqlmajorver < 11
		BEGIN
			SELECT @sqlservice = N'MSSQL$' + @instancename
			SELECT @sqlagentservice = N'SQLAgent$' + @instancename
		END 
		SELECT @olapservice = N'MSOLAP$' + @instancename
		SELECT @rsservice = N'ReportServer$' + @instancename 
	END

	IF @sqlmajorver = 9
	BEGIN
		SELECT @dtsservice = N'MsDtsServer'
	END
	ELSE
	BEGIN
		SELECT @dtsservice = N'MsDtsServer' + CONVERT(VARCHAR, @sqlmajorver) + '0'
	END

	IF (SELECT ISNULL(FULLTEXTSERVICEPROPERTY('IsFulltextInstalled'),0)) = 1
	BEGIN
		IF (@instancename IS NULL) AND @sqlmajorver = 10
		BEGIN 
			SELECT @ftservice = N'MSSQLFDLauncher'
		END 
		ELSE IF (@instancename IS NOT NULL) AND @sqlmajorver = 10
		BEGIN 
			SELECT @ftservice = N'MSSQLFDLauncher$' + @instancename
		END
		ELSE IF (@instancename IS NULL) AND @sqlmajorver = 9
		BEGIN 
			SELECT @ftservice = N'msftesql'
		END
		ELSE IF (@instancename IS NOT NULL) AND @sqlmajorver = 9 
		BEGIN 
			SELECT @ftservice = N'msftesql$' + @instancename
		END
	END

	SELECT @browservice = N'SQLBrowser'

	IF @sqlmajorver < 11
	BEGIN
		SELECT @regkeysqlservice = N'SYSTEM\CurrentControlSet\Services\' + @sqlservice
		SELECT @regkeysqlagentservice = N'SYSTEM\CurrentControlSet\Services\' + @sqlagentservice
		IF (SELECT ISNULL(FULLTEXTSERVICEPROPERTY('IsFulltextInstalled'),0)) = 1
		BEGIN
			SELECT @regkeyftservice = N'SYSTEM\CurrentControlSet\Services\' + @ftservice
		END
	END
	SELECT @regkeyolapservice = N'SYSTEM\CurrentControlSet\Services\' + @olapservice
	SELECT @regkeyrsservice = N'SYSTEM\CurrentControlSet\Services\' + @rsservice
	SELECT @regkeydtsservice = N'SYSTEM\CurrentControlSet\Services\' + @dtsservice
	SELECT @regkeybrowservice = N'SYSTEM\CurrentControlSet\Services\' + @browservice
	
	-- Service status
	IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#RegResult'))
	CREATE TABLE #RegResult (ResultValue bit)
	IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#ServiceStatus'))
	CREATE TABLE #ServiceStatus (ServiceStatus VARCHAR(128))

	IF @sqlmajorver < 11 OR (@sqlmajorver = 10 AND @sqlminorver = 50 AND @sqlbuild >= 2500)
	BEGIN
		BEGIN TRY
			INSERT INTO #RegResult (ResultValue)
			EXEC master.sys.xp_regread N'HKEY_LOCAL_MACHINE', @regkeysqlservice
			IF (SELECT TOP 1 ResultValue FROM #RegResult) = 1 
			BEGIN
				INSERT INTO #ServiceStatus (ServiceStatus)
				EXEC master.sys.xp_servicecontrol N'QUERYSTATE', @sqlservice
				SELECT @statussqlservice = ServiceStatus FROM #ServiceStatus
				TRUNCATE TABLE #ServiceStatus;
			END
			ELSE
			BEGIN
				SET @statussqlservice = 'Not Installed'
			END
			TRUNCATE TABLE #RegResult;
		END TRY
		BEGIN CATCH
			SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
			SELECT @ErrorMessage = 'Service Accounts and Status subsection - Error raised in TRY block 1. ' + ERROR_MESSAGE()
			RAISERROR (@ErrorMessage, 16, 1);
		END CATCH
	END
	ELSE
	BEGIN
		SET @sqlcmd = N'SELECT @statussqlserviceOUT = status_desc FROM sys.dm_server_services WHERE servicename LIKE ''SQL Server%'' AND servicename NOT LIKE ''SQL Server Agent%''';
		SET @params = N'@statussqlserviceOUT NVARCHAR(20) OUTPUT';
		EXECUTE sp_executesql @sqlcmd, @params, @statussqlserviceOUT=@statussqlservice OUTPUT;
		IF @statussqlservice IS NULL
		BEGIN
			SET @statussqlservice = 'Not Installed'
		END
	END

	IF @sqlmajorver < 11 OR (@sqlmajorver = 10 AND @sqlminorver = 50 AND @sqlbuild >= 2500)
	BEGIN
		BEGIN TRY
			INSERT INTO #RegResult (ResultValue)
			EXEC master.sys.xp_regread N'HKEY_LOCAL_MACHINE', @regkeysqlagentservice
			IF (SELECT TOP 1 ResultValue FROM #RegResult) = 1 
			BEGIN
				INSERT INTO #ServiceStatus (ServiceStatus)
				EXEC master.sys.xp_servicecontrol N'QUERYSTATE', @sqlagentservice
				SELECT @statussqlagentservice = ServiceStatus FROM #ServiceStatus
				TRUNCATE TABLE #ServiceStatus;
			END
			ELSE
			BEGIN
				SET @statussqlagentservice = 'Not Installed'
			END
			TRUNCATE TABLE #RegResult;
		END TRY
		BEGIN CATCH
			SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
			SELECT @ErrorMessage = 'Service Accounts and Status subsection - Error raised in TRY block 2. ' + ERROR_MESSAGE()
			RAISERROR (@ErrorMessage, 16, 1);
		END CATCH
	END
	ELSE
	BEGIN
		SET @sqlcmd = N'SELECT @statussqlagentserviceOUT = status_desc FROM sys.dm_server_services WHERE servicename LIKE ''SQL Server Agent%''';
		SET @params = N'@statussqlagentserviceOUT NVARCHAR(20) OUTPUT';
		EXECUTE sp_executesql @sqlcmd, @params, @statussqlagentserviceOUT=@statussqlagentservice OUTPUT;
		IF @statussqlagentservice IS NULL
		BEGIN
			SET @statussqlagentservice = 'Not Installed'
		END
	END

	IF @sqlmajorver < 11 OR (@sqlmajorver = 10 AND @sqlminorver = 50 AND @sqlbuild >= 2500)
	BEGIN
		IF (SELECT ISNULL(FULLTEXTSERVICEPROPERTY('IsFulltextInstalled'),0)) = 1
		BEGIN
			BEGIN TRY
				INSERT INTO #RegResult (ResultValue)
				EXEC master.sys.xp_regread N'HKEY_LOCAL_MACHINE', @regkeyftservice
				IF (SELECT TOP 1 ResultValue FROM #RegResult) = 1 
				BEGIN
					INSERT INTO #ServiceStatus (ServiceStatus)
					EXEC master.sys.xp_servicecontrol N'QUERYSTATE', @ftservice
					SELECT @statusftservice = ServiceStatus FROM #ServiceStatus
					TRUNCATE TABLE #ServiceStatus;
				END
				ELSE
				BEGIN
					SET @statusftservice = '[INFORMATION: Service is not installed]'
				END
				TRUNCATE TABLE #RegResult;
			END TRY
			BEGIN CATCH
				SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
				SELECT @ErrorMessage = 'Service Accounts and Status subsection - Error raised in TRY block 3. ' + ERROR_MESSAGE()
				RAISERROR (@ErrorMessage, 16, 1);
			END CATCH
		END
	END
	ELSE
	BEGIN
		SET @sqlcmd = N'SELECT @statusftserviceOUT = status_desc FROM sys.dm_server_services WHERE servicename LIKE ''SQL Full-text Filter Daemon Launcher%''';
		SET @params = N'@statusftserviceOUT NVARCHAR(20) OUTPUT';
		EXECUTE sp_executesql @sqlcmd, @params, @statusftserviceOUT=@statusftservice OUTPUT;
		IF @statusftservice IS NULL
		BEGIN
			SET @statusftservice = '[INFORMATION: Service is not installed]'
		END
	END

	BEGIN TRY
		INSERT INTO #RegResult (ResultValue)
		EXEC master.sys.xp_regread N'HKEY_LOCAL_MACHINE', @regkeyolapservice
		IF (SELECT TOP 1 ResultValue FROM #RegResult) = 1 
		BEGIN
			INSERT INTO #ServiceStatus (ServiceStatus)
			EXEC master.sys.xp_servicecontrol N'QUERYSTATE', @olapservice
			SELECT @statusolapservice = ServiceStatus FROM #ServiceStatus
			TRUNCATE TABLE #ServiceStatus;
		END
		ELSE
		BEGIN
			SET @statusolapservice = 'Not Installed'
		END
		TRUNCATE TABLE #RegResult;
	END TRY
		BEGIN CATCH
		SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
		SELECT @ErrorMessage = 'Service Accounts and Status subsection - Error raised in TRY block 4. ' + ERROR_MESSAGE()
		RAISERROR (@ErrorMessage, 16, 1);
	END CATCH

	BEGIN TRY
		INSERT INTO #RegResult (ResultValue)
		EXEC master.sys.xp_regread N'HKEY_LOCAL_MACHINE', @regkeyrsservice
		IF (SELECT TOP 1 ResultValue FROM #RegResult) = 1 
		BEGIN
			INSERT INTO #ServiceStatus (ServiceStatus)
			EXEC master.sys.xp_servicecontrol N'QUERYSTATE', @rsservice
			SELECT @statusrsservice = ServiceStatus FROM #ServiceStatus
			TRUNCATE TABLE #ServiceStatus;
		END
		ELSE
		BEGIN
			SET @statusrsservice = 'Not Installed'
		END
		TRUNCATE TABLE #RegResult;
	END TRY
	BEGIN CATCH
		SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
		SELECT @ErrorMessage = 'Service Accounts and Status subsection - Error raised in TRY block 5. ' + ERROR_MESSAGE()
		RAISERROR (@ErrorMessage, 16, 1);
	END CATCH

	BEGIN TRY
		INSERT INTO #RegResult (ResultValue)
		EXEC master.sys.xp_regread N'HKEY_LOCAL_MACHINE', @regkeydtsservice
		IF (SELECT TOP 1 ResultValue FROM #RegResult) = 1 
		BEGIN
			INSERT INTO #ServiceStatus (ServiceStatus)
			EXEC master.sys.xp_servicecontrol N'QUERYSTATE', @dtsservice
			SELECT @statusdtsservice = ServiceStatus FROM #ServiceStatus
			TRUNCATE TABLE #ServiceStatus;
		END
		ELSE
		BEGIN
			SET @statusdtsservice = 'Not Installed'
		END
		TRUNCATE TABLE #RegResult;
	END TRY
	BEGIN CATCH
		SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
		SELECT @ErrorMessage = 'Service Accounts and Status subsection - Error raised in TRY block 6. ' + ERROR_MESSAGE()
		RAISERROR (@ErrorMessage, 16, 1);
	END CATCH

	BEGIN TRY
		INSERT INTO #RegResult (ResultValue)
		EXEC master.sys.xp_regread N'HKEY_LOCAL_MACHINE', @regkeybrowservice
		IF (SELECT TOP 1 ResultValue FROM #RegResult) = 1 
		BEGIN
			INSERT INTO #ServiceStatus (ServiceStatus)
			EXEC master.sys.xp_servicecontrol N'QUERYSTATE', @browservice
			SELECT @statusbrowservice = ServiceStatus FROM #ServiceStatus
			TRUNCATE TABLE #ServiceStatus;
		END
		ELSE
		BEGIN
			SET @statusbrowservice = 'Not Installed'
		END
	END TRY
	BEGIN CATCH
		SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
		SELECT @ErrorMessage = 'Service Accounts and Status subsection - Error raised in TRY block 7. ' + ERROR_MESSAGE()
		RAISERROR (@ErrorMessage, 16, 1);
	END CATCH

	DROP TABLE #RegResult;
	DROP TABLE #ServiceStatus;

	-- Accounts
	IF @sqlmajorver < 11 OR (@sqlmajorver = 10 AND @sqlminorver = 50 AND @sqlbuild >= 2500)
	BEGIN
		BEGIN TRY
			EXEC master.sys.xp_regread N'HKEY_LOCAL_MACHINE', @regkeysqlservice, N'ObjectName', @accntsqlservice OUTPUT, NO_OUTPUT
			EXEC master.sys.xp_regread N'HKEY_LOCAL_MACHINE', @regkeysqlagentservice, N'ObjectName', @accntsqlagentservice OUTPUT, NO_OUTPUT
			EXEC master.sys.xp_regread N'HKEY_LOCAL_MACHINE', @regkeyftservice, N'ObjectName', @accntftservice OUTPUT, NO_OUTPUT
		END TRY
		BEGIN CATCH
			SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
			SELECT @ErrorMessage = 'Service Accounts and Status subsection - Error raised in TRY block 8. ' + ERROR_MESSAGE()
			RAISERROR (@ErrorMessage, 16, 1);
		END CATCH
	END
	ELSE
	BEGIN
		BEGIN TRY
			SET @sqlcmd = N'SELECT @accntsqlserviceOUT = service_account FROM sys.dm_server_services WHERE servicename LIKE ''SQL Server%'' AND servicename NOT LIKE ''SQL Server Agent%''';
			SET @params = N'@accntsqlserviceOUT NVARCHAR(128) OUTPUT';
			EXECUTE sp_executesql @sqlcmd, @params, @accntsqlserviceOUT=@accntsqlservice OUTPUT;
			SET @sqlcmd = N'SELECT @accntsqlagentserviceOUT = service_account FROM sys.dm_server_services WHERE servicename LIKE ''SQL Server Agent%''';
			SET @params = N'@accntsqlagentserviceOUT NVARCHAR(128) OUTPUT';
			EXECUTE sp_executesql @sqlcmd, @params, @accntsqlagentserviceOUT=@accntsqlagentservice OUTPUT;
			SET @sqlcmd = N'SELECT @accntftserviceOUT = service_account FROM sys.dm_server_services WHERE servicename LIKE ''SQL Full-text Filter Daemon Launcher%''';
			SET @params = N'@accntftserviceOUT NVARCHAR(128) OUTPUT';
			EXECUTE sp_executesql @sqlcmd, @params, @accntftserviceOUT=@accntftservice OUTPUT;
		END TRY
		BEGIN CATCH
			SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
			SELECT @ErrorMessage = 'Service Accounts and Status subsection - Error raised in TRY block 9. ' + ERROR_MESSAGE()
			RAISERROR (@ErrorMessage, 16, 1);
		END CATCH
	END
	
	BEGIN TRY
		EXEC master.sys.xp_regread N'HKEY_LOCAL_MACHINE', @regkeyolapservice, N'ObjectName', @accntolapservice OUTPUT, NO_OUTPUT
		EXEC master.sys.xp_regread N'HKEY_LOCAL_MACHINE', @regkeyrsservice, N'ObjectName', @accntrsservice OUTPUT, NO_OUTPUT
		EXEC master.sys.xp_regread N'HKEY_LOCAL_MACHINE', @regkeydtsservice, N'ObjectName', @accntdtsservice OUTPUT, NO_OUTPUT
		EXEC master.sys.xp_regread N'HKEY_LOCAL_MACHINE', @regkeybrowservice, N'ObjectName', @accntbrowservice OUTPUT, NO_OUTPUT
	END TRY
	BEGIN CATCH
		SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
		SELECT @ErrorMessage = 'Service Accounts and Status subsection - Error raised in TRY block 10. ' + ERROR_MESSAGE()
		RAISERROR (@ErrorMessage, 16, 1);
	END CATCH
	
	SELECT 'Service_Account_checks' AS [Category], 'Service_Status' AS [Check], 'SQL_Server' AS [Service], @statussqlservice AS [Status], @accntsqlservice AS [Account],
		CASE WHEN @statussqlservice = 'Not Installed' THEN '[INFORMATION: Service is not installed]'
			WHEN @statussqlservice LIKE 'Stopped%' THEN '[WARNING: Service is stopped]'
			WHEN @accntsqlservice IS NULL THEN '[WARNING: Could not detect account for check]' 
			WHEN @accntsqlservice = 'NT AUTHORITY\LOCALSERVICE' THEN '[WARNING: Running SQL Server under this account is not supported]'
			WHEN @clustered = 1 AND @accntsqlservice = 'NT AUTHORITY\SYSTEM' THEN '[WARNING: Running SQL Server under this account is not supported]' 
			WHEN @clustered = 1 AND @accntsqlservice = 'LocalSystem' THEN '[WARNING: Running SQL Server under this account is not supported]' 
			WHEN @clustered = 1 AND @accntsqlservice = 'NT AUTHORITY\NETWORKSERVICE' THEN '[WARNING: Running SQL Server under this account is not supported]' 
			WHEN @clustered = 0 AND @accntsqlservice = 'NT AUTHORITY\SYSTEM' THEN '[WARNING: Running SQL Server under this account is not recommended]' 
			WHEN @clustered = 0 AND @accntsqlservice = 'LocalSystem' THEN '[WARNING: Running SQL Server under this account is not recommended]' 
			WHEN @clustered = 0 AND @accntsqlservice = 'NT AUTHORITY\NETWORKSERVICE' THEN '[WARNING: Running SQL Server under this account is not recommended]'
			-- MSA for WS2008R2 or higher, SQL Server 2012 or higher, non-clustered (http://msdn.microsoft.com/en-us/library/ms143504(v=SQL.110).aspx#Default_Accts)
			WHEN @clustered = 0 AND @sqlmajorver >= 11 AND @winver >= 6.1 AND @accntsqlservice <> 'NT SERVICE\MSSQLSERVER' AND @accntsqlservice NOT LIKE 'NT SERVICE\MSSQL$%' THEN '[INFORMATION: SQL Server is not running with the default account]'
			ELSE '[OK]' 
		END AS [Deviation]
	UNION ALL
	SELECT 'Service_Account_checks' AS [Category], 'Service_Status' AS [Check], 'SQL_Server_Agent' AS [Service], @statussqlagentservice AS [Status], @accntsqlagentservice AS [Account],
		CASE WHEN @statussqlagentservice = 'Not Installed' THEN '[INFORMATION: Service is not installed]'
			WHEN @statussqlagentservice LIKE 'Stopped%' THEN '[WARNING: Service is stopped]'
			WHEN @accntsqlagentservice IS NULL THEN '[WARNING: Could not detect account for check]' 
			WHEN @accntsqlagentservice = 'NT AUTHORITY\LOCALSERVICE' THEN '[WARNING: Running SQL Server Agent under this account is not supported]'
			WHEN @accntsqlagentservice = @accntsqlservice THEN '[WARNING: Running SQL Server Agent under the same account as SQL Server is not recommended]' 
			WHEN @clustered = 1 AND @accntsqlagentservice = 'NT AUTHORITY\SYSTEM' THEN '[WARNING: Running SQL Server Agent under this account is not supported]' 
			WHEN @clustered = 1 AND @accntsqlagentservice = 'NT AUTHORITY\NETWORKSERVICE' THEN '[WARNING: Running SQL Server Agent under this account is not supported]' 
			WHEN @clustered = 0 AND @accntsqlagentservice = 'NT AUTHORITY\SYSTEM' THEN '[WARNING: Running SQL Server Agent under this account is not recommended]' 
			WHEN @clustered = 0 AND @accntsqlagentservice = 'NT AUTHORITY\NETWORKSERVICE' THEN '[WARNING: Running SQL Server Agent under this account is not recommended]' 
			WHEN @winver IS NULL THEN '[WARNING: Could not determine Windows version for check]'
			-- MSA for WS2008R2 or higher, SQL Server 2012 or higher, non-clustered (http://msdn.microsoft.com/en-us/library/ms143504(v=SQL.110).aspx#Default_Accts)
			WHEN @clustered = 0 AND @sqlmajorver >= 11 AND @winver >= 6.1 AND @accntsqlagentservice <> 'NT SERVICE\SQLSERVERAGENT' AND @accntsqlagentservice NOT LIKE 'NT SERVICE\SQLAGENT$%' THEN '[INFORMATION: SQL Server Agent is not running with the default account]'
			ELSE '[OK]' 
		END AS [Deviation]
	UNION ALL
	SELECT 'Service_Account_checks' AS [Category], 'Service_Status' AS [Check], 'SQL_Server_Analysis_Services' AS [Service], @statusolapservice AS [Status], @accntolapservice AS [Account],
		CASE WHEN @statusolapservice = 'Not Installed' THEN '[INFORMATION: Service is not installed]'
			WHEN @statusolapservice LIKE 'Stopped%' THEN '[WARNING: Service is stopped]'
			WHEN @accntolapservice IS NULL THEN '[WARNING: Could not detect account for check]' 
			WHEN @accntolapservice = @accntsqlservice THEN '[WARNING: Running SQL Server Analysis Services under the same account as SQL Server is not recommended]' 
			WHEN @clustered = 0 AND @sqlmajorver <= 10 AND @accntolapservice <> 'NT AUTHORITY\NETWORKSERVICE' AND @accntdtsservice <> 'NT AUTHORITY\LOCALSERVICE' THEN '[INFORMATION: SQL Server Analysis Services is not running with the default account]'
			WHEN @winver IS NULL THEN '[WARNING: Could not determine Windows version for check]'
			WHEN @clustered = 0 AND @sqlmajorver >= 11 AND @winver <= 6.0 AND @accntolapservice <> 'NT AUTHORITY\NETWORKSERVICE' THEN '[INFORMATION: SQL Server Analysis Services is not running with the default account]'
			-- MSA for WS2008R2 or higher, SQL Server 2005 or higher, non-clustered (http://msdn.microsoft.com/en-us/library/ms143504(v=SQL.110).aspx#Default_Accts)
			WHEN @clustered = 0 AND @sqlmajorver >= 11 AND @winver >= 6.1 AND @accntolapservice <> 'NT SERVICE\MSSQLServerOLAPService' AND @accntolapservice NOT LIKE 'NT SERVICE\MSOLAP$%' THEN '[INFORMATION: SQL Server Analysis Services is not running with the default account]'
			ELSE '[OK]' 
		END AS [Deviation]
	UNION ALL
	SELECT 'Service_Account_checks' AS [Category], 'Service_Status' AS [Check], 'SQL_Server_Integration_Services' AS [Service], @statusdtsservice AS [Status], @accntdtsservice AS [Account],
		CASE WHEN @statusdtsservice = 'Not Installed' THEN '[INFORMATION: Service is not installed]'
			WHEN @statusdtsservice LIKE 'Stopped%' THEN '[WARNING: Service is stopped]'
			WHEN @accntdtsservice IS NULL THEN '[WARNING: Could not detect account for check]' 
			WHEN @accntdtsservice = @accntsqlservice THEN '[WARNING: Running SQL Server Integration Services under the same account as SQL Server is not recommended]' 
			WHEN @winver IS NULL THEN '[WARNING: Could not determine Windows version for check]'
			WHEN @winver <= 6.0 AND @accntdtsservice <> 'NT AUTHORITY\NETWORKSERVICE' AND @accntdtsservice <> 'NT AUTHORITY\LOCALSYSTEM' THEN '[INFORMATION: SQL Server Integration Services is not running with the default account]'
			-- MSA for WS2008R2 or higher, SQL Server 2012 or higher (http://msdn.microsoft.com/en-us/library/ms143504(v=SQL.110).aspx#Default_Accts)
			WHEN @sqlmajorver >= 11 AND @winver >= 6.1 AND @accntdtsservice NOT IN ('NT SERVICE\MSDTSSERVER100', 'NT SERVICE\MSDTSSERVER110') THEN '[INFORMATION: SQL Server Integration Services is not running with the default account]'
			ELSE '[OK]' 
		END AS [Deviation]
	UNION ALL
	SELECT 'Service_Account_checks' AS [Category], 'Service_Status' AS [Check], 'SQL_Server_Reporting_Services' AS [Service], @statusrsservice AS [Status], @accntrsservice AS [Account],
		CASE WHEN @statusrsservice = 'Not Installed' THEN '[INFORMATION: Service is not installed]'
			WHEN @statusrsservice LIKE 'Stopped%' THEN '[WARNING: Service is stopped]'
			WHEN @accntrsservice IS NULL THEN '[WARNING: Could not detect account for check]' 
			WHEN @accntrsservice = @accntsqlservice THEN '[WARNING: Running SQL Server Reporting Services under the same account as SQL Server is not recommended]' 
			WHEN @clustered = 0 AND @sqlmajorver <= 10 AND @accntrsservice <> 'NT AUTHORITY\NETWORKSERVICE' AND @accntdtsservice <> 'NT AUTHORITY\LOCALSYSTEM' THEN '[INFORMATION: SQL Server Reporting Services is not running with the default account]'
			WHEN @winver IS NULL THEN '[WARNING: Could not determine Windows version for check]'
			WHEN @sqlmajorver >= 11 AND @winver <= 6.0 AND @accntrsservice <> 'NT AUTHORITY\NETWORKSERVICE' THEN '[INFORMATION: SQL Server Reporting Services is not running with the default account]'
			-- MSA for WS2008R2 or higher, SQL Server 2012 or higher (http://msdn.microsoft.com/en-us/library/ms143504(v=SQL.110).aspx#Default_Accts)
			WHEN @sqlmajorver >= 11 AND @winver >= 6.1 AND @accntrsservice <> 'NT SERVICE\ReportServer' AND @accntrsservice NOT LIKE 'NT SERVICE\ReportServer$%' THEN '[INFORMATION: SQL Server Reporting Services is not running with the default account]'
			ELSE '[OK]' 
		END AS [Deviation]
	UNION ALL
	SELECT 'Service_Account_checks' AS [Category], 'Service_Status' AS [Check], 'Full-Text' AS [Service], ISNULL(@statusftservice, 'Not Installed') AS [Status], ISNULL(@accntftservice,'') AS [Account], 
		CASE WHEN (SELECT ISNULL(FULLTEXTSERVICEPROPERTY('IsFulltextInstalled'),0)) = 1 THEN 
			CASE WHEN @statusftservice = 'Not Installed' THEN '[INFORMATION: Service is not installed]'
				WHEN @statusftservice LIKE 'Stopped%' THEN '[WARNING: Service is stopped]'
				WHEN @accntftservice IS NULL THEN '[WARNING: Could not detect account for check]' 
				WHEN @accntftservice = @accntsqlservice THEN '[WARNING: Running Full-Text Daemon under the same account as SQL Server is not recommended]' 
				WHEN @accntftservice = 'NT AUTHORITY\SYSTEM' THEN '[WARNING: Running Full-Text Service under this account is not recommended]' 
				WHEN @winver IS NULL THEN '[WARNING: Could not determine Windows version for check]'
				WHEN @sqlmajorver <= 10 AND @accntftservice = 'NT AUTHORITY\NETWORKSERVICE' THEN '[WARNING: Running Full-Text Service under this account is not recommended]' 
				WHEN @sqlmajorver <= 10 AND @accntftservice <> 'NT AUTHORITY\LOCALSERVICE' THEN '[WARNING: Full-Text Daemon is not running with the default account]'
				WHEN @sqlmajorver >= 11 AND @winver <= 6.0 AND @accntftservice <> 'NT AUTHORITY\LOCALSERVICE' THEN '[WARNING: Full-Text Daemon is not running with the default account]'
				-- MSA for WS2008R2 or higher, SQL Server 2012 or higher (http://msdn.microsoft.com/en-us/library/ms143504(v=SQL.110).aspx#Default_Accts)
				WHEN @sqlmajorver >= 11 AND @winver >= 6.1 AND @accntftservice <> 'NT SERVICE\MSSQLFDLauncher' AND @accntftservice NOT LIKE 'NT SERVICE\MSSQLFDLauncher$%' THEN '[WARNING: Full-Text Daemon is not running with the default account]'
			ELSE '[OK]' END 
		ELSE '[INFORMATION: Service is not installed]' 
		END AS [Deviation]
	UNION ALL
	SELECT 'Service_Account_checks' AS [Category], 'Service_Status' AS [Check], 'SQL_Server_Browser' AS [Service], @statusbrowservice AS [Status], @accntbrowservice AS [Account],
		CASE WHEN @statusbrowservice = 'Not Installed' THEN '[INFORMATION: Service is not installed]'
			WHEN @statusbrowservice LIKE 'Stopped%' AND @instancename IS NOT NULL THEN '[WARNING: Service is stopped on a named instance]'
			WHEN @statusbrowservice LIKE 'Stopped%' AND @instancename IS NULL THEN '[WARNING: Service is stopped]'
			WHEN @accntbrowservice IS NULL THEN '[WARNING: Could not detect account for check]' 
			WHEN @accntbrowservice = @accntsqlservice THEN '[WARNING: Running SQL Server Browser under the same account as SQL Server is not recommended]' 
			WHEN @accntbrowservice <> 'NT AUTHORITY\LOCALSERVICE' THEN '[WARNING: SQL Server Browser is not running with the default account]'
			ELSE '[OK]' 
		END AS [Deviation];
END
ELSE
BEGIN
	RAISERROR('[WARNING: Only a sysadmin can run the "Service Accounts Status" checks. Otherwise, you must be a granted EXECUTE permissions on xp_regread and xp_servicecontrol. Bypassing check]', 16, 1, N'sysadmin')
	--RETURN
END;

--------------------------------------------------------------------------------------------------------------------------------
-- Service Accounts and SPN registration subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'  |-Starting Service Accounts and SPN registration', 10, 1) WITH NOWAIT
IF @accntsqlservice IS NOT NULL AND @accntsqlservice NOT IN ('NT AUTHORITY\LOCALSERVICE','NT AUTHORITY\SYSTEM','LocalSystem','NT AUTHORITY\NETWORKSERVICE') AND @allow_xpcmdshell = 1
BEGIN
	IF ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) = 1 -- Is sysadmin
		OR ((ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) <> 1 
			AND (SELECT COUNT(credential_id) FROM sys.credentials WHERE name = '##xp_cmdshell_proxy_account##') > 0)) -- Is not sysadmin but proxy account exists
		OR ((ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) <> 1 
			AND (SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_cmdshell') > 0))
	BEGIN
		RAISERROR ('    |-Configuration options set for SPN check', 10, 1) WITH NOWAIT
		SELECT @sao = CAST([value] AS smallint) FROM sys.configurations WITH (NOLOCK) WHERE [name] = 'show advanced options'
		SELECT @xcmd = CAST([value] AS smallint) FROM sys.configurations WITH (NOLOCK) WHERE [name] = 'xp_cmdshell'
		IF @sao = 0
		BEGIN
			EXEC sp_configure 'show advanced options', 1; RECONFIGURE WITH OVERRIDE;
		END
		IF @xcmd = 0
		BEGIN
			EXEC sp_configure 'xp_cmdshell', 1; RECONFIGURE WITH OVERRIDE;
		END

		BEGIN TRY
			DECLARE /*@CMD NVARCHAR(4000),*/ @line int, @linemax int, @SPN VARCHAR(8000), @SPNMachine VARCHAR(8000)
			IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#xp_cmdshell_AcctSPNoutput'))
			DROP TABLE #xp_cmdshell_AcctSPNoutput;
			IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#xp_cmdshell_AcctSPNoutput'))
			CREATE TABLE #xp_cmdshell_AcctSPNoutput (line int IDENTITY(1,1) PRIMARY KEY, [Output] VARCHAR (8000));
			
			IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#xp_cmdshell_DupSPNoutput'))
			DROP TABLE #xp_cmdshell_DupSPNoutput;
			IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#xp_cmdshell_DupSPNoutput'))
			CREATE TABLE #xp_cmdshell_DupSPNoutput (line int IDENTITY(1,1) PRIMARY KEY, [Output] VARCHAR (8000));
			
			IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#FinalDupSPN'))
			DROP TABLE #FinalDupSPN;
			IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#FinalDupSPN'))
			CREATE TABLE #FinalDupSPN ([SPN] VARCHAR (8000), [Accounts] VARCHAR (8000));
			
			IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#ScopedDupSPN'))
			DROP TABLE #ScopedDupSPN;
			IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#ScopedDupSPN'))
			CREATE TABLE #ScopedDupSPN ([SPN] VARCHAR (8000), [Accounts] VARCHAR (8000));

			SELECT @CMD = N'SETSPN -P -L ' + @accntsqlservice 
			INSERT INTO #xp_cmdshell_AcctSPNoutput ([Output])
			EXEC master.dbo.xp_cmdshell @CMD;

			SET @CMD = N'SETSPN -P -X'
			INSERT INTO #xp_cmdshell_DupSPNoutput ([Output])
			EXEC master.dbo.xp_cmdshell @CMD;

			SELECT @SPNMachine = '%MSSQLSvc/' + CONVERT(NVARCHAR(100),SERVERPROPERTY('MachineName')) + '%';

			IF EXISTS (SELECT TOP 1 b.line FROM #xp_cmdshell_AcctSPNoutput a INNER JOIN #xp_cmdshell_DupSPNoutput b ON REPLACE(UPPER(a.[Output]),CHAR(9), '') = LEFT(REPLACE(UPPER(b.[Output]),CHAR(9), ''), LEN(REPLACE(UPPER(a.[Output]),' ', ''))))
			BEGIN
				DECLARE curSPN CURSOR FAST_FORWARD FOR SELECT b.line, REPLACE(a.[Output], CHAR(9), '') FROM #xp_cmdshell_AcctSPNoutput a INNER JOIN #xp_cmdshell_DupSPNoutput b ON REPLACE(UPPER(a.[Output]),CHAR(9), '') = LEFT(REPLACE(UPPER(b.[Output]),CHAR(9), ''), LEN(REPLACE(UPPER(a.[Output]),' ', ''))) WHERE a.[Output] LIKE '%MSSQLSvc%'
				OPEN curSPN
				FETCH NEXT FROM curSPN INTO @line, @SPN

				WHILE @@FETCH_STATUS = 0
				BEGIN
					SELECT TOP 1 @linemax = line FROM #xp_cmdshell_DupSPNoutput WHERE line > @line AND [Output] IS NULL;
					INSERT INTO #FinalDupSPN
					SELECT QUOTENAME(@SPN), QUOTENAME(REPLACE([Output], CHAR(9), '')) FROM #xp_cmdshell_DupSPNoutput WHERE line > @line AND line < @linemax;
				
					IF EXISTS (SELECT [Output] FROM #xp_cmdshell_DupSPNoutput WHERE line = @line AND [Output] LIKE @SPNMachine)
					BEGIN
						INSERT INTO #ScopedDupSPN
						SELECT QUOTENAME(@SPN), QUOTENAME(REPLACE([Output], CHAR(9), '')) FROM #xp_cmdshell_DupSPNoutput WHERE line > @line AND line < @linemax;
					END
					FETCH NEXT FROM curSPN INTO @line, @SPN
				END

				CLOSE curSPN
				DEALLOCATE curSPN
			END

			IF EXISTS (SELECT TOP 1 [Output] FROM #xp_cmdshell_AcctSPNoutput WHERE [Output] LIKE '%MSSQLSvc%')
			BEGIN				
				IF EXISTS (SELECT [Output] FROM #xp_cmdshell_AcctSPNoutput WHERE [Output] LIKE '%MSSQLSvc%' AND [Output] LIKE @SPNMachine)
				BEGIN
					SELECT 'Service_Account_checks' AS [Category], 'MSSQLSvc_SPNs_SvcAcct_CurrServer' AS [Check], '[OK]' AS [Deviation], QUOTENAME(REPLACE([Output], CHAR(9), '')) AS SPN FROM #xp_cmdshell_AcctSPNoutput WHERE [Output] LIKE @SPNMachine
				END
				ELSE
				BEGIN
					SELECT 'Service_Account_checks' AS [Category], 'MSSQLSvc_SPNs_SvcAcct_CurrServer' AS [Check], '[WARNING: There is no registered MSSQLSvc SPN for the current service account in the scoped server name, preventing the use of Kerberos authentication]' AS [Deviation];
				END

				IF EXISTS (SELECT [Output] FROM #xp_cmdshell_AcctSPNoutput WHERE [Output] LIKE '%MSSQLSvc%' AND [Output] NOT LIKE @SPNMachine)
				BEGIN
					SELECT 'Service_Account_checks' AS [Category], 'MSSQLSvc_SPNs_SvcAcct' AS [Check], '[INFORMATION: There are other MSSQLSvc SPNs registered for the current service account]' AS [Deviation], QUOTENAME(REPLACE([Output], CHAR(9), '')) AS SPN FROM #xp_cmdshell_AcctSPNoutput WHERE [Output] LIKE '%MSSQLSvc%' AND [Output] NOT LIKE @SPNMachine
				END
			END
			ELSE
			BEGIN
				SELECT 'Service_Account_checks' AS [Category], 'MSSQLSvc_SPNs_SvcAcct' AS [Check], '[WARNING: There is no registered MSSQLSvc SPN for the current service account, preventing the use of Kerberos authentication]' AS [Deviation];
			END

			IF (SELECT COUNT(*) FROM #ScopedDupSPN) > 0
			BEGIN
				SELECT 'Service_Account_checks' AS [Category], 'Dup_MSSQLSvc_SPNs_Acct_CurrServer' AS [Check], '[WARNING: There are duplicate registered MSSQLSvc SPNs in the domain, for the SPN in the scoped server name]' AS [Deviation], REPLACE([SPN], CHAR(9), ''), [Accounts] AS [Information] FROM #ScopedDupSPN
			END
			ELSE
			BEGIN
				SELECT 'Service_Account_checks' AS [Category], 'Dup_MSSQLSvc_SPNs_Acct_CurrServer' AS [Check], '[OK]' AS [Deviation];
			END

			IF (SELECT COUNT(*) FROM #FinalDupSPN) > 0
			BEGIN
				SELECT 'Service_Account_checks' AS [Category], 'Dup_MSSQLSvc_SPNs_Acct' AS [Check], '[WARNING: There are duplicate registered MSSQLSvc SPNs in the domain]' AS [Deviation], [SPN], [Accounts] FROM #FinalDupSPN
			END
			ELSE
			BEGIN
				SELECT 'Service_Account_checks' AS [Category], 'Dup_MSSQLSvc_SPNs_Acct' AS [Check], '[OK]' AS [Deviation];
			END
		END TRY
		BEGIN CATCH
			SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
			SELECT @ErrorMessage = 'Service Accounts and SPN registration subsection - Error raised in TRY block 9. ' + ERROR_MESSAGE()
			RAISERROR (@ErrorMessage, 16, 1);
		END CATCH
		
		IF @xcmd = 0
		BEGIN
			EXEC sp_configure 'xp_cmdshell', 0; RECONFIGURE WITH OVERRIDE;
		END
		IF @sao = 0
		BEGIN
			EXEC sp_configure 'show advanced options', 0; RECONFIGURE WITH OVERRIDE;
		END
	END
	ELSE
	BEGIN
		RAISERROR('[WARNING: Only a sysadmin can run the "Service Accounts and SPN registration" check. A regular user can also run this check if a xp_cmdshell proxy account exists. Bypassing check]', 16, 1, N'xp_cmdshellproxy')
		RAISERROR('[WARNING: If not sysadmin, then must be a granted EXECUTE permissions on the following extended sprocs to run checks: xp_cmdshell. Bypassing check]', 16, 1, N'extended_sprocs')
		--RETURN
	END
END
ELSE
BEGIN
	RAISERROR('  |- [INFORMATION: "Service Accounts and SPN registration" check was skipped: either xp_cmdshell was not allowed or the service account is not a domain account.]', 10, 1, N'disallow_xp_cmdshell')
	--RETURN
END;

RAISERROR (N'|-Starting Instance Checks', 10, 1) WITH NOWAIT
--------------------------------------------------------------------------------------------------------------------------------
-- Recommended build check subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'  |-Starting Recommended build check', 10, 1) WITH NOWAIT
SELECT 'Instance_checks' AS [Category], 'Recommended_Build' AS [Check],
	CASE WHEN (@sqlmajorver = 9 AND @sqlbuild < 5000) 
			OR (@sqlmajorver = 10 AND @sqlminorver = 0 AND @sqlbuild < 6000) 
			OR (@sqlmajorver = 10 AND @sqlminorver = 50 AND @sqlbuild < 6000) 
			OR (@sqlmajorver = 11 AND @sqlbuild < 5548) 
			OR (@sqlmajorver = 12 AND @sqlbuild < 2430)
		THEN '[WARNING: current service pack has been superseded in the current SQL Server version. Install the latest service pack as soon as possible.]'
		ELSE '[OK]'
	END AS [Deviation], 
	CASE WHEN @sqlmajorver = 9 THEN '2005'
		WHEN @sqlmajorver = 10 AND @sqlminorver = 0 THEN '2008'
		WHEN @sqlmajorver = 10 AND @sqlminorver = 50 THEN '2008R2'
		WHEN @sqlmajorver = 11 THEN '2012'
		WHEN @sqlmajorver = 12 THEN '2014'
	END AS [Product_Major_Version],
	CONVERT(VARCHAR(128), SERVERPROPERTY('ProductLevel')) AS Product_Level;

--------------------------------------------------------------------------------------------------------------------------------
-- Backup checks subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'  |-Starting Backup checks', 10, 1) WITH NOWAIT
DECLARE @nolog int, @nobck int, @nolog24h int, @neverlog int, @neverbck int

-- No Full backups
SELECT @neverbck = COUNT(DISTINCT d.name) 
FROM master.sys.databases d (NOLOCK)
INNER JOIN #tmpdbs_userchoice tuc ON d.database_id = tuc.[dbid]
WHERE database_id NOT IN (2,3)
	AND source_database_id IS NULL -- no snapshots
	AND d.name NOT IN (SELECT b.database_name FROM msdb.dbo.backupset b WHERE b.type = 'D' AND b.is_copy_only = 0) -- Full backup and no COPY_ONLY backups

-- No Full backups in last 7 days
;WITH cteFullBcks (cnt) AS (SELECT DISTINCT database_name AS cnt
FROM msdb.dbo.backupset b (NOLOCK)
INNER JOIN #tmpdbs_userchoice tuc ON b.database_name = tuc.[dbname]
WHERE b.type = 'D' -- Full backup
	AND b.is_copy_only = 0 -- No COPY_ONLY backups
	AND database_name IN (SELECT name FROM master.sys.databases (NOLOCK)
		WHERE database_id NOT IN (2,3)
			AND source_database_id IS NULL) -- no snapshots
GROUP BY database_name
HAVING MAX(backup_finish_date) <= DATEADD(dd, -7, DATEADD(dd, DATEDIFF(dd, 0, GETDATE()) + 1, 0)))
SELECT @nobck = COUNT(cnt)
FROM cteFullBcks;

-- Last Log backup precedes last full or diff backup, and DB in Full or Bulk-logged RM
;WITH cteLogBcks (cnt) AS (SELECT DISTINCT database_name 
FROM msdb.dbo.backupset b (NOLOCK)
INNER JOIN #tmpdbs_userchoice tuc ON b.database_name = tuc.[dbname]
WHERE b.type = 'L' -- Log backup
	AND database_name IN (SELECT name FROM master.sys.databases (NOLOCK)
		WHERE database_id NOT IN (2,3)
			AND source_database_id IS NULL -- no snapshots
			AND recovery_model < 3) -- not SIMPLE recovery model
GROUP BY [database_name]
HAVING MAX(backup_finish_date) < (SELECT MAX(backup_finish_date) FROM msdb.dbo.backupset c (NOLOCK) WHERE c.type IN ('D','I') -- Full or Differential backup
								AND c.is_copy_only = 0 -- No COPY_ONLY backups
								AND c.database_name = b.database_name))
SELECT @nolog = COUNT(cnt)
FROM cteLogBcks;

-- No Log backup since last full or diff backup, and DB in Full or Bulk-logged RM
SELECT @neverlog = COUNT(DISTINCT database_name)
FROM msdb.dbo.backupset b (NOLOCK)
INNER JOIN #tmpdbs_userchoice tuc ON b.database_name = tuc.[dbname]
WHERE database_name IN (SELECT name 
			FROM master.sys.databases (NOLOCK)
			WHERE database_id NOT IN (2,3)
				AND source_database_id IS NULL -- no snapshots
				AND recovery_model < 3) -- not SIMPLE recovery model
	AND EXISTS (SELECT DISTINCT database_name 
			FROM msdb.dbo.backupset c (NOLOCK)
			WHERE c.type IN ('D','I') -- Full or Differential backup
			AND c.is_copy_only = 0 -- No COPY_ONLY backups
			AND c.database_name = b.database_name) -- Log backup
	AND NOT EXISTS (SELECT DISTINCT database_name 
			FROM msdb.dbo.backupset c (NOLOCK)
			WHERE c.type = 'L' -- Log Backup
			AND c.database_name = b.database_name);

-- Log backup since last full or diff backup is older than 24h, and DB in Full ar Bulk-logged RM
;WITH cteLogBcks2 (cnt) AS (SELECT DISTINCT database_name 
FROM msdb.dbo.backupset b (NOLOCK)
INNER JOIN #tmpdbs_userchoice tuc ON b.database_name = tuc.[dbname]
WHERE b.type = 'L' -- Log backup
	AND database_name IN (SELECT name FROM master.sys.databases (NOLOCK)
		WHERE database_id NOT IN (2,3)
			AND source_database_id IS NULL -- no snapshots
			AND recovery_model < 3) -- not SIMPLE recovery model
GROUP BY database_name
HAVING MAX(backup_finish_date) > (SELECT MAX(backup_finish_date) FROM msdb.dbo.backupset c (NOLOCK) WHERE c.type IN ('D','I') -- Full or Differential backup
								AND c.is_copy_only = 0 -- No COPY_ONLY backups
								AND c.database_name = b.database_name)
	AND MAX(backup_finish_date) <= DATEADD(hh, -24, GETDATE()))
SELECT @nolog24h = COUNT(cnt)
FROM cteLogBcks2;

IF @nobck > 0 OR @neverbck > 0
BEGIN
	SELECT 'Instance_checks' AS [Category], 'No_Full_Backups' AS [Check], '[WARNING: Some databases do not have any Full backups, or the last Full backup is over 7 days]' AS [Deviation]
	-- No full backups in last 7 days
	SELECT DISTINCT 'Instance_checks' AS [Category], 'No_Full_Backups' AS [Information], database_name AS [Database_Name], MAX(backup_finish_date) AS Lst_Full_Backup
	FROM msdb.dbo.backupset b (NOLOCK)
	INNER JOIN #tmpdbs_userchoice tuc ON b.database_name = tuc.[dbname]
	WHERE b.type = 'D' -- Full backup
		AND b.is_copy_only = 0 -- No COPY_ONLY backups
		AND database_name IN (SELECT name FROM master.sys.databases (NOLOCK)
			WHERE database_id NOT IN (2,3)
				AND source_database_id IS NULL) -- no snapshots
	GROUP BY database_name
	HAVING MAX(backup_finish_date) <= DATEADD(dd, -7, DATEADD(dd, DATEDIFF(dd, 0, GETDATE()) + 1, 0))
	UNION ALL
	-- No full backups in history
	SELECT DISTINCT 'Instance_checks' AS [Category], 'No_Full_Backups' AS [Information], d.name AS [Database_Name], NULL AS Lst_Full_Backup
	FROM master.sys.databases d (NOLOCK)
	INNER JOIN #tmpdbs_userchoice tuc ON d.database_id = tuc.[dbid]
	WHERE database_id NOT IN (2,3)
		AND source_database_id IS NULL -- no snapshots
		AND recovery_model < 3 -- not SIMPLE recovery model
		AND d.name NOT IN (SELECT b.database_name FROM msdb.dbo.backupset b WHERE b.type = 'D' AND b.is_copy_only = 0) -- Full backup and no COPY_ONLY backups
		AND d.name NOT IN (SELECT b.database_name FROM msdb.dbo.backupset b WHERE b.type = 'L') -- Log backup
	ORDER BY [Database_Name]
END
ELSE
BEGIN
	SELECT 'Instance_checks' AS [Category], 'No_Full_Backups' AS [Check], '[OK]' AS [Deviation]
END;

IF @nolog > 0 OR @neverlog > 0
BEGIN
	SELECT 'Instance_checks' AS [Category], 'No_Log_Bcks_since_LstFullorDiff' AS [Check], '[WARNING: Some databases in Full or Bulk-Logged recovery model do not have any corresponding transaction Log backups since the last Full or Differential backup]' AS [Deviation]
	;WITH Bck AS (SELECT database_name, MAX(backup_finish_date) AS backup_finish_date
					FROM msdb.dbo.backupset (NOLOCK) b
					INNER JOIN #tmpdbs_userchoice tuc ON b.database_name = tuc.[dbname]
					WHERE [type] IN ('D','I') -- Full or Differential backup
					GROUP BY database_name)
	-- Log backups since last full or diff is older than 24h
	SELECT DISTINCT 'Instance_checks' AS [Category], 'No_Log_Bcks_since_LstFullorDiff' AS [Information], database_name AS [Database_Name], MAX(backup_finish_date) AS Lst_Log_Backup,
		(SELECT backup_finish_date FROM Bck c WHERE c.database_name = b.database_name) AS Lst_FullDiff_Backup
	FROM msdb.dbo.backupset b (NOLOCK)
	INNER JOIN #tmpdbs_userchoice tuc ON b.database_name = tuc.[dbname]
	WHERE b.type = 'L' -- Log backup
		AND database_name IN (SELECT name FROM master.sys.databases (NOLOCK)
			WHERE database_id NOT IN (2,3)
				AND source_database_id IS NULL -- no snapshots
				AND recovery_model < 3) -- not SIMPLE recovery model
	GROUP BY [database_name]
	HAVING MAX(backup_finish_date) < (SELECT backup_finish_date FROM Bck c WHERE c.database_name = b.database_name)
	UNION ALL
	-- No log backup in history but full backup exists
	SELECT DISTINCT 'Instance_checks' AS [Category], 'No_Log_Bcks_since_LstFullorDiff' AS [Information], database_name AS [Database_Name], NULL AS Lst_Log_Backup, MAX(backup_finish_date) AS Lst_FullDiff_Backup
	FROM msdb.dbo.backupset b (NOLOCK)
	INNER JOIN #tmpdbs_userchoice tuc ON b.database_name = tuc.[dbname]
	WHERE database_name IN (SELECT name 
				FROM master.sys.databases (NOLOCK)
				WHERE database_id NOT IN (2,3)
					AND source_database_id IS NULL -- no snapshots
					AND recovery_model < 3) -- not SIMPLE recovery model
		AND EXISTS (SELECT DISTINCT database_name 
				FROM msdb.dbo.backupset c (NOLOCK)
				WHERE c.type IN ('D','I') -- Full or Differential backup
				AND c.is_copy_only = 0 -- No COPY_ONLY backups
				AND c.database_name = b.database_name) -- Log backup
		AND NOT EXISTS (SELECT DISTINCT database_name 
				FROM msdb.dbo.backupset c (NOLOCK)
				WHERE c.type = 'L' -- Log Backup
				AND c.database_name = b.database_name)
	GROUP BY database_name
	ORDER BY database_name;
END
ELSE
BEGIN
	SELECT 'Instance_checks' AS [Category], 'No_Log_Bcks_since_LstFullorDiff' AS [Check], '[OK]' AS [Deviation]
END;

IF @nolog24h > 0
BEGIN
	SELECT 'Instance_checks' AS [Category], 'Log_Bcks_since_LstFullorDiff_are_older_than_24H' AS [Check], '[WARNING: Some databases in Full or Bulk-Logged recovery model have their latest log backup older than 24H]' AS [Deviation]
	SELECT DISTINCT 'Instance_checks' AS [Category], 'Log_Bcks_since_LstFullorDiff_are_older_than_24H' AS [Information], database_name AS [Database_Name], MAX(backup_finish_date) AS Lst_Log_Backup
	FROM msdb.dbo.backupset b (NOLOCK)
	INNER JOIN #tmpdbs_userchoice tuc ON b.database_name = tuc.[dbname]
	WHERE b.type = 'L' -- Log backup
		AND database_name IN (SELECT name FROM master.sys.databases (NOLOCK)
			WHERE database_id NOT IN (2,3)
				AND recovery_model < 3) -- not SIMPLE recovery model
	GROUP BY database_name
	HAVING MAX(backup_finish_date) > (SELECT MAX(backup_finish_date) FROM msdb.dbo.backupset c (NOLOCK) WHERE c.type IN ('D', 'I') -- Full or Differential backup
									AND c.is_copy_only = 0 -- No COPY_ONLY backups
									AND c.database_name = b.database_name)
		AND MAX(backup_finish_date) <= DATEADD(hh, -24, GETDATE())
	ORDER BY [database_name];
END
ELSE
BEGIN
	SELECT 'Instance_checks' AS [Category], 'Log_Bcks_since_LstFullorDiff_are_older_than_24H' AS [Check], '[OK]' AS [Deviation]
END;

--------------------------------------------------------------------------------------------------------------------------------
-- Global trace flags subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'  |-Starting Global trace flags', 10, 1) WITH NOWAIT
DECLARE @tracestatus TABLE (TraceFlag NVARCHAR(40), [Status] tinyint, [Global] tinyint, [Session] tinyint);

INSERT INTO @tracestatus 
EXEC ('DBCC TRACESTATUS WITH NO_INFOMSGS')

IF @sqlmajorver >= 11
BEGIN
	DECLARE @dbname0 VARCHAR(1000), @dbid0 int, @sqlcmd0 NVARCHAR(4000), @has_colstrix int

	IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblColStoreIXs'))
	DROP TABLE #tblColStoreIXs;
	IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblColStoreIXs'))
	CREATE TABLE #tblColStoreIXs ([DBName] VARCHAR(1000), [Schema] VARCHAR(100), [Table] VARCHAR(255), [Object] VARCHAR(255));

	UPDATE #tmpdbs0
	SET isdone = 0;

	UPDATE #tmpdbs0
	SET isdone = 1
	WHERE [state] <> 0 OR [dbid] < 5;

	UPDATE #tmpdbs0
	SET isdone = 1
	WHERE [role] = 2 AND secondary_role_allow_connections = 0;

	IF (SELECT COUNT(id) FROM #tmpdbs0 WHERE isdone = 0) > 0
	BEGIN	
		WHILE (SELECT COUNT(id) FROM #tmpdbs0 WHERE isdone = 0) > 0
		BEGIN
			SELECT TOP 1 @dbname0 = [dbname], @dbid0 = [dbid] FROM #tmpdbs0 WHERE isdone = 0

			SET @sqlcmd0 = 'USE ' + QUOTENAME(@dbname0) + ';
SELECT ''' + @dbname0 + ''' AS [DBName], QUOTENAME(t.name), QUOTENAME(o.[name]), i.name 
FROM sys.indexes AS i (NOLOCK)
INNER JOIN sys.objects AS o (NOLOCK) ON o.[object_id] = i.[object_id]
INNER JOIN sys.tables AS mst (NOLOCK) ON mst.[object_id] = i.[object_id]
INNER JOIN sys.schemas AS t (NOLOCK) ON t.[schema_id] = mst.[schema_id]
WHERE i.[type] IN (5,6,7)' -- 5 = Clustered columnstore; 6 = Nonclustered columnstore; 7 = Nonclustered hash

			BEGIN TRY
				INSERT INTO #tblColStoreIXs
				EXECUTE sp_executesql @sqlcmd0
			END TRY
			BEGIN CATCH
				SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
				SELECT @ErrorMessage = 'Global trace flags subsection - Error raised in TRY block. ' + ERROR_MESSAGE()
				RAISERROR (@ErrorMessage, 16, 1);
			END CATCH
			
			UPDATE #tmpdbs0
			SET isdone = 1
			WHERE [dbid] = @dbid0
		END
	END;
	
	SELECT @has_colstrix = COUNT(*) FROM #tblColStoreIXs
END;

IF (SELECT COUNT(TraceFlag) FROM @tracestatus WHERE [Global]=1) = 0
BEGIN
	SELECT 'Instance_checks' AS [Category], 'Global_Trace_Flags' AS [Check], '[There are no Global Trace Flags active]' AS [Deviation]
END;

-- Plan affecting TFs: http://support.microsoft.com/kb/2801413
IF EXISTS (SELECT TraceFlag FROM @tracestatus WHERE [Global] = 1)
BEGIN
	IF EXISTS (SELECT TraceFlag FROM @tracestatus WHERE [Global] = 1 AND TraceFlag = 634)
	BEGIN
		SELECT 'Instance_checks' AS [Category], 'Global_Trace_Flags' AS [Check], 
			'[INFORMATION: TF634 disables the background columnstore compression task]' 
			AS [Deviation], TraceFlag
		FROM @tracestatus 
		WHERE [Global] = 1 AND TraceFlag = 634
	END;

	IF EXISTS (SELECT TraceFlag FROM @tracestatus WHERE [Global] = 1 AND TraceFlag = 661)
	BEGIN
		SELECT 'Instance_checks' AS [Category], 'Global_Trace_Flags' AS [Check], 
			'[INFORMATION: TF661 disables the ghost cleanup background task]' --http://support.microsoft.com/kb/920093  
			AS [Deviation], TraceFlag
		FROM @tracestatus 
		WHERE [Global] = 1 AND TraceFlag = 661
	END;

	IF EXISTS (SELECT TraceFlag FROM @tracestatus WHERE [Global] = 1 AND TraceFlag = 845)
	BEGIN
		SELECT 'Instance_checks' AS [Category], 'Global_Trace_Flags' AS [Check], 
			CASE WHEN SERVERPROPERTY('EngineEdition') = 2 --Standard SKU
					AND ((@sqlmajorver = 10 AND ((@sqlminorver = 0 AND @sqlbuild >= 2714) OR @sqlminorver = 50)) 
						OR (@sqlmajorver = 9 AND @sqlbuild >= 4226))
					THEN '[INFORMATION: TF845 supports locking pages in memory in SQL Server Standard Editions]'
				WHEN SERVERPROPERTY('EngineEdition') = 2 --Standard SKU
					AND @sqlmajorver = 11
					THEN '[WARNING: TF845 is not needed in SQL 2012]'
			ELSE '[WARNING: Verify need to set a Non-default TF with current system build and configuration]'
			END AS [Deviation], TraceFlag
		FROM @tracestatus 
		WHERE [Global] = 1 AND TraceFlag = 845
	END;
	
	IF EXISTS (SELECT TraceFlag FROM @tracestatus WHERE [Global] = 1 AND TraceFlag = 834)
	BEGIN
		SELECT 'Instance_checks' AS [Category], 'Global_Trace_Flags' AS [Check],
			CASE WHEN @sqlmajorver >= 11
				AND @has_colstrix > 0
				THEN '[WARNING: TF834 (Large Page Support for BP) is discouraged when Columnstore Indexes are used]' --http://support.microsoft.com/kb/920093
			ELSE '[WARNING: Verify need to set a Non-default TF with current system build and configuration]'
			END AS [Deviation], TraceFlag
		FROM @tracestatus
		WHERE [Global] = 1 AND TraceFlag = 834
	END;
	
	IF EXISTS (SELECT TraceFlag FROM @tracestatus WHERE [Global] = 1 AND TraceFlag = 1117)
	BEGIN
		SELECT 'Instance_checks' AS [Category], 'Global_Trace_Flags' AS [Check], 
			'[INFORMATION: TF1117 autogrows all files at the same time and affects all databases]' 
			AS [Deviation], TraceFlag
		FROM @tracestatus 
		WHERE [Global] = 1 AND TraceFlag = 1117
	END;
	
	IF EXISTS (SELECT TraceFlag FROM @tracestatus WHERE [Global] = 1 AND TraceFlag = 1118)
	BEGIN
		SELECT 'Instance_checks' AS [Category], 'Global_Trace_Flags' AS [Check],
			'[INFORMATION: TF1118 forces uniform extent allocations instead of mixed page allocations]' --http://support.microsoft.com/kb/328551
			AS [Deviation], TraceFlag
		FROM @tracestatus 
		WHERE [Global] = 1 AND TraceFlag = 1118
	END;
	
	IF EXISTS (SELECT TraceFlag FROM @tracestatus WHERE [Global] = 1 AND TraceFlag = 1211)
	BEGIN
		SELECT 'Instance_checks' AS [Category], 'Global_Trace_Flags' AS [Check],
			'[WARNING: TF1211 disables lock escalation based on memory pressure, or based on number of locks, increasing the amount of locks held]'
			AS [Deviation], TraceFlag
		FROM @tracestatus 
		WHERE [Global] = 1 AND TraceFlag = 1211
	END;
	
	IF EXISTS (SELECT TraceFlag FROM @tracestatus WHERE [Global] = 1 AND TraceFlag = 1224)
	BEGIN
		SELECT 'Instance_checks' AS [Category], 'Global_Trace_Flags' AS [Check],
			'[WARNING: TF1224 disables lock escalation based on the number of locks, and only escalates locks under memory pressure, increasing the amount of locks held]'
			AS [Deviation], TraceFlag
		FROM @tracestatus 
		WHERE [Global] = 1 AND TraceFlag = 1224
	END;
	
	IF EXISTS (SELECT TraceFlag FROM @tracestatus WHERE [Global] = 1 AND TraceFlag = 1229)
	BEGIN
		SELECT 'Instance_checks' AS [Category], 'Global_Trace_Flags' AS [Check],
			'[WARNING: TF1229 disables lock partitioning, which is a locking mechanism optimization on 16+ CPU servers]' --http://blogs.msdn.com/b/psssql/archive/2012/08/31/strange-sch-s-sch-m-deadlock-on-machines-with-16-or-more-schedulers.aspx
			AS [Deviation], TraceFlag
		FROM @tracestatus 
		WHERE [Global] = 1 AND TraceFlag = 1229
	END;
	
	IF EXISTS (SELECT TraceFlag FROM @tracestatus WHERE [Global] = 1 AND TraceFlag = 2330)
	BEGIN
		SELECT 'Instance_checks' AS [Category], 'Global_Trace_Flags' AS [Check],
			CASE WHEN @sqlmajorver = 9
				THEN '[INFORMATION: TF2330 supresses data collection into sys.dm_db_index_usage_stats, which can lead to a non-yielding condition in SQL 2005]' --http://support.microsoft.com/default.aspx?scid=kb;en-US;2003031
			ELSE '[WARNING: Verify need to set a Non-default TF with current system build and configuration]'
			END AS [Deviation], TraceFlag
		FROM @tracestatus 
		WHERE [Global] = 1 AND TraceFlag = 2330
	END;
	
	IF EXISTS (SELECT TraceFlag FROM @tracestatus WHERE [Global] = 1 AND TraceFlag = 2335)
	BEGIN
		SELECT 'Instance_checks' AS [Category], 'Global_Trace_Flags' AS [Check],
			CASE WHEN @sqlmajorver >= 9
				AND @maxservermem >= 100000 * 1024 -- 100GB
				AND @maxservermem <> 2147483647
				THEN '[INFORMATION: TF2335 generates plans that are more conservative in terms of memory consumption when executing a query. Recommended when server has more than 100GB of memory]' --http://support.microsoft.com/kb/2413549/en-us
			WHEN @sqlmajorver >= 9
				AND @maxservermem < 100000 * 1024 -- 100GB
				AND @maxservermem <> 2147483647
				THEN '[WARNING: TF2335 should not be set on servers with less than 100GB of memory]' --http://support.microsoft.com/kb/2413549/en-us
			ELSE '[WARNING: Verify need to set a Non-default TF with current system build and configuration]'
			END AS [Deviation], TraceFlag
		FROM @tracestatus 
		WHERE [Global] = 1 AND TraceFlag = 2335
	END;
	
	IF EXISTS (SELECT TraceFlag FROM @tracestatus WHERE [Global] = 1 AND TraceFlag = 2371)
	BEGIN
		SELECT 'Instance_checks' AS [Category], 'Global_Trace_Flags' AS [Check],
			CASE WHEN (@sqlmajorver = 10 AND @sqlminorver = 50 AND @sqlbuild >= 2500) OR @sqlmajorver >= 11
				THEN '[INFORMATION: TF2371 changes the fixed rate of the 20pct threshold for update statistics into a dynamic percentage rate]' --http://blogs.msdn.com/b/saponsqlserver/archive/2011/09/07/changes-to-automatic-update-statistics-in-sql-server-traceflag-2371.aspx
			ELSE '[WARNING: Verify need to set a Non-default TF with current system build and configuration]'
			END AS [Deviation], TraceFlag
		FROM @tracestatus 
		WHERE [Global] = 1 AND TraceFlag = 2371
	END;
	
	IF EXISTS (SELECT TraceFlag FROM @tracestatus WHERE [Global] = 1 AND TraceFlag = 4199)
	BEGIN
		SELECT 'Instance_checks' AS [Category], 'Global_Trace_Flags' AS [Check],
			CASE WHEN (@sqlmajorver = 10 AND @sqlminorver = 0 AND @sqlbuild >= 1787 AND @sqlbuild < 1818)
					OR (@sqlmajorver = 10 AND @sqlminorver = 0 AND @sqlbuild >= 2531 AND @sqlbuild < 2766)
					OR (@sqlmajorver >= 10 AND @sqlminorver = 50 AND @sqlbuild >= 1600 AND @sqlbuild < 1702)
				THEN '[WARNING: TF4135 should be used instead of TF4199 in this SQL build]'
			END AS [Deviation], TraceFlag
		FROM @tracestatus 
		WHERE [Global] = 1 AND TraceFlag = 4199
	END;
	
	IF EXISTS (SELECT TraceFlag FROM @tracestatus WHERE [Global] = 1 AND TraceFlag = 4135)
	BEGIN
		SELECT 'Instance_checks' AS [Category], 'Global_Trace_Flags' AS [Check],
			CASE WHEN (@sqlmajorver = 10 AND @sqlminorver = 0 AND @sqlbuild >= 1818 AND @sqlbuild < 2531)
					OR (@sqlmajorver = 10 AND @sqlminorver = 50 AND @sqlbuild >= 1702)
				THEN '[WARNING: TF4199 should be used instead of TF4135 in this SQL build]'
			END AS [Deviation], TraceFlag
		FROM @tracestatus 
		WHERE [Global] = 1 AND TraceFlag = 4135
	END;
	
	IF EXISTS (SELECT TraceFlag FROM @tracestatus WHERE [Global] = 1 AND TraceFlag = 4135)
	BEGIN
		SELECT 'Instance_checks' AS [Category], 'Global_Trace_Flags' AS [Check],
			CASE WHEN (@sqlmajorver = 10 AND @sqlminorver = 0 AND @sqlbuild >= 1787 AND @sqlbuild < 1818)
					OR (@sqlmajorver = 10 AND @sqlminorver = 0 AND @sqlbuild >= 2531 AND @sqlbuild < 2766)
					OR (@sqlmajorver = 10 AND @sqlminorver = 0 AND @sqlbuild >= 2766)
					OR (@sqlmajorver = 10 AND @sqlminorver = 50 AND @sqlbuild >= 1600 AND @sqlbuild < 1702)
				THEN '[INFORMATION: TF4135 supports fixes and enhancements on the query optimizer]'
			END AS [Deviation], TraceFlag	
		FROM @tracestatus 
		WHERE [Global] = 1 AND TraceFlag = 4135
	END;
	
	IF EXISTS (SELECT TraceFlag FROM @tracestatus WHERE [Global] = 1 AND TraceFlag = 4136)
	BEGIN
		SELECT 'Instance_checks' AS [Category], 'Global_Trace_Flags' AS [Check],
			'[WARNING: TF4136 disables the parameter sniffing process, which is equivalent to adding an OPTIMIZE FOR UNKNOWN hint to each query which references a parameter]' --http://support.microsoft.com/kb/980653/en-us
			AS [Deviation], TraceFlag
		FROM @tracestatus 
		WHERE [Global] = 1 AND TraceFlag = 4136
	END;
	
	IF EXISTS (SELECT TraceFlag FROM @tracestatus WHERE [Global] = 1 AND TraceFlag = 4137)
	BEGIN
		SELECT 'Instance_checks' AS [Category], 'Global_Trace_Flags' AS [Check],
			CASE WHEN (@sqlmajorver >= 10 AND @sqlminorver = 0 AND @sqlbuild >= 5794)
					OR (@sqlmajorver >= 10 AND @sqlminorver = 0 AND @sqlbuild >= 4326)
					OR (@sqlmajorver >= 10 AND @sqlminorver = 50 AND @sqlbuild >= 2806)
					OR (@sqlmajorver >= 11 AND @sqlbuild >= 2316)
				THEN '[INFORMATION: TF4137 supports fixes and enhancements on the query optimizer]' --http://support.microsoft.com/kb/2658214
			END AS [Deviation], TraceFlag
		FROM @tracestatus 
		WHERE [Global] = 1 AND TraceFlag = 4137
	END;
	
	IF EXISTS (SELECT TraceFlag FROM @tracestatus WHERE [Global] = 1 AND TraceFlag = 4199)
	BEGIN
		SELECT 'Instance_checks' AS [Category], 'Global_Trace_Flags' AS [Check],
			CASE WHEN (@sqlmajorver = 9 AND @sqlbuild >= 4266)
					OR (@sqlmajorver = 10 AND @sqlminorver = 0 AND @sqlbuild >= 1818 AND @sqlbuild < 2531)
					OR (@sqlmajorver = 10 AND @sqlminorver = 0 AND @sqlbuild >= 2766)
					OR (@sqlmajorver = 10 AND @sqlminorver = 50 AND @sqlbuild >= 1702)
					OR @sqlmajorver >= 11
				THEN '[INFORMATION: TF4199 supports fixes and enhancements on the query optimizer]' --http://support.microsoft.com/kb/2801413
			END AS [Deviation], TraceFlag
		FROM @tracestatus 
		WHERE [Global] = 1 AND TraceFlag = 4199
	END;
	
	IF EXISTS (SELECT TraceFlag FROM @tracestatus WHERE [Global] = 1 AND TraceFlag = 8015)
	BEGIN
		SELECT 'Instance_checks' AS [Category], 'Global_Trace_Flags' AS [Check],
			'[WARNING: TF8015 ignores NUMA detection]' --http://blogs.msdn.com/b/psssql/archive/2010/04/02/how-it-works-soft-numa-i-o-completion-thread-lazy-writer-workers-and-memory-nodes.aspx
			AS [Deviation], TraceFlag
		FROM @tracestatus 
		WHERE [Global] = 1 AND TraceFlag = 8015
	END;
	
	IF EXISTS (SELECT TraceFlag FROM @tracestatus WHERE [Global] = 1 AND TraceFlag = 8048)
	BEGIN
		SELECT 'Instance_checks' AS [Category], 'Global_Trace_Flags' AS [Check],
			CASE WHEN (@cpucount/@numa) > 8
				THEN '[INFORMATION: TF8048 changes memory grants on NUMA from NODE based partitioning to CPU based partitioning]' --http://blogs.msdn.com/b/psssql/archive/2011/09/01/sql-server-2008-2008-r2-on-newer-machines-with-more-than-8-cpus-presented-per-numa-node-may-need-trace-flag-8048.aspx
			ELSE '[WARNING: Verify need to set a Non-default TF with current system build and configuration]'
			END AS [Deviation], TraceFlag
		FROM @tracestatus 
		WHERE [Global] = 1 AND TraceFlag = 8048
	END;
END;

IF NOT EXISTS (SELECT TraceFlag FROM @tracestatus WHERE [Global] = 1 AND TraceFlag = 2335)
	AND @sqlmajorver >= 9
	AND @maxservermem >= 100000 * 1024 -- 100GB
	AND @maxservermem <> 2147483647
BEGIN
	SELECT 'Instance_checks' AS [Category], 'Global_Trace_Flags' AS [Check], 
		'[INFORMATION: Consider enabling TF2335 to generate plans that are more conservative in terms of memory consumption when executing a query. Recommended when server has more than 100GB of memory]' --http://support.microsoft.com/kb/2413549/en-us
		AS [Deviation]
END;
		
IF NOT EXISTS (SELECT TraceFlag FROM @tracestatus WHERE [Global] = 1 AND TraceFlag = 2371)
	AND ((@sqlmajorver = 10 AND @sqlminorver = 50 AND @sqlbuild >= 2500) OR @sqlmajorver >= 11)
BEGIN
	SELECT 'Instance_checks' AS [Category], 'Global_Trace_Flags' AS [Check], 
		'[INFORMATION: Consider enabling TF2371 to change the 20pct fixed rate threshold for update statistics into a dynamic percentage rate]' --http://blogs.msdn.com/b/saponsqlserver/archive/2011/09/07/changes-to-automatic-update-statistics-in-sql-server-traceflag-2371.aspx
		AS [Deviation]
END;

IF NOT EXISTS (SELECT TraceFlag FROM @tracestatus WHERE [Global] = 1 AND TraceFlag = 8048) AND (@cpucount/@numa) > 8 AND @sqlmajorver > 9
BEGIN
	SELECT 'Instance_checks' AS [Category], 'Global_Trace_Flags' AS [Check],
		'[INFORMATION: Consider enabling TF8048 to change memory grants on NUMA from NODE based partitioning to CPU based partitioning. Look in dm_os_wait_stats and dm_os_spin_stats for wait types (CMEMTHREAD and SOS_SUSPEND_QUEUE). Microsoft CSS usually sees the spins jump into the trillions and the waits become a hot spot]' --http://blogs.msdn.com/b/psssql/archive/2011/09/01/sql-server-2008-2008-r2-on-newer-machines-with-more-than-8-cpus-presented-per-numa-node-may-need-trace-flag-8048.aspx
		AS [Deviation];
	
	-- If the top consumers are partitioned by Node, then use startup trace flag 8048 to further partition by CPU.
	IF @sqlmajorver < 11
	BEGIN
		SELECT 'Instance_checks' AS [Category], '8048_Trace_Flag' AS [Check], [type], 
			SUM(page_size_in_bytes)/8192 AS [pages], 
			SUM(page_size_in_bytes)/1024 AS pages_in_KB,
			CASE WHEN (0x20 = creation_options & 0x20) THEN 'Global PMO. Cannot be partitioned by CPU/NUMA Node. TF8048 not applicable.'
				WHEN (0x40 = creation_options & 0x40) THEN 'Partitioned by CPU. TF8048 not applicable.'
				WHEN (0x80 = creation_options & 0x80) THEN 'Partitioned by Node. Use TF8048 to further partition by CPU'
				ELSE 'Unknown' END AS [Comment]
		FROM sys.dm_os_memory_objects
		GROUP BY [type], creation_options
		ORDER BY SUM(page_size_in_bytes) DESC;
	END
	ELSE
	BEGIN
		SET @sqlcmd = N'SELECT ''Instance_checks'' AS [Category], ''8048_Trace_Flag'' AS [Check], [type], 
	SUM(pages_in_bytes)/8192 AS [pages], 
	SUM(pages_in_bytes)/1024 AS pages_in_KB,
	CASE WHEN (0x20 = creation_options & 0x20) THEN ''Global PMO. Cannot be partitioned by CPU/NUMA Node. TF8048 not applicable.''
		WHEN (0x40 = creation_options & 0x40) THEN ''Partitioned by CPU. TF8048 not applicable.''
		WHEN (0x80 = creation_options & 0x80) THEN ''Partitioned by Node. Use TF8048 to further partition by CPU''
		ELSE ''Unknown'' END AS [Comment]
FROM sys.dm_os_memory_objects
GROUP BY [type], creation_options
ORDER BY SUM(pages_in_bytes) DESC;'
		EXECUTE sp_executesql @sqlcmd
	END;
END;
		
IF NOT EXISTS (SELECT TraceFlag FROM @tracestatus WHERE [Global] = 1 AND TraceFlag IN (4135,4199))
BEGIN
	SELECT 'Instance_checks' AS [Category], 'Global_Trace_Flags' AS [Check], 
		CASE WHEN @sqlmajorver = 9 AND @sqlbuild >= 4266 
			THEN '[INFORMATION: Consider enabling TF4199 to support fixes and enhancements on the query optimizer]'
		WHEN @sqlmajorver = 10 AND @sqlminorver = 0 AND @sqlbuild >= 1787 AND @sqlbuild < 1818 
			THEN '[INFORMATION: Consider enabling TF4135 to support fixes and enhancements on the query optimizer]'
		WHEN @sqlmajorver = 10 AND @sqlminorver = 0 AND @sqlbuild >= 2531 AND @sqlbuild < 2766 
			THEN '[INFORMATION: Consider enabling TF4135 to support fixes and enhancements on the query optimizer]'
		WHEN @sqlmajorver = 10 AND @sqlminorver = 0 AND @sqlbuild > = 2766 
			THEN '[INFORMATION: Consider enabling TF4135 to support fixes and enhancements on the query optimizer]'
		WHEN @sqlmajorver = 10 AND @sqlminorver = 50 AND @sqlbuild >= 1600 AND @sqlbuild < 1702 
			THEN '[INFORMATION: Consider enabling TF4135 to support fixes and enhancements on the query optimizer]'
		WHEN @sqlmajorver = 10 AND @sqlminorver = 0 AND @sqlbuild >= 1818 AND @sqlbuild < 2531 
			THEN '[INFORMATION: Consider enabling TF4199 to support fixes and enhancements on the query optimizer]'
		WHEN @sqlmajorver = 10 AND @sqlminorver = 50 AND @sqlbuild >= 1702 
			THEN '[INFORMATION: Consider enabling TF4199 to support fixes and enhancements on the query optimizer]'
		WHEN @sqlmajorver >= 11 
			THEN '[INFORMATION: Consider enabling TF4199 to support fixes and enhancements on the query optimizer]' 
		END AS [Deviation]
END;
	
--------------------------------------------------------------------------------------------------------------------------------
-- System configurations subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'  |-Starting System configurations', 10, 1) WITH NOWAIT
-- Focus on:
-- backup compression default
-- clr enabled (only enable if needed)
-- lightweight pooling (should be zero)
-- max degree of parallelism 
-- max server memory (MB) (set to an appropriate value)
-- priority boost (should be zero)
-- remote admin connections (should be enabled in a cluster configuration, to allow remote DAC)
-- scan for startup procs (should be disabled unless business requirement, like replication)
-- min memory per query (default is 1024KB)
-- allow updates (no effect in 2005 or above, but should be off)
-- max worker threads (should be zero in 2005 or above)
-- affinity mask and affinity I/O mask (must not overlap)

DECLARE @awe tinyint, @ssp bit, @bckcomp bit, @clr bit, @costparallel tinyint, @chain bit, @lpooling bit
DECLARE @adhoc smallint, @pboost bit, @qtimeout int, @cmdshell bit, @deftrace bit, @remote bit
DECLARE @minmemqry int, @allowupd bit, @mwthreads int, @recinterval int, @netsize smallint
DECLARE @ixmem smallint, @adhocqry bit, @locks int, @qrywait int--, @mwthreads_count int
DECLARE @affin int, @affinIO int, @affin64 int, @affin64IO int, @block_threshold int, @oleauto int

--SELECT @mwthreads_count = max_workers_count FROM sys.dm_os_sys_info;

SELECT @awe = CONVERT(tinyint, [value]) FROM sys.configurations WHERE [Name] = 'awe enabled';
SELECT @bckcomp = CONVERT(bit, [value]) FROM sys.configurations WHERE [Name] = 'backup compression default';
SELECT @clr = CONVERT(bit, [value]) FROM sys.configurations WHERE [Name] = 'clr enabled';
SELECT @costparallel = CONVERT(tinyint, [value]) FROM sys.configurations WHERE [Name] = 'cost threshold for parallelism';
SELECT @chain = CONVERT(bit, [value]) FROM sys.configurations WHERE [Name] = 'cross db ownership chaining';
SELECT @lpooling = CONVERT(bit, [value]) FROM sys.configurations WHERE [Name] = 'lightweight pooling';
SELECT @pboost = CONVERT(bit, [value]) FROM sys.configurations WHERE [Name] = 'priority boost';
SELECT @qtimeout = CONVERT(int, [value]) FROM sys.configurations WHERE [Name] = 'remote query timeout (s)';
SELECT @cmdshell = CONVERT(bit, [value]) FROM sys.configurations WHERE [Name] = 'xp_cmdshell';
SELECT @deftrace = CONVERT(bit, [value]) FROM sys.configurations WHERE [Name] = 'default trace enabled';
SELECT @remote = CONVERT(bit, [value]) FROM sys.configurations WHERE [Name] = 'remote admin connections';
SELECT @ssp = CONVERT(bit, [value]) FROM sys.configurations WHERE [Name] = 'scan for startup procs';
SELECT @minmemqry = CONVERT(int, [value]) FROM sys.configurations WHERE [Name] = 'min memory per query (KB)';
SELECT @allowupd = CONVERT(bit, [value]) FROM sys.configurations WHERE [Name] = 'allow updates';
SELECT @mwthreads = CONVERT(smallint, [value]) FROM sys.configurations WHERE [Name] = 'max worker threads';
SELECT @recinterval = CONVERT(int, [value]) FROM sys.configurations WHERE [Name] = 'recovery interval (min)';
SELECT @netsize = CONVERT(smallint, [value]) FROM sys.configurations WHERE [Name] = 'network packet size (B)';
SELECT @ixmem = CONVERT(smallint, [value]) FROM sys.configurations WHERE [Name] = 'index create memory (KB)';
SELECT @locks = CONVERT(int, [value]) FROM sys.configurations WHERE [Name] = 'locks';
SELECT @qrywait = CONVERT(int, [value]) FROM sys.configurations WHERE [Name] = 'query wait (s)';
SELECT @adhocqry = CONVERT(bit, [value]) FROM sys.configurations WHERE [Name] = 'Ad Hoc Distributed Queries';
SELECT @adhoc = CONVERT(bit, [value]) FROM sys.configurations WHERE [Name] = 'optimize for ad hoc workloads';
SELECT @affin = CONVERT(int, [value]) FROM sys.configurations (NOLOCK) WHERE name = 'affinity mask';
SELECT @affinIO = CONVERT(int, [value]) FROM sys.configurations (NOLOCK) WHERE name = 'affinity I/O mask';
SELECT @affin64 = CONVERT(int, [value]) FROM sys.configurations (NOLOCK) WHERE name = 'affinity64 mask';
SELECT @affin64IO = CONVERT(int, [value]) FROM sys.configurations (NOLOCK) WHERE name = 'affinity64 I/O mask';
SELECT @block_threshold = CONVERT(int, [value]) FROM sys.configurations (NOLOCK) WHERE name = 'blocked process threshold (s)';
SELECT @oleauto = CONVERT(int, [value]) FROM sys.configurations (NOLOCK) WHERE name = 'Ole Automation Procedures';

SELECT 'Instance_checks' AS [Category], 'System_Configurations' AS [Check], 'Allow updates' AS [Setting], @allowupd AS [Current Value], CASE WHEN @allowupd = 0 THEN '[OK]' ELSE '[WARNING: Microsoft does not support direct catalog updates]' END AS [Deviation], '' AS [Comment]
UNION ALL
SELECT 'Instance_checks' AS [Category], 'System_Configurations' AS [Check], 'Ad Hoc Distributed Queries' AS [Setting], @adhocqry AS [Current Value], CASE WHEN @adhocqry = 0 THEN '[OK]' ELSE '[WARNING: Ad Hoc Distributed Queries are enabled]' END AS [Deviation], '' AS [Comment]
UNION ALL
SELECT 'Instance_checks' AS [Category], 'System_Configurations' AS [Check], 'AWE' AS [Setting], @awe AS [Current Value], CASE WHEN @sqlmajorver < 11 AND @arch = 32 AND @systemmem >= 4000 AND @awe = 0 THEN '[WARNING: Current AWE setting is not optimal for this configuration]' WHEN @sqlmajorver < 11 AND @arch IS NULL THEN '[WARNING: Could not determine architecture needed for check]' WHEN @sqlmajorver > 10 THEN '[INFORMATION: AWE is not used from SQL Server 2012 onwards]' ELSE '[OK]' END AS [Deviation], '' AS [Comment]
UNION ALL
SELECT 'Instance_checks' AS [Category], 'System_Configurations' AS [Check], 'Affinity Mask' AS [Setting], @affin AS [Current Value], CASE WHEN (@affin & @affinIO <> 0) OR (@affin & @affinIO <> 0 AND @affin64 & @affin64IO <> 0) THEN '[WARNING: Current Affinity Mask and Affinity I/O Mask are overlaping]' ELSE '[OK]' END AS [Deviation], '[INFORMATION: Configured values for AffinityMask = ' + CONVERT(VARCHAR(10), @affin) + '; Affinity64Mask = ' + CONVERT(VARCHAR(10), @affin64) + '; AffinityIOMask = ' + CONVERT(VARCHAR(10), @affinIO) + '; Affinity64IOMask = ' + CONVERT(VARCHAR(10), @affin64IO) + ']' AS [Comment]
UNION ALL
SELECT 'Instance_checks' AS [Category], 'System_Configurations' AS [Check], 'Affinity I/O Mask' AS [Setting], @affinIO AS [Current Value], CASE WHEN (@affin & @affinIO <> 0) OR (@affin & @affinIO <> 0 AND @affin64 & @affin64IO <> 0) THEN '[WARNING: Current Affinity Mask and Affinity I/O Mask are overlaping]' ELSE '[OK]' END AS [Deviation], '[INFORMATION: Configured values for AffinityMask = ' + CONVERT(VARCHAR(10), @affin) + '; Affinity64Mask = ' + CONVERT(VARCHAR(10), @affin64) + '; AffinityIOMask = ' + CONVERT(VARCHAR(10), @affinIO) + '; Affinity64IOMask = ' + CONVERT(VARCHAR(10), @affin64IO) + ']' AS [Comment]
UNION ALL
SELECT 'Instance_checks' AS [Category], 'System_Configurations' AS [Check], 'Backup Compression' AS [Setting], @bckcomp AS [Current Value], CASE WHEN @sqlmajorver > 9 AND @bckcomp = 0 THEN '[INFORMATION: Backup compression setting is not the recommended value]' WHEN @sqlmajorver < 10 THEN '[NA]' ELSE '[OK]' END AS [Deviation], '' AS [Comment]
UNION ALL
SELECT 'Instance_checks' AS [Category], 'System_Configurations' AS [Check], 'Blocked Process Threshold' AS [Setting], @block_threshold AS [Current Value], CASE WHEN @block_threshold > 0 AND @block_threshold < 5 THEN '[WARNING: Blocked Process Threshold setting is not the recommended value. If not disabled, value should be higher than 4]' WHEN @block_threshold >= 5 THEN '[INFORMATION: Blocked Process Threshold setting is not the default value]' ELSE '[OK]' END AS [Deviation], '' AS [Comment]
UNION ALL
SELECT 'Instance_checks' AS [Category], 'System_Configurations' AS [Check], 'CLR' AS [Setting], @clr AS [Current Value], CASE WHEN @clr = 1 THEN '[INFORMATION: CLR user code execution setting is enabled]' ELSE '[OK]' END AS [Deviation], '' AS [Comment]
UNION ALL
SELECT 'Instance_checks' AS [Category], 'System_Configurations' AS [Check], 'Cost threshold for Parallelism' AS [Setting], @costparallel AS [Current Value], CASE WHEN @costparallel = 5 THEN '[OK]' ELSE '[WARNING: Cost threshold for Parallelism setting is not the default value]' END AS [Deviation], '' AS [Comment]
UNION ALL
SELECT 'Instance_checks' AS [Category], 'System_Configurations' AS [Check], 'Cross DB ownership Chaining' AS [Setting], @chain AS [Current Value], CASE WHEN @chain = 1 THEN '[WARNING: Cross DB ownership chaining setting is not the recommended value]' ELSE '[OK]' END AS [Deviation], '' AS [Comment]
UNION ALL
SELECT 'Instance_checks' AS [Category], 'System_Configurations' AS [Check], 'Default trace' AS [Setting], @deftrace AS [Current Value], CASE WHEN @deftrace = 0 THEN '[WARNING: Default trace setting is NOT enabled]' ELSE '[OK]' END AS [Deviation], '' AS [Comment]
UNION ALL
SELECT 'Instance_checks' AS [Category], 'System_Configurations' AS [Check], 'Index create memory (KB)' AS [Setting], @ixmem AS [Current Value], CASE WHEN @ixmem = 0 THEN '[OK]' WHEN @ixmem > 0 AND @ixmem < @minmemqry THEN '[WARNING: Index create memory should not be less than Min memory per query]' ELSE '[WARNING: Index create memory is not the default value]' END AS [Deviation], '' AS [Comment]
UNION ALL
SELECT 'Instance_checks' AS [Category], 'System_Configurations' AS [Check], 'Lightweight pooling' AS [Setting], @lpooling AS [Current Value], CASE WHEN @lpooling = 1 THEN '[WARNING: Lightweight pooling setting is not the recommended value]' ELSE '[OK]' END AS [Deviation], '' AS [Comment]
UNION ALL
SELECT 'Instance_checks' AS [Category], 'System_Configurations' AS [Check], 'Locks' AS [Setting], @locks AS [Current Value], CASE WHEN @locks = 0 THEN '[OK]' ELSE '[WARNING: Locks option is not set with the default value]' END AS [Deviation], '' AS [Comment]
UNION ALL
SELECT 'Instance_checks' AS [Category], 'System_Configurations' AS [Check], 'Max worker threads' AS [Setting], @mwthreads AS [Current Value], CASE WHEN @mwthreads = 0 THEN '[OK]' WHEN @mwthreads > 2048 AND @arch = 64 THEN '[WARNING: Max worker threads is larger than 2048 on a x64 system]' WHEN @mwthreads > 1024 AND @arch = 32 THEN '[WARNING: Max worker threads is larger than 1024 on a x86 system]' ELSE '[WARNING: Max worker threads is not the default value]' END AS [Deviation], CASE WHEN @mwthreads = 0 THEN '[INFORMATION: Configured workers = ' + CONVERT(VARCHAR(10),@mwthreads_count) + ']' ELSE '' END AS [Comment]
UNION ALL
SELECT 'Instance_checks' AS [Category], 'System_Configurations' AS [Check], 'Min memory per query (KB)' AS [Setting], @minmemqry AS [Current Value], CASE WHEN @minmemqry = 1024 THEN '[OK]' ELSE '[WARNING: Min memory per query (KB) setting is not the default value]' END AS [Deviation], '' AS [Comment]
UNION ALL
SELECT 'Instance_checks' AS [Category], 'System_Configurations' AS [Check], 'Network packet size (B)' AS [Setting], @netsize AS [Current Value], CASE WHEN @netsize = 4096 THEN '[OK]' ELSE '[WARNING: Network packet size is not the default value]' END AS [Deviation], '' AS [Comment]
UNION ALL
SELECT 'Instance_checks' AS [Category], 'System_Configurations' AS [Check], 'Ole Automation Procedures' AS [Setting], @oleauto AS [Current Value], CASE WHEN @oleauto = 1 THEN '[WARNING: Ole Automation Procedures setting is not the recommended value]' ELSE '[OK]' END AS [Deviation], '' AS [Comment]
UNION ALL
SELECT 'Instance_checks' AS [Category], 'System_Configurations' AS [Check], 'Optimize for ad-hoc workloads' AS [Setting], @adhoc AS [Current Value], CASE WHEN @sqlmajorver > 9 AND @adhoc = 0 THEN '[INFORMATION: Consider enabling the Optimize for ad hoc workloads setting on heavy OLTP ad-hoc worloads to conserve resources]' WHEN @sqlmajorver < 10 THEN '[NA]' ELSE '[OK]' END AS [Deviation], CASE WHEN @sqlmajorver > 9 AND @adhoc = 0 THEN '[INFORMATION: Should be ON if SQL Server 2008 or higher and OLTP workload]' ELSE '' END AS [Comment]
UNION ALL
SELECT 'Instance_checks' AS [Category], 'System_Configurations' AS [Check], 'Priority Boost' AS [Setting], @pboost AS [Current Value], CASE WHEN @pboost = 1 THEN '[CRITICAL: Priority boost setting is not the recommended value]' ELSE '[OK]' END AS [Deviation], '' AS [Comment]
UNION ALL
SELECT 'Instance_checks' AS [Category], 'System_Configurations' AS [Check], 'Query wait (s)' AS [Setting], @qrywait AS [Current Value], CASE WHEN @qrywait = -1 THEN '[OK]' ELSE '[CRITICAL: Query wait is not the default value]' END AS [Deviation], '' AS [Comment]
UNION ALL
SELECT 'Instance_checks' AS [Category], 'System_Configurations' AS [Check], 'Recovery Interval (min)' AS [Setting], @recinterval AS [Current Value], CASE WHEN @recinterval = 0 THEN '[OK]' ELSE '[WARNING: Recovery interval is not the default value]' END AS [Deviation], '' AS [Comment]
UNION ALL
SELECT 'Instance_checks' AS [Category], 'System_Configurations' AS [Check], 'Remote Admin Connections' AS [Setting], @remote AS [Current Value], CASE WHEN @remote = 0 AND @clustered = 1 THEN '[WARNING: Consider enabling the DAC listener to access a remote connections on a clustered configuration]' WHEN @remote = 0 AND @clustered = 0 THEN '[INFORMATION: Consider enabling remote connections access to the DAC listener on a stand-alone configuration, should local resources be exhausted]' ELSE '[OK]' END AS [Deviation], '' AS [Comment]
UNION ALL
SELECT 'Instance_checks' AS [Category], 'System_Configurations' AS [Check], 'Remote query timeout' AS [Setting], @qtimeout AS [Current Value], CASE WHEN @qtimeout = 600 THEN '[OK]' ELSE '[WARNING: Remote query timeout is not the default value]' END AS [Deviation], '' AS [Comment]
UNION ALL
SELECT 'Instance_checks' AS [Category], 'System_Configurations' AS [Check], 'Startup Stored Procedures' AS [Setting], @ssp AS [Current Value], CASE WHEN @ssp = 1 AND (@replication IS NULL OR @replication = 0) THEN '[WARNING: Scanning for startup stored procedures setting is not the recommended value]' ELSE '[OK]' END AS [Deviation], '' AS [Comment]
UNION ALL
SELECT 'Instance_checks' AS [Category], 'System_Configurations' AS [Check], 'xp_cmdshell' AS [Setting], @cmdshell AS [Current Value], CASE WHEN @cmdshell = 1 THEN '[WARNING: xp_cmdshell setting is enabled]' ELSE '[OK]' END AS [Deviation], '' AS [Comment];

IF (SELECT COUNT([Name]) FROM master.sys.configurations WHERE [value] <> [value_in_use] AND [is_dynamic] = 0) > 0
BEGIN
	SELECT 'Instance_checks' AS [Category], 'System_Configurations_Pending'AS [Check], '[WARNING: There are system configurations with differences between running and configured values]' AS [Deviation]
	SELECT 'Instance_checks' AS [Category], 'System_Configurations_Pending'AS [Information], [Name] AS [Setting],
		[value] AS 'Config_Value',
		[value_in_use] AS 'Run_Value'
	FROM master.sys.configurations (NOLOCK)
	WHERE [value] <> [value_in_use] AND [is_dynamic] = 0;
END
ELSE
BEGIN
	SELECT 'Instance_checks' AS [Category], 'System_Configurations_Pending'AS [Check], '[OK]' AS [Deviation]
END;

--------------------------------------------------------------------------------------------------------------------------------
-- IFI subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'  |-Starting IFI', 10, 1) WITH NOWAIT
IF @allow_xpcmdshell = 1
BEGIN
	DECLARE @ifi bit
	IF ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) = 1 -- Is sysadmin
		OR ((ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) <> 1 
			AND (SELECT COUNT(credential_id) FROM sys.credentials WHERE name = '##xp_cmdshell_proxy_account##') > 0)) -- Is not sysadmin but proxy account exists
		OR ((ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) <> 1 
			AND (SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_cmdshell') > 0))
	BEGIN
		RAISERROR ('    |-Configuration options set for IFI check', 10, 1) WITH NOWAIT
		SELECT @sao = CAST([value] AS smallint) FROM sys.configurations WITH (NOLOCK) WHERE [name] = 'show advanced options'
		SELECT @xcmd = CAST([value] AS smallint) FROM sys.configurations WITH (NOLOCK) WHERE [name] = 'xp_cmdshell'
		IF @sao = 0
		BEGIN
			EXEC sp_configure 'show advanced options', 1; RECONFIGURE WITH OVERRIDE;
		END
		IF @xcmd = 0
		BEGIN
			EXEC sp_configure 'xp_cmdshell', 1; RECONFIGURE WITH OVERRIDE;
		END

		BEGIN TRY
			DECLARE @xp_cmdshell_output2 TABLE ([Output] VARCHAR (8000));
			SET @CMD = ('whoami /priv')
			INSERT INTO @xp_cmdshell_output2
			EXEC master.dbo.xp_cmdshell @CMD;
			
			IF EXISTS (SELECT * FROM @xp_cmdshell_output2 WHERE [Output] LIKE '%SeManageVolumePrivilege%')
			BEGIN
				SELECT 'Instance_checks' AS [Category], 'Instant_Initialization' AS [Check], '[OK]' AS [Deviation];
				SET @ifi = 1;
			END
			ELSE
			BEGIN
				SELECT 'Instance_checks' AS [Category], 'Instant_Initialization' AS [Check], '[WARNING: Instant File Initialization is disabled. This can impact data file autogrowth times]' AS [Deviation];
				SET @ifi = 0
			END
		END TRY
		BEGIN CATCH
			SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
			SELECT @ErrorMessage = 'IFI subsection - Error raised in TRY block. ' + ERROR_MESSAGE()
			RAISERROR (@ErrorMessage, 16, 1);
		END CATCH

		IF @xcmd = 0
		BEGIN
			EXEC sp_configure 'xp_cmdshell', 0; RECONFIGURE WITH OVERRIDE;
		END
		IF @sao = 0
		BEGIN
			EXEC sp_configure 'show advanced options', 0; RECONFIGURE WITH OVERRIDE;
		END
	END
	ELSE
	BEGIN
		RAISERROR('[WARNING: Only a sysadmin can run the "Instant Initialization" check. A regular user can also run this check if a xp_cmdshell proxy account exists. Bypassing check]', 16, 1, N'xp_cmdshellproxy')
		RAISERROR('[WARNING: If not sysadmin, then must be a granted EXECUTE permissions on the following extended sprocs to run checks: xp_cmdshell. Bypassing check]', 16, 1, N'extended_sprocs')
		--RETURN
	END
END
ELSE
BEGIN
	RAISERROR('  |- [INFORMATION: "Instant Initialization" check was skipped because xp_cmdshell was not allowed.]', 10, 1, N'disallow_xp_cmdshell')
	--RETURN
END;

--------------------------------------------------------------------------------------------------------------------------------
-- Full Text Configurations subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'  |-Starting Full Text Configurations', 10, 1) WITH NOWAIT
DECLARE @FullTextDefaultPath NVARCHAR(512), @fterr tinyint
DECLARE @fttbl TABLE ([KeyExist] int)
DECLARE @FullTextDetails TABLE (FullText_ResourceUsage tinyint,
	[DefaultPath] NVARCHAR(512),
	[ConnectTimeout] int,
	[DataTimeout] int,
	[AllowUnsignedBinaries] bit,
	[LoadOSResourcesEnabled] bit,
	[CatalogUpgradeOption] tinyint)
SET @fterr = 0

IF (SELECT ISNULL(FULLTEXTSERVICEPROPERTY('IsFulltextInstalled'),0)) = 1
BEGIN
	IF (ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) = 1) OR ((SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_instance_regread') = 1)
	BEGIN
		BEGIN TRY
			INSERT INTO @fttbl
			EXEC master..xp_instance_regread N'HKEY_LOCAL_MACHINE', N'SOFTWARE\Microsoft\MSSQLServer\Setup' -- check if Full-Text path exists

			IF (SELECT [KeyExist] FROM @fttbl) = 1
			BEGIN
				EXEC master..xp_instance_regread N'HKEY_LOCAL_MACHINE', N'SOFTWARE\Microsoft\MSSQLServer\Setup', N'FullTextDefaultPath', @FullTextDefaultPath OUTPUT, NO_OUTPUT;
			END
		END TRY
		BEGIN CATCH
			SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
			SELECT @ErrorMessage = 'Full Text Configurations subsection - Error raised in TRY block. ' + ERROR_MESSAGE()
			RAISERROR (@ErrorMessage, 16, 1);
		END CATCH
	END
	ELSE
	BEGIN
		RAISERROR('[WARNING: Missing permissions for full "Full Text Configurations" checks. Bypassing Full Text path check]', 16, 1, N'sysadmin')
		--RETURN
	END
	
	INSERT INTO @FullTextDetails
	SELECT FULLTEXTSERVICEPROPERTY('ResourceUsage'), ISNULL(@FullTextDefaultPath, N'') AS [Default Path],
	ISNULL(FULLTEXTSERVICEPROPERTY('ConnectTimeout'),0), ISNULL(FULLTEXTSERVICEPROPERTY('DataTimeout'),0),
	CASE WHEN @sqlmajorver >= 9 THEN
			FULLTEXTSERVICEPROPERTY('VerifySignature') ELSE NULL 
	END AS [AllowUnsignedBinaries],
	CASE WHEN @sqlmajorver >= 9 THEN
		FULLTEXTSERVICEPROPERTY('LoadOSResources') ELSE NULL 
	END AS [LoadOSResourcesEnabled],
	CASE WHEN @sqlmajorver >= 10 THEN
		FULLTEXTSERVICEPROPERTY('UpgradeOption') ELSE NULL 
	END AS [CatalogUpgradeOption];
	
	IF @sqlmajorver <= 9 AND (SELECT FullText_ResourceUsage FROM @FullTextDetails) <> 3
	BEGIN
		SELECT 'Instance_checks' AS [Category], 'Full_Text' AS [Check], '[INFORMATION: FullText Resource usage setting is not default]' AS [Deviation],
			CASE WHEN FullText_ResourceUsage < 3 THEN '[Least Aggressive Usage Level]'
					WHEN FullText_ResourceUsage = 4 THEN '[More Aggressive Usage Level]'
					WHEN FullText_ResourceUsage = 5 THEN '[Most Aggressive Usage Level]'
			END AS [Comment]
		FROM @FullTextDetails;
		SET @fterr = @fterr + 1
	END
	IF @sqlmajorver >= 9 AND (SELECT [AllowUnsignedBinaries] FROM @FullTextDetails) = 0
	BEGIN
		SELECT 'Instance_checks' AS [Category], 'Full_Text' AS [Check], '[WARNING: FullText Binaries verification setting is not default]' AS [Deviation], 
			'[Do not verify whether or not binaries are signed]' AS [Comment];
		SET @fterr = @fterr + 1
	END
	IF @sqlmajorver >= 9 AND (SELECT [LoadOSResourcesEnabled] FROM @FullTextDetails) = 1
	BEGIN
		SELECT 'Instance_checks' AS [Category], 'Full_Text' AS [Check], '[WARNING: FullText OS Resource utilization setting is not default]' AS [Deviation], 
			'[Load OS filters and word breakers]' AS [Comment];
		SET @fterr = @fterr + 1
	END
	IF @fterr = 0
	BEGIN
		SELECT 'Instance_checks' AS [Category], 'Full_Text' AS [Check], '[OK]' AS [Deviation], 
			'[All FullText settings are aligned with defaults]' AS [Comment];
	END
END;

IF (SELECT ISNULL(FULLTEXTSERVICEPROPERTY('IsFulltextInstalled'),0)) = 0
BEGIN
	SELECT 'Instance_checks' AS [Category], 'Full_Text' AS [Check], NULL AS [Deviation], '[FullText search is not installed]' AS [Comment];
END;

--------------------------------------------------------------------------------------------------------------------------------
-- Deprecated features subsection
--------------------------------------------------------------------------------------------------------------------------------
IF @ptochecks = 1
BEGIN
	RAISERROR (N'  |-Starting Deprecated features', 10, 1) WITH NOWAIT
	IF (SELECT COUNT(instance_name) FROM sys.dm_os_performance_counters WHERE [object_name] = 'SQLServer:Deprecated Features' AND cntr_value > 0) > 0
	BEGIN
		SELECT 'Instance_checks' AS [Category], 'Deprecated_features' AS [Check], '[WARNING: Deprecated features are being used. These features are scheduled to be removed in a future release of SQL Server]' AS [Deviation]
		SELECT 'Instance_checks' AS [Category], 'Deprecated_features' AS [Information], instance_name, cntr_value AS [Times_used_since_startup]
		FROM sys.dm_os_performance_counters (NOLOCK)
		WHERE [object_name] LIKE '%Deprecated Features%' AND cntr_value > 0
		ORDER BY instance_name;
	END
	ELSE
	BEGIN
		SELECT 'Instance_checks' AS [Category], 'Deprecated_features' AS [Check], '[OK]' AS [Deviation]
	END;
END;

--------------------------------------------------------------------------------------------------------------------------------
-- Default data collections subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'  |-Starting default data collections', 10, 1) WITH NOWAIT
IF EXISTS (SELECT TOP 1 id FROM sys.traces WHERE is_default = 1 AND status = 1)
BEGIN
	SELECT 'Instance_checks' AS [Category], 'Default_Trace' AS [Check], '[OK]' AS [Deviation]
END
ELSE
BEGIN
	SELECT 'Instance_checks' AS [Category], 'Default_Trace' AS [Information], '[WARNING: No default trace was found or is not active]' AS [Deviation], '[Default trace provides troubleshooting assistance to database administrators by ensuring that they have the log data necessary to diagnose problems the first time they occur]' AS [Comment]
END;

IF EXISTS (SELECT TOP 1 id FROM sys.traces WHERE [path] LIKE '%blackbox%.trc' AND status = 1)
BEGIN
	SELECT 'Instance_checks' AS [Category], 'Blackbox_Trace' AS [Check], '[WARNING: Blackbox trace is configured and running]' AS [Deviation], '[This trace is designed to behave similarly to an airplane black box, to help you diagnose intermittent server crashes. It is quite a bit heavier than the default trace]' AS [Comment]
END
ELSE
BEGIN
	SELECT 'Instance_checks' AS [Category], 'Blackbox_Trace' AS [Information], '[OK]' AS [Deviation]
END;

IF EXISTS (SELECT TOP 1 id FROM sys.traces WHERE (is_default = 1 OR [path] LIKE '%blackbox%.trc') AND status = 1)
BEGIN
	SELECT 'Instance_checks' AS [Category], 'Default_or_Blackbox_Trace' AS [Information], [id] As trace_id, [path], max_size, max_files, buffer_count, buffer_size, is_default, event_count, dropped_event_count, start_time, last_event_time 
	FROM sys.traces
	WHERE (is_default = 1 OR [path] LIKE '%blackbox%.trc') AND status = 1
END;

IF @sqlmajorver > 10
BEGIN
	IF EXISTS (SELECT TOP 1 name FROM sys.dm_xe_sessions WHERE [name] = 'system_health')
	BEGIN
		SELECT 'Instance_checks' AS [Category], 'xEvent_Session_SystemHealth' AS [Check], '[OK]' AS [Deviation]
	END
	ELSE
	BEGIN
		SELECT 'Instance_checks' AS [Category], 'xEvent_Session_SystemHealth' AS [Information], '[WARNING: The system_health xEvent session is not active]' AS [Deviation], '[This session starts automatically when the SQL Server Database Engine starts, and runs without any noticeable performance effects. The session collects system data that you can use to help troubleshoot performance issues in the Database Engine]' AS [Comment]
	END;

	IF EXISTS (SELECT TOP 1 name FROM sys.dm_xe_sessions WHERE [name] = 'sp_server_diagnostics session')
	BEGIN
		SELECT 'Instance_checks' AS [Category], 'xEvent_Session_sp_server_diagnostics' AS [Check], '[OK]' AS [Deviation]
	END
	ELSE
	BEGIN
		SELECT 'Instance_checks' AS [Category], 'xEvent_Session_sp_server_diagnostics' AS [Information], '[WARNING: The sp_server_diagnostics xEvent session is not active]' AS [Deviation], '[This session starts automatically when the SQL Server Database Engine starts, and runs without any noticeable performance effects. The session collects system data that you can use to help troubleshoot performance issues in the Database Engine]' AS [Comment]
	END;

	IF EXISTS (SELECT TOP 1 name FROM sys.dm_xe_sessions WHERE [name] IN ('system_health', 'sp_server_diagnostics session'))
	BEGIN
		SELECT 'Instance_checks' AS [Category], 'xEvent_Session_SystemHealth_sp_server_diagnostics' AS [Information], name, pending_buffers, total_regular_buffers, regular_buffer_size, total_large_buffers, large_buffer_size, total_buffer_size, buffer_policy_desc, flag_desc, 
			dropped_event_count, dropped_buffer_count, blocked_event_fire_time, create_time, largest_event_dropped_size
		FROM sys.dm_xe_sessions
		WHERE [name] IN ('system_health', 'sp_server_diagnostics session')
	END;
END;


RAISERROR (N'|-Starting Database and tempDB Checks', 10, 1) WITH NOWAIT

--------------------------------------------------------------------------------------------------------------------------------
-- User objects in master DB
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'  |-Starting User Objects in master DB', 10, 1) WITH NOWAIT
IF (SELECT COUNT(name) FROM master.sys.all_objects WHERE is_ms_shipped = 0 AND [type] IN ('AF','FN','P','IF','PC','TF','TR','T','V')) >= 1
BEGIN
	SELECT 'Database_checks' AS [Category], 'User_Objects_in_master' AS [Check], '[WARNING: User objects are created in the master database]' AS [Deviation]
	SELECT 'Database_checks' AS [Category], 'User_Objects_in_master' AS [Information], ss.name AS [Schema_Name], sao.name AS [Object_Name], sao.[type_desc] AS [Object_Type], sao.create_date, sao.modify_date 
	FROM master.sys.all_objects sao
	INNER JOIN master.sys.schemas ss ON sao.[schema_id] = ss.[schema_id]
	WHERE sao.is_ms_shipped = 0
	AND sao.[type] IN ('AF','FN','P','IF','PC','TF','TR','T','V')
	ORDER BY sao.name, sao.type_desc;
END
ELSE
BEGIN
	SELECT 'Database_checks' AS [Category], 'User_Objects_in_master' AS [Check], '[OK]' AS [Deviation]
END;

--------------------------------------------------------------------------------------------------------------------------------
-- DBs with collation <> master subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'  |-Starting DBs with collation <> master', 10, 1) WITH NOWAIT
DECLARE @master_collate NVARCHAR(128), @dif_collate int
SELECT @master_collate = collation_name FROM master.sys.databases (NOLOCK) WHERE database_id = 1;
SELECT @dif_collate = COUNT(collation_name) FROM master.sys.databases (NOLOCK) WHERE collation_name <> @master_collate;

IF @dif_collate >= 1
BEGIN
	SELECT 'Database_checks' AS [Category], 'Collations' AS [Check], '[WARNING: Some user databases collation differ from the master Database_Collation]' AS [Deviation]
	SELECT 'Database_checks' AS [Category], 'Collations' AS [Information], name AS [Database_Name], collation_name AS [Database_Collation], @master_collate AS [Master_Collation]
	FROM master.sys.databases (NOLOCK)
	WHERE collation_name <> @master_collate;
END
ELSE
BEGIN
	SELECT 'Database_checks' AS [Category], 'Collations' AS [Check], '[OK]' AS [Deviation]
END;

--------------------------------------------------------------------------------------------------------------------------------
-- DBs with skewed compatibility level subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'  |-Starting DBs with skewed compatibility level', 10, 1) WITH NOWAIT
DECLARE @dif_compat int
SELECT @dif_compat = COUNT([compatibility_level]) FROM master.sys.databases (NOLOCK) WHERE [compatibility_level] <> @sqlmajorver * 10;

IF @dif_compat >= 1
BEGIN
	SELECT 'Database_checks' AS [Category], 'Compatibility_Level' AS [Check], '[WARNING: Some user databases have a non-optimal compatibility level]' AS [Deviation]
	SELECT 'Database_checks' AS [Category], 'Compatibility_Level' AS [Information], name AS [Database_Name], [compatibility_level] AS [Compatibility_Level]
	FROM master.sys.databases (NOLOCK)
	WHERE [compatibility_level] <> @sqlmajorver * 10;
END
ELSE
BEGIN
	SELECT 'Database_checks' AS [Category], 'Compatibility_Level' AS [Check], '[OK]' AS [Deviation]
END;

--------------------------------------------------------------------------------------------------------------------------------
-- User DBs with non-default options subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'  |-Starting User DBs with non-default options', 10, 1) WITH NOWAIT
DECLARE @cnt int, @cnt_i int
DECLARE @is_auto_close_on bit, @is_auto_shrink_on bit, @page_verify_option bit
DECLARE @is_auto_create_stats_on bit, @is_auto_update_stats_on bit
DECLARE @is_db_chaining_on bit, @is_indirect_checkpoint_on bit
DECLARE @is_trustworthy_on bit, @is_parameterization_forced bit

DECLARE @dbopterrtb TABLE (id int, 
	name sysname, 
	is_auto_close_on bit, 
	is_auto_shrink_on bit, 
	page_verify_option tinyint, 
	page_verify_option_desc NVARCHAR(60),
	is_auto_create_stats_on bit, 
	is_auto_update_stats_on bit,
	is_db_chaining_on bit,
	is_indirect_checkpoint_on bit,
	is_trustworthy_on bit,
	is_parameterization_forced bit)

IF @sqlmajorver < 11
BEGIN
	SET @sqlcmd = 'SELECT ROW_NUMBER() OVER(ORDER BY name), name, is_auto_close_on, 
	is_auto_shrink_on, page_verify_option, page_verify_option_desc,	
	is_auto_create_stats_on, is_auto_update_stats_on, 
	is_db_chaining_on, 0 AS is_indirect_checkpoint_on, is_trustworthy_on, is_parameterization_forced
FROM master.sys.databases (NOLOCK)
WHERE database_id > 4 OR name = ''model'''
END
ELSE
BEGIN
	SET @sqlcmd = 'SELECT ROW_NUMBER() OVER(ORDER BY name), name, is_auto_close_on, 
	is_auto_shrink_on, page_verify_option, page_verify_option_desc,	
	is_auto_create_stats_on, is_auto_update_stats_on, 
	is_db_chaining_on, CASE WHEN target_recovery_time_in_seconds > 0 THEN 1 ELSE 0 END AS is_indirect_checkpoint_on, 
	is_trustworthy_on, is_parameterization_forced
FROM master.sys.databases (NOLOCK)
WHERE database_id > 4 OR name = ''model'''
END;

INSERT INTO @dbopterrtb
EXECUTE sp_executesql @sqlcmd;

SET @cnt = (SELECT COUNT(id) FROM @dbopterrtb)
SET @cnt_i = 1

SELECT @is_auto_close_on = 0, @is_auto_shrink_on = 0, @page_verify_option = 0, @is_auto_create_stats_on = 0, @is_auto_update_stats_on = 0, @is_db_chaining_on = 0, @is_indirect_checkpoint_on = 0, @is_trustworthy_on = 0, @is_parameterization_forced = 0

WHILE @cnt_i <> @cnt
BEGIN 
	SELECT @is_auto_close_on = CASE WHEN is_auto_close_on = 1 AND @is_auto_close_on = 0 THEN 1 ELSE @is_auto_close_on END,
		@is_auto_shrink_on = CASE WHEN is_auto_shrink_on = 1 AND @is_auto_shrink_on = 0 THEN 1 ELSE @is_auto_shrink_on END, 
		@page_verify_option = CASE WHEN page_verify_option <> 2 AND @page_verify_option = 0 THEN 1 ELSE @page_verify_option END, 
		@is_auto_create_stats_on = CASE WHEN is_auto_create_stats_on = 0 AND @is_auto_create_stats_on = 0 THEN 1 ELSE @is_auto_create_stats_on END, 
		@is_auto_update_stats_on = CASE WHEN is_auto_update_stats_on = 0 AND @is_auto_update_stats_on = 0 THEN 1 ELSE @is_auto_update_stats_on END, 
		@is_db_chaining_on = CASE WHEN is_db_chaining_on = 1 AND @is_db_chaining_on = 0 THEN 1 ELSE @is_db_chaining_on END,
		@is_indirect_checkpoint_on = CASE WHEN is_indirect_checkpoint_on = 1 AND @is_indirect_checkpoint_on = 0 THEN 1 ELSE @is_indirect_checkpoint_on END,
		@is_trustworthy_on = CASE WHEN is_trustworthy_on = 1 AND @is_trustworthy_on = 0 THEN 1 ELSE @is_trustworthy_on END,
		@is_parameterization_forced = CASE WHEN is_parameterization_forced = 1 AND @is_parameterization_forced = 0 THEN 1 ELSE @is_parameterization_forced END
	FROM @dbopterrtb
	WHERE id = @cnt_i;
	SET @cnt_i = @cnt_i + 1
END

IF @is_auto_close_on = 1 OR @is_auto_shrink_on = 1 OR @page_verify_option = 1 OR @is_auto_create_stats_on = 1 OR @is_auto_update_stats_on = 1 OR @is_db_chaining_on = 1 OR @is_indirect_checkpoint_on = 1
BEGIN
	SELECT 'Database_checks' AS [Category], 'Database_Options' AS [Check], '[WARNING: Some user databases may have Non-optimal_Settings]' AS [Deviation]
	SELECT 'Database_checks' AS [Category], 'Database_Options' AS [Information],
		name AS [Database_Name],
		RTRIM(
			CASE WHEN is_auto_close_on = 1 THEN 'Auto_Close;' ELSE '' END + 
			CASE WHEN is_auto_shrink_on = 1 THEN 'Auto_Shrink;' ELSE '' END +
			CASE WHEN page_verify_option <> 2 THEN 'Page_Verify;' ELSE '' END +
			CASE WHEN is_auto_create_stats_on = 0 THEN 'Auto_Create_Stats;' ELSE '' END +
			CASE WHEN is_auto_update_stats_on = 0 THEN 'Auto_Update_Stats;' ELSE '' END +
			CASE WHEN is_db_chaining_on = 1 THEN 'DB_Chaining;' ELSE '' END +
			CASE WHEN is_indirect_checkpoint_on = 1 THEN 'Indirect_Checkpoint;' ELSE '' END +
			CASE WHEN is_trustworthy_on = 1 THEN 'Trustworthy_bit;' ELSE '' END +
			CASE WHEN is_parameterization_forced = 1 THEN 'Forced_Parameterization;' ELSE '' END
		) AS [Non-optimal_Settings],
		CASE WHEN is_auto_close_on = 1 THEN 'ON' ELSE 'OFF' END AS [Auto_Close],
		CASE WHEN is_auto_shrink_on = 1 THEN 'ON' ELSE 'OFF' END AS [Auto_Shrink], 
		page_verify_option_desc AS [Page_Verify], 
		CASE WHEN is_auto_create_stats_on = 1 THEN 'ON' ELSE 'OFF' END AS [Auto_Create_Stats],
		CASE WHEN is_auto_update_stats_on = 1 THEN 'ON' ELSE 'OFF' END AS [Auto_Update_Stats], 
		CASE WHEN is_db_chaining_on = 1 THEN 'ON' ELSE 'OFF' END AS [DB_Chaining],
		CASE WHEN is_indirect_checkpoint_on = 1 THEN 'ON' ELSE 'OFF' END AS [Indirect_Checkpoint], -- Meant just as a warning that Indirect_Checkpoint is ON. Should be OFF in OLTP systems. Check for high Background writer pages/sec counter.
		CASE WHEN is_trustworthy_on = 1 THEN 'ON' ELSE 'OFF' END AS [Trustworthy_bit],
		CASE WHEN is_parameterization_forced = 1 THEN 'ON' ELSE 'OFF' END AS [Forced_Parameterization]
	FROM @dbopterrtb
	WHERE is_auto_close_on = 1 OR is_auto_shrink_on = 1 OR page_verify_option <> 2 OR is_db_chaining_on = 1 OR is_auto_create_stats_on = 0 
		OR is_auto_update_stats_on = 0 OR is_indirect_checkpoint_on = 1 OR is_trustworthy_on = 1 OR is_parameterization_forced = 1;
END
ELSE
BEGIN
	SELECT 'Database_checks' AS [Category], 'Database_Options' AS [Check], '[OK]' AS [Deviation]
END;

IF (SELECT COUNT(*) FROM master.sys.databases (NOLOCK) WHERE is_auto_update_stats_on = 0 AND is_auto_update_stats_async_on = 1) > 0
BEGIN
	SELECT 'Database_checks' AS [Category], 'Database_Options_Disabled_Async_AutoUpdate' AS [Check], '[WARNING: Some databases have Auto_Update_Statistics_Asynchronously ENABLED while Auto_Update_Statistics is DISABLED. If asynch auto statistics update is intended, also enable Auto_Update_Statistics]' AS [Deviation]
	SELECT 'Database_checks' AS [Category], 'Database_Options_Disabled_Async_AutoUpdate' AS [Check], [name] FROM master.sys.databases (NOLOCK) WHERE is_auto_update_stats_on = 0 AND is_auto_update_stats_async_on = 1
END
ELSE
BEGIN
	SELECT 'Database_checks' AS [Category], 'Database_Options_Disabled_Async_AutoUpdate' AS [Check], '[OK]' AS [Deviation]
END;

--------------------------------------------------------------------------------------------------------------------------------
-- DBs with Sparse files subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'  |-Starting DBs with Sparse files', 10, 1) WITH NOWAIT
-- http://blogs.msdn.com/b/psssql/archive/2011/02/21/did-your-backup-program-utility-leave-your-sql-server-running-in-an-squirrely-scenario-version-2.aspx
-- http://blogs.msdn.com/b/jorgepc/archive/2010/11/25/what-are-sparse-files-and-why-should-i-care-as-sql-server-dba.aspx
IF (SELECT COUNT(sd.database_id) FROM sys.databases sd INNER JOIN sys.master_files smf ON sd.database_id = smf.database_id WHERE sd.source_database_id IS NULL AND smf.is_sparse = 1) > 0
BEGIN
	SELECT 'Database_checks' AS [Category], 'DB_nonSnap_Sparse' AS [Check], '[WARNING: Sparse files were detected that do not belong to a Database Snapshot. You might also notice unexplained performance degradation when query data from these files]' AS [Deviation]
	SELECT 'Database_checks' AS [Category], 'DB_nonSnap_Sparse' AS [Information], DB_NAME(sd.database_id) AS database_name, smf.name, smf.physical_name
	FROM sys.databases sd 
	INNER JOIN sys.master_files smf ON sd.database_id = smf.database_id
	WHERE sd.source_database_id IS NULL AND smf.is_sparse = 1
END
ELSE
BEGIN
	SELECT 'Database_checks' AS [Category], 'DB_nonSnap_Sparse' AS [Check], '[OK]' AS [Deviation]
END;

--------------------------------------------------------------------------------------------------------------------------------
-- DBs Autogrow in percentage subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'  |-Starting DBs Autogrow in percentage', 10, 1) WITH NOWAIT
IF (SELECT COUNT(is_percent_growth) FROM sys.master_files WHERE is_percent_growth = 1) > 0
BEGIN
	SELECT 'Database_checks' AS [Category], 'Percent_Autogrows' AS [Check], '[WARNING: Some database files have a growth ratio set in percentage. Over time, this could lead to uncontrolled disk space allocation and extended time to perform these growths]' AS [Deviation]
	SELECT 'Database_checks' AS [Category], 'Percent_Autogrows' AS [Information], database_id,
		DB_NAME(database_id) AS [Database_Name], 
		mf.name AS [Logical_Name],
		mf.size*8 AS [Current_Size_KB],
		mf.type_desc AS [File_Type],
		mf.[state_desc] AS [File_State],
		CASE WHEN is_percent_growth = 1 THEN 'pct' ELSE 'pages' END AS [Growth_Type],
		CASE WHEN is_percent_growth = 1 THEN mf.growth ELSE mf.growth*8 END AS [Growth_Amount_KB],
		CASE WHEN is_percent_growth = 1 AND mf.growth > 0 THEN ((mf.size*8)*CONVERT(bigint, mf.growth))/100 
			WHEN is_percent_growth = 0 AND mf.growth > 0 THEN mf.growth*8 
			ELSE 0 END AS [Next_Growth_KB],
		CASE WHEN @ifi = 0 AND mf.type = 0 THEN 'Instant File Initialization is disabled'
			WHEN @ifi = 1 AND mf.type = 0 THEN 'Instant File Initialization is enabled'
			ELSE '' END AS [Comments],
		mf.is_read_only
	FROM sys.master_files mf (NOLOCK)
	WHERE is_percent_growth = 1
	GROUP BY database_id, mf.name, mf.size, is_percent_growth, mf.growth, mf.type_desc, mf.[type], mf.[state_desc], mf.is_read_only
	ORDER BY DB_NAME(mf.database_id), mf.name
END
ELSE
BEGIN
	SELECT 'Database_checks' AS [Category], 'Percent_Autogrows' AS [Check], '[OK]' AS [Deviation]
END;

--------------------------------------------------------------------------------------------------------------------------------
-- DBs Autogrowth > 1GB in Logs or Data (when IFI is disabled) subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'  |-Starting DBs Autogrowth > 1GB in Logs or Data (when IFI is disabled)', 10, 1) WITH NOWAIT
IF (SELECT COUNT(growth) FROM sys.master_files 
	WHERE type >= CASE WHEN @ifi = 1 THEN 0 ELSE 1 END 
		AND type < 2 
		AND ((is_percent_growth = 1 AND ((size*8)*growth)/100 > 1048576) 
		OR (is_percent_growth = 0 AND growth*8 > 1048576))) > 0
BEGIN
	SELECT 'Database_checks' AS [Category], 'Large_Autogrows' AS [Check], '[WARNING: Some database files have set growth over 1GB. This could lead to extended growth times, slowing down your system]' AS [Deviation]
	SELECT 'Database_checks' AS [Category], 'Large_Autogrows' AS [Information], database_id,
		DB_NAME(database_id) AS [Database_Name], 
		mf.name AS [Logical_Name],
		mf.size*8 AS [Current_Size_KB],
		mf.type_desc AS [File_Type],
		mf.[state_desc] AS [File_State],
		CASE WHEN is_percent_growth = 1 THEN 'pct' ELSE 'pages' END AS [Growth_Type],
		CASE WHEN is_percent_growth = 1 THEN mf.growth ELSE mf.growth*8 END AS [Growth_Amount],
		CASE WHEN is_percent_growth = 1 AND mf.growth > 0 THEN ((CONVERT(bigint,mf.size)*8)*mf.growth)/100 
			WHEN is_percent_growth = 0 AND mf.growth > 0 THEN mf.growth*8 
			ELSE 0 END AS [Next_Growth_KB],
		CASE WHEN @ifi = 0 AND mf.type = 0 THEN 'Instant File Initialization is disabled'
			WHEN @ifi = 1 AND mf.type = 0 THEN 'Instant File Initialization is enabled'
			ELSE '' END AS [Comments],
		mf.is_read_only
	FROM sys.master_files mf (NOLOCK)
	WHERE mf.type >= CASE WHEN @ifi = 1 THEN 0 ELSE 1 END 
		AND mf.type < 2 
		AND (CASE WHEN is_percent_growth = 1 AND mf.growth > 0 THEN ((CONVERT(bigint,mf.size)*8)*mf.growth)/100 
			WHEN is_percent_growth = 0 AND mf.growth > 0 THEN mf.growth*8 
			ELSE 0 END) > 1048576
	GROUP BY database_id, mf.name, mf.size, is_percent_growth, mf.growth, mf.type_desc, mf.[type], mf.[state_desc], mf.is_read_only
	ORDER BY DB_NAME(mf.database_id), mf.name
END
ELSE
BEGIN
	SELECT 'Database_checks' AS [Category], 'Large_Autogrows' AS [Check], '[OK]' AS [Deviation]
END;

--------------------------------------------------------------------------------------------------------------------------------
-- VLF subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'  |-Starting VLF', 10, 1) WITH NOWAIT
IF ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) = 1
BEGIN
	DECLARE /*@dbid int,*/ @query VARCHAR(1000)/*, @dbname VARCHAR(1000)*/, @count int, @count_used int, @logsize DECIMAL(20,1), @usedlogsize DECIMAL(20,1), @avgvlfsize DECIMAL(20,1)
	DECLARE @potsize DECIMAL(20,1), @n_iter int, @n_iter_final int, @initgrow DECIMAL(20,1), @n_init_iter int

	IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#log_info1'))
	DROP TABLE #log_info1;
	IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#log_info1'))
	CREATE TABLE #log_info1 (dbname VARCHAR(100), 
		Actual_log_size_MB DECIMAL(20,1), 
		Used_Log_size_MB DECIMAL(20,1),
		Potential_log_size_MB DECIMAL(20,1), 
		Actual_VLFs int,
		Used_VLFs int,
		Avg_VLF_size_KB DECIMAL(20,1),
		Potential_VLFs int, 
		Growth_iterations int,
		Log_Initial_size_MB DECIMAL(20,1),
		File_autogrow_MB DECIMAL(20,1))
	
	IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#log_info2'))
	DROP TABLE #log_info2;
	IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#log_info2'))
	CREATE TABLE #log_info2 (dbname VARCHAR(100), 
		Actual_VLFs int, 
		VLF_size_KB DECIMAL(20,1), 
		growth_iteration int)
		
	UPDATE #tmpdbs0
	SET isdone = 0;

	UPDATE #tmpdbs0
	SET isdone = 1
	WHERE [state] <> 0;

	UPDATE #tmpdbs0
	SET isdone = 1
	WHERE [role] = 2 AND secondary_role_allow_connections = 0;

	IF (SELECT COUNT(id) FROM #tmpdbs0 WHERE isdone = 0) > 0
	BEGIN
		WHILE (SELECT COUNT(id) FROM #tmpdbs0 WHERE isdone = 0) > 0
		BEGIN
			SELECT TOP 1 @dbname = [dbname], @dbid = [dbid] FROM #tmpdbs0 WHERE isdone = 0
			
			IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#log_info3'))
			DROP TABLE #log_info3;
			IF NOT EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#log_info3'))
			CREATE TABLE #log_info3 (recoveryunitid int NULL,
				fileid tinyint,
				file_size bigint,
				start_offset bigint,
				FSeqNo int,
				[status] tinyint,
				parity tinyint,
				create_lsn numeric(25,0))
			SET @query = 'DBCC LOGINFO (' + '''' + @dbname + ''') WITH NO_INFOMSGS'
			IF @sqlmajorver < 11
			BEGIN
				INSERT INTO #log_info3 (fileid, file_size, start_offset, FSeqNo, [status], parity, create_lsn)
				EXEC (@query)
			END
			ELSE
			BEGIN
				INSERT INTO #log_info3 (recoveryunitid, fileid, file_size, start_offset, FSeqNo, [status], parity, create_lsn)
				EXEC (@query)
			END

			SET @count = @@ROWCOUNT
			SET @count_used = (SELECT COUNT(fileid) FROM #log_info3 l WHERE l.[status] = 2)
			SET @logsize = (SELECT (MIN(l.start_offset) + SUM(l.file_size))/1048576.00 FROM #log_info3 l)
			SET @usedlogsize = (SELECT (MIN(l.start_offset) + SUM(CASE WHEN l.status <> 0 THEN l.file_size ELSE 0 END))/1048576.00 FROM #log_info3 l)
			SET @avgvlfsize = (SELECT AVG(l.file_size)/1024.00 FROM #log_info3 l)

			INSERT INTO #log_info2
			SELECT @dbname, COUNT(create_lsn), MIN(l.file_size)/1024.00,
				ROW_NUMBER() OVER(ORDER BY l.create_lsn) FROM #log_info3 l 
			GROUP BY l.create_lsn 
			ORDER BY l.create_lsn

			DROP TABLE #log_info3;

			-- Grow logs in MB instead of GB because of known issue prior to SQL 2012.
			-- More detail here: http://www.sqlskills.com/BLOGS/PAUL/post/Bug-log-file-growth-broken-for-multiples-of-4GB.aspx
			-- and http://connect.microsoft.com/SQLServer/feedback/details/481594/log-growth-not-working-properly-with-specific-growth-sizes-vlfs-also-not-created-appropriately
			-- or https://connect.microsoft.com/SQLServer/feedback/details/357502/transaction-log-file-size-will-not-grow-exactly-4gb-when-filegrowth-4gb
			IF @sqlmajorver >= 11
			BEGIN
				SET @n_iter = (SELECT CASE WHEN @logsize <= 64 THEN 1
					WHEN @logsize > 64 AND @logsize < 256 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/256, 0)
					WHEN @logsize >= 256 AND @logsize < 1024 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/512, 0)
					WHEN @logsize >= 1024 AND @logsize < 4096 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/1024, 0)
					WHEN @logsize >= 4096 AND @logsize < 8192 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/2048, 0)
					WHEN @logsize >= 8192 AND @logsize < 16384 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/4096, 0)
					WHEN @logsize >= 16384 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/8192, 0)
					END)
				SET @potsize = (SELECT CASE WHEN @logsize <= 64 THEN 1*64
					WHEN @logsize > 64 AND @logsize < 256 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/256, 0)*256
					WHEN @logsize >= 256 AND @logsize < 1024 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/512, 0)*512
					WHEN @logsize >= 1024 AND @logsize < 4096 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/1024, 0)*1024
					WHEN @logsize >= 4096 AND @logsize < 8192 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/2048, 0)*2048
					WHEN @logsize >= 8192 AND @logsize < 16384 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/4096, 0)*4096
					WHEN @logsize >= 16384 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/8192, 0)*8192
					END)
			END
			ELSE
			BEGIN
				SET @n_iter = (SELECT CASE WHEN @logsize <= 64 THEN 1
					WHEN @logsize > 64 AND @logsize < 256 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/256, 0)
					WHEN @logsize >= 256 AND @logsize < 1024 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/512, 0)
					WHEN @logsize >= 1024 AND @logsize < 4096 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/1024, 0)
					WHEN @logsize >= 4096 AND @logsize < 8192 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/2048, 0)
					WHEN @logsize >= 8192 AND @logsize < 16384 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/4000, 0)
					WHEN @logsize >= 16384 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/8000, 0)
					END)
				SET @potsize = (SELECT CASE WHEN @logsize <= 64 THEN 1*64
					WHEN @logsize > 64 AND @logsize < 256 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/256, 0)*256
					WHEN @logsize >= 256 AND @logsize < 1024 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/512, 0)*512
					WHEN @logsize >= 1024 AND @logsize < 4096 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/1024, 0)*1024
					WHEN @logsize >= 4096 AND @logsize < 8192 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/2048, 0)*2048
					WHEN @logsize >= 8192 AND @logsize < 16384 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/4000, 0)*4000
					WHEN @logsize >= 16384 THEN ROUND(CONVERT(FLOAT, ROUND(@logsize, -2))/8000, 0)*8000
					END)
			END
			
			-- If the proposed log size is smaller than current log, and also smaller than 4GB,
			-- and there is less than 512MB of diff between the current size and proposed size, add 1 grow.
			SET @n_iter_final = @n_iter
			IF @logsize > @potsize AND @potsize <= 4096 AND ABS(@logsize - @potsize) < 512
			BEGIN
				SET @n_iter_final = @n_iter + 1
			END
			-- If the proposed log size is larger than current log, and also larger than 50GB, 
			-- and there is less than 1GB of diff between the current size and proposed size, take 1 grow.
			ELSE IF @logsize < @potsize AND @potsize <= 51200 AND ABS(@logsize - @potsize) > 1024
			BEGIN
				SET @n_iter_final = @n_iter - 1
			END

			IF @potsize = 0 
			BEGIN 
				SET @potsize = 64 
			END
			IF @n_iter = 0 
			BEGIN 
				SET @n_iter = 1
			END
			
			SET @potsize = (SELECT CASE WHEN @n_iter < @n_iter_final THEN @potsize + (@potsize/@n_iter) 
					WHEN @n_iter > @n_iter_final THEN @potsize - (@potsize/@n_iter) 
					ELSE @potsize END)
			
			SET @n_init_iter = @n_iter_final
			IF @potsize >= 8192
			BEGIN
				SET @initgrow = @potsize/@n_iter_final
			END
			IF @potsize >= 64 AND @potsize <= 512
			BEGIN
				SET @n_init_iter = 1
				SET @initgrow = 512
			END
			IF @potsize > 512 AND @potsize <= 1024
			BEGIN
				SET @n_init_iter = 1
				SET @initgrow = 1023
			END
			IF @potsize > 1024 AND @potsize < 8192
			BEGIN
				SET @n_init_iter = 1
				SET @initgrow = @potsize
			END

			INSERT INTO #log_info1
			VALUES(@dbname, @logsize, @usedlogsize, @potsize, @count, @count_used, @avgvlfsize, 
				CASE WHEN @potsize <= 64 THEN (@potsize/(@potsize/@n_init_iter))*4
					WHEN @potsize > 64 AND @potsize < 1024 THEN (@potsize/(@potsize/@n_init_iter))*8
					WHEN @potsize >= 1024 THEN (@potsize/(@potsize/@n_init_iter))*16
					END,
				@n_init_iter, @initgrow, 
				CASE WHEN (@potsize/@n_iter_final) <= 1024 THEN (@potsize/@n_iter_final) ELSE 1024 END
				);

			UPDATE #tmpdbs0
			SET isdone = 1
			WHERE [dbid] = @dbid
		END
	END;

	IF (SELECT COUNT(dbname) FROM #log_info1 WHERE Actual_VLFs >= 50) > 0
	BEGIN
		SELECT 'Database_checks' AS [Category], 'Virtual_Log_Files' AS [Check], '[WARNING: Some user databases have many VLFs. Please review these]' AS [Deviation]
		SELECT 'Database_checks' AS [Category], 'Virtual_Log_Files' AS [Information], dbname AS [Database_Name], Actual_log_size_MB, Used_Log_size_MB,
			Potential_log_size_MB, Actual_VLFs, Used_VLFs, Potential_VLFs, Growth_iterations, Log_Initial_size_MB, File_autogrow_MB
		FROM #log_info1
		WHERE Actual_VLFs >= 50 -- My rule of thumb is 50 VLFs. Your mileage may vary.
		ORDER BY dbname;
		
		SELECT 'Database_checks' AS [Category], 'Virtual_Log_Files_per_growth' AS [Information], #log_info2.dbname AS [Database_Name], #log_info2.Actual_VLFs AS VLFs_remain_per_spawn, VLF_size_KB, growth_iteration
		FROM #log_info2
		INNER JOIN #log_info1 ON #log_info2.dbname = #log_info1.dbname
		WHERE #log_info1.Actual_VLFs >= 50 -- My rule of thumb is 50 VLFs. Your mileage may vary.
		ORDER BY #log_info2.dbname, growth_iteration

		SELECT 'Database_checks' AS [Category], 'Virtual_Log_Files_agg_per_size' AS [Information], #log_info2.dbname AS [Database_Name], SUM(#log_info2.Actual_VLFs) AS VLFs_per_size, VLF_size_KB
		FROM #log_info2
		INNER JOIN #log_info1 ON #log_info2.dbname = #log_info1.dbname
		WHERE #log_info1.Actual_VLFs >= 50 -- My rule of thumb is 50 VLFs. Your mileage may vary.
		GROUP BY #log_info2.dbname, VLF_size_KB
		ORDER BY #log_info2.dbname, VLF_size_KB DESC
	END
	ELSE
	BEGIN
		SELECT 'Database_checks' AS [Category], 'Virtual_Log_Files' AS [Check], '[OK]' AS [Deviation]

		/*
		SELECT 'Database_checks' AS [Category], 'Virtual_Log_Files' AS [Information], dbname AS [Database_Name], Actual_log_size_MB, Used_Log_size_MB, Actual_VLFs, Used_VLFs
		FROM #log_info1
		ORDER BY dbname;
		
		SELECT 'Database_checks' AS [Category], 'Virtual_Log_Files_per_growth' AS [Information], dbname AS [Database_Name], Actual_VLFs AS VLFs_remain_per_spawn, VLF_size_KB, growth_iteration
		FROM #log_info2
		ORDER BY dbname, growth_iteration

		SELECT 'Database_checks' AS [Category], 'Virtual_Log_Files_agg_per_size' AS [Information], dbname AS [Database_Name], SUM(Actual_VLFs) AS VLFs_per_size, VLF_size_KB
		FROM #log_info2
		GROUP BY dbname, VLF_size_KB
		ORDER BY dbname, VLF_size_KB DESC
		*/
	END
END
ELSE
BEGIN
	RAISERROR('[WARNING: Only a sysadmin can run the "VLF" check. Bypassing check]', 16, 1, N'sysadmin')
	--RETURN
END;

--------------------------------------------------------------------------------------------------------------------------------
-- Data files and Logs / tempDB and user Databases / Backups and Database files in same volume (Mountpoint aware) subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'  |-Starting Data files and Logs / tempDB and user Databases / Backups and Database files in same volume (Mountpoint aware)', 10, 1) WITH NOWAIT
IF @allow_xpcmdshell = 1
BEGIN
	DECLARE /*@dbid int,*/ @ctr2 int, @ctr3 int, @ctr4 int, @pserr bit
	SET @pserr = 0
	IF ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) = 1 -- Is sysadmin
	OR ((ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) <> 1 
		AND (SELECT COUNT(credential_id) FROM sys.credentials WHERE name = '##xp_cmdshell_proxy_account##') > 0) -- Is not sysadmin but proxy account exists
		AND (SELECT COUNT(l.name)
		FROM sys.server_permissions p (NOLOCK) INNER JOIN sys.server_principals l (NOLOCK)
		ON p.grantee_principal_id = l.principal_id
			AND p.class = 100 -- Server
			AND p.state IN ('G', 'W') -- Granted or Granted with Grant
			AND l.is_disabled = 0
			AND p.permission_name = 'ALTER SETTINGS'
			AND QUOTENAME(l.name) = QUOTENAME(USER_NAME())) = 0) -- Is not sysadmin but has alter settings permission 
	OR ((ISNULL(IS_SRVROLEMEMBER(N'sysadmin'), 0) <> 1 
		AND ((SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_fileexist') > 0 AND
		(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_instance_regread') > 0 AND
		(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_regread') > 0 AND
		(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'sp_OAGetErrorInfo') > 0 AND
		(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'sp_OACreate') > 0 AND
		(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'sp_OADestroy') > 0 AND
		(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_cmdshell') > 0 AND
		(SELECT COUNT([name]) FROM @permstbl WHERE [name] = 'xp_regenumvalues') > 0)))
	BEGIN
		IF @sqlmajorver < 11 OR (@sqlmajorver = 10 AND @sqlminorver = 50 AND @sqlbuild <= 2500)
		BEGIN
			DECLARE @pstbl TABLE ([KeyExist] int)
			BEGIN TRY
				INSERT INTO @pstbl
				EXEC master.sys.xp_regread N'HKEY_LOCAL_MACHINE', N'SOFTWARE\Microsoft\PowerShell\1' -- check if Powershell is installed
			END TRY
			BEGIN CATCH
				SELECT ERROR_NUMBER() AS ErrorNumber, ERROR_MESSAGE() AS ErrorMessage;
				SELECT @ErrorMessage = 'Data files and Logs in same volume (Mountpoint aware) subsection - Error raised in TRY block. ' + ERROR_MESSAGE()
				RAISERROR (@ErrorMessage, 16, 1);
			END CATCH

			SELECT @sao = CAST([value] AS smallint) FROM sys.configurations WITH (NOLOCK) WHERE [name] = 'show advanced options'
			SELECT @xcmd = CAST([value] AS smallint) FROM sys.configurations WITH (NOLOCK) WHERE [name] = 'xp_cmdshell'
			SELECT @ole = CAST([value] AS smallint) FROM sys.configurations WITH (NOLOCK) WHERE [name] = 'Ole Automation Procedures'

			RAISERROR ('  |-Configuration options set for Data and Log location check', 10, 1) WITH NOWAIT
			IF @sao = 0
			BEGIN
				EXEC sp_configure 'show advanced options', 1; RECONFIGURE WITH OVERRIDE;
			END
			IF @xcmd = 0
			BEGIN
				EXEC sp_configure 'xp_cmdshell', 1; RECONFIGURE WITH OVERRIDE;
			END
			IF @ole = 0
			BEGIN
				EXEC sp_configure 'Ole Automation Procedures', 1; RECONFIGURE WITH OVERRIDE;
			END
		
			IF (SELECT [KeyExist] FROM @pstbl) = 1
			BEGIN
				DECLARE @ctr int
				DECLARE @output_hw_tot TABLE ([PS_OUTPUT] NVARCHAR(2048));
				DECLARE @output_hw_format TABLE ([volid] smallint IDENTITY(1,1), [HD_Volume] NVARCHAR(2048) NULL)
				
				IF @custompath IS NULL
				BEGIN
					IF @sqlmajorver < 11
					BEGIN
						EXEC master..xp_instance_regread N'HKEY_LOCAL_MACHINE',N'Software\Microsoft\MSSQLServer\Setup',N'SQLPath', @path OUTPUT
						SET @path = @path + '\LOG'
					END
					ELSE
					BEGIN
						SET @sqlcmd = N'SELECT @pathOUT = LEFT([path], LEN([path])-1) FROM sys.dm_os_server_diagnostics_log_configurations';
						SET @params = N'@pathOUT NVARCHAR(2048) OUTPUT';
						EXECUTE sp_executesql @sqlcmd, @params, @pathOUT=@path OUTPUT;
					END
					
					-- Create COM object with FSO
					EXEC @OLEResult = master.dbo.sp_OACreate 'Scripting.FileSystemObject', @FSO OUT
					IF @OLEResult <> 0
					BEGIN
						EXEC sp_OAGetErrorInfo @FSO, @src OUT, @desc OUT
						SELECT @ErrorMessage = 'Error Creating COM Component 0x%x, %s, %s'
						RAISERROR (@ErrorMessage, 16, 1, @OLEResult, @src, @desc);
					END
					ELSE
					BEGIN
						EXEC @OLEResult = master.dbo.sp_OAMethod @FSO, 'FolderExists', @existout OUT, @path
						IF @OLEResult <> 0
						BEGIN
							EXEC sp_OAGetErrorInfo @FSO, @src OUT, @desc OUT
							SELECT @ErrorMessage = 'Error Calling FolderExists Method 0x%x, %s, %s'
							RAISERROR (@ErrorMessage, 16, 1, @OLEResult, @src, @desc);
						END
						ELSE
						BEGIN
							IF @existout <> 1
							BEGIN
								SET @path = CONVERT(NVARCHAR(500), SERVERPROPERTY('ErrorLogFileName'))
								SET @path = LEFT(@path,LEN(@path)-CHARINDEX('\', REVERSE(@path)))
							END 
						END
						EXEC @OLEResult = sp_OADestroy @FSO
					END
				END
				ELSE
				BEGIN
					SELECT @path = CASE WHEN @custompath LIKE '%\' THEN LEFT(@custompath, LEN(@custompath)-1) ELSE @custompath END
				END
				
				SET @FileName = @path + '\checkbp_' + RTRIM(@server) + '.ps1'
				
				EXEC master.dbo.xp_fileexist @FileName, @existout out
				IF @existout = 0
				BEGIN 
					-- Scan for local disks
					SET @Text1 = '[string] $serverName = ''localhost''
$vols = Get-WmiObject -computername $serverName -query "select Name from Win32_Volume where Capacity <> NULL and DriveType = 3"
foreach($vol in $vols)
{
	[string] $drive = "{0}" -f $vol.name
	Write-Output $drive
}'
					-- Create COM object with FSO
					EXEC @OLEResult = master.dbo.sp_OACreate 'Scripting.FileSystemObject', @FS OUT
					IF @OLEResult <> 0
					BEGIN
						EXEC sp_OAGetErrorInfo @FS, @src OUT, @desc OUT
						SELECT @ErrorMessage = 'Error Creating COM Component 0x%x, %s, %s'
						RAISERROR (@ErrorMessage, 16, 1, @OLEResult, @src, @desc);
					END

					--Open file
					EXEC @OLEResult = master.dbo.sp_OAMethod @FS, 'OpenTextFile', @FileID OUT, @FileName, 2, 1
					IF @OLEResult <> 0
					BEGIN
						EXEC sp_OAGetErrorInfo @FS, @src OUT, @desc OUT
						SELECT @ErrorMessage = 'Error Calling OpenTextFile Method 0x%x, %s, %s' + CHAR(10) + 'Could not create file ' + @FileName
						RAISERROR (@ErrorMessage, 16, 1, @OLEResult, @src, @desc);
					END
					ELSE
					BEGIN
						SELECT @ErrorMessage = '    |-Created file ' + @FileName
						RAISERROR (@ErrorMessage, 10, 1) WITH NOWAIT
					END

					--Write Text1
					EXEC @OLEResult = master.dbo.sp_OAMethod @FileID, 'WriteLine', NULL, @Text1
					IF @OLEResult <> 0
					BEGIN
						EXEC sp_OAGetErrorInfo @FS, @src OUT, @desc OUT
						SELECT @ErrorMessage = 'Error Calling WriteLine Method 0x%x, %s, %s' + CHAR(10) + 'Could not write to file ' + @FileName
						RAISERROR (@ErrorMessage, 16, 1, @OLEResult, @src, @desc);
					END

					EXEC @OLEResult = sp_OADestroy @FileID
					EXEC @OLEResult = sp_OADestroy @FS
				END;
				ELSE
				BEGIN
					SELECT @ErrorMessage = '    |-Reusing file ' + @FileName
					RAISERROR (@ErrorMessage, 10, 1) WITH NOWAIT
				END

				IF @psver = 1
				BEGIN
					SET @CMD = 'powershell -NoLogo -NoProfile "' + @FileName + '" -ExecutionPolicy RemoteSigned'
				END
				ELSE
				BEGIN
					SET @CMD = 'powershell -NoLogo -NoProfile -File "' + @FileName + '" -ExecutionPolicy RemoteSigned'
				END;

				INSERT INTO @output_hw_tot 
				EXEC master.dbo.xp_cmdshell @CMD

				SET @CMD = 'del /Q "' + @FileName + '"'
				EXEC master.dbo.xp_cmdshell @CMD, NO_OUTPUT

				IF (SELECT COUNT([PS_OUTPUT]) 
				FROM @output_hw_tot WHERE [PS_OUTPUT] LIKE '%cannot be loaded because%'
					OR [PS_OUTPUT] LIKE '%scripts is disabled%'
					OR [PS_OUTPUT] LIKE '%scripts est désactivée%') = 0
				BEGIN
					INSERT INTO @output_hw_format ([HD_Volume])
					SELECT RTRIM([PS_OUTPUT]) 
					FROM @output_hw_tot 
					WHERE [PS_OUTPUT] IS NOT NULL
				END
				ELSE
				BEGIN
					SET @pserr = 1
					RAISERROR ('[WARNING: Powershell script cannot be loaded because the execution of scripts is disabled on this system.
To change the execution policy, type the following command in Powershell console: Set-ExecutionPolicy RemoteSigned
The Set-ExecutionPolicy cmdlet enables you to determine which Windows PowerShell scripts (if any) will be allowed to run on your computer. 
Windows PowerShell has four different execution policies:
	Restricted - No scripts can be run. Windows PowerShell can be used only in interactive mode.
	AllSigned - Only scripts signed by a trusted publisher can be run.
	RemoteSigned - Downloaded scripts must be signed by a trusted publisher before they can be run.
		|- REQUIRED by BP Check
	Unrestricted - No restrictions; all Windows PowerShell scripts can be run.]
',16,1);
				END
		
				SET @CMD2 = 'del ' + @FileName
				EXEC master.dbo.xp_cmdshell @CMD2, NO_OUTPUT;
			END
			ELSE
			BEGIN
				SET @pserr = 1
				RAISERROR ('[WARNING: Powershell is not present. Bypassing Data files and Logs in same volume check]',16,1);
			END
			
			IF @xcmd = 0
			BEGIN
				EXEC sp_configure 'xp_cmdshell', 0; RECONFIGURE WITH OVERRIDE;
			END
			IF @ole = 0
			BEGIN
				EXEC sp_configure 'Ole Automation Procedures', 0; RECONFIGURE WITH OVERRIDE;
			END
			IF @sao = 0
			BEGIN
				EXEC sp_configure 'show advanced options', 0; RECONFIGURE WITH OVERRIDE;
			END
		END
		ELSE
		BEGIN
			INSERT INTO @output_hw_format ([HD_Volume])
			EXEC ('SELECT DISTINCT(volume_mount_point) FROM sys.master_files mf CROSS APPLY sys.dm_os_volume_stats (database_id, [file_id]) WHERE mf.[file_id] < 65537')
		END;

		IF @pserr = 0
		BEGIN
			-- select mountpoints only
			DECLARE @intertbl TABLE (physical_name nvarchar(260))
			INSERT INTO @intertbl
			SELECT physical_name
			FROM sys.master_files t1 (NOLOCK) 
			INNER JOIN @output_hw_format t2 ON LEFT(physical_name, LEN(t2.HD_Volume)) = RTRIM(t2.HD_Volume)
			WHERE ([database_id] > 4 OR [database_id] = 2)
				AND [database_id] <> 32767 AND LEN(t2.HD_Volume) > 3

			-- select database files in mountpoints		
			DECLARE @filetbl TABLE (database_id int, type tinyint, file_id int, physical_name nvarchar(260), volid smallint)
			INSERT INTO @filetbl
			SELECT database_id, type, file_id, physical_name, volid
			FROM sys.master_files t1 (NOLOCK) 
			INNER JOIN @output_hw_format t2 ON LEFT(physical_name, LEN(t2.HD_Volume)) = RTRIM(t2.HD_Volume)
			WHERE ([database_id] > 4 OR [database_id] = 2) AND [database_id] <> 32767 AND LEN(t2.HD_Volume) > 3
			UNION ALL
			-- select database files not in mountpoints
			SELECT database_id, type, file_id, physical_name, volid
			FROM sys.master_files t1 (NOLOCK) 
			INNER JOIN @output_hw_format t2 ON LEFT(physical_name, LEN(t2.HD_Volume)) = RTRIM(t2.HD_Volume)
			WHERE ([database_id] > 4 OR [database_id] = 2) AND [database_id] <> 32767 AND physical_name NOT IN (SELECT physical_name FROM @intertbl)
				
			SELECT @ctr = COUNT(DISTINCT(t1.[database_id])) FROM @filetbl t1 
			INNER JOIN @filetbl t2 ON t1.database_id = t2.database_id
				AND t1.[type] <> t2.[type]
				AND ((t1.[type] = 1 AND t2.[type] <> 1) OR (t2.[type] = 1 AND t1.[type] <> 1))
				AND t1.volid = t2.volid;

			IF @ctr > 0
			BEGIN
				SELECT 'Database_checks' AS [Category], 'Data_and_Log_locations' AS [Check], '[WARNING: Some user databases have Data and Log files in the same physical volume]' AS [Deviation]
				SELECT DISTINCT 'Database_checks' AS [Category], 'Data_and_Log_locations' AS [Information], DB_NAME(mf.[database_id]) AS [Database_Name], type_desc AS [Type], mf.physical_name
				FROM sys.master_files mf (NOLOCK) INNER JOIN @filetbl t1 ON mf.database_id = t1.database_id AND mf.physical_name = t1.physical_name
					INNER JOIN @filetbl t2 ON t1.database_id = t2.database_id
						AND t1.[type] <> t2.[type]
						AND ((t1.[type] = 1 AND t2.[type] <> 1) OR (t2.[type] = 1 AND t1.[type] <> 1))
						AND t1.volid = t2.volid
				ORDER BY mf.physical_name OPTION (RECOMPILE);
			END
			ELSE
			BEGIN
				SELECT 'Database_checks' AS [Category], 'Data_and_Log_locations' AS [Check], '[OK]' AS [Deviation]
			END;

			-- select backup mountpoints only
			DECLARE @interbcktbl TABLE (physical_device_name nvarchar(260))
			INSERT INTO @interbcktbl
			SELECT physical_device_name
			FROM msdb.dbo.backupmediafamily t1 (NOLOCK) 
			INNER JOIN @output_hw_format t2 ON LEFT(physical_device_name, LEN(t2.HD_Volume)) = RTRIM(t2.HD_Volume)
			WHERE LEN(t2.HD_Volume) > 3

			-- select backups in mountpoints only
			DECLARE @bcktbl TABLE (physical_device_name nvarchar(260), HD_Volume nvarchar(260))
			INSERT INTO @bcktbl
			SELECT physical_device_name, RTRIM(t2.HD_Volume)
			FROM msdb.dbo.backupmediafamily t1 (NOLOCK) 
			INNER JOIN @output_hw_format t2 ON LEFT(physical_device_name, LEN(t2.HD_Volume)) = RTRIM(t2.HD_Volume)
			WHERE LEN(t2.HD_Volume) > 3
			-- select backups not in mountpoints
			UNION ALL
			SELECT physical_device_name, RTRIM(t2.HD_Volume)
			FROM msdb.dbo.backupmediafamily t1 (NOLOCK)
			INNER JOIN @output_hw_format t2 ON LEFT(physical_device_name, LEN(t2.HD_Volume)) = RTRIM(t2.HD_Volume)
			WHERE physical_device_name NOT IN (SELECT physical_device_name FROM @interbcktbl);

			SELECT @ctr4 = COUNT(DISTINCT(physical_device_name)) FROM @bcktbl;

			IF @ctr4 > 0
			BEGIN
				SELECT 'Database_checks' AS [Category], 'Backup_and_Database_locations' AS [Check], '[WARNING: Some backups and database files are in the same physical volume]' AS [Deviation]
				SELECT DISTINCT 'Database_checks' AS [Category], 'Backup_and_Database_locations' AS [Information], physical_device_name AS [Backup_Location], HD_Volume AS [Volume_with_DB_Files]
				FROM @bcktbl
				OPTION (RECOMPILE);
			END
			ELSE
			BEGIN
				SELECT 'Database_checks' AS [Category], 'Backup_and_Database_locations' AS [Check], '[OK]' AS [Deviation]
			END;

			-- select tempDB mountpoints only
			DECLARE @intertbl2 TABLE (physical_name nvarchar(260))
			INSERT INTO @intertbl2
			SELECT physical_name
			FROM sys.master_files t1 (NOLOCK) INNER JOIN @output_hw_format t2
			ON LEFT(physical_name, LEN(t2.HD_Volume)) = RTRIM(t2.HD_Volume)
			WHERE [database_id] = 2 AND LEN(t2.HD_Volume) > 3 AND [type] = 0
			
			-- select user DBs mountpoints only
			DECLARE @intertbl3 TABLE (physical_name nvarchar(260))
			INSERT INTO @intertbl3
			SELECT physical_name
			FROM sys.master_files t1 (NOLOCK) INNER JOIN @output_hw_format t2
			ON LEFT(physical_name, LEN(t2.HD_Volume)) = RTRIM(t2.HD_Volume)
			WHERE [database_id] > 4 AND [database_id] <> 32767 AND LEN(t2.HD_Volume) > 3 AND [type] = 0
			
			-- select tempDB files in mountpoints		
			DECLARE @tempDBtbl TABLE (database_id int, type tinyint, file_id int, physical_name nvarchar(260), volid smallint)
			INSERT INTO @tempDBtbl
			SELECT database_id, type, file_id, physical_name, volid
			FROM sys.master_files t1 (NOLOCK) INNER JOIN @output_hw_format t2 ON LEFT(physical_name, LEN(t2.HD_Volume)) = RTRIM(t2.HD_Volume)
			WHERE [database_id] = 2 AND LEN(t2.HD_Volume) > 3 AND [type] = 0
			UNION ALL
			SELECT database_id, type, file_id, physical_name, volid
			FROM sys.master_files t1 (NOLOCK) INNER JOIN @output_hw_format t2 ON LEFT(physical_name, LEN(t2.HD_Volume)) = RTRIM(t2.HD_Volume)
			WHERE [database_id] = 2 AND [type] = 0 AND physical_name NOT IN (SELECT physical_name FROM @intertbl2)

			-- select user DBs files in mountpoints		
			DECLARE @otherstbl TABLE (database_id int, type tinyint, file_id int, physical_name nvarchar(260), volid smallint)
			INSERT INTO @otherstbl
			SELECT database_id, type, file_id, physical_name, volid
			FROM sys.master_files t1 (NOLOCK) INNER JOIN @output_hw_format t2 ON LEFT(physical_name, LEN(t2.HD_Volume)) = RTRIM(t2.HD_Volume)
			WHERE [database_id] > 4 AND [database_id] <> 32767 AND LEN(t2.HD_Volume) > 3 AND [type] = 0
			UNION ALL
			SELECT database_id, type, file_id, physical_name, volid
			FROM sys.master_files t1 (NOLOCK) INNER JOIN @output_hw_format t2 ON LEFT(physical_name, LEN(t2.HD_Volume)) = RTRIM(t2.HD_Volume)
			WHERE [database_id] > 4 AND [database_id] <> 32767 AND [type] = 0 AND physical_name NOT IN (SELECT physical_name FROM @intertbl3)

			SELECT @ctr2 = COUNT(*) FROM @tempDBtbl WHERE LEFT(physical_name, 1) = 'C'

			SELECT @ctr3 = COUNT(DISTINCT(t1.[database_id])) FROM @otherstbl t1 INNER JOIN @tempDBtbl t2 ON t1.volid = t2.volid;

			IF @ctr3 > 0
			BEGIN
				SELECT 'tempDB_checks' AS [Category], 'tempDB_location' AS [Check], '[WARNING: tempDB is on the same physical volume as user databases]' AS [Deviation];
			END
			ELSE IF @ctr2 > 0
			BEGIN
				SELECT 'tempDB_checks' AS [Category], 'tempDB_location' AS [Check], '[WARNING: tempDB is on C: drive]' AS [Deviation]
			END
			ELSE
			BEGIN
				SELECT 'tempDB_checks' AS [Category], 'tempDB_location' AS [Check], '[OK]' AS [Deviation]
			END;
			
			IF @ctr2 > 0 OR @ctr3 > 0
			BEGIN
				SELECT DISTINCT 'tempDB_checks' AS [Category], 'tempDB_location' AS [Information], DB_NAME(mf.[database_id]) AS [Database_Name], type_desc AS [Type], mf.physical_name
				FROM sys.master_files mf (NOLOCK) INNER JOIN @otherstbl t1 ON mf.database_id = t1.database_id AND mf.physical_name = t1.physical_name
					INNER JOIN @tempDBtbl t2 ON t1.volid = t2.volid
				UNION ALL
				SELECT DISTINCT 'tempDB_checks' AS [Category], 'tempDB_location' AS [Information], DB_NAME(mf.[database_id]) AS [Database_Name], type_desc AS [Type], mf.physical_name
				FROM sys.master_files mf (NOLOCK) INNER JOIN @tempDBtbl t1 ON mf.database_id = t1.database_id AND mf.physical_name = t1.physical_name
				ORDER BY DB_NAME(mf.[database_id]) OPTION (RECOMPILE);
			END
		END
		ELSE
		BEGIN
			SELECT 'Database_checks' AS [Category], 'Data_and_Log_locations' AS [Check], '[WARNING: Could not gather information on file locations]' AS [Deviation]
			SELECT 'tempDB_checks' AS [Category], 'tempDB_location' AS [Check], '[WARNING: Could not gather information on file locations]' AS [Deviation]
		END
	END
	ELSE
	BEGIN
		RAISERROR('[WARNING: Only a sysadmin can run the "Data files and Logs / tempDB and user Databases in same volume" checks. A regular user can also run this check if a xp_cmdshell proxy account exists. Bypassing check]', 16, 1, N'xp_cmdshellproxy')
		RAISERROR('[WARNING: If not sysadmin, then must be a granted EXECUTE permissions on the following extended sprocs to run checks: sp_OACreate, sp_OADestroy, sp_OAGetErrorInfo, xp_cmdshell, xp_instance_regread, xp_regread, xp_fileexist and xp_regenumvalues. Bypassing check]', 16, 1, N'extended_sprocs')
		--RETURN
	END
END
ELSE
BEGIN
	RAISERROR('  |- [INFORMATION: "Data files and Logs / tempDB and user Databases in same volume" check was skipped because xp_cmdshell was not allowed.]', 10, 1, N'disallow_xp_cmdshell')
	--RETURN
END;

--------------------------------------------------------------------------------------------------------------------------------
-- tempDB data file configurations subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'  |-Starting tempDB data file configurations', 10, 1) WITH NOWAIT
DECLARE @tdb_files int, @online_count int, @filesizes smallint
SELECT @tdb_files = COUNT(physical_name) FROM sys.master_files (NOLOCK) WHERE database_id = 2 AND [type] = 0;
SELECT @online_count = COUNT(cpu_id) FROM sys.dm_os_schedulers WHERE is_online = 1 AND scheduler_id < 255 AND parent_node_id < 64;
SELECT @filesizes = COUNT(DISTINCT size) FROM tempdb.sys.database_files WHERE [type] = 0;

IF (SELECT CASE WHEN @filesizes = 1 AND ((@tdb_files >= 4 AND @tdb_files <= 8 AND @tdb_files % 4 = 0) /*OR (@tdb_files >= 8 AND @tdb_files % 4 = 0)*/ 
	OR (@tdb_files >= (@online_count / 2) AND @tdb_files >= 8 AND @tdb_files % 4 = 0)) THEN 0 ELSE 1 END) = 0
BEGIN
	SELECT 'tempDB_checks' AS [Category], 'tempDB_files' AS [Check], '[OK]' AS [Deviation]
	SELECT 'tempDB_checks' AS [Category], 'tempDB_files' AS [Information], physical_name AS [tempDB_Files], CAST((size*8)/1024.0 AS DECIMAL(18,2)) AS [File_Size_MB]
	FROM tempdb.sys.database_files (NOLOCK)
	WHERE type = 0;
END
ELSE 
BEGIN
	SELECT 'tempDB_checks' AS [Category], 'tempDB_files' AS [Check], 
		CASE WHEN @tdb_files < 4 THEN '[WARNING: tempDB has only ' + CONVERT(VARCHAR(10), @tdb_files) + ' file(s). Consider creating between 4 and 8 tempDB data files, 1 per each 2 cores, of the same size]'
			WHEN @filesizes = 1 AND @tdb_files < (@online_count / 2) AND @tdb_files >= 8 AND @tdb_files % 4 = 0 THEN '[INFORMATION: Number of Data files to Scheduler ratio might not be Optimal. Consider creating 1 data file per each 2 cores, in multiples of 4, all of the same size]'
			WHEN @filesizes > 1 AND @tdb_files >= 4 AND @tdb_files % 4 > 0 THEN '[WARNING: Data file sizes do not match and Number of data files is not multiple of 4]'
			WHEN @filesizes = 1 AND @tdb_files >= 4 AND @tdb_files % 4 > 0 THEN '[WARNING: Number of data files is not multiple of 4]'
			WHEN @filesizes > 1 AND @tdb_files >= 4 AND @tdb_files % 4 = 0 THEN '[WARNING: Data file sizes do not match]'
			ELSE '[OK]' END AS [Deviation];
	SELECT 'tempDB_checks' AS [Category], 'tempDB_files' AS [Information], physical_name AS [tempDB_Files], CAST((size*8)/1024.0 AS DECIMAL(18,2)) AS [File_Size_MB]
	FROM tempdb.sys.database_files (NOLOCK)
	WHERE type = 0;
END;

--------------------------------------------------------------------------------------------------------------------------------
-- tempDB data files autogrow of equal size subsection
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'  |-Starting tempDB Files autogrow of equal size', 10, 1) WITH NOWAIT
IF (SELECT COUNT(DISTINCT growth) FROM sys.master_files WHERE [database_id] = 2 AND [type] = 0) > 1
	OR (SELECT COUNT(DISTINCT is_percent_growth) FROM sys.master_files WHERE [database_id] = 2 AND [type] = 0) > 1
BEGIN
	SELECT 'tempDB_checks' AS [Category], 'tempDB_files_Autogrow' AS [Check], '[WARNING: Some tempDB data files have different growth settings]' AS [Deviation]
	SELECT 'tempDB_checks' AS [Category], 'tempDB_files_Autogrow' AS [Information], 
		DB_NAME(2) AS [Database_Name], 
		mf.name AS [Logical_Name],
		mf.[size]*8 AS [Current_Size_KB],
		mf.type_desc AS [File_Type],
		CASE WHEN is_percent_growth = 1 THEN 'pct' ELSE 'pages' END AS [Growth_Type],
		CASE WHEN is_percent_growth = 1 THEN mf.growth ELSE mf.growth*8 END AS [Growth_Amount],
		CASE WHEN is_percent_growth = 1 AND mf.growth > 0 THEN ((mf.size*8)*CONVERT(bigint, mf.growth))/100 
			WHEN is_percent_growth = 0 AND mf.growth > 0 THEN mf.growth*8 
			ELSE 0 END AS [Next_Growth_KB],
		CASE WHEN @ifi = 0 AND mf.type = 0 THEN 'Instant File Initialization is disabled'
			WHEN @ifi = 1 AND mf.type = 0 THEN 'Instant File Initialization is enabled'
			ELSE '' END AS [Comments]
	FROM tempdb.sys.database_files mf (NOLOCK)
	WHERE [type] = 0
	GROUP BY mf.name, mf.[size], is_percent_growth, mf.growth, mf.type_desc, mf.[type]
	ORDER BY 3, 4
END
ELSE
BEGIN
	SELECT 'tempDB_checks' AS [Category], 'tempDB_files_Autogrow' AS [Check], '[OK]' AS [Deviation]
END;

--------------------------------------------------------------------------------------------------------------------------------
-- Clean up temp objects 
--------------------------------------------------------------------------------------------------------------------------------
RAISERROR (N'Clearing up temporary objects', 10, 1) WITH NOWAIT

IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#dbinfo')) 
DROP TABLE #dbinfo;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#output_dbinfo')) 
DROP TABLE #output_dbinfo;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblIOStall')) 
DROP TABLE #tblIOStall;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tmpdbs1')) 
DROP TABLE #tmpdbs1;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tmpdbs0')) 
DROP TABLE #tmpdbs0;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblPerfCount')) 
DROP TABLE #tblPerfCount;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.tblPerfThresholds'))
DROP TABLE tempdb.dbo.tblPerfThresholds;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblHypObj')) 
DROP TABLE #tblHypObj;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblIxs1')) 
DROP TABLE #tblIxs1;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblIxs2')) 
DROP TABLE #tblIxs2;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblIxs3')) 
DROP TABLE #tblIxs3;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblIxs4')) 
DROP TABLE #tblIxs4;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblIxs5')) 
DROP TABLE #tblIxs5;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblIxs6')) 
DROP TABLE #tblIxs6;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblFK')) 
DROP TABLE #tblFK;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#dbcc')) 
DROP TABLE #dbcc;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#avail_logs')) 
DROP TABLE #avail_logs;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#log_info1')) 
DROP TABLE #log_info1;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#log_info2')) 
DROP TABLE #log_info2;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tmpobjectnames'))
DROP TABLE #tmpobjectnames;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tmpfinalobjectnames'))
DROP TABLE #tmpfinalobjectnames;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblWaits'))
DROP TABLE #tblWaits;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblFinalWaits'))
DROP TABLE #tblFinalWaits;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblLatches'))
DROP TABLE #tblLatches;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblFinalLatches'))
DROP TABLE #tblFinalLatches;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#IndexCreation'))
DROP TABLE #IndexCreation;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#IndexRedundant'))
DROP TABLE #IndexRedundant;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblBlkChains'))
DROP TABLE #tblBlkChains;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblStatsSamp'))
DROP TABLE #tblStatsSamp;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblSpinlocksBefore'))
DROP TABLE #tblSpinlocksBefore;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblSpinlocksAfter'))
DROP TABLE #tblSpinlocksAfter;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblFinalSpinlocks'))
DROP TABLE #tblFinalSpinlocks;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#pagerepair'))
DROP TABLE #pagerepair;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tmp_dm_io_virtual_file_stats'))
DROP TABLE #tmp_dm_io_virtual_file_stats;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tmp_dm_exec_query_stats')) 
DROP TABLE #tmp_dm_exec_query_stats;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#dm_exec_query_stats')) 
DROP TABLE #dm_exec_query_stats;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblPendingIOReq'))
DROP TABLE #tblPendingIOReq;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblPendingIO'))
DROP TABLE #tblPendingIO;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#qpwarnings')) 
DROP TABLE #qpwarnings;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblStatsUpd'))
DROP TABLE #tblStatsUpd;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblPerSku'))
DROP TABLE #tblPerSku;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblColStoreIXs'))
DROP TABLE #tblColStoreIXs;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#SystemHealthSessionData'))
DROP TABLE #SystemHealthSessionData;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tmpdbfiledetail'))
DROP TABLE #tmpdbfiledetail;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblHints'))
DROP TABLE #tblHints;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblTriggers'))
DROP TABLE #tblTriggers;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tmpIPS'))
DROP TABLE #tmpIPS;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblCode'))
DROP TABLE #tblCode;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblWorking'))
DROP TABLE #tblWorking;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tmpdbs_userchoice'))
DROP TABLE #tmpdbs_userchoice;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#xp_cmdshell_CluNodesOutput'))
DROP TABLE #xp_cmdshell_CluNodesOutput;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#xp_cmdshell_CluOutput'))
DROP TABLE #xp_cmdshell_CluOutput;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#xp_cmdshell_Nodes'))
DROP TABLE #xp_cmdshell_Nodes;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#xp_cmdshell_QFEOutput'))
DROP TABLE #xp_cmdshell_QFEOutput;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#xp_cmdshell_QFEFinal'))
DROP TABLE #xp_cmdshell_QFEFinal;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#RegResult'))
DROP TABLE #RegResult;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#ServiceStatus'))
DROP TABLE #ServiceStatus;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#xp_cmdshell_AcctSPNoutput'))
DROP TABLE #xp_cmdshell_AcctSPNoutput;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#xp_cmdshell_DupSPNoutput'))
DROP TABLE #xp_cmdshell_DupSPNoutput;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#FinalDupSPN'))
DROP TABLE #FinalDupSPN;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#ScopedDupSPN'))
DROP TABLE #ScopedDupSPN;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblDRI'))
DROP TABLE #tblDRI;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tblInMemDBs'))
DROP TABLE #tblInMemDBs;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tmpXIS'))
DROP TABLE #tmpXIS;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tmpXNCIS'))
DROP TABLE #tmpXNCIS;
IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID('tempdb.dbo.#tmpIPS_CI'))
DROP TABLE #tmpIPS_CI;
EXEC ('USE tempdb; IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID(''tempdb.dbo.fn_perfctr'')) DROP FUNCTION dbo.fn_perfctr')
EXEC ('USE tempdb; IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID(''tempdb.dbo.fn_createindex_allcols'')) DROP FUNCTION dbo.fn_createindex_allcols')
EXEC ('USE tempdb; IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID(''tempdb.dbo.fn_createindex_keycols'')) DROP FUNCTION dbo.fn_createindex_keycols')
EXEC ('USE tempdb; IF EXISTS (SELECT [object_id] FROM tempdb.sys.objects (NOLOCK) WHERE [object_id] = OBJECT_ID(''tempdb.dbo.fn_createindex_includecols'')) DROP FUNCTION dbo.fn_createindex_includecols')
RAISERROR (N'All done!', 10, 1) WITH NOWAIT
GO