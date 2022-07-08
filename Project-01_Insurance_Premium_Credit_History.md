# Project 1 - Calculate Insurance Premium based on Credit History

**Linux shell, script, python, Sqoop, HDFS, Hive, Hbase and Phoenix.**

## Project Synopsis:

This project helps the Financial institutions such as Insurance and Banking systems to analyze the
Creditcard defaulters in order to define the Insurance premium accordingly. It uses a wide variety of online
and offline customer data including the customer transactions, customer master data, insurance data
etc to generate insights about individual consumer behaviors and preferences. 
This tool builds the strategies, Key Performance Indicators (KPI) definitions and
implementation roadmaps that assists the  clients in their Analytics & Information ecosystem
journey right from Strategy definition to large scale Global Implementations & Support using the bigdata
ecosystems.

As a part of Enterprise Data lake, the data will be persisted into Hadoop file system, HBASE and
Hive are injected using Sqoop for data exchange from DB sources, SFTP/SCP for file transfer from Cloud
platform. Data transformation and processing such as aggregation, filtering, grouping using Hive for
Batch data and Phoenix for online data access on Hbase for realtime data aggregation and reporting as
per the reporting needs.

## Architecture

![Project Architecture](resources/images/Project-01_Architecture.png)

## Data Flow:

1. Data ingestion and acquisition is done through Sqoop from RDBMS and into Linux file system
from Cloud or any other sources.
2. Merge the 2 dataset using hive and split the defaulters and non-defaulters into 2 data sets and
load into hdfs.
3. Create a hive table with header line count as 1.
4. Create one more fixed width hive table to load the fixed width states_fixed width data.
5. Create and load penality data into the hive table serialized with orc.
6. Export the Defaulters and Non Defaulters data into HDFS with comma delimiter, where non
defaulters data added with trailer data for consumer systems validation.
7. Perform dist copy of non defaulters data from Prod cluster to non prod clusters that will be used
for analytics purposes.
8. Run a shell script to pull the data from Cloud S3, validate, remove trailer data, move to HDFS
and archive the data for backup
9. Create a partitioned table and run a shell script to which has to generate the load command and
it has to load the data into the below insurance table automatically
10. Create a fixed width hive table to load the fixed width states_fixedwidth data using Regex Serde
11. Apply Data Governance - Redaction and Masking using Hive and Python
12. Data Provisioning to the Consumers into legacy Databases using Sqoop including Validation
13. Complex type ETL using Hive
14. Data movement & migration from DB to HBase using Sqoop
15. Data movement & migration from Hive to HBase using Storage Handler
Sqoop Direct Import to HBase
16. Data Provisioning to the Consumers using Apache Phoenix to support Realtime aggregation and
Low Latency Query processing
17. Write queries to build cubes at different levels to aggregate the data in realtime to populate in
the report such as average age, sum of bill amount, average bill amount etc.,

## Prerequisites:

Ensure that the following services are up and running

1. Start Hadoop
2. history server
3. mysql service
4. hive in mr mode
5. hive metastore
6. hbase
7. phoenix client

## Environment Setup


### Start the Following services:
**Hadoop:**

stop-all.sh
start-all.sh

mr-jobhistory-daemon.sh start historyserver

**Login, start mysql service and exit:**

sudo service mysqld stop
sudo service mysqld start

sudo mysql -u root -p

password: root

**Hive Metastore**

nohup hive --service metastore >> /dev/null 2>&1 &

### Extraction of Source code & data:

Download into /home/hduser/projects and Untar the data provided in creditcard_insurance.tar.gz

``` 
[hduser@localhost projects]$ pwd
/home/hduser/projects

[hduser@localhost projects]$ ls -l
total 5068
-rwxrw-rw-. 1 hduser hduser 5186680 Sep 20  2020 creditcard_insurance.tar.gz

[hduser@localhost projects]$ tar xvzf creditcard_insurance.tar.gz 
creditcard_insurance/
creditcard_insurance/1_dbscript
creditcard_insurance/creditcard.txt~
creditcard_insurance/zookeeper.out
creditcard_insurance/custmaster
creditcard_insurance/Inceptez_Banking_insurance_project2_2020_with_solution.pdf
creditcard_insurance/default of credit card clients.txt
creditcard_insurance/custmaster.xlsx
creditcard_insurance/1_1_creditdataload.sql
creditcard_insurance/custmaster.txt
creditcard_insurance/creditcard_defaulters_cst~
creditcard_insurance/batch_flow.sh~
creditcard_insurance/3_Hive Realtime interview questions.docx
creditcard_insurance/batch_flow.sh
creditcard_insurance/states_fixedwidth~
creditcard_insurance/creditdataload.sql~
creditcard_insurance/project flow.txt~
creditcard_insurance/hivepart.sh
creditcard_insurance/creditcard_defaulters_pst.txt
creditcard_insurance/insuranceinfo.csv
creditcard_insurance/2_creditcard_defaulters_cst
creditcard_insurance/creditdataload.sql
creditcard_insurance/3_2_hivestatefixedwidthload.hql
creditcard_insurance/custs.csv
creditcard_insurance/sfm_insuredata.sh
creditcard_insurance/creditcard_defaulters_pst~
creditcard_insurance/default of credit card clients.xls
creditcard_insurance/2_2_creditcard_defaulters_pst
creditcard_insurance/states.csv~
creditcard_insurance/custmaster.java
creditcard_insurance/creditcard_defaulters_cst.txt
creditcard_insurance/mask_insure.command
creditcard_insurance/mask_insure.py
creditcard_insurance/_HBASE_SUCCESS
creditcard_insurance/3_1_hiveinsuranceload.hql
creditcard_insurance/states_fixedwidth
creditcard_insurance/project flow.txt
creditcard_insurance/states.csv
creditcard_insurance/1_dbscript~
creditcard_insurance/Inceptez_Banking_insurance_project2_2020_without_solution.pdf
creditcard_insurance/custmaster~
creditcard_insurance/middlegrade.java
```

### DataSet:
All extracted scripts, datasets and other files will be in the given below location.

```
[hduser@localhost projects]$ ls -l
total 5072
drwxr-xr-x. 2 hduser hduser    4096 Jul  8 10:00 creditcard_insurance
-rwxrw-rw-. 1 hduser hduser 5186680 Sep 20  2020 creditcard_insurance.tar.gz

[hduser@localhost projects]$ cd creditcard_insurance/

[hduser@localhost creditcard_insurance]$ ls -l
total 17328
-rw-r--r--. 1 hduser hduser     866 May 28  2018 1_1_creditdataload.sql
-rw-r--r--. 1 hduser hduser     867 May 20  2018 1_dbscript
-rw-r--r--. 1 hduser hduser  686476 May 13  2018 2_2_creditcard_defaulters_pst
-rw-r--r--. 1 hduser hduser  876362 May 13  2018 2_creditcard_defaulters_cst
-rw-r--r--. 1 hduser hduser     480 May 28  2018 3_1_hiveinsuranceload.hql
-rw-r--r--. 1 hduser hduser     378 May 28  2018 3_2_hivestatefixedwidthload.hql
-rw-r--r--. 1 hduser hduser   18388 May 28  2018 3_Hive Realtime interview questions.docx
-rw-r--r--. 1 hduser hduser    1801 May 28  2018 batch_flow.sh
-rw-r--r--. 1 hduser hduser  853917 May 20  2018 creditcard_defaulters_cst.txt
-rw-r--r--. 1 hduser hduser  668919 May 20  2018 creditcard_defaulters_pst.txt
-rw-r--r--. 1 hduser hduser     865 May 28  2018 creditdataload.sql
-rw-r--r--. 1 hduser hduser  740227 May 29  2018 custmaster
-rw-rw-r--. 1 hduser hduser   16724 Sep 19  2020 custmaster.java
-rw-r--r--. 1 hduser hduser  740279 May 20  2018 custmaster.txt
-rw-r--r--. 1 hduser hduser  627658 May 20  2018 custmaster.xlsx
-rw-r--r--. 1 hduser hduser  391355 Jun 18  2015 custs.csv
-rw-r--r--. 1 hduser hduser 1432838 May 20  2018 default of credit card clients.txt
-rw-r--r--. 1 hduser hduser 8005120 May 20  2018 default of credit card clients.xls
-rw-rw-r--. 1 hduser hduser       0 Sep 19  2020 _HBASE_SUCCESS
-rw-r--r--. 1 hduser hduser    1874 Oct 15  2020 hivepart.sh
-rw-r--r--. 1 hduser hduser 1042475 Oct 16  2020 Inceptez_Banking_insurance_project2_2020_without_solution.pdf
-rw-r--r--. 1 hduser hduser 1060398 Oct 16  2020 Inceptez_Banking_insurance_project2_2020_with_solution.pdf
-rw-r--r--. 1 hduser hduser  414551 Oct  9  2020 insuranceinfo.csv
-rw-r--r--. 1 hduser hduser     169 Sep 18  2020 mask_insure.command
-rw-r--r--. 1 hduser hduser     468 Sep 18  2020 mask_insure.py
-rw-rw-r--. 1 hduser hduser   20624 Sep 19  2020 middlegrade.java
-rw-r--r--. 1 hduser hduser   11706 Jun  3  2018 project flow.txt
-rw-r--r--. 1 hduser hduser    2525 Sep 16  2020 sfm_insuredata.sh
-rw-r--r--. 1 hduser hduser     646 May 28  2018 states.csv
-rw-r--r--. 1 hduser hduser    1173 May 28  2018 states_fixedwidth
-rw-rw-r--. 1 hduser hduser   50961 Sep 20  2020 zookeeper.out

```

### Data preparation in the source systems:

create database if not exists customersdb;

use customersdb;

drop table if exists credits_pst;

drop table if exists credits_cst;

drop table if exists custmaster;

create table if not exists credits_pst (id integer, lmt integer, sex integer, edu integer, marital integer,
age integer, pay integer, billamt integer, defaulter integer, issuerid1 integer, issuerid2 integer, tz varchar(3));

create table if not exists credits_cst (id integer, lmt integer, sex integer, edu integer, marital integer,
age integer, pay integer, billamt integer, defaulter integer, issuerid1 integer, issuerid2 integer, tz varchar(3));

create table if not exists custmaster (id integer, fname varchar(100), lname varchar(100),
ageval integer, profession varchar(100));

source /home/hduser/projects/creditcard_insurance/2_2_creditcard_defaulters_pst

source /home/hduser/projects/creditcard_insurance/2_creditcard_defaulters_cst

source /home/hduser/projects/creditcard_insurance/custmaster

## Project Execution

### Data Ingestion using Sqoop and Hive

Merge the 2 dataset using Hive and split the defaulters and non-defaulters into 2 data sets and load into hdfs.

1. Create a database in hive as insure.

create database if not exists insure;

``` 
hive> create database if not exists insure;
OK
Time taken: 1.872 seconds
```

2. Sqoop import the data into hive table insure.credits_pst and insure.credits_cst with options
such as hive overwrite, number of mappers 2, where hive table created by sqoop with id as bigint, 
billamt as float (this is do able with –map-column-hive option)

sqoop import \
    --driver com.mysql.cj.jdbc.Driver \
    --connect jdbc:mysql://localhost/customersdb \
    --username root \
    --password Root123$ \
    --table credits_pst \
    -m 1 \
    --target-dir /user/hduser/projects/creditcard_insurance/credits_pst/ \
    --delete-target-dir \
    --split-by id \
    --hive-import \
    --hive-table insure.credits_pst \
    --hive-overwrite \
    --map-column-hive id=bigint,billamt=float;

hadoop fs -ls /user/hduser/projects/

USE insure;
SHOW TABLES;
DESCRIBE credits_pst;

``` 
[hduser@localhost ~]$ sqoop import \
>     --driver com.mysql.cj.jdbc.Driver \
>     --connect jdbc:mysql://localhost/customersdb \
>     --username root \
>     --password Root123$ \
>     --table credits_pst \
>     -m 1 \
>     --target-dir /user/hduser/projects/creditcard_insurance/credits_pst/ \
>     --delete-target-dir \
>     --split-by id \
>     --hive-import \
>     --hive-table insure.credits_pst \
>     --hive-overwrite \
>     --map-column-hive id=bigint,billamt=float;
Warning: /usr/local/hbase does not exist! HBase imports will fail.
Please set $HBASE_HOME to the root of your HBase installation.
Warning: /usr/local/sqoop/../hcatalog does not exist! HCatalog jobs will fail.
Please set $HCAT_HOME to the root of your HCatalog installation.
Warning: /usr/local/sqoop/../accumulo does not exist! Accumulo imports will fail.
Please set $ACCUMULO_HOME to the root of your Accumulo installation.
Warning: /usr/local/sqoop/../zookeeper does not exist! Accumulo imports will fail.
Please set $ZOOKEEPER_HOME to the root of your Zookeeper installation.
22/07/08 15:45:44 INFO sqoop.Sqoop: Running Sqoop version: 1.4.6
22/07/08 15:45:44 WARN tool.BaseSqoopTool: Setting your password on the command-line is insecure. Consider using -P instead.
22/07/08 15:45:44 INFO tool.BaseSqoopTool: Using Hive-specific delimiters for output. You can override
22/07/08 15:45:44 INFO tool.BaseSqoopTool: delimiters with --fields-terminated-by, etc.
22/07/08 15:45:44 WARN sqoop.ConnFactory: Parameter --driver is set to an explicit driver however appropriate connection manager is not being set (via --connection-manager). Sqoop is going to fall back to org.apache.sqoop.manager.GenericJdbcManager. Please specify explicitly which connection manager should be used next time.
22/07/08 15:45:44 INFO manager.SqlManager: Using default fetchSize of 1000
22/07/08 15:45:44 INFO tool.CodeGenTool: Beginning code generation
22/07/08 15:45:46 INFO manager.SqlManager: Executing SQL statement: SELECT t.* FROM credits_pst AS t WHERE 1=0
22/07/08 15:45:46 INFO manager.SqlManager: Executing SQL statement: SELECT t.* FROM credits_pst AS t WHERE 1=0
22/07/08 15:45:46 INFO orm.CompilationManager: HADOOP_MAPRED_HOME is /usr/local/hadoop
Note: /tmp/sqoop-hduser/compile/e96797cfdf69ed16b0f9cb60b686de70/credits_pst.java uses or overrides a deprecated API.
Note: Recompile with -Xlint:deprecation for details.
22/07/08 15:45:53 INFO orm.CompilationManager: Writing jar file: /tmp/sqoop-hduser/compile/e96797cfdf69ed16b0f9cb60b686de70/credits_pst.jar
22/07/08 15:45:53 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
22/07/08 15:45:55 INFO tool.ImportTool: Destination directory /user/hduser/projects/creditcard_insurance/credits_pst is not present, hence not deleting.
22/07/08 15:45:55 INFO mapreduce.ImportJobBase: Beginning import of credits_pst
22/07/08 15:45:55 INFO Configuration.deprecation: mapred.jar is deprecated. Instead, use mapreduce.job.jar
22/07/08 15:45:55 INFO manager.SqlManager: Executing SQL statement: SELECT t.* FROM credits_pst AS t WHERE 1=0
22/07/08 15:45:55 INFO Configuration.deprecation: mapred.map.tasks is deprecated. Instead, use mapreduce.job.maps
22/07/08 15:45:56 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
22/07/08 15:46:03 INFO db.DBInputFormat: Using read commited transaction isolation
22/07/08 15:46:04 INFO mapreduce.JobSubmitter: number of splits:1
22/07/08 15:46:04 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1657271823979_0001
22/07/08 15:46:06 INFO impl.YarnClientImpl: Submitted application application_1657271823979_0001
22/07/08 15:46:07 INFO mapreduce.Job: The url to track the job: http://Inceptez:8088/proxy/application_1657271823979_0001/
22/07/08 15:46:07 INFO mapreduce.Job: Running job: job_1657271823979_0001
22/07/08 15:46:33 INFO mapreduce.Job: Job job_1657271823979_0001 running in uber mode : false
22/07/08 15:46:33 INFO mapreduce.Job:  map 0% reduce 0%
22/07/08 15:46:47 INFO mapreduce.Job:  map 100% reduce 0%
22/07/08 15:46:47 INFO mapreduce.Job: Job job_1657271823979_0001 completed successfully
22/07/08 15:46:47 INFO mapreduce.Job: Counters: 30
	File System Counters
		FILE: Number of bytes read=0
		FILE: Number of bytes written=133911
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=87
		HDFS: Number of bytes written=203687
		HDFS: Number of read operations=4
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=2
	Job Counters 
		Launched map tasks=1
		Other local map tasks=1
		Total time spent by all maps in occupied slots (ms)=11132
		Total time spent by all reduces in occupied slots (ms)=0
		Total time spent by all map tasks (ms)=11132
		Total vcore-seconds taken by all map tasks=11132
		Total megabyte-seconds taken by all map tasks=11399168
	Map-Reduce Framework
		Map input records=4389
		Map output records=4389
		Input split bytes=87
		Spilled Records=0
		Failed Shuffles=0
		Merged Map outputs=0
		GC time elapsed (ms)=1083
		CPU time spent (ms)=4880
		Physical memory (bytes) snapshot=179142656
		Virtual memory (bytes) snapshot=2125553664
		Total committed heap usage (bytes)=129499136
	File Input Format Counters 
		Bytes Read=0
	File Output Format Counters 
		Bytes Written=203687
22/07/08 15:46:48 INFO mapreduce.ImportJobBase: Transferred 198.9131 KB in 52.0621 seconds (3.8207 KB/sec)
22/07/08 15:46:48 INFO mapreduce.ImportJobBase: Retrieved 4389 records.
22/07/08 15:46:48 INFO manager.SqlManager: Executing SQL statement: SELECT t.* FROM credits_pst AS t WHERE 1=0
22/07/08 15:46:48 INFO manager.SqlManager: Executing SQL statement: SELECT t.* FROM credits_pst AS t WHERE 1=0
22/07/08 15:46:48 INFO hive.HiveImport: Loading uploaded data into Hive
22/07/08 15:46:51 INFO hive.HiveImport: which: no hbase in (/usr/local/bin:/usr/local/sbin:/usr/bin:/usr/sbin:/bin:/sbin:/usr/lib/jvm/java-1.8.0-openjdk:/usr/lib/jvm/java-1.8.0-openjdk/jre//bin:/usr/local/hadoop/bin:/usr/local/hadoop/sbin:/usr/local/sqoop/bin:/usr/local/hive/bin:/usr/local/hbase/bin:/usr/local/zookeeper/bin:/usr/local/phoenix/bin:/usr/local/kafka/bin:/usr/local/nifi/bin:/usr/local/spark/bin:/usr/local/spark/sbin:/tmp/inceptez/bin/:/usr/local/oozie/bin:/tmp/bin:/usr/local/oozie/bin:/usr/local/pycharm/bin/:/home/hduser/.local/bin:/home/hduser/bin:/usr/lib/jvm/java-1.8.0-openjdk:/usr/lib/jvm/java-1.8.0-openjdk/jre//bin:/usr/local/hadoop/bin:/usr/local/hadoop/sbin:/usr/local/sqoop/bin:/usr/local/hive/bin:/usr/local/hbase/bin:/usr/local/zookeeper/bin:/usr/local/phoenix/bin:/usr/local/kafka/bin:/usr/local/nifi/bin:/usr/local/spark/bin:/usr/local/spark/sbin:/tmp/inceptez/bin/:/usr/local/oozie/bin:/tmp/bin:/usr/local/oozie/bin:/usr/local/pycharm/bin/)
22/07/08 15:46:57 INFO hive.HiveImport: SLF4J: Class path contains multiple SLF4J bindings.
22/07/08 15:46:57 INFO hive.HiveImport: SLF4J: Found binding in [jar:file:/usr/local/hive/lib/log4j-slf4j-impl-2.6.2.jar!/org/slf4j/impl/StaticLoggerBinder.class]
22/07/08 15:46:57 INFO hive.HiveImport: SLF4J: Found binding in [jar:file:/usr/local/hadoop/share/hadoop/common/lib/slf4j-log4j12-1.7.10.jar!/org/slf4j/impl/StaticLoggerBinder.class]
22/07/08 15:46:57 INFO hive.HiveImport: SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
22/07/08 15:46:57 INFO hive.HiveImport: SLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]
22/07/08 15:47:00 INFO hive.HiveImport: 
22/07/08 15:47:00 INFO hive.HiveImport: Logging initialized using configuration in jar:file:/usr/local/hive/lib/hive-common-2.3.9.jar!/hive-log4j2.properties Async: true
22/07/08 15:47:08 INFO hive.HiveImport: OK
22/07/08 15:47:08 INFO hive.HiveImport: Time taken: 5.919 seconds
22/07/08 15:47:09 INFO hive.HiveImport: Loading data to table insure.credits_pst
22/07/08 15:47:11 INFO hive.HiveImport: OK
22/07/08 15:47:11 INFO hive.HiveImport: Time taken: 2.613 seconds
22/07/08 15:47:11 INFO hive.HiveImport: Hive import complete.

[hduser@localhost ~]$ hadoop fs -ls /user/hduser/projects/
Found 1 items
drwxr-xr-x   - hduser hadoop          0 2022-07-08 15:47 /user/hduser/projects/creditcard_insurance

hive> USE insure;
OK
Time taken: 0.087 seconds

hive> SHOW TABLES;
OK
credits_pst
Time taken: 0.167 seconds, Fetched: 1 row(s)

hive> DESCRIBE credits_pst;
OK
id                  	bigint              	                    
lmt                 	int                 	                    
sex                 	int                 	                    
edu                 	int                 	                    
marital             	int                 	                    
age                 	int                 	                    
pay                 	int                 	                    
billamt             	float               	                    
defaulter           	int                 	                    
issuerid1           	int                 	                    
issuerid2           	int                 	                    
tz                  	string              	                    
Time taken: 0.327 seconds, Fetched: 12 row(s)

```

sqoop import \
    --driver com.mysql.cj.jdbc.Driver \
    --connect jdbc:mysql://localhost/customersdb \
    --username root \
    --password Root123$ \
    --table credits_cst \
    -m 1 \
    --target-dir /user/hduser/projects/creditcard_insurance/credits_cst/ \
    --delete-target-dir \
    --split-by id \
    --hive-import \
    --hive-table insure.credits_cst \
    --hive-overwrite \
    --map-column-hive id=bigint,billamt=float;

hadoop fs -ls /user/hduser/projects/

USE insure;
SHOW TABLES;
DESCRIBE credits_cst;

``` 
[hduser@localhost ~]$ sqoop import \
>     --driver com.mysql.cj.jdbc.Driver \
>     --connect jdbc:mysql://localhost/customersdb \
>     --username root \
>     --password Root123$ \
>     --table credits_cst \
>     -m 1 \
>     --target-dir /user/hduser/projects/creditcard_insurance/credits_cst/ \
>     --delete-target-dir \
>     --split-by id \
>     --hive-import \
>     --hive-table insure.credits_cst \
>     --hive-overwrite \
>     --map-column-hive id=bigint,billamt=float;
Warning: /usr/local/hbase does not exist! HBase imports will fail.
Please set $HBASE_HOME to the root of your HBase installation.
Warning: /usr/local/sqoop/../hcatalog does not exist! HCatalog jobs will fail.
Please set $HCAT_HOME to the root of your HCatalog installation.
Warning: /usr/local/sqoop/../accumulo does not exist! Accumulo imports will fail.
Please set $ACCUMULO_HOME to the root of your Accumulo installation.
Warning: /usr/local/sqoop/../zookeeper does not exist! Accumulo imports will fail.
Please set $ZOOKEEPER_HOME to the root of your Zookeeper installation.
22/07/08 15:53:58 INFO sqoop.Sqoop: Running Sqoop version: 1.4.6
22/07/08 15:53:58 WARN tool.BaseSqoopTool: Setting your password on the command-line is insecure. Consider using -P instead.
22/07/08 15:53:58 INFO tool.BaseSqoopTool: Using Hive-specific delimiters for output. You can override
22/07/08 15:53:58 INFO tool.BaseSqoopTool: delimiters with --fields-terminated-by, etc.
22/07/08 15:53:59 WARN sqoop.ConnFactory: Parameter --driver is set to an explicit driver however appropriate connection manager is not being set (via --connection-manager). Sqoop is going to fall back to org.apache.sqoop.manager.GenericJdbcManager. Please specify explicitly which connection manager should be used next time.
22/07/08 15:53:59 INFO manager.SqlManager: Using default fetchSize of 1000
22/07/08 15:53:59 INFO tool.CodeGenTool: Beginning code generation
22/07/08 15:54:00 INFO manager.SqlManager: Executing SQL statement: SELECT t.* FROM credits_cst AS t WHERE 1=0
22/07/08 15:54:00 INFO manager.SqlManager: Executing SQL statement: SELECT t.* FROM credits_cst AS t WHERE 1=0
22/07/08 15:54:00 INFO orm.CompilationManager: HADOOP_MAPRED_HOME is /usr/local/hadoop
Note: /tmp/sqoop-hduser/compile/39f2de0c9e7a94f471c773bc792e3a2a/credits_cst.java uses or overrides a deprecated API.
Note: Recompile with -Xlint:deprecation for details.
22/07/08 15:54:03 INFO orm.CompilationManager: Writing jar file: /tmp/sqoop-hduser/compile/39f2de0c9e7a94f471c773bc792e3a2a/credits_cst.jar
22/07/08 15:54:04 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
22/07/08 15:54:05 INFO tool.ImportTool: Destination directory /user/hduser/projects/creditcard_insurance/credits_cst is not present, hence not deleting.
22/07/08 15:54:05 INFO mapreduce.ImportJobBase: Beginning import of credits_cst
22/07/08 15:54:05 INFO Configuration.deprecation: mapred.jar is deprecated. Instead, use mapreduce.job.jar
22/07/08 15:54:05 INFO manager.SqlManager: Executing SQL statement: SELECT t.* FROM credits_cst AS t WHERE 1=0
22/07/08 15:54:05 INFO Configuration.deprecation: mapred.map.tasks is deprecated. Instead, use mapreduce.job.maps
22/07/08 15:54:05 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
22/07/08 15:54:09 INFO db.DBInputFormat: Using read commited transaction isolation
22/07/08 15:54:10 INFO mapreduce.JobSubmitter: number of splits:1
22/07/08 15:54:11 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1657271823979_0002
22/07/08 15:54:12 INFO impl.YarnClientImpl: Submitted application application_1657271823979_0002
22/07/08 15:54:12 INFO mapreduce.Job: The url to track the job: http://Inceptez:8088/proxy/application_1657271823979_0002/
22/07/08 15:54:12 INFO mapreduce.Job: Running job: job_1657271823979_0002
22/07/08 15:54:24 INFO mapreduce.Job: Job job_1657271823979_0002 running in uber mode : false
22/07/08 15:54:24 INFO mapreduce.Job:  map 0% reduce 0%
22/07/08 15:54:35 INFO mapreduce.Job:  map 100% reduce 0%
22/07/08 15:54:35 INFO mapreduce.Job: Job job_1657271823979_0002 completed successfully
22/07/08 15:54:36 INFO mapreduce.Job: Counters: 30
	File System Counters
		FILE: Number of bytes read=0
		FILE: Number of bytes written=133911
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=87
		HDFS: Number of bytes written=259153
		HDFS: Number of read operations=4
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=2
	Job Counters 
		Launched map tasks=1
		Other local map tasks=1
		Total time spent by all maps in occupied slots (ms)=8595
		Total time spent by all reduces in occupied slots (ms)=0
		Total time spent by all map tasks (ms)=8595
		Total vcore-seconds taken by all map tasks=8595
		Total megabyte-seconds taken by all map tasks=8801280
	Map-Reduce Framework
		Map input records=5611
		Map output records=5611
		Input split bytes=87
		Spilled Records=0
		Failed Shuffles=0
		Merged Map outputs=0
		GC time elapsed (ms)=759
		CPU time spent (ms)=5030
		Physical memory (bytes) snapshot=179671040
		Virtual memory (bytes) snapshot=2125893632
		Total committed heap usage (bytes)=143654912
	File Input Format Counters 
		Bytes Read=0
	File Output Format Counters 
		Bytes Written=259153
22/07/08 15:54:36 INFO mapreduce.ImportJobBase: Transferred 253.0791 KB in 30.5572 seconds (8.2822 KB/sec)
22/07/08 15:54:36 INFO mapreduce.ImportJobBase: Retrieved 5611 records.
22/07/08 15:54:36 INFO manager.SqlManager: Executing SQL statement: SELECT t.* FROM credits_cst AS t WHERE 1=0
22/07/08 15:54:36 INFO manager.SqlManager: Executing SQL statement: SELECT t.* FROM credits_cst AS t WHERE 1=0
22/07/08 15:54:36 INFO hive.HiveImport: Loading uploaded data into Hive
22/07/08 15:54:39 INFO hive.HiveImport: which: no hbase in (/usr/local/bin:/usr/local/sbin:/usr/bin:/usr/sbin:/bin:/sbin:/usr/lib/jvm/java-1.8.0-openjdk:/usr/lib/jvm/java-1.8.0-openjdk/jre//bin:/usr/local/hadoop/bin:/usr/local/hadoop/sbin:/usr/local/sqoop/bin:/usr/local/hive/bin:/usr/local/hbase/bin:/usr/local/zookeeper/bin:/usr/local/phoenix/bin:/usr/local/kafka/bin:/usr/local/nifi/bin:/usr/local/spark/bin:/usr/local/spark/sbin:/tmp/inceptez/bin/:/usr/local/oozie/bin:/tmp/bin:/usr/local/oozie/bin:/usr/local/pycharm/bin/:/home/hduser/.local/bin:/home/hduser/bin:/usr/lib/jvm/java-1.8.0-openjdk:/usr/lib/jvm/java-1.8.0-openjdk/jre//bin:/usr/local/hadoop/bin:/usr/local/hadoop/sbin:/usr/local/sqoop/bin:/usr/local/hive/bin:/usr/local/hbase/bin:/usr/local/zookeeper/bin:/usr/local/phoenix/bin:/usr/local/kafka/bin:/usr/local/nifi/bin:/usr/local/spark/bin:/usr/local/spark/sbin:/tmp/inceptez/bin/:/usr/local/oozie/bin:/tmp/bin:/usr/local/oozie/bin:/usr/local/pycharm/bin/)
22/07/08 15:54:45 INFO hive.HiveImport: SLF4J: Class path contains multiple SLF4J bindings.
22/07/08 15:54:45 INFO hive.HiveImport: SLF4J: Found binding in [jar:file:/usr/local/hive/lib/log4j-slf4j-impl-2.6.2.jar!/org/slf4j/impl/StaticLoggerBinder.class]
22/07/08 15:54:45 INFO hive.HiveImport: SLF4J: Found binding in [jar:file:/usr/local/hadoop/share/hadoop/common/lib/slf4j-log4j12-1.7.10.jar!/org/slf4j/impl/StaticLoggerBinder.class]
22/07/08 15:54:45 INFO hive.HiveImport: SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
22/07/08 15:54:45 INFO hive.HiveImport: SLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]
22/07/08 15:54:48 INFO hive.HiveImport: 
22/07/08 15:54:48 INFO hive.HiveImport: Logging initialized using configuration in jar:file:/usr/local/hive/lib/hive-common-2.3.9.jar!/hive-log4j2.properties Async: true
22/07/08 15:54:56 INFO hive.HiveImport: OK
22/07/08 15:54:56 INFO hive.HiveImport: Time taken: 5.516 seconds
22/07/08 15:54:56 INFO hive.HiveImport: Loading data to table insure.credits_cst
22/07/08 15:54:58 INFO hive.HiveImport: OK
22/07/08 15:54:58 INFO hive.HiveImport: Time taken: 2.256 seconds
22/07/08 15:54:58 INFO hive.HiveImport: Hive import complete.

[hduser@localhost ~]$ hadoop fs -ls /user/hduser/projects/
Found 1 items

drwxr-xr-x   - hduser hadoop          0 2022-07-08 15:54 /user/hduser/projects/creditcard_insurance

hive> SHOW TABLES;
OK
credits_cst
credits_pst
Time taken: 0.105 seconds, Fetched: 2 row(s)

hive> DESCRIBE credits_cst;
OK
id                  	bigint              	                    
lmt                 	int                 	                    
sex                 	int                 	                    
edu                 	int                 	                    
marital             	int                 	                    
age                 	int                 	                    
pay                 	int                 	                    
billamt             	float               	                    
defaulter           	int                 	                    
issuerid1           	int                 	                    
issuerid2           	int                 	                    
tz                  	string              	                    
Time taken: 0.177 seconds, Fetched: 12 row(s)

```

