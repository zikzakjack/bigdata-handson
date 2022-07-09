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

### ETL / ELT using Hive

3. Filter the cst and pst tables with the billamt > 0 and union all both dataset and load into a
single table called insure. using create table as select (CTAS)

CREATE TABLE insure.credits_all AS
SELECT * FROM insure.credits_pst WHERE billamt > 0
UNION ALL
SELECT * FROM insure.credits_cst WHERE billamt>0;

SHOW TABLES;

DESCRIBE credits_all;

SELECT COUNT(*) FROM credits_all;

``` 
hive> CREATE TABLE insure.credits_all AS
    > SELECT * FROM insure.credits_pst WHERE billamt > 0
    > UNION ALL
    > SELECT * FROM insure.credits_cst WHERE billamt>0;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = hduser_20220708161825_d76d0eb4-658c-44a2-b436-08134f242666
Total jobs = 3
Launching Job 1 out of 3
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1657271823979_0003, Tracking URL = http://Inceptez:8088/proxy/application_1657271823979_0003/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1657271823979_0003
Hadoop job information for Stage-1: number of mappers: 2; number of reducers: 0
2022-07-08 16:19:40,868 Stage-1 map = 0%,  reduce = 0%
2022-07-08 16:20:07,808 Stage-1 map = 50%,  reduce = 0%, Cumulative CPU 23.4 sec
2022-07-08 16:20:08,883 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 26.69 sec
MapReduce Total cumulative CPU time: 26 seconds 690 msec
Ended Job = job_1657271823979_0003
Stage-4 is filtered out by condition resolver.
Stage-3 is selected by condition resolver.
Stage-5 is filtered out by condition resolver.
Launching Job 3 out of 3
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1657271823979_0004, Tracking URL = http://Inceptez:8088/proxy/application_1657271823979_0004/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1657271823979_0004
Hadoop job information for Stage-3: number of mappers: 1; number of reducers: 0
2022-07-08 16:20:56,720 Stage-3 map = 0%,  reduce = 0%
2022-07-08 16:21:09,841 Stage-3 map = 100%,  reduce = 0%, Cumulative CPU 6.39 sec
MapReduce Total cumulative CPU time: 6 seconds 390 msec
Ended Job = job_1657271823979_0004
Moving data to directory hdfs://localhost:54310/user/hive/warehouse/insure.db/credits_all
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 2   Cumulative CPU: 26.69 sec   HDFS Read: 478498 HDFS Write: 441423 SUCCESS
Stage-Stage-3: Map: 1   Cumulative CPU: 6.39 sec   HDFS Read: 443888 HDFS Write: 441263 SUCCESS
Total MapReduce CPU Time Spent: 33 seconds 80 msec
OK
Time taken: 168.963 seconds

hive> SHOW TABLES;
OK
credits_all
credits_cst
credits_pst
Time taken: 0.089 seconds, Fetched: 3 row(s)

hive> DESCRIBE credits_all;
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
Time taken: 0.176 seconds, Fetched: 12 row(s)

hive> SELECT COUNT(*) FROM credits_all;
OK
9087
Time taken: 0.468 seconds, Fetched: 1 row(s)

```

4. Note: We can merge the step 2 and 3 and make the above 2 sqoop statements as 1 sqoop
statement to directly import data into the insure.credits_all by writing –query with billamt > 0
filter and union of both credits_pst and credits_cst at the MYSQL level itself finally convert the
insure.cstpstreorder into external table. If you have some time try it out..

SELECT * FROM credits_pst WHERE billamt > 0
UNION ALL
SELECT * FROM credits_cst WHERE billamt > 0


sqoop import \
    --driver com.mysql.cj.jdbc.Driver \
    --connect jdbc:mysql://localhost/customersdb \
    --username root \
    --password Root123$ \
    --query     'WITH credits_all AS (SELECT * FROM credits_pst WHERE billamt > 0 UNION ALL SELECT * FROM credits_cst WHERE billamt>0) SELECT * FROM credits_all WHERE $CONDITIONS' \
    -m 1 \
    --target-dir /user/hduser/projects/creditcard_insurance/credits_all_temp/ \
    --delete-target-dir \
    --split-by id \
    --hive-import \
    --hive-table insure.credits_all_temp \
    --hive-overwrite \
    --map-column-hive id=bigint,billamt=float;

SHOW TABLES;

DESCRIBE credits_all_temp;

SELECT COUNT(*) FROM credits_all_temp;

``` 
[hduser@localhost ~]$ sqoop import \
>     --driver com.mysql.cj.jdbc.Driver \
>     --connect jdbc:mysql://localhost/customersdb \
>     --username root \
>     --password Root123$ \
>     --query     'WITH credits_all AS (SELECT * FROM credits_pst WHERE billamt > 0 UNION ALL SELECT * FROM credits_cst WHERE billamt>0) SELECT * FROM credits_all WHERE $CONDITIONS' \
>     -m 1 \
>     --target-dir /user/hduser/projects/creditcard_insurance/credits_all_temp/ \
>     --delete-target-dir \
>     --split-by id \
>     --hive-import \
>     --hive-table insure.credits_all_temp \
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
22/07/08 16:42:11 INFO sqoop.Sqoop: Running Sqoop version: 1.4.6
22/07/08 16:42:11 WARN tool.BaseSqoopTool: Setting your password on the command-line is insecure. Consider using -P instead.
22/07/08 16:42:11 INFO tool.BaseSqoopTool: Using Hive-specific delimiters for output. You can override
22/07/08 16:42:11 INFO tool.BaseSqoopTool: delimiters with --fields-terminated-by, etc.
22/07/08 16:42:11 WARN sqoop.ConnFactory: Parameter --driver is set to an explicit driver however appropriate connection manager is not being set (via --connection-manager). Sqoop is going to fall back to org.apache.sqoop.manager.GenericJdbcManager. Please specify explicitly which connection manager should be used next time.
22/07/08 16:42:11 INFO manager.SqlManager: Using default fetchSize of 1000
22/07/08 16:42:11 INFO tool.CodeGenTool: Beginning code generation
22/07/08 16:42:12 INFO manager.SqlManager: Executing SQL statement: WITH credits_all AS (SELECT * FROM credits_pst WHERE billamt > 0 UNION ALL SELECT * FROM credits_cst WHERE billamt>0) SELECT * FROM credits_all WHERE  (1 = 0) 
22/07/08 16:42:12 INFO manager.SqlManager: Executing SQL statement: WITH credits_all AS (SELECT * FROM credits_pst WHERE billamt > 0 UNION ALL SELECT * FROM credits_cst WHERE billamt>0) SELECT * FROM credits_all WHERE  (1 = 0) 
22/07/08 16:42:12 INFO orm.CompilationManager: HADOOP_MAPRED_HOME is /usr/local/hadoop
Note: /tmp/sqoop-hduser/compile/f43466a7c65a4d47e89b49afc6c21643/QueryResult.java uses or overrides a deprecated API.
Note: Recompile with -Xlint:deprecation for details.
22/07/08 16:42:15 INFO orm.CompilationManager: Writing jar file: /tmp/sqoop-hduser/compile/f43466a7c65a4d47e89b49afc6c21643/QueryResult.jar
22/07/08 16:42:16 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
22/07/08 16:42:17 INFO tool.ImportTool: Destination directory /user/hduser/projects/creditcard_insurance/credits_all_temp is not present, hence not deleting.
22/07/08 16:42:17 INFO mapreduce.ImportJobBase: Beginning query import.
22/07/08 16:42:17 INFO Configuration.deprecation: mapred.jar is deprecated. Instead, use mapreduce.job.jar
22/07/08 16:42:17 INFO Configuration.deprecation: mapred.map.tasks is deprecated. Instead, use mapreduce.job.maps
22/07/08 16:42:18 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
22/07/08 16:42:22 INFO db.DBInputFormat: Using read commited transaction isolation
22/07/08 16:42:22 INFO mapreduce.JobSubmitter: number of splits:1
22/07/08 16:42:22 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1657271823979_0005
22/07/08 16:42:23 INFO impl.YarnClientImpl: Submitted application application_1657271823979_0005
22/07/08 16:42:23 INFO mapreduce.Job: The url to track the job: http://Inceptez:8088/proxy/application_1657271823979_0005/
22/07/08 16:42:23 INFO mapreduce.Job: Running job: job_1657271823979_0005
22/07/08 16:42:35 INFO mapreduce.Job: Job job_1657271823979_0005 running in uber mode : false
22/07/08 16:42:35 INFO mapreduce.Job:  map 0% reduce 0%
22/07/08 16:42:47 INFO mapreduce.Job:  map 100% reduce 0%
22/07/08 16:42:48 INFO mapreduce.Job: Job job_1657271823979_0005 completed successfully
22/07/08 16:42:48 INFO mapreduce.Job: Counters: 30
	File System Counters
		FILE: Number of bytes read=0
		FILE: Number of bytes written=133858
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=87
		HDFS: Number of bytes written=423089
		HDFS: Number of read operations=4
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=2
	Job Counters 
		Launched map tasks=1
		Other local map tasks=1
		Total time spent by all maps in occupied slots (ms)=9155
		Total time spent by all reduces in occupied slots (ms)=0
		Total time spent by all map tasks (ms)=9155
		Total vcore-seconds taken by all map tasks=9155
		Total megabyte-seconds taken by all map tasks=9374720
	Map-Reduce Framework
		Map input records=9087
		Map output records=9087
		Input split bytes=87
		Spilled Records=0
		Failed Shuffles=0
		Merged Map outputs=0
		GC time elapsed (ms)=967
		CPU time spent (ms)=4840
		Physical memory (bytes) snapshot=182198272
		Virtual memory (bytes) snapshot=2124414976
		Total committed heap usage (bytes)=136314880
	File Input Format Counters 
		Bytes Read=0
	File Output Format Counters 
		Bytes Written=423089
22/07/08 16:42:48 INFO mapreduce.ImportJobBase: Transferred 413.1729 KB in 30.5185 seconds (13.5384 KB/sec)
22/07/08 16:42:48 INFO mapreduce.ImportJobBase: Retrieved 9087 records.
22/07/08 16:42:48 INFO manager.SqlManager: Executing SQL statement: WITH credits_all AS (SELECT * FROM credits_pst WHERE billamt > 0 UNION ALL SELECT * FROM credits_cst WHERE billamt>0) SELECT * FROM credits_all WHERE  (1 = 0) 
22/07/08 16:42:48 INFO manager.SqlManager: Executing SQL statement: WITH credits_all AS (SELECT * FROM credits_pst WHERE billamt > 0 UNION ALL SELECT * FROM credits_cst WHERE billamt>0) SELECT * FROM credits_all WHERE  (1 = 0) 
22/07/08 16:42:48 INFO hive.HiveImport: Loading uploaded data into Hive
22/07/08 16:42:51 INFO hive.HiveImport: which: no hbase in (/usr/local/bin:/usr/local/sbin:/usr/bin:/usr/sbin:/bin:/sbin:/usr/lib/jvm/java-1.8.0-openjdk:/usr/lib/jvm/java-1.8.0-openjdk/jre//bin:/usr/local/hadoop/bin:/usr/local/hadoop/sbin:/usr/local/sqoop/bin:/usr/local/hive/bin:/usr/local/hbase/bin:/usr/local/zookeeper/bin:/usr/local/phoenix/bin:/usr/local/kafka/bin:/usr/local/nifi/bin:/usr/local/spark/bin:/usr/local/spark/sbin:/tmp/inceptez/bin/:/usr/local/oozie/bin:/tmp/bin:/usr/local/oozie/bin:/usr/local/pycharm/bin/:/home/hduser/.local/bin:/home/hduser/bin:/usr/lib/jvm/java-1.8.0-openjdk:/usr/lib/jvm/java-1.8.0-openjdk/jre//bin:/usr/local/hadoop/bin:/usr/local/hadoop/sbin:/usr/local/sqoop/bin:/usr/local/hive/bin:/usr/local/hbase/bin:/usr/local/zookeeper/bin:/usr/local/phoenix/bin:/usr/local/kafka/bin:/usr/local/nifi/bin:/usr/local/spark/bin:/usr/local/spark/sbin:/tmp/inceptez/bin/:/usr/local/oozie/bin:/tmp/bin:/usr/local/oozie/bin:/usr/local/pycharm/bin/)
22/07/08 16:42:58 INFO hive.HiveImport: SLF4J: Class path contains multiple SLF4J bindings.
22/07/08 16:42:58 INFO hive.HiveImport: SLF4J: Found binding in [jar:file:/usr/local/hive/lib/log4j-slf4j-impl-2.6.2.jar!/org/slf4j/impl/StaticLoggerBinder.class]
22/07/08 16:42:58 INFO hive.HiveImport: SLF4J: Found binding in [jar:file:/usr/local/hadoop/share/hadoop/common/lib/slf4j-log4j12-1.7.10.jar!/org/slf4j/impl/StaticLoggerBinder.class]
22/07/08 16:42:58 INFO hive.HiveImport: SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
22/07/08 16:42:58 INFO hive.HiveImport: SLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]
22/07/08 16:43:01 INFO hive.HiveImport: 
22/07/08 16:43:01 INFO hive.HiveImport: Logging initialized using configuration in jar:file:/usr/local/hive/lib/hive-common-2.3.9.jar!/hive-log4j2.properties Async: true
22/07/08 16:43:10 INFO hive.HiveImport: OK
22/07/08 16:43:10 INFO hive.HiveImport: Time taken: 6.424 seconds
22/07/08 16:43:11 INFO hive.HiveImport: Loading data to table insure.credits_all_temp
22/07/08 16:43:12 INFO hive.HiveImport: OK
22/07/08 16:43:12 INFO hive.HiveImport: Time taken: 2.16 seconds
22/07/08 16:43:12 INFO hive.HiveImport: Hive import complete.

hive> SHOW TABLES;
OK
credits_all
credits_all_temp
credits_cst
credits_pst
Time taken: 0.087 seconds, Fetched: 4 row(s)

hive> DESCRIBE credits_all_temp;
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
Time taken: 0.216 seconds, Fetched: 12 row(s)

hive> SELECT COUNT(*) FROM credits_all_temp;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = hduser_20220708164418_c2786a9a-d758-4477-b8ae-0ce2fa40fd04
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1657271823979_0006, Tracking URL = http://Inceptez:8088/proxy/application_1657271823979_0006/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1657271823979_0006
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-07-08 16:45:12,461 Stage-1 map = 0%,  reduce = 0%
2022-07-08 16:45:29,690 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 5.67 sec
2022-07-08 16:45:41,624 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 11.07 sec
MapReduce Total cumulative CPU time: 11 seconds 70 msec
Ended Job = job_1657271823979_0006
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 11.07 sec   HDFS Read: 432175 HDFS Write: 104 SUCCESS
Total MapReduce CPU Time Spent: 11 seconds 70 msec
OK
9087
Time taken: 84.197 seconds, Fetched: 1 row(s)
```

5. Create another external table insure.cstpstpenality in orc file format (show create table
insure.cstpstreorder) to get the ddl of the above table and alter as per the below columns for
faster table creation and populate with the below ETL transformations applied in the above
table cstpstreorder as given below

CREATE EXTERNAL TABLE penalties (id BIGINT, issuerid1 INT, issuerid2 INT, lmt INT, newlmt INT, sex INT, edu INT, marital INT, pay INT, billamt FLOAT, newbillamt FLOAT, defaulter INT)
STORED AS ORC
LOCATION '/user/hduser/projects/creditcard_insurance/penalties';

INSERT INTO TABLE penalties SELECT id, issuerid1, issuerid2, lmt, CASE defaulter WHEN 1
THEN lmt - (lmt * 0.04) ELSE lmt END AS newlmt, sex, edu, marital, pay, billamt, CASE defaulter WHEN
1 THEN billamt + (billamt * 0.02) ELSE billamt END AS newbillamt, defaulter
FROM credits_all;

DESCRIBE penalties;

SHOW CREATE TABLE penalties;

SELECT COUNT(*) FROM penalties;

hadoop fs -ls /user/hduser/projects/creditcard_insurance/penalties

``` 
hive> CREATE EXTERNAL TABLE penalties (id BIGINT, issuerid1 INT, issuerid2 INT, lmt INT, newlmt INT, sex INT, edu INT, marital INT, pay INT, billamt FLOAT, newbillamt FLOAT, defaulter INT)
    > STORED AS ORC
    > LOCATION '/user/hduser/projects/creditcard_insurance/penalties';
OK
Time taken: 0.293 seconds

hive> INSERT INTO TABLE penalties SELECT id, issuerid1, issuerid2, lmt, CASE defaulter WHEN 1
    > THEN lmt - (lmt * 0.04) ELSE lmt END AS newlmt, sex, edu, marital, pay, billamt, CASE defaulter WHEN
    > 1 THEN billamt + (billamt * 0.02) ELSE billamt END AS newbillamt, defaulter
    > FROM credits_all;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = hduser_20220708230112_32ab97f8-c1bd-4387-911b-44ce6a6121f3
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1657271823979_0007, Tracking URL = http://Inceptez:8088/proxy/application_1657271823979_0007/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1657271823979_0007
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 0
2022-07-08 23:02:03,322 Stage-1 map = 0%,  reduce = 0%
2022-07-08 23:02:19,601 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 11.79 sec
MapReduce Total cumulative CPU time: 11 seconds 790 msec
Ended Job = job_1657271823979_0007
Stage-4 is selected by condition resolver.
Stage-3 is filtered out by condition resolver.
Stage-5 is filtered out by condition resolver.
Moving data to directory hdfs://localhost:54310/user/hduser/projects/creditcard_insurance/penalties/.hive-staging_hive_2022-07-08_23-01-12_270_6608941167670410851-1/-ext-10000
Loading data to table insure.penalties
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1   Cumulative CPU: 11.79 sec   HDFS Read: 448640 HDFS Write: 115948 SUCCESS
Total MapReduce CPU Time Spent: 11 seconds 790 msec
OK
Time taken: 71.013 seconds

hive> DESCRIBE penalties;
OK
id                  	bigint              	                    
issuerid1           	int                 	                    
issuerid2           	int                 	                    
lmt                 	int                 	                    
newlmt              	int                 	                    
sex                 	int                 	                    
edu                 	int                 	                    
marital             	int                 	                    
pay                 	int                 	                    
billamt             	float               	                    
newbillamt          	float               	                    
defaulter           	int                 	                    
Time taken: 0.18 seconds, Fetched: 12 row(s)

hive> SHOW CREATE TABLE penalties;
OK
CREATE EXTERNAL TABLE `penalties`(
  `id` bigint, 
  `issuerid1` int, 
  `issuerid2` int, 
  `lmt` int, 
  `newlmt` int, 
  `sex` int, 
  `edu` int, 
  `marital` int, 
  `pay` int, 
  `billamt` float, 
  `newbillamt` float, 
  `defaulter` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'hdfs://localhost:54310/user/hduser/projects/creditcard_insurance/penalties'
TBLPROPERTIES (
  'transient_lastDdlTime'='1657301542')
Time taken: 0.308 seconds, Fetched: 23 row(s)

hive> SELECT COUNT(*) FROM penalties;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = hduser_20220708230336_d1729edc-06b3-4384-83e1-e3a03d064484
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1657271823979_0008, Tracking URL = http://Inceptez:8088/proxy/application_1657271823979_0008/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1657271823979_0008
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-07-08 23:04:22,955 Stage-1 map = 0%,  reduce = 0%
2022-07-08 23:04:39,201 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 7.73 sec
2022-07-08 23:04:52,258 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 12.77 sec
MapReduce Total cumulative CPU time: 12 seconds 770 msec
Ended Job = job_1657271823979_0008
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 12.77 sec   HDFS Read: 25686 HDFS Write: 104 SUCCESS
Total MapReduce CPU Time Spent: 12 seconds 770 msec
OK
9087
Time taken: 77.596 seconds, Fetched: 1 row(s)

[hduser@localhost ~]$ hadoop fs -ls /user/hduser/projects/creditcard_insurance/penalties
Found 1 items
-rwxr-xr-x   1 hduser hadoop     115870 2022-07-08 23:02 /user/hduser/projects/creditcard_insurance/penalties/000000_0

```
## Data Provisioning using Hive, Sqoop and DistCp

Export and overwrite the above data into /user/hduser/projects/creditcard_insurance/defaultersout/ 
we will be using this defaultersout data in a later point of time and /user/hduser/projects/creditcard_insurance/nondefaultersout/
locations with defaulter=1 and defaulter=0 respectively using ‘,’ delimiter. We will be
sending this nondefaultersout data to external systems using Distcp/SFTP.

INSERT OVERWRITE DIRECTORY '/user/hduser/projects/creditcard_insurance/defaultersout/'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ', '
SELECT * FROM penalties
WHERE defaulter=1;

hadoop fs -ls /user/hduser/projects/creditcard_insurance/defaultersout/

hadoop fs -text /user/hduser/projects/creditcard_insurance/defaultersout/* | head

INSERT OVERWRITE DIRECTORY '/user/hduser/projects/creditcard_insurance/nondefaultersout/'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ', '
SELECT CONCAT(id, ', ', issuerid1, ', ', issuerid2, ', ', lmt, ', ', newlmt, ', ', sex, ', ', edu, ', ', marital, ',', pay, ', ', billamt, ', ', newbillamt, ', ', defaulter ) AS cnt from penalties WHERE defaulter=0
UNION
SELECT CONCAT('Trl|', count(1)) AS cnt FROM (SELECT * FROM penalties WHERE defaulter=0)  tmp;

hadoop fs -ls /user/hduser/projects/creditcard_insurance/nondefaultersout/

hadoop fs -text /user/hduser/projects/creditcard_insurance/nondefaultersout/* | head

hadoop fs -text /user/hduser/projects/creditcard_insurance/nondefaultersout/* | tail

``` 
hive> INSERT OVERWRITE DIRECTORY '/user/hduser/projects/creditcard_insurance/defaultersout/'
    > ROW FORMAT DELIMITED 
    > FIELDS TERMINATED BY ', '
    > SELECT * FROM penalties
    > WHERE defaulter=1;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = hduser_20220709122747_4f93115d-08ab-4435-b231-a450aaf31ab7
Total jobs = 3
Launching Job 1 out of 3
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1657271823979_0013, Tracking URL = http://Inceptez:8088/proxy/application_1657271823979_0013/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1657271823979_0013
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 0
2022-07-09 12:28:33,947 Stage-1 map = 0%,  reduce = 0%
2022-07-09 12:28:48,775 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 9.56 sec
MapReduce Total cumulative CPU time: 9 seconds 560 msec
Ended Job = job_1657271823979_0013
Stage-3 is selected by condition resolver.
Stage-2 is filtered out by condition resolver.
Stage-4 is filtered out by condition resolver.
Moving data to directory hdfs://localhost:54310/user/hduser/projects/creditcard_insurance/defaultersout/.hive-staging_hive_2022-07-09_12-27-47_432_173590083897411425-1/-ext-10000
Moving data to directory /user/hduser/projects/creditcard_insurance/defaultersout
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1   Cumulative CPU: 9.56 sec   HDFS Read: 137218 HDFS Write: 113870 SUCCESS
Total MapReduce CPU Time Spent: 9 seconds 560 msec
OK
Time taken: 64.698 seconds

[hduser@localhost creditcard_insurance]$ hadoop fs -ls /user/hduser/projects/creditcard_insurance/defaultersout/
Found 1 items
-rwxr-xr-x   1 hduser hadoop     113870 2022-07-09 12:28 /user/hduser/projects/creditcard_insurance/defaultersout/000000_0

[hduser@localhost creditcard_insurance]$ hadoop fs -text /user/hduser/projects/creditcard_insurance/defaultersout/* | head
1,21989,21989,20000,19200,2,2,1,2,3913.0,3991.26,1
2,38344,38344,120000,115200,2,2,2,-1,2682.0,2735.64,1
14,44580,44580,70000,67200,1,2,2,1,65802.0,67118.04,1
17,46944,46944,20000,19200,1,1,2,0,15376.0,15683.52,1
22,17100,17100,120000,115200,2,2,1,-1,316.0,322.32,1
23,17100,17100,70000,67200,2,2,2,2,41087.0,41908.74,1
24,17100,17100,450000,432000,2,1,1,-2,5512.0,5622.24,1
32,33851,33851,50000,48000,1,2,2,2,30518.0,31128.36,1
47,84251,84251,20000,19200,2,1,2,0,14028.0,14308.56,1
48,86830,86830,150000,144000,2,5,2,0,4463.0,4552.26,1
text: Unable to write to output stream.

hive> INSERT OVERWRITE DIRECTORY '/user/hduser/projects/creditcard_insurance/nondefaultersout/'
    > ROW FORMAT DELIMITED 
    > FIELDS TERMINATED BY ', '
    > SELECT CONCAT(id, ', ', issuerid1, ', ', issuerid2, ', ', lmt, ', ', newlmt, ', ', sex, ', ', edu, ', ', marital, ',', pay, ', ', billamt, ', ', newbillamt, ', ', defaulter ) AS cnt from penalties WHERE defaulter=0
    > UNION
    > SELECT CONCAT('Trl|', count(1)) AS cnt FROM (SELECT * FROM penalties WHERE defaulter=0)  tmp;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = hduser_20220709123039_8e9bdb38-1b33-4730-9509-be550865a702
Total jobs = 2
Launching Job 1 out of 2
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1657271823979_0014, Tracking URL = http://Inceptez:8088/proxy/application_1657271823979_0014/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1657271823979_0014
Hadoop job information for Stage-3: number of mappers: 1; number of reducers: 1
2022-07-09 12:31:24,668 Stage-3 map = 0%,  reduce = 0%
2022-07-09 12:31:43,525 Stage-3 map = 100%,  reduce = 0%, Cumulative CPU 13.46 sec
2022-07-09 12:31:56,547 Stage-3 map = 100%,  reduce = 100%, Cumulative CPU 22.11 sec
MapReduce Total cumulative CPU time: 22 seconds 110 msec
Ended Job = job_1657271823979_0014
Launching Job 2 out of 2
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1657271823979_0015, Tracking URL = http://Inceptez:8088/proxy/application_1657271823979_0015/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1657271823979_0015
Hadoop job information for Stage-2: number of mappers: 2; number of reducers: 1
2022-07-09 12:32:38,495 Stage-2 map = 0%,  reduce = 0%
2022-07-09 12:33:01,903 Stage-2 map = 50%,  reduce = 0%, Cumulative CPU 12.31 sec
2022-07-09 12:33:02,966 Stage-2 map = 100%,  reduce = 0%, Cumulative CPU 29.73 sec
2022-07-09 12:33:14,818 Stage-2 map = 100%,  reduce = 100%, Cumulative CPU 37.29 sec
MapReduce Total cumulative CPU time: 37 seconds 290 msec
Ended Job = job_1657271823979_0015
Moving data to directory /user/hduser/projects/creditcard_insurance/nondefaultersout
MapReduce Jobs Launched: 
Stage-Stage-3: Map: 1  Reduce: 1   Cumulative CPU: 22.11 sec   HDFS Read: 27570 HDFS Write: 122 SUCCESS
Stage-Stage-2: Map: 2  Reduce: 1   Cumulative CPU: 37.29 sec   HDFS Read: 148377 HDFS Write: 466582 SUCCESS
Total MapReduce CPU Time Spent: 59 seconds 400 msec
OK
Time taken: 156.069 seconds

[hduser@localhost creditcard_insurance]$ hadoop fs -ls /user/hduser/projects/creditcard_insurance/nondefaultersout/
Found 1 items
-rwxr-xr-x   1 hduser hadoop     466582 2022-07-09 12:33 /user/hduser/projects/creditcard_insurance/nondefaultersout/000000_0

[hduser@localhost creditcard_insurance]$ hadoop fs -text /user/hduser/projects/creditcard_insurance/nondefaultersout/* | head
1000, 38897, 38897, 120000, 120000, 1, 2, 2,2, 113348.0, 113348.0, 0
10000, 99389, 99389, 230000, 230000, 1, 2, 1,0, 19505.0, 19505.0, 0
1001, 43274, 43274, 100000, 100000, 1, 2, 1,0, 94453.0, 94453.0, 0
1002, 43274, 43274, 200000, 200000, 2, 2, 1,0, 81865.0, 81865.0, 0
1003, 48121, 48121, 90000, 90000, 2, 2, 1,-1, 4989.0, 4989.0, 0
1006, 48129, 48129, 140000, 140000, 1, 1, 2,-1, 2937.0, 2937.0, 0
1007, 49193, 49193, 200000, 200000, 2, 2, 2,-1, 1371.0, 1371.0, 0
1008, 51398, 51398, 170000, 170000, 2, 2, 1,0, 166246.0, 166246.0, 0
1009, 51398, 51398, 30000, 30000, 1, 2, 2,0, 19785.0, 19785.0, 0
101, 57451, 57451, 140000, 140000, 1, 1, 2,-2, 672.0, 672.0, 0
text: Unable to write to output stream.

[hduser@localhost creditcard_insurance]$ hadoop fs -text /user/hduser/projects/creditcard_insurance/nondefaultersout/* | tail
9984, 56707, 56707, 130000, 130000, 2, 3, 1,0, 101885.0, 101885.0, 0
9986, 60013, 60013, 480000, 480000, 2, 1, 2,-1, 12759.0, 12759.0, 0
9987, 63474, 63474, 160000, 160000, 2, 1, 1,-1, 9293.0, 9293.0, 0
9988, 63474, 63474, 10000, 10000, 1, 1, 2,0, 6599.0, 6599.0, 0
9989, 63474, 63474, 360000, 360000, 2, 2, 2,-1, 8552.0, 8552.0, 0
9990, 63474, 63474, 260000, 260000, 2, 2, 1,0, 251811.0, 251811.0, 0
9991, 68420, 68420, 330000, 330000, 2, 2, 1,0, 47644.0, 47644.0, 0
9997, 95417, 95417, 80000, 80000, 2, 2, 2,-2, 3946.0, 3946.0, 0
9998, 99389, 99389, 200000, 200000, 1, 3, 1,0, 138877.0, 138877.0, 0
Trl|7065

```

### Data Provisioning to the Consumers using DistCP
Copy the data non defaulters data from one cluster (Prod) to another cluster (Non Prod) for analytics purpose.

hadoop distcp -overwrite hdfs://localhost:54310/user/hduser/projects/creditcard_insurance/nondefaultersout/ hdfs://localhost:54310/tmp/promocustomers

hadoop fs -ls hdfs://localhost:54310/user/hduser/projects/creditcard_insurance/nondefaultersout/

hadoop fs -ls hdfs://localhost:54310/tmp/promocustomers/

``` 

[hduser@localhost creditcard_insurance]$ hadoop distcp -overwrite hdfs://localhost:54310/user/hduser/projects/creditcard_insurance/nondefaultersout/ hdfs://localhost:54310/tmp/promocustomers
22/07/09 12:37:36 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
22/07/09 12:37:38 INFO tools.DistCp: Input Options: DistCpOptions{atomicCommit=false, syncFolder=false, deleteMissing=false, ignoreFailures=false, maxMaps=20, sslConfigurationFile='null', copyStrategy='uniformsize', sourceFileListing=null, sourcePaths=[hdfs://localhost:54310/user/hduser/projects/creditcard_insurance/nondefaultersout], targetPath=hdfs://localhost:54310/tmp/promocustomers, targetPathExists=false, preserveRawXattrs=false}
22/07/09 12:37:38 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
22/07/09 12:37:39 INFO Configuration.deprecation: io.sort.mb is deprecated. Instead, use mapreduce.task.io.sort.mb
22/07/09 12:37:39 INFO Configuration.deprecation: io.sort.factor is deprecated. Instead, use mapreduce.task.io.sort.factor
22/07/09 12:37:39 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
22/07/09 12:37:40 INFO mapreduce.JobSubmitter: number of splits:1
22/07/09 12:37:40 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1657271823979_0016
22/07/09 12:37:40 INFO impl.YarnClientImpl: Submitted application application_1657271823979_0016
22/07/09 12:37:41 INFO mapreduce.Job: The url to track the job: http://Inceptez:8088/proxy/application_1657271823979_0016/
22/07/09 12:37:41 INFO tools.DistCp: DistCp job-id: job_1657271823979_0016
22/07/09 12:37:41 INFO mapreduce.Job: Running job: job_1657271823979_0016
22/07/09 12:37:51 INFO mapreduce.Job: Job job_1657271823979_0016 running in uber mode : false
22/07/09 12:37:51 INFO mapreduce.Job:  map 0% reduce 0%
22/07/09 12:38:00 INFO mapreduce.Job:  map 100% reduce 0%
22/07/09 12:38:01 INFO mapreduce.Job: Job job_1657271823979_0016 completed successfully
22/07/09 12:38:02 INFO mapreduce.Job: Counters: 33
	File System Counters
		FILE: Number of bytes read=0
		FILE: Number of bytes written=118461
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=467001
		HDFS: Number of bytes written=466582
		HDFS: Number of read operations=18
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=4
	Job Counters 
		Launched map tasks=1
		Other local map tasks=1
		Total time spent by all maps in occupied slots (ms)=6467
		Total time spent by all reduces in occupied slots (ms)=0
		Total time spent by all map tasks (ms)=6467
		Total vcore-seconds taken by all map tasks=6467
		Total megabyte-seconds taken by all map tasks=6622208
	Map-Reduce Framework
		Map input records=1
		Map output records=0
		Input split bytes=135
		Spilled Records=0
		Failed Shuffles=0
		Merged Map outputs=0
		GC time elapsed (ms)=127
		CPU time spent (ms)=1920
		Physical memory (bytes) snapshot=167469056
		Virtual memory (bytes) snapshot=2116947968
		Total committed heap usage (bytes)=144179200
	File Input Format Counters 
		Bytes Read=284
	File Output Format Counters 
		Bytes Written=0
	org.apache.hadoop.tools.mapred.CopyMapper$Counter
		BYTESCOPIED=466582
		BYTESEXPECTED=466582
		COPY=1

[hduser@localhost creditcard_insurance]$ hadoop fs -ls hdfs://localhost:54310/user/hduser/projects/creditcard_insurance/nondefaultersout/
Found 1 items
-rwxr-xr-x   1 hduser hadoop     466582 2022-07-09 12:33 hdfs://localhost:54310/user/hduser/projects/creditcard_insurance/nondefaultersout/000000_0

[hduser@localhost creditcard_insurance]$ hadoop fs -ls hdfs://localhost:54310/tmp/promocustomers/
Found 1 items
-rw-r--r--   1 hduser supergroup     466582 2022-07-09 12:37 hdfs://localhost:54310/tmp/promocustomers/000000_0

```


### Data Ingestion from external systems using Linux Shell Script

Execute the below shell script to pull the data from Cloud S3, validate, remove trailer data, 
move to HDFS and archive the data for backup.
Ensure data is imported from cloud to hdfs and get the date and take a note of the timestamp in the file.

bash sfm_insuredata.sh https://s3.us-east-2.amazonaws.com/com.inceptez/datafolder/insuranceinfo.csv

hadoop fs -ls /user/hduser/projects/creditcard_insurance/insurance_clouddata

``` 

[hduser@localhost creditcard_insurance]$ bash sfm_insuredata.sh https://s3.us-east-2.amazonaws.com/com.inceptez/datafolder/insuranceinfo.csv
src dir is present
archival path exists
--2022-07-09 12:14:33--  https://s3.us-east-2.amazonaws.com/com.inceptez/datafolder/insuranceinfo.csv
Resolving s3.us-east-2.amazonaws.com (s3.us-east-2.amazonaws.com)... 52.219.98.177
Connecting to s3.us-east-2.amazonaws.com (s3.us-east-2.amazonaws.com)|52.219.98.177|:443... connected.
HTTP request sent, awaiting response... 200 OK
Length: 414551 (405K) [text/csv]
Saving to: ‘/tmp/clouddata/creditcard_insurance’

100%[===================================================================================================================================>] 414,551      336KB/s   in 1.2s   

2022-07-09 12:14:35 (336 KB/s) - ‘/tmp/clouddata/creditcard_insurance’ saved [414551/414551]

trailer count is 3823
file count is 3823
Remove the trailer line in the file
Data copied to HDFS successfully

[hduser@localhost creditcard_insurance]$ hadoop fs -ls /user/hduser/projects/creditcard_insurance/insurance_clouddata
Found 2 items
-rw-r--r--   1 hduser hadoop          0 2022-07-09 12:14 /user/hduser/projects/creditcard_insurance/insurance_clouddata/_SUCCESS
-rw-r--r--   1 hduser hadoop     414543 2022-07-09 12:14 /user/hduser/projects/creditcard_insurance/insurance_clouddata/creditcard_insurance_2022070912

[hduser@localhost creditcard_insurance]$ hadoop fs -text /user/hduser/projects/creditcard_insurance/insurance_clouddata/creditcard_insurance_2022070912 | head
IssuerId,IssuerId2,BusinessYear,StateCode,SourceName,NetworkName,NetworkURL,RowNumber,MarketCoverage,DentalOnlyPlan
21989,21989,2018,AK,HIOS,ODS Premier,https://www.modahealth.com/ProviderSearch/faces/webpages/home.xhtml?dn=ods,13,,
38344,38344,2018,AK,HIOS,HeritagePlus,https://www.premera.com/wa/visitor/,13,,
38536,38536,2018,AK,HIOS,Lincoln Dental Connect,http://lfg.go2dental.com/member/dental_search/searchprov.cgi?P=LFGDentalConnect&Network=L,13,,
42507,42507,2018,AK,HIOS,DentalGuard Preferred,https://www.guardiananytime.com/fpapp/FPWeb/dentalSearch.process,13,,
73836,73836,2018,AK,HIOS,Moda Plus AK Regional,https://www.modahealth.com/ProviderSearch/faces/webpages/home.xhtml?dn=ods,13,,
73836,73836,2018,AK,HIOS,Moda Plus Providence,https://www.modahealth.com/ProviderSearch/faces/webpages/home.xhtml?dn=ods,14,,
74819,74819,2018,AK,HIOS,DenteMax,http://www2.dentemax.com/,13,,
84859,84859,2018,AK,HIOS,Ameritas PPO Dental Network,www.standard.com,13,,
12538,12538,2018,AL,HIOS,DenteMax,http://www2.dentemax.com/,13,,
text: Unable to write to output stream.

[hduser@localhost creditcard_insurance]$ hadoop fs -text /user/hduser/projects/creditcard_insurance/insurance_clouddata/creditcard_insurance_2022070912 | tail
,,,,,,,13,SHOP (Small Group),Yes
,,,,,,,13,SHOP (Small Group),Yes
,,,,,,,13,SHOP (Small Group),Yes
,,,,,,,13,SHOP (Small Group),Yes
,,,,,,,13,Individual,Yes
,,,,,,,13,SHOP (Small Group),Yes
,,,,,,,13,SHOP (Small Group),Yes
,,,,,,,13,Individual,Yes
,,,,,,,13,SHOP (Small Group),Yes
,,,,,,,13,SHOP (Small Group),Yes

```

## ETL & ELT using Hive

**OPTION: 1**

DROP TABLE IF EXISTS insurance;

CREATE TABLE insurance (IssuerId1 int, IssuerId2 int, BusinessYear int, StateCode string, SourceName string,
NetworkName string, NetworkURL string, RowNumber int, MarketCoverage string, DentalOnlyPlan string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
TBLPROPERTIES ("skip.header.line.count"="1");

SHOW TABLES;

LOAD DATA INPATH '/user/hduser/projects/creditcard_insurance/insurance_clouddata' INTO TABLE insurance;

SELECT COUNT(*) FROM insurance;

``` 
hive> DROP TABLE IF EXISTS insurance;
OK
Time taken: 0.062 seconds

hive> CREATE TABLE insurance (IssuerId1 int, IssuerId2 int, BusinessYear int, StateCode string, SourceName string,
    > NetworkName string, NetworkURL string, RowNumber int, MarketCoverage string, DentalOnlyPlan string)
    > ROW FORMAT DELIMITED
    > FIELDS TERMINATED BY ','
    > TBLPROPERTIES ("skip.header.line.count"="1");
OK
Time taken: 0.555 seconds

hive> SHOW TABLES;
OK
credits_all
credits_all_temp
credits_cst
credits_pst
insurance
penalties
Time taken: 0.074 seconds, Fetched: 6 row(s)

hive> LOAD DATA INPATH '/user/hduser/projects/creditcard_insurance/insurance_clouddata' INTO TABLE insurance;
Loading data to table insure.insurance
OK
Time taken: 1.337 seconds

hive> SELECT COUNT(*) FROM insurance;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = hduser_20220709125301_9f2af74d-2e4a-4f03-98c3-c08d63dad83d
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1657271823979_0017, Tracking URL = http://Inceptez:8088/proxy/application_1657271823979_0017/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1657271823979_0017
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-07-09 12:53:49,154 Stage-1 map = 0%,  reduce = 0%
2022-07-09 12:54:04,041 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 5.73 sec
2022-07-09 12:54:15,920 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 11.16 sec
MapReduce Total cumulative CPU time: 11 seconds 160 msec
Ended Job = job_1657271823979_0017
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 11.16 sec   HDFS Read: 423548 HDFS Write: 104 SUCCESS
Total MapReduce CPU Time Spent: 11 seconds 160 msec
OK
3822
Time taken: 76.918 seconds, Fetched: 1 row(s)

```

**OPTION 2:**

USE insure;

CREATE TABLE insurance_temp (IssuerId1 INT, IssuerId2 INT, BusinessYear INT, StateCode STRING, SourceName STRING,
NetworkName STRING, NetworkURL STRING, RowNumber INT, MarketCoverage STRING, DentalOnlyPlan STRING)
Partitioned by (datadt DATE, hr INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
TBLPROPERTIES ("skip.header.line.count"="1");

SELECT COUNT(*) FROM insurance_temp;

-- reusing the data file under hive table as the data at the previous external location will not be available now  
bash /home/hduser/projects/creditcard_insurance/hivepart.sh /user/hive/warehouse/insure.db/insurance insure.insurance_temp creditcard_insurance

SELECT COUNT(*) FROM insurance_temp;

-- Delete the invalid data with null issuerid1 and issuerid2 using insert select query
INSERT OVERWRITE TABLE insurance_temp partition(datadt, hr) SELECT * FROM insurance_temp WHERE issuerid1 IS NOT NULL and issuerid2 IS NOT NULL;

``` 
hive> 
    > CREATE TABLE insurance_temp (IssuerId1 INT, IssuerId2 INT, BusinessYear INT, StateCode STRING, SourceName STRING,
    > NetworkName STRING, NetworkURL STRING, RowNumber INT, MarketCoverage STRING, DentalOnlyPlan STRING)
    > Partitioned by (datadt DATE, hr INT)
    > ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    > TBLPROPERTIES ("skip.header.line.count"="1");
OK
Time taken: 0.236 seconds

hive> SELECT COUNT(*) FROM insurance_temp;
OK
0
Time taken: 0.418 seconds, Fetched: 1 row(s)

[hduser@localhost creditcard_insurance]$ bash /home/hduser/projects/creditcard_insurance/hivepart.sh /user/hive/warehouse/insure.db/insurance insure.insurance_temp creditcard_insurance
/home/hduser/projects/creditcard_insurance/hivepart.sh is starting
/user/hive/warehouse/insure.db/insurance is the path
insure.insurance_temp is the tablename
creditcard_insurance is the filename prefix
file with path name is /user/hive/warehouse/insure.db/insurance/creditcard_insurance_2022070912
creditcard_insurance_2022070912
2022-07-09
12
loading hive table
which: no hbase in (/usr/local/bin:/usr/local/sbin:/usr/bin:/usr/sbin:/bin:/sbin:/usr/lib/jvm/java-1.8.0-openjdk:/usr/lib/jvm/java-1.8.0-openjdk/jre//bin:/usr/local/hadoop/bin:/usr/local/hadoop/sbin:/usr/local/sqoop/bin:/usr/local/hive/bin:/usr/local/hbase/bin:/usr/local/zookeeper/bin:/usr/local/phoenix/bin:/usr/local/kafka/bin:/usr/local/nifi/bin:/usr/local/spark/bin:/usr/local/spark/sbin:/tmp/inceptez/bin/:/usr/local/oozie/bin:/tmp/bin:/usr/local/oozie/bin:/usr/local/pycharm/bin/:/home/hduser/.local/bin:/home/hduser/bin:/usr/lib/jvm/java-1.8.0-openjdk:/usr/lib/jvm/java-1.8.0-openjdk/jre//bin:/usr/local/hadoop/bin:/usr/local/hadoop/sbin:/usr/local/sqoop/bin:/usr/local/hive/bin:/usr/local/hbase/bin:/usr/local/zookeeper/bin:/usr/local/phoenix/bin:/usr/local/kafka/bin:/usr/local/nifi/bin:/usr/local/spark/bin:/usr/local/spark/sbin:/tmp/inceptez/bin/:/usr/local/oozie/bin:/tmp/bin:/usr/local/oozie/bin:/usr/local/pycharm/bin/)
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/usr/local/hive/lib/log4j-slf4j-impl-2.6.2.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/usr/local/hadoop/share/hadoop/common/lib/slf4j-log4j12-1.7.10.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]

Logging initialized using configuration in jar:file:/usr/local/hive/lib/hive-common-2.3.9.jar!/hive-log4j2.properties Async: true
Loading data to table insure.insurance_temp partition (datadt=2022-07-09, hr=12)
OK
Time taken: 8.897 seconds

hive> SELECT COUNT(*) FROM insurance_temp;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = hduser_20220709132125_c8e2512e-c718-40c8-ac69-ef39e162d990
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1657271823979_0018, Tracking URL = http://Inceptez:8088/proxy/application_1657271823979_0018/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1657271823979_0018
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-07-09 13:22:05,323 Stage-1 map = 0%,  reduce = 0%
2022-07-09 13:22:19,960 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 6.04 sec
2022-07-09 13:22:33,101 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 12.5 sec
MapReduce Total cumulative CPU time: 12 seconds 500 msec
Ended Job = job_1657271823979_0018
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 12.5 sec   HDFS Read: 424232 HDFS Write: 104 SUCCESS
Total MapReduce CPU Time Spent: 12 seconds 500 msec
OK
3822
Time taken: 69.789 seconds, Fetched: 1 row(s)

hive> INSERT OVERWRITE TABLE insurance_temp partition(datadt, hr) SELECT * FROM insurance_temp WHERE issuerid1 IS NOT NULL and issuerid2 IS NOT NULL;
FAILED: SemanticException [Error 10096]: Dynamic partition strict mode requires at least one static partition column. To turn this off set hive.exec.dynamic.partition.mode=nonstrict

hive> set hive.exec.dynamic.partition.mode=nonstrict;

hive> INSERT OVERWRITE TABLE insurance_temp partition(datadt, hr) SELECT * FROM insurance_temp WHERE issuerid1 IS NOT NULL and issuerid2 IS NOT NULL;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = hduser_20220709133002_7fe99b1f-24d9-445e-b4fd-ae11f12cbe16
Total jobs = 3
Launching Job 1 out of 3
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1657271823979_0019, Tracking URL = http://Inceptez:8088/proxy/application_1657271823979_0019/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1657271823979_0019
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 0
2022-07-09 13:30:44,258 Stage-1 map = 0%,  reduce = 0%
2022-07-09 13:30:59,440 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 9.65 sec
MapReduce Total cumulative CPU time: 9 seconds 650 msec
Ended Job = job_1657271823979_0019
Stage-4 is selected by condition resolver.
Stage-3 is filtered out by condition resolver.
Stage-5 is filtered out by condition resolver.
Moving data to directory hdfs://localhost:54310/user/hive/warehouse/insure.db/insurance_temp/.hive-staging_hive_2022-07-09_13-30-02_294_1874925130189078363-1/-ext-10000
Loading data to table insure.insurance_temp partition (datadt=null, hr=null)

Loaded : 1/1 partitions.
	 Time taken to load dynamic partitions: 0.491 seconds
	 Time taken for adding to write entity : 0.001 seconds
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1   Cumulative CPU: 9.65 sec   HDFS Read: 421088 HDFS Write: 395620 SUCCESS
Total MapReduce CPU Time Spent: 9 seconds 650 msec
OK
Time taken: 59.746 seconds

hive> SELECT COUNT(*) FROM insurance_temp;
OK
3363
Time taken: 0.325 seconds, Fetched: 1 row(s)

```

**Create one more fixed width hive table to load the fixed width states_fixedwidth data using Regex Serde**

DROP TABLE IF EXISTS insure.state_master;

CREATE EXTERNAL TABLE insure.state_master (statecd STRING, statedesc STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'
WITH SERDEPROPERTIES ("input.regex" = "(.{2})(.{20})" )
LOCATION '/user/hduser/projects/creditcard_insurance/states';

LOAD DATA LOCAL INPATH '/home/hduser/projects/creditcard_insurance/states_fixedwidth' OVERWRITE INTO TABLE INSURE.state_master;

hadoop fs -ls /user/hduser/projects/creditcard_insurance/states/

hadoop fs -text /user/hduser/projects/creditcard_insurance/states/* | head


``` 
hive> DROP TABLE IF EXISTS insure.state_master;
OK
Time taken: 0.035 seconds

hive> 
    > CREATE EXTERNAL TABLE insure.state_master (statecd STRING, statedesc STRING)
    > ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'
    > WITH SERDEPROPERTIES ("input.regex" = "(.{2})(.{20})" )
    > LOCATION '/user/hduser/projects/creditcard_insurance/states';
OK
Time taken: 0.192 seconds

hive> LOAD DATA LOCAL INPATH '/home/hduser/projects/creditcard_insurance/states_fixedwidth' OVERWRITE INTO TABLE INSURE.state_master;
Loading data to table insure.state_master
OK
Time taken: 1.424 seconds

[hduser@localhost creditcard_insurance]$ hadoop fs -ls /user/hduser/projects/creditcard_insurance/states/
Found 1 items
-rwxr-xr-x   1 hduser hadoop       1173 2022-07-09 13:39 /user/hduser/projects/creditcard_insurance/states/states_fixedwidth

[hduser@localhost creditcard_insurance]$ hadoop fs -text /user/hduser/projects/creditcard_insurance/states/* | head
ALAlabama             
AKAlaska              
AZArizona             
ARArkansas            
CACalifornia          
COColorado            
CTConnecticut         
DEDelaware            
DCDistrict of Columbia
FLFlorida             

```

**Create a managed table on top of the hive output defaulter’s dataset created in step 5.b given above.**

USE insure;

CREATE TABLE defaulters (id INT, IssuerId1 INT, IssuerId2 INT, lmt INT, newlmt DOUBLE, sex INT, edu INT, marital INT, pay INT, billamt INT, newbillamt FLOAT, defaulter INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/hduser/projects/creditcard_insurance/defaultersout';

SELECT COUNT(*) FROM defaulters;

``` 
hive> CREATE TABLE defaulters (id INT, IssuerId1 INT, IssuerId2 INT, lmt INT, newlmt DOUBLE, sex INT, edu INT, marital INT, pay INT, billamt INT, newbillamt FLOAT, defaulter INT)
    > ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    > LOCATION '/user/hduser/projects/creditcard_insurance/defaultersout';
OK
Time taken: 0.237 seconds

hive> SELECT COUNT(*) FROM defaulters;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = hduser_20220709140928_e4df43ff-ace8-4157-8ef2-78a6d166f5e7
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1657271823979_0020, Tracking URL = http://Inceptez:8088/proxy/application_1657271823979_0020/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1657271823979_0020
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-07-09 14:10:10,366 Stage-1 map = 0%,  reduce = 0%
2022-07-09 14:10:24,560 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 5.72 sec
2022-07-09 14:10:36,321 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 11.56 sec
MapReduce Total cumulative CPU time: 11 seconds 560 msec
Ended Job = job_1657271823979_0020
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 11.56 sec   HDFS Read: 123218 HDFS Write: 104 SUCCESS
Total MapReduce CPU Time Spent: 11 seconds 560 msec
OK
2022
Time taken: 68.882 seconds, Fetched: 1 row(s)

```

**Create a final managed table (later convert to external table) in orc with snappy compression and load
the above 2 tables joined by applying different functions. This table should not allow duplicates
when it is empty or if not using overwrite option.**


CREATE TABLE insurancestg (IssuerId int, BusinessYear int, StateCode string, statedesc string, 
SourceName string, NetworkName string, NetworkURL string, RowNumber int, MarketCoverage string, 
DentalOnlyPlan string, id int, lmt int, newlmt int, reduced_lmt int, sex varchar(6), grade varchar(20), marital int, 
pay int, billamt int, newbillamt float, penality float, defaulter int)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ', '
LOCATION '/user/hduser/projects/creditcard_insurance/insurancestg';

INSERT OVERWRITE TABLE insurancestg SELECT CONCAT(i.IssuerId1, i.IssuerId2) as issuerid, 
i.businessyear, i.statecode, s.statedesc as statedesc, i.sourcename, 
i.networkname, i.networkurl, i.rownumber, i.marketcoverage, i.dentalonlyplan, 
d.id, d.lmt, d.newlmt, d.newlmt-d.lmt as reduced_lmt, case when d.sex=1 then 'male' else 'female' end as sex, 
case when d.edu=1 then 'lower grade' when d.edu=2 then 'lower middle grade' when d.edu=3 then
'middle grade' when d.edu=4 then 'higher grade' when d.edu=5 then 'doctrate grade' end as grade, d.marital, d.pay, d.billamt, d.newbillamt, d.newbillamt-d.billamt as penality, d.defaulter
from insurance_temp i inner join defaulters d
on (i.IssuerId1=d.IssuerId1 and i.IssuerId2=d.IssuerId2)
inner join state_master s
on (i.statecode=s.statecd);

hadoop fs -rm -r -f /user/hduser/projects/creditcard_insurance/insuranceorc;

drop table if exists insuranceorc;

CREATE TABLE insuranceorc (IssuerId int, BusinessYear int, StateCode string, statedesc string, SourceName
string, NetworkName string, NetworkURL string, RowNumber int, MarketCoverage string, DentalOnlyPlan
string, id int, lmt int, newlmt int, reduced_lmt int, sex varchar(6), grade varchar(20), marital int, pay
int, billamt int, newbillamt float, penality int, defaulter int)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ', '
STORED AS ORC
LOCATION '/user/hduser/projects/creditcard_insurance/insuranceorc'
TBLPROPERTIES ("immutable"="true", "orc.compress"="SNAPPY");

Insert into insuranceorc select * from insurancestg where issuerid is not null;

**Retry running the above same insert query once again and see what happens??
If you get the below error then drop the above table and recreate and insert.**

FAILED: SemanticException [Error 10256]: Inserting into a non-empty immutable table is not allowed
insuranceorc

**Convert the above table from managed to external, usually we use the below statement if we can’t
create external table in the initial stage itself for example sqoop import hive table can’t be created as
external initially.**

alter table insuranceorc SET TBLPROPERTIES('EXTERNAL'='TRUE');

**write common table expression queries in hive**

with 
    T1 as ( select max(penality) as penalitymale from insuranceorc where sex='male'), 
    T2 as ( select max(penality) as penalityfemale from insuranceorc where sex='female')
select penalitymale, penalityfemale from T1 inner join T2
ON 1=1;

``` 
hive> CREATE TABLE insurancestg (IssuerId int, BusinessYear int, StateCode string, statedesc string, 
    > SourceName string, NetworkName string, NetworkURL string, RowNumber int, MarketCoverage string, 
    > DentalOnlyPlan string, id int, lmt int, newlmt int, reduced_lmt int, sex varchar(6), grade varchar(20), marital int, 
    > pay int, billamt int, newbillamt float, penality float, defaulter int)
    > ROW FORMAT DELIMITED 
    > FIELDS TERMINATED BY ', '
    > LOCATION '/user/hduser/projects/creditcard_insurance/insurancestg';
OK
Time taken: 0.237 seconds
hive> 

[hduser@localhost creditcard_insurance]$ hadoop fs -ls /user/hduser/projects/creditcard_insurance/insurancestg

hive> INSERT OVERWRITE TABLE insurancestg SELECT CONCAT(i.IssuerId1, i.IssuerId2) as issuerid, 
    > i.businessyear, i.statecode, s.statedesc as statedesc, i.sourcename, 
    > i.networkname, i.networkurl, i.rownumber, i.marketcoverage, i.dentalonlyplan, 
    > d.id, d.lmt, d.newlmt, d.newlmt-d.lmt as reduced_lmt, case when d.sex=1 then 'male' else 'female' end as sex, 
    > case when d.edu=1 then 'lower grade' when d.edu=2 then 'lower middle grade' when d.edu=3 then
    > 'middle grade' when d.edu=4 then 'higher grade' when d.edu=5 then 'doctrate grade' end as grade, d.marital, d.pay, d.billamt, d.newbillamt, d.newbillamt-d.billamt as penality, d.defaulter
    > from insurance i inner join defaulters d
    > on (i.IssuerId1=d.IssuerId1 and i.IssuerId2=d.IssuerId2)
    > inner join state_master s
    > on (i.statecode=s.statecd);
No Stats for insure@insurance, Columns: dentalonlyplan, networkname, rownumber, businessyear, sourcename, statecode, issuerid1, issuerid2, networkurl, marketcoverage
No Stats for insure@defaulters, Columns: marital, lmt, billamt, sex, edu, pay, defaulter, id, issuerid1, issuerid2, newlmt, newbillamt
No Stats for insure@state_master, Columns: statedesc, statecd
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = hduser_20220709142436_3a2d90c2-874e-4773-b5a5-7dddfa7e6cd9
Total jobs = 1
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/usr/local/hive/lib/log4j-slf4j-impl-2.6.2.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/usr/local/hadoop/share/hadoop/common/lib/slf4j-log4j12-1.7.10.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]
2022-07-09 14:24:57	Starting to launch local task to process map join;	maximum memory = 477626368
2022-07-09 14:25:01	Dump the side-table for tag: 1 with group count: 51 into file: file:/tmp/hduser/0b265da6-06bd-4c2f-8c44-1e48704e1424/hive_2022-07-09_14-24-36_614_296737652532275489-1/-local-10003/HashTable-Stage-6/MapJoin-mapfile01--.hashtable
2022-07-09 14:25:01	Uploaded 1 File to: file:/tmp/hduser/0b265da6-06bd-4c2f-8c44-1e48704e1424/hive_2022-07-09_14-24-36_614_296737652532275489-1/-local-10003/HashTable-Stage-6/MapJoin-mapfile01--.hashtable (2412 bytes)
2022-07-09 14:25:01	Dump the side-table for tag: 0 with group count: 0 into file: file:/tmp/hduser/0b265da6-06bd-4c2f-8c44-1e48704e1424/hive_2022-07-09_14-24-36_614_296737652532275489-1/-local-10003/HashTable-Stage-6/MapJoin-mapfile10--.hashtable
2022-07-09 14:25:01	Uploaded 1 File to: file:/tmp/hduser/0b265da6-06bd-4c2f-8c44-1e48704e1424/hive_2022-07-09_14-24-36_614_296737652532275489-1/-local-10003/HashTable-Stage-6/MapJoin-mapfile10--.hashtable (260 bytes)
2022-07-09 14:25:01	End of local task; Time Taken: 3.576 sec.
Execution completed successfully
MapredLocal task succeeded
Launching Job 1 out of 1
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1657271823979_0021, Tracking URL = http://Inceptez:8088/proxy/application_1657271823979_0021/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1657271823979_0021
Hadoop job information for Stage-6: number of mappers: 1; number of reducers: 0
2022-07-09 14:25:46,072 Stage-6 map = 0%,  reduce = 0%
2022-07-09 14:26:01,971 Stage-6 map = 100%,  reduce = 0%, Cumulative CPU 9.8 sec
MapReduce Total cumulative CPU time: 9 seconds 800 msec
Ended Job = job_1657271823979_0021
Loading data to table insure.insurancestg
MapReduce Jobs Launched: 
Stage-Stage-6: Map: 1   Cumulative CPU: 9.8 sec   HDFS Read: 20287 HDFS Write: 45 SUCCESS
Total MapReduce CPU Time Spent: 9 seconds 800 msec
OK
Time taken: 88.037 seconds

[hduser@localhost creditcard_insurance]$ hadoop fs -ls /user/hduser/projects/creditcard_insurance/insurancestg
Found 1 items
-rwxr-xr-x   1 hduser hadoop          0 2022-07-09 14:26 /user/hduser/projects/creditcard_insurance/insurancestg/000000_0

hive> CREATE TABLE insuranceorc (IssuerId int, BusinessYear int, StateCode string, statedesc string, SourceName
    > string, NetworkName string, NetworkURL string, RowNumber int, MarketCoverage string, DentalOnlyPlan
    > string, id int, lmt int, newlmt int, reduced_lmt int, sex varchar(6), grade varchar(20), marital int, pay
    > int, billamt int, newbillamt float, penality int, defaulter int)
    > ROW FORMAT DELIMITED 
    > FIELDS TERMINATED BY ', '
    > STORED AS ORC
    > LOCATION '/user/hduser/projects/creditcard_insurance/insuranceorc'
    > TBLPROPERTIES ("immutable"="true", "orc.compress"="SNAPPY");
OK
Time taken: 0.267 seconds

hive> Insert into insuranceorc select * from insurancestg where issuerid is not null;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = hduser_20220709143437_96da8b79-88c3-4ac1-bac5-c3b77f15d7c4
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1657271823979_0022, Tracking URL = http://Inceptez:8088/proxy/application_1657271823979_0022/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1657271823979_0022
Hadoop job information for Stage-1: number of mappers: 0; number of reducers: 0
2022-07-09 14:35:24,426 Stage-1 map = 0%,  reduce = 0%
Ended Job = job_1657271823979_0022
Stage-4 is selected by condition resolver.
Stage-3 is filtered out by condition resolver.
Stage-5 is filtered out by condition resolver.
Moving data to directory hdfs://localhost:54310/user/hduser/projects/creditcard_insurance/insuranceorc/.hive-staging_hive_2022-07-09_14-34-37_398_9017981043722056196-1/-ext-10000
Loading data to table insure.insuranceorc
MapReduce Jobs Launched: 
Stage-Stage-1:  HDFS Read: 0 HDFS Write: 0 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
Time taken: 51.468 seconds

hive> Insert into insuranceorc select * from insurancestg where issuerid is not null;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = hduser_20220709145310_7907032d-87a1-4779-bdee-eeccb10148a8
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1657271823979_0028, Tracking URL = http://Inceptez:8088/proxy/application_1657271823979_0028/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1657271823979_0028
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 0
2022-07-09 14:53:51,544 Stage-1 map = 0%,  reduce = 0%
2022-07-09 14:54:06,519 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 10.83 sec
MapReduce Total cumulative CPU time: 10 seconds 830 msec
Ended Job = job_1657271823979_0028
Stage-4 is selected by condition resolver.
Stage-3 is filtered out by condition resolver.
Stage-5 is filtered out by condition resolver.
Moving data to directory hdfs://localhost:54310/user/hduser/projects/creditcard_insurance/insuranceorc/.hive-staging_hive_2022-07-09_14-53-10_154_3490019164892019181-1/-ext-10000
Loading data to table insure.insuranceorc
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1   Cumulative CPU: 10.83 sec   HDFS Read: 3064243 HDFS Write: 27776 SUCCESS
Total MapReduce CPU Time Spent: 10 seconds 830 msec
OK
Time taken: 58.449 seconds

hive> Insert into insuranceorc select * from insurancestg where issuerid is not null;
FAILED: SemanticException [Error 10256]: Inserting into a non-empty immutable table is not allowed insuranceorc

hive> alter table insuranceorc SET TBLPROPERTIES('EXTERNAL'='TRUE');
OK
Time taken: 0.212 seconds

hive> with 
    >     T1 as ( select max(penality) as penalitymale from insuranceorc where sex='male'), 
    >     T2 as ( select max(penality) as penalityfemale from insuranceorc where sex='female')
    > select penalitymale, penalityfemale from T1 inner join T2
    > ON 1=1;
FAILED: SemanticException Cartesian products are disabled for safety reasons. If you know what you are doing, please sethive.strict.checks.cartesian.product to false and that hive.mapred.mode is not set to 'strict' to proceed. Note that if you may get errors or incorrect results if you make a mistake while using some of the unsafe features.
hive> set hive.strict.checks.cartesian.product=false;
hive> with 
    >     T1 as ( select max(penality) as penalitymale from insuranceorc where sex='male'), 
    >     T2 as ( select max(penality) as penalityfemale from insuranceorc where sex='female')
    > select penalitymale, penalityfemale from T1 inner join T2
    > ON 1=1;
Warning: Map Join MAPJOIN[25][bigTable=?] in task 'Stage-4:MAPRED' is a cross product
Warning: Map Join MAPJOIN[26][bigTable=?] in task 'Stage-5:MAPRED' is a cross product
Warning: Shuffle Join JOIN[16][tables = [$hdt$_0, $hdt$_1]] in Stage 'Stage-2:MAPRED' is a cross product
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = hduser_20220709145755_8a1a93bd-5e9e-4024-95a6-2745459b6cc9
Total jobs = 5
Launching Job 1 out of 5
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1657271823979_0029, Tracking URL = http://Inceptez:8088/proxy/application_1657271823979_0029/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1657271823979_0029
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-07-09 14:58:35,817 Stage-1 map = 0%,  reduce = 0%
2022-07-09 14:58:50,078 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 8.7 sec
2022-07-09 14:59:02,206 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 14.01 sec
MapReduce Total cumulative CPU time: 14 seconds 10 msec
Ended Job = job_1657271823979_0029
Launching Job 2 out of 5
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1657271823979_0030, Tracking URL = http://Inceptez:8088/proxy/application_1657271823979_0030/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1657271823979_0030
Hadoop job information for Stage-3: number of mappers: 1; number of reducers: 1
2022-07-09 14:59:43,224 Stage-3 map = 0%,  reduce = 0%
2022-07-09 14:59:57,073 Stage-3 map = 100%,  reduce = 0%, Cumulative CPU 8.83 sec
2022-07-09 15:00:09,003 Stage-3 map = 100%,  reduce = 100%, Cumulative CPU 14.31 sec
MapReduce Total cumulative CPU time: 14 seconds 310 msec
Ended Job = job_1657271823979_0030
Stage-7 is selected by condition resolver.
Stage-8 is filtered out by condition resolver.
Stage-2 is filtered out by condition resolver.
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/usr/local/hive/lib/log4j-slf4j-impl-2.6.2.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/usr/local/hadoop/share/hadoop/common/lib/slf4j-log4j12-1.7.10.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]
2022-07-09 15:00:29	Starting to launch local task to process map join;	maximum memory = 477626368
2022-07-09 15:00:32	Dump the side-table for tag: 1 with group count: 1 into file: file:/tmp/hduser/ceefa210-2ee5-416c-ba3f-de881af16e43/hive_2022-07-09_14-57-55_569_1507857202222233526-1/-local-10006/HashTable-Stage-4/MapJoin-mapfile41--.hashtable
2022-07-09 15:00:32	Uploaded 1 File to: file:/tmp/hduser/ceefa210-2ee5-416c-ba3f-de881af16e43/hive_2022-07-09_14-57-55_569_1507857202222233526-1/-local-10006/HashTable-Stage-4/MapJoin-mapfile41--.hashtable (280 bytes)
2022-07-09 15:00:32	End of local task; Time Taken: 2.415 sec.
Execution completed successfully
MapredLocal task succeeded
Launching Job 4 out of 5
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1657271823979_0031, Tracking URL = http://Inceptez:8088/proxy/application_1657271823979_0031/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1657271823979_0031
Hadoop job information for Stage-4: number of mappers: 1; number of reducers: 0
2022-07-09 15:01:13,171 Stage-4 map = 0%,  reduce = 0%
2022-07-09 15:01:25,952 Stage-4 map = 100%,  reduce = 0%, Cumulative CPU 4.82 sec
MapReduce Total cumulative CPU time: 4 seconds 820 msec
Ended Job = job_1657271823979_0031
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 14.01 sec   HDFS Read: 31215 HDFS Write: 116 SUCCESS
Stage-Stage-3: Map: 1  Reduce: 1   Cumulative CPU: 14.31 sec   HDFS Read: 31224 HDFS Write: 116 SUCCESS
Stage-Stage-4: Map: 1   Cumulative CPU: 4.82 sec   HDFS Read: 4317 HDFS Write: 109 SUCCESS
Total MapReduce CPU Time Spent: 33 seconds 140 msec
OK
8718	5779
Time taken: 211.52 seconds, Fetched: 1 row(s)

```

