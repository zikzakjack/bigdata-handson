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

