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

