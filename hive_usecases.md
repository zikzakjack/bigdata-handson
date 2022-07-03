# Hive Sample Usecases

Most common Hive Usecases are schema migration, Csv serde, json, fixed width , sqoop export/import with more options,
DML,ETL/ELT. This document covers samples from those areas. Data files used to illustrate the usecases are under [resources/data/hiveusecases](resources/data/hiveusecases).

# Usecase 1: ETL/ELT, DML operation & Benchmarking in Hive

## ETL Using Hive Queries and few functions

Hive can do ETL and ELT, lets explore how can we achieve several business logics in hive by loading
staging tables, de-normalized tables with joins, concatenation, summation, aggregations, analytical
queries etc.,

### Data Ingestion:

**Create a new Database & Table to load Customer data**

Create database custdb;

use custdb;

create table customer(custno string, firstname string, lastname string, age int,profession string)
row format delimited 
fields terminated by ',';

load data local inpath '/home/hduser/hive/data/custs' into table customer;

``` 
hive> Create database custdb;
OK
Time taken: 0.36 seconds

hive> use custdb;
OK
Time taken: 0.066 seconds

hive> create table customer(custno string, firstname string, lastname string, age int,profession string)
    > row format delimited 
    > fields terminated by ',';
OK
Time taken: 0.335 seconds

hive> load data local inpath '/home/hduser/hive/data/custs' into table customer;
Loading data to table custdb.customer
OK
Time taken: 1.388 seconds

```

**Create a new Database & Table to load Transactions data**

Create database retail;

use retail;

create table txnrecords(txnno INT, txndate STRING, custno INT, amount DOUBLE, 
category STRING, product STRING, city STRING, state STRING, spendby STRING)
row format delimited 
fields terminated by ','
stored as textfile;

load data local inpath '/home/hduser/hive/data/txns' into table txnrecords;


``` 
hive> Create database retail;
OK
Time taken: 0.36 seconds

hive> use retail;
OK
Time taken: 0.066 seconds

hive> create table txnrecords(txnno INT, txndate STRING, custno INT, amount DOUBLE, 
    > category STRING, product STRING, city STRING, state STRING, spendby STRING)
    > row format delimited 
    > fields terminated by ','
    > stored as textfile;
OK
Time taken: 0.335 seconds

hive> load data local inpath '/home/hduser/hive/data/txns' OVERWRITE into table txnrecords;
Loading data to table custdb.customer
OK
Time taken: 1.388 seconds

```

### Data Curation:

Create a new table by joining customer and transaction data located in different databases such as retail and custdb prefixing schema name, 
here we are denormalizing the tables into a single fat table with added ETL operations and stored as 
external table partitioned based on current date

use custdb;

create external table ext_cust_txn_part (custno int,fullname string,age int,profession
string,amount double,product string,spendby string,agecat varchar(100),modifiedamout float)
partitioned by (datadt date)
row format delimited 
fields terminated by ','
location '/user/hduser/custtxn';

insert into table ext_cust_txn_part partition(datadt)
select a.custno, upper(concat(a.firstname, ' ',a.lastname)),
a.age, a.profession, b.amount, b.product, b.spendby,
case when age<30 then 'low'
    when age>=30 and age < 50 then 'middle'
    when age>=50 then 'old' else 'others' 
end as agecat,
case when spendby= 'credit' then b.amount+(b.amount*0.05) else b.amount 
end as modifiedamount, current_date
from custdb.customer a JOIN retail.txnrecords b
ON a.custno = b.custno;

select * from ext_cust_txn_part limit 10;

``` 

hive> use custdb;
OK
Time taken: 0.072 seconds

hive> create external table ext_cust_txn_part (custno int,fullname string,age int,profession
    > string,amount double,product string,spendby string,agecat varchar(100),modifiedamout float)
    > partitioned by (datadt date)
    > row format delimited 
    > fields terminated by ','
    > location '/user/hduser/custtxn';
OK
Time taken: 0.316 seconds

hive> insert into table ext_cust_txn_part partition(datadt)
    > select a.custno, upper(concat(a.firstname, ' ',a.lastname)),
    > a.age, a.profession, b.amount, b.product, b.spendby,
    > case when age<30 then 'low'
    >     when age>=30 and age < 50 then 'middle'
    >     when age>=50 then 'old' else 'others' 
    > end as agecat,
    > case when spendby= 'credit' then b.amount+(b.amount*0.05) else b.amount 
    > end as modifiedamount, current_date
    > from custdb.customer a JOIN retail.txnrecords b
    > ON a.custno = b.custno;
Query ID = hduser_20220703150642_6c715337-c31f-4aab-ada9-c84e55a96d61
Total jobs = 1
2022-07-03 15:07:06	Starting to launch local task to process map join;	maximum memory = 477626368
2022-07-03 15:07:10	Dump the side-table for tag: 0 with group count: 9999 into file: file:/tmp/hduser/b41b7a4b-b222-46c1-851f-c4731d88de03/hive_2022-07-03_15-06-42_573_4520059930933267496-1/-local-10002/HashTable-Stage-4/MapJoin-mapfile00--.hashtable
2022-07-03 15:07:11	Uploaded 1 File to: file:/tmp/hduser/b41b7a4b-b222-46c1-851f-c4731d88de03/hive_2022-07-03_15-06-42_573_4520059930933267496-1/-local-10002/HashTable-Stage-4/MapJoin-mapfile00--.hashtable (624627 bytes)
2022-07-03 15:07:11	End of local task; Time Taken: 4.955 sec.
Execution completed successfully
MapredLocal task succeeded
Launching Job 1 out of 1
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1656670722551_0019, Tracking URL = http://Inceptez:8088/proxy/application_1656670722551_0019/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1656670722551_0019
Hadoop job information for Stage-4: number of mappers: 8; number of reducers: 0
2022-07-03 15:08:03,677 Stage-4 map = 0%,  reduce = 0%
2022-07-03 15:09:04,355 Stage-4 map = 0%,  reduce = 0%, Cumulative CPU 23.08 sec
2022-07-03 15:09:20,172 Stage-4 map = 6%,  reduce = 0%, Cumulative CPU 55.56 sec
2022-07-03 15:09:24,536 Stage-4 map = 31%,  reduce = 0%, Cumulative CPU 63.86 sec
2022-07-03 15:09:26,130 Stage-4 map = 44%,  reduce = 0%, Cumulative CPU 66.34 sec
2022-07-03 15:09:27,388 Stage-4 map = 50%,  reduce = 0%, Cumulative CPU 69.18 sec
2022-07-03 15:09:28,471 Stage-4 map = 75%,  reduce = 0%, Cumulative CPU 70.68 sec
2022-07-03 15:09:52,702 Stage-4 map = 88%,  reduce = 0%, Cumulative CPU 89.85 sec
2022-07-03 15:09:54,924 Stage-4 map = 100%,  reduce = 0%, Cumulative CPU 93.8 sec
MapReduce Total cumulative CPU time: 1 minutes 33 seconds 800 msec
Ended Job = job_1656670722551_0019
Loading data to table custdb.ext_cust_txn_part partition (datadt=null)

Loaded : 1/1 partitions.
	 Time taken to load dynamic partitions: 0.32 seconds
	 Time taken for adding to write entity : 0.001 seconds
MapReduce Jobs Launched: 
Stage-Stage-4: Map: 8   Cumulative CPU: 93.8 sec   HDFS Read: 8581667 HDFS Write: 7405773 SUCCESS
Total MapReduce CPU Time Spent: 1 minutes 33 seconds 800 msec
OK
Time taken: 195.756 seconds

hive> select * from ext_cust_txn_part limit 10;
OK
4006979	KATIE CONNOR	21	Social worker	105.5	Downhill Skiing	credit	low	110.775	2022-07-03
4007312	WILLIE ELLIS	60	Coach	91.75	Water Polo	credit	old	96.3375	2022-07-03
4001845	ELISABETH HARRELL	55	Computer hardware engineer	189.27	Shooting Games	credit	old	198.7335	2022-07-03
4000904	NATHAN BURGESS	47	Librarian	22.19	Baseball	cash	middle	22.19	2022-07-03
4009714	TINA FLOWERS	27	Real estate agent	35.22	Outdoor Playsets	cash	low	35.22	2022-07-03
4003205	AARON LI	38	Recreation and fitness worker	87.56	Snowshoeing	credit	middle	91.938	2022-07-03
4006761	GORDON OWENS	33	Politician	58.43	Team Handball	credit	middle	61.3515	2022-07-03
4000028	DWIGHT MONROE	45	Economist	94.77	Kitesurfing	credit	middle	99.5085	2022-07-03
4000306	STANLEY GLOVER	44	Nurse	18.46	Luge	cash	middle	18.46	2022-07-03
4003886	JULIE HOYLE	73	Agricultural and food scientist	161.65	Fencing	credit	old	169.7325	2022-07-03
Time taken: 0.347 seconds, Fetched: 10 row(s)
hive> 

```
### Data Visualization/Analytics/Aggregation/Reporting/Discovery


