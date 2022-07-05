
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

Creating aggregation tables that will be considered as cube or deriving KPIs (Key Performance Indicator)
or deriving Metrics and used for quick reporting purposes. Here we are creating a sequence number or a
surrogate key column using olap functions like rownumber over and aggregating the age and amount in
multiple dimensions with the addition of current_date also.


create external table cust_trxn_aggr1 (seqno int, product string, profession string, level string, 
sumamt double, avgamount double, maxamt double, avgage int, currentdate date)
row format delimited 
fields terminated by ',';

insert overwrite table cust_trxn_aggr1
select row_number() over(), product, profession, agecat,
sum(amount), avg(amount), max(amount), avg(age), current_date()
from ext_cust_txn_part
where datadt=current_date
group by product, profession, agecat, current_date();

select * from cust_trxn_aggr1 limit 10;

create external table cust_trxn_aggr2 (seqno int, profession string, level string, sumamt double,
avgamount double, maxamt double, avgage int, currentdate date)
row format delimited 
fields terminated by ',';

insert overwrite table cust_trxn_aggr2
select row_number() over(), profession, agecat,
sum(amount), avg(amount), max(amount), avg(age), current_date()
from ext_cust_txn_part
where datadt=current_date
group by profession, agecat, current_date();

select * from cust_trxn_aggr2 limit 10;

``` 
hive> 
    > create external table cust_trxn_aggr1 (seqno int, product string, profession string, level string, 
    > sumamt double, avgamount double, maxamt double, avgage int, currentdate date)
    > row format delimited 
    > fields terminated by ',';
OK
Time taken: 0.257 seconds

hive> 
    > insert overwrite table cust_trxn_aggr1
    > select row_number() over(), product, profession, agecat,
    > sum(amount), avg(amount), max(amount), avg(age), current_date()
    > from ext_cust_txn_part
    > where datadt=current_date
    > group by product, profession, agecat, current_date();
Query ID = hduser_20220703154258_8c772766-ece0-4744-bbbd-256ac010bf4f
Total jobs = 2
Launching Job 1 out of 2
Number of reduce tasks not specified. Defaulting to jobconf value of: 4
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1656670722551_0022, Tracking URL = http://Inceptez:8088/proxy/application_1656670722551_0022/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1656670722551_0022
Hadoop job information for Stage-1: number of mappers: 5; number of reducers: 4
2022-07-03 15:43:44,868 Stage-1 map = 0%,  reduce = 0%
2022-07-03 15:44:34,380 Stage-1 map = 13%,  reduce = 0%, Cumulative CPU 23.29 sec
2022-07-03 15:44:35,636 Stage-1 map = 20%,  reduce = 0%, Cumulative CPU 24.79 sec
2022-07-03 15:44:37,005 Stage-1 map = 40%,  reduce = 0%, Cumulative CPU 27.97 sec
2022-07-03 15:44:38,384 Stage-1 map = 60%,  reduce = 0%, Cumulative CPU 30.16 sec
2022-07-03 15:44:39,746 Stage-1 map = 73%,  reduce = 0%, Cumulative CPU 32.5 sec
2022-07-03 15:44:41,015 Stage-1 map = 87%,  reduce = 0%, Cumulative CPU 34.67 sec
2022-07-03 15:44:42,171 Stage-1 map = 93%,  reduce = 0%, Cumulative CPU 35.98 sec
2022-07-03 15:44:43,252 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 37.06 sec
2022-07-03 15:45:13,223 Stage-1 map = 100%,  reduce = 50%, Cumulative CPU 45.49 sec
2022-07-03 15:45:16,966 Stage-1 map = 100%,  reduce = 56%, Cumulative CPU 47.49 sec
2022-07-03 15:45:18,354 Stage-1 map = 100%,  reduce = 75%, Cumulative CPU 50.2 sec
2022-07-03 15:45:20,917 Stage-1 map = 100%,  reduce = 92%, Cumulative CPU 57.39 sec
2022-07-03 15:45:24,232 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 61.59 sec
MapReduce Total cumulative CPU time: 1 minutes 1 seconds 590 msec
Ended Job = job_1656670722551_0022
Launching Job 2 out of 2
Number of reduce tasks not specified. Defaulting to jobconf value of: 4
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1656670722551_0023, Tracking URL = http://Inceptez:8088/proxy/application_1656670722551_0023/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1656670722551_0023
Hadoop job information for Stage-2: number of mappers: 2; number of reducers: 4
2022-07-03 15:46:07,519 Stage-2 map = 0%,  reduce = 0%
2022-07-03 15:46:29,612 Stage-2 map = 100%,  reduce = 0%, Cumulative CPU 9.75 sec
2022-07-03 15:46:55,147 Stage-2 map = 100%,  reduce = 17%, Cumulative CPU 12.58 sec
2022-07-03 15:47:00,048 Stage-2 map = 100%,  reduce = 33%, Cumulative CPU 14.86 sec
2022-07-03 15:47:06,499 Stage-2 map = 100%,  reduce = 50%, Cumulative CPU 16.96 sec
2022-07-03 15:47:10,399 Stage-2 map = 100%,  reduce = 67%, Cumulative CPU 27.97 sec
2022-07-03 15:47:12,922 Stage-2 map = 100%,  reduce = 83%, Cumulative CPU 30.44 sec
2022-07-03 15:47:16,472 Stage-2 map = 100%,  reduce = 92%, Cumulative CPU 35.88 sec
2022-07-03 15:47:17,545 Stage-2 map = 100%,  reduce = 100%, Cumulative CPU 40.33 sec
MapReduce Total cumulative CPU time: 40 seconds 330 msec
Ended Job = job_1656670722551_0023
Loading data to table custdb.cust_trxn_aggr1
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 5  Reduce: 4   Cumulative CPU: 61.59 sec   HDFS Read: 7468422 HDFS Write: 1475085 SUCCESS
Stage-Stage-2: Map: 2  Reduce: 4   Cumulative CPU: 40.33 sec   HDFS Read: 1512269 HDFS Write: 1467248 SUCCESS
Total MapReduce CPU Time Spent: 1 minutes 41 seconds 920 msec
OK
Time taken: 262.096 seconds


hive> select * from cust_trxn_aggr1 limit 10;
OK
1	Yoga & Pilates	Writer	old	214.82	71.60666666666667	105.48	72	2022-07-03
2	Yoga & Pilates	Writer	middle	430.65	143.54999999999998	166.23	36	2022-07-03
3	Weightlifting Machine Accessories	Writer	old	206.69	103.345	129.62	62	2022-07-03
4	Weightlifting Machine Accessories	Writer	middle	292.78	146.39	157.35	37	2022-07-03
5	Weight Benches	Writer	old	422.49	140.83	197.39	61	2022-07-03
6	Weight Benches	Writer	middle	115.63	115.63	115.63	43	2022-07-03
7	Water Tubing	Writer	old	90.03	90.03	90.03	59	2022-07-03
8	Water Tubing	Writer	middle	343.81	171.905	174.98	34	2022-07-03
9	Water Tables	Writer	old	165.74	82.87	153.72	68	2022-07-03
10	Water Tables	Writer	middle	139.05	139.05	139.05	33	2022-07-03
Time taken: 0.246 seconds, Fetched: 10 row(s)

hive> 
    > 
    > create external table cust_trxn_aggr2 (seqno int, profession string, level string, sumamt double,
    > avgamount double, maxamt double, avgage int, currentdate date)
    > row format delimited 
    > fields terminated by ',';
OK
Time taken: 0.219 seconds


hive> insert overwrite table cust_trxn_aggr2
    > select row_number() over(), profession, agecat,
    > sum(amount), avg(amount), max(amount), avg(age), current_date()
    > from ext_cust_txn_part
    > where datadt=current_date
    > group by profession, agecat, current_date();
Query ID = hduser_20220703155004_9ee61b7c-c984-42bf-84b9-627a57ed4426
Total jobs = 2
Launching Job 1 out of 2
Number of reduce tasks not specified. Defaulting to jobconf value of: 4
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1656670722551_0024, Tracking URL = http://Inceptez:8088/proxy/application_1656670722551_0024/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1656670722551_0024
Hadoop job information for Stage-1: number of mappers: 5; number of reducers: 4
2022-07-03 15:50:48,169 Stage-1 map = 0%,  reduce = 0%
2022-07-03 15:51:31,615 Stage-1 map = 7%,  reduce = 0%, Cumulative CPU 5.48 sec
2022-07-03 15:51:34,513 Stage-1 map = 20%,  reduce = 0%, Cumulative CPU 15.15 sec
2022-07-03 15:51:37,240 Stage-1 map = 27%,  reduce = 0%, Cumulative CPU 24.12 sec
2022-07-03 15:51:38,504 Stage-1 map = 47%,  reduce = 0%, Cumulative CPU 26.09 sec
2022-07-03 15:51:39,781 Stage-1 map = 87%,  reduce = 0%, Cumulative CPU 30.02 sec
2022-07-03 15:51:40,919 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 30.99 sec
2022-07-03 15:52:08,044 Stage-1 map = 100%,  reduce = 17%, Cumulative CPU 33.35 sec
2022-07-03 15:52:10,570 Stage-1 map = 100%,  reduce = 33%, Cumulative CPU 36.08 sec
2022-07-03 15:52:11,885 Stage-1 map = 100%,  reduce = 50%, Cumulative CPU 39.0 sec
2022-07-03 15:52:13,243 Stage-1 map = 100%,  reduce = 58%, Cumulative CPU 40.85 sec
2022-07-03 15:52:14,449 Stage-1 map = 100%,  reduce = 67%, Cumulative CPU 42.32 sec
2022-07-03 15:52:15,671 Stage-1 map = 100%,  reduce = 75%, Cumulative CPU 43.84 sec
2022-07-03 15:52:18,969 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 48.7 sec
MapReduce Total cumulative CPU time: 48 seconds 700 msec
Ended Job = job_1656670722551_0024
Launching Job 2 out of 2
Number of reduce tasks not specified. Defaulting to jobconf value of: 4
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1656670722551_0025, Tracking URL = http://Inceptez:8088/proxy/application_1656670722551_0025/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1656670722551_0025
Hadoop job information for Stage-2: number of mappers: 1; number of reducers: 4
2022-07-03 15:53:05,536 Stage-2 map = 0%,  reduce = 0%
2022-07-03 15:53:19,076 Stage-2 map = 100%,  reduce = 0%, Cumulative CPU 3.85 sec
2022-07-03 15:53:46,986 Stage-2 map = 100%,  reduce = 17%, Cumulative CPU 6.09 sec
2022-07-03 15:53:53,805 Stage-2 map = 100%,  reduce = 33%, Cumulative CPU 8.87 sec
2022-07-03 15:54:00,157 Stage-2 map = 100%,  reduce = 42%, Cumulative CPU 13.81 sec
2022-07-03 15:54:02,485 Stage-2 map = 100%,  reduce = 58%, Cumulative CPU 15.95 sec
2022-07-03 15:54:03,613 Stage-2 map = 100%,  reduce = 67%, Cumulative CPU 20.49 sec
2022-07-03 15:54:05,885 Stage-2 map = 100%,  reduce = 83%, Cumulative CPU 23.52 sec
2022-07-03 15:54:09,330 Stage-2 map = 100%,  reduce = 92%, Cumulative CPU 27.65 sec
2022-07-03 15:54:10,386 Stage-2 map = 100%,  reduce = 100%, Cumulative CPU 31.84 sec
MapReduce Total cumulative CPU time: 31 seconds 840 msec
Ended Job = job_1656670722551_0025
Loading data to table custdb.cust_trxn_aggr2
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 5  Reduce: 4   Cumulative CPU: 48.7 sec   HDFS Read: 7465119 HDFS Write: 10736 SUCCESS
Stage-Stage-2: Map: 1  Reduce: 4   Cumulative CPU: 31.84 sec   HDFS Read: 45661 HDFS Write: 11751 SUCCESS
Total MapReduce CPU Time Spent: 1 minutes 20 seconds 540 msec
OK
Time taken: 248.844 seconds


hive> select * from cust_trxn_aggr2 limit 10;
OK
1	Writer	old	47038.84	103.15535087719297	199.58	63	2022-07-03
2	Writer	middle	30291.260000000002	107.79807829181496	199.37	38	2022-07-03
3	Secretary	low	27404.750000000007	100.38369963369966	199.37	26	2022-07-03
4	Real estate agent	old	98292.50000000001	103.14008394543548	199.98	62	2022-07-03
5	Real estate agent	middle	74028.65000000002	100.58240489130438	199.7	37	2022-07-03
6	Politician	low	29811.899999999998	101.40102040816326	198.39	25	2022-07-03
7	Police officer	low	26103.29	103.58448412698414	199.68	25	2022-07-03
8	Pilot	low	31530.72999999999	103.71950657894733	198.97	26	2022-07-03
9	Pharmacist	low	45804.39000000001	105.7838106235566	199.93	25	2022-07-03
10	Librarian	low	37614.79	103.9082596685083	198.03	25	2022-07-03
Time taken: 0.363 seconds, Fetched: 10 row(s)

```

## Performing DML statements in Hive

Question is , can we do DML/ACID transactions in hive as HDFS doesn't support changes??
Ans: Hive is not for DML/ACID, but it supports in case if we need, but it is cosly to apply.
Try to update/delete the customer table created above, it won’t work in general.

``` 
hive> update customer set profession='IT' where custno= 4000001;
FAILED: SemanticException [Error 10294]: Attempt to do update or delete using transaction manager that does not support these operations.

hive> delete from customer where custno= 4000002;
FAILED: SemanticException [Error 10294]: Attempt to do update or delete using transaction manager that does not support these operations.

```

**Solution :**

1. Create another hive table with buckets, stored in orc format and transactional table properties true
for the DML support

create table customerdml(custno string, firstname string, lastname string, age int,profession string)
clustered by (custno) into 3 buckets
stored as orc
TBLPROPERTIES ('transactional'='true');

``` 
hive> create table customerdml(custno string, firstname string, lastname string, age int,profession string)
    > clustered by (custno) into 3 buckets
    > stored as orc
    > TBLPROPERTIES ('transactional'='true');
OK
Time taken: 0.375 seconds
```
2. Enable Transaction Support

set hive.support.concurrency=true;

set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

```
hive> set hive.support.concurrency=true;
hive> set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
```

3. Copy the data from customer to the customerdml table.

insert into customerdml select * from customer;

**tablelocation/delta_1/3 buckets**

``` 
hive> insert into customerdml select * from customer;
Query ID = hduser_20220703165153_602671e1-2941-4762-ad8a-82e36aa8b2ec
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 3
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1656670722551_0026, Tracking URL = http://Inceptez:8088/proxy/application_1656670722551_0026/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1656670722551_0026
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 3
2022-07-03 16:52:40,028 Stage-1 map = 0%,  reduce = 0%
2022-07-03 16:52:55,057 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 5.44 sec
2022-07-03 16:53:17,556 Stage-1 map = 100%,  reduce = 22%, Cumulative CPU 8.67 sec
2022-07-03 16:53:20,096 Stage-1 map = 100%,  reduce = 33%, Cumulative CPU 10.75 sec
2022-07-03 16:53:21,435 Stage-1 map = 100%,  reduce = 56%, Cumulative CPU 15.02 sec
2022-07-03 16:53:25,007 Stage-1 map = 100%,  reduce = 67%, Cumulative CPU 17.64 sec
2022-07-03 16:53:29,338 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 23.18 sec
MapReduce Total cumulative CPU time: 23 seconds 180 msec
Ended Job = job_1656670722551_0026
Loading data to table custdb.customerdml
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 3   Cumulative CPU: 23.18 sec   HDFS Read: 409979 HDFS Write: 90618 SUCCESS
Total MapReduce CPU Time Spent: 23 seconds 180 msec
OK
Time taken: 99.572 seconds
```
4. Update

Select * from customerdml where custno=4000001;

update customerdml set profession='IT' where custno= 4000001;

Select * from customerdml where custno=4000001;

**tablelocation/delta_2/0 bucket (only updated data)**

``` 
hive> Select * from customerdml where custno=4000001;
OK
4000001	Kristina	Chung	55	Pilot
Time taken: 0.423 seconds, Fetched: 1 row(s)

hive> update customerdml set profession='IT' where custno= 4000001;
Query ID = hduser_20220703165452_0b93e310-fa82-49a0-80a0-d43786133236
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 3
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1656670722551_0027, Tracking URL = http://Inceptez:8088/proxy/application_1656670722551_0027/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1656670722551_0027
Hadoop job information for Stage-1: number of mappers: 3; number of reducers: 3
2022-07-03 16:55:33,491 Stage-1 map = 0%,  reduce = 0%
2022-07-03 16:56:09,201 Stage-1 map = 33%,  reduce = 0%, Cumulative CPU 15.23 sec
2022-07-03 16:56:11,486 Stage-1 map = 67%,  reduce = 0%, Cumulative CPU 18.1 sec
2022-07-03 16:56:12,644 Stage-1 map = 89%,  reduce = 0%, Cumulative CPU 26.56 sec
2022-07-03 16:56:13,735 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 27.13 sec
2022-07-03 16:56:38,661 Stage-1 map = 100%,  reduce = 56%, Cumulative CPU 35.34 sec
2022-07-03 16:56:40,887 Stage-1 map = 100%,  reduce = 67%, Cumulative CPU 36.4 sec
2022-07-03 16:56:41,957 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 40.7 sec
MapReduce Total cumulative CPU time: 40 seconds 700 msec
Ended Job = job_1656670722551_0027
Loading data to table custdb.customerdml
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 3  Reduce: 3   Cumulative CPU: 40.7 sec   HDFS Read: 159775 HDFS Write: 1112 SUCCESS
Total MapReduce CPU Time Spent: 40 seconds 700 msec
OK
Time taken: 112.838 seconds

hive> Select * from customerdml where custno=4000001;
OK
4000001	Kristina	Chung	55	IT
Time taken: 0.431 seconds, Fetched: 1 row(s)
```
5. Delete

Select * from customerdml where custno in (4000001,4000002,4000003);

delete from customerdml where custno= 4000002;

Select * from customerdml where custno in (4000001,4000002,4000003);

**tablelocation/delta_3/1 bucket (only deleted flag)**

``` 
hive> Select * from customerdml where custno in (4000001,4000002,4000003);
OK
4000001	Kristina	Chung	55	IT
4000002	Paige	Chen	77	Teacher
4000003	Sherri	Melton	34	Firefighter
Time taken: 0.384 seconds, Fetched: 3 row(s)

hive> delete from customerdml where custno= 4000002;
Query ID = hduser_20220703170308_67e5c3ea-a37a-4c3e-bf14-4a61fb4f80d9
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 3
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1656670722551_0028, Tracking URL = http://Inceptez:8088/proxy/application_1656670722551_0028/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1656670722551_0028
Hadoop job information for Stage-1: number of mappers: 3; number of reducers: 3
2022-07-03 17:03:52,138 Stage-1 map = 0%,  reduce = 0%
2022-07-03 17:04:24,812 Stage-1 map = 22%,  reduce = 0%, Cumulative CPU 6.31 sec
2022-07-03 17:04:26,119 Stage-1 map = 33%,  reduce = 0%, Cumulative CPU 12.68 sec
2022-07-03 17:04:28,379 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 20.41 sec
2022-07-03 17:04:53,256 Stage-1 map = 100%,  reduce = 33%, Cumulative CPU 24.94 sec
2022-07-03 17:04:55,538 Stage-1 map = 100%,  reduce = 89%, Cumulative CPU 31.95 sec
2022-07-03 17:04:56,646 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 34.13 sec
MapReduce Total cumulative CPU time: 34 seconds 130 msec
Ended Job = job_1656670722551_0028
Loading data to table custdb.customerdml
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 3  Reduce: 3   Cumulative CPU: 34.13 sec   HDFS Read: 103801 HDFS Write: 711 SUCCESS
Total MapReduce CPU Time Spent: 34 seconds 130 msec
OK
Time taken: 111.44 seconds

hive> Select * from customerdml where custno in (4000001,4000002,4000003);
OK
4000001	Kristina	Chung	55	IT
4000003	Sherri	Melton	34	Firefighter
Time taken: 0.335 seconds, Fetched: 2 row(s)
```

**Admin’s Scope: schedule to run compaction in off peak time**

alter table customerdml compact 'major';

minor -> 20 deltas to 4 deltas

major -> 20 or 4 delta to 1 delta (final)

``` 
hive> alter table customerdml compact 'major';
Compaction enqueued with id 1
OK
Time taken: 0.137 seconds
```

## Benchmarking Hive using different file format storage:

The purpose of doing benchmarking is to identify the best functionality or the feature to be used by
iterating with different options, here we are going to create textfile, orc, avro and parquet format tables
to check the performance between all these tables and the data size it occupied.

### Textfile table

create table staging_txn(txnno INT, txndate STRING, custno INT, amount DOUBLE, category STRING,
product STRING, city STRING, state STRING, spendby STRING)
row format delimited 
fields terminated by ',' 
lines terminated by '\n'
stored as textfile;

Note: the below load command is only possible for textfile table and we can’t use load command for any
other serialized format like orc/parquet/avro/json.

LOAD DATA LOCAL INPATH '/home/hduser/hive/data/txns' OVERWRITE INTO TABLE staging_txn;

``` 
hive> create table staging_txn(txnno INT, txndate STRING, custno INT, amount DOUBLE, category STRING,
    > product STRING, city STRING, state STRING, spendby STRING)
    > row format delimited 
    > fields terminated by ',' 
    > lines terminated by '\n'
    > stored as textfile;
OK
Time taken: 0.418 seconds

hive> LOAD DATA LOCAL INPATH '/home/hduser/hive/data/txns' OVERWRITE INTO TABLE staging_txn;
Loading data to table custdb.staging_txn
OK
Time taken: 1.372 seconds

```

### Parquet file table:

create table txn_parquet(txnno INT, txndate STRING, custno INT, amount DOUBLE, category STRING,
product STRING, city STRING, state STRING, spendby STRING)
row format delimited 
fields terminated by ',' 
lines terminated by '\n'
stored as parquetfile;

Insert into table txn_parquet select txnno, txndate, custno, amount, category, product, city, state, spendby from staging_txn;

``` 
hive> create table txn_parquet(txnno INT, txndate STRING, custno INT, amount DOUBLE,category STRING,
    > product STRING, city STRING, state STRING, spendby STRING)
    > row format delimited fields terminated by ',' lines terminated by '\n'
    > stored as parquetfile;
OK
Time taken: 0.418 seconds

hive> Insert into table txn_parquet select txnno,txndate,custno,amount,category, product,city,state,spendby
    > from staging_txn;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = hduser_20220704074015_55cc9ce6-d153-43ce-9b02-6187196bed64
Total jobs = 3
Launching Job 1 out of 3
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1656670722551_0034, Tracking URL = http://Inceptez:8088/proxy/application_1656670722551_0034/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1656670722551_0034
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 0
2022-07-04 07:41:10,603 Stage-1 map = 0%,  reduce = 0%
2022-07-04 07:41:35,632 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 14.97 sec
MapReduce Total cumulative CPU time: 14 seconds 970 msec
Ended Job = job_1656670722551_0034
Stage-4 is selected by condition resolver.
Stage-3 is filtered out by condition resolver.
Stage-5 is filtered out by condition resolver.
Moving data to directory hdfs://localhost:54310/user/hive/warehouse/custdb.db/txn_parquet/.hive-staging_hive_2022-07-04_07-40-15_307_1159523726538226546-1/-ext-10000
Loading data to table custdb.txn_parquet
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1   Cumulative CPU: 14.97 sec   HDFS Read: 8477417 HDFS Write: 1344545 SUCCESS
Total MapReduce CPU Time Spent: 14 seconds 970 msec
OK
Time taken: 85.1 seconds

hive> select count(*) from txn_parquet;
OK
95904
Time taken: 0.429 seconds, Fetched: 1 row(s)


```

### Orc file table:

create table txn_orc(txnno INT, txndate STRING, custno INT, amount DOUBLE, category
STRING, product STRING, city STRING, state STRING, spendby STRING)
row format delimited fields terminated by ',' lines terminated by '\n'
stored as orcfile;

Insert into table txn_orc select txnno,txndate,custno,amount,category, product,city,state,spendby from
staging_txn;

``` 

hive> create table txn_orc(txnno INT, txndate STRING, custno INT, amount DOUBLE, category
    > STRING, product STRING, city STRING, state STRING, spendby STRING)
    > row format delimited fields terminated by ',' lines terminated by '\n'
    > stored as orcfile;
OK
Time taken: 0.499 seconds

hive> Insert into table txn_orc select txnno,txndate,custno,amount,category, product,city,state,spendby from
    > staging_txn;
Query ID = hduser_20220704075122_2263fd14-2e9c-4656-b83c-30f3d512dd9d
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1656670722551_0035, Tracking URL = http://Inceptez:8088/proxy/application_1656670722551_0035/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1656670722551_0035
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 0
2022-07-04 07:52:18,849 Stage-1 map = 0%,  reduce = 0%
2022-07-04 07:52:42,454 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 14.27 sec
MapReduce Total cumulative CPU time: 14 seconds 270 msec
Ended Job = job_1656670722551_0035
Stage-4 is selected by condition resolver.
Stage-3 is filtered out by condition resolver.
Stage-5 is filtered out by condition resolver.
Moving data to directory hdfs://localhost:54310/user/hive/warehouse/custdb.db/txn_orc/.hive-staging_hive_2022-07-04_07-51-22_453_1447259106034328135-1/-ext-10000
Loading data to table custdb.txn_orc
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1   Cumulative CPU: 14.27 sec   HDFS Read: 8477405 HDFS Write: 966191 SUCCESS
Total MapReduce CPU Time Spent: 14 seconds 270 msec
OK
Time taken: 82.432 seconds

hive> select count(*) from txn_orc;
OK
95904
Time taken: 0.392 seconds, Fetched: 1 row(s)
hive> 

```
### Avro file table:

Note: As we have some issue in the avro-1.7.4.jar provided, we need to change it to the higher version
avro-1.8.2.jar to run the below usecase. Download the higher version jar from the below link:
https://repo1.maven.org/maven2/org/apache/avro/avro/1.8.2/avro-1.8.2.jar

rm /usr/local/hadoop/share/hadoop/common/lib/avro-1.7.4.jar

cp /home/hduser/Downloads/avro-1.8.2.jar /usr/local/hadoop/share/hadoop/common/lib/

create table txn_avro(txnno INT, txndate STRING, custno INT, amount DOUBLE, category STRING, product STRING, city STRING, state STRING, spendby STRING)
row format delimited
fields terminated by ',' 
lines terminated by '\n'
stored as avrofile;

Insert into table txn_avro select txnno,txndate,custno,amount,category, product,city,state,spendby from staging_txn;

``` 
hive> create table txn_avro(txnno INT, txndate STRING, custno INT, amount DOUBLE, category STRING, product STRING, city STRING, state STRING, spendby STRING)
    > row format delimited
    > fields terminated by ',' 
    > lines terminated by '\n'
    > stored as avrofile;
OK
Time taken: 0.826 seconds

hive> Insert into table txn_avro select txnno,txndate,custno,amount,category, product,city,state,spendby from staging_txn;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = hduser_20220704151228_010c5a8a-ad5a-4680-b5f2-641f082cedf7
Total jobs = 3
Launching Job 1 out of 3
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1656670722551_0036, Tracking URL = http://Inceptez:8088/proxy/application_1656670722551_0036/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1656670722551_0036
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 0
2022-07-04 15:13:25,638 Stage-1 map = 0%,  reduce = 0%
2022-07-04 15:13:49,249 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 14.48 sec
MapReduce Total cumulative CPU time: 14 seconds 480 msec
Ended Job = job_1656670722551_0036
Stage-4 is selected by condition resolver.
Stage-3 is filtered out by condition resolver.
Stage-5 is filtered out by condition resolver.
Moving data to directory hdfs://localhost:54310/user/hive/warehouse/custdb.db/txn_avro/.hive-staging_hive_2022-07-04_15-12-28_857_51634277877011944-1/-ext-10000
Loading data to table custdb.txn_avro
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1   Cumulative CPU: 14.48 sec   HDFS Read: 8477975 HDFS Write: 8467287 SUCCESS
Total MapReduce CPU Time Spent: 14 seconds 480 msec
OK
Time taken: 83.396 seconds

hive> select count(*) from txn_avro;
OK
95904
Time taken: 0.395 seconds, Fetched: 1 row(s)

```
**Benchmarking:**

select count(txnno),category from staging_txn group by category;

select count(txnno),category from txn_orc group by category;

select count(txnno),category from txn_parquet group by category;

select count(txnno),category from txn_avro group by category;

``` 
hive> select count(txnno),category from staging_txn group by category;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = hduser_20220704151630_2f6ddabf-3efb-4d33-9bc8-f674b334c151
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1656670722551_0037, Tracking URL = http://Inceptez:8088/proxy/application_1656670722551_0037/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1656670722551_0037
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-07-04 15:17:15,672 Stage-1 map = 0%,  reduce = 0%
2022-07-04 15:17:31,203 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 5.27 sec
2022-07-04 15:17:44,615 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 10.17 sec
MapReduce Total cumulative CPU time: 10 seconds 170 msec
Ended Job = job_1656670722551_0037
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 10.17 sec   HDFS Read: 8481587 HDFS Write: 539 SUCCESS
Total MapReduce CPU Time Spent: 10 seconds 170 msec
OK
1933	Air Sports
3140	Combat Sports
792	Dancing
14097	Exercise & Fitness
6946	Games
6261	Gymnastics
5277	Indoor Games
3789	Jumping
5459	Outdoor Play Equipment
16145	Outdoor Recreation
1174	Puzzles
3110	Racquet Sports
11565	Team Sports
10071	Water Sports
6145	Winter Sports
Time taken: 75.966 seconds, Fetched: 15 row(s)


hive> select count(txnno),category from txn_orc group by category;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = hduser_20220704151829_2ec75345-ea5c-45cb-adf3-1cba7d933dd1
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1656670722551_0038, Tracking URL = http://Inceptez:8088/proxy/application_1656670722551_0038/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1656670722551_0038
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-07-04 15:19:12,577 Stage-1 map = 0%,  reduce = 0%
2022-07-04 15:19:29,268 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 6.68 sec
2022-07-04 15:19:43,526 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 11.34 sec
MapReduce Total cumulative CPU time: 11 seconds 340 msec
Ended Job = job_1656670722551_0038
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 11.34 sec   HDFS Read: 72391 HDFS Write: 539 SUCCESS
Total MapReduce CPU Time Spent: 11 seconds 340 msec
OK
1933	Air Sports
3140	Combat Sports
792	Dancing
14097	Exercise & Fitness
6946	Games
6261	Gymnastics
5277	Indoor Games
3789	Jumping
5459	Outdoor Play Equipment
16145	Outdoor Recreation
1174	Puzzles
3110	Racquet Sports
11565	Team Sports
10071	Water Sports
6145	Winter Sports
Time taken: 75.24 seconds, Fetched: 15 row(s)

hive> select count(txnno),category from txn_parquet group by category;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = hduser_20220704152033_e5b0fb6b-616f-451e-872b-90f0c523e83c
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1656670722551_0039, Tracking URL = http://Inceptez:8088/proxy/application_1656670722551_0039/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1656670722551_0039
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-07-04 15:21:18,814 Stage-1 map = 0%,  reduce = 0%
2022-07-04 15:21:36,007 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 7.8 sec
2022-07-04 15:21:49,364 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 11.81 sec
MapReduce Total cumulative CPU time: 11 seconds 810 msec
Ended Job = job_1656670722551_0039
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 11.81 sec   HDFS Read: 443606 HDFS Write: 539 SUCCESS
Total MapReduce CPU Time Spent: 11 seconds 810 msec
OK
1933	Air Sports
3140	Combat Sports
792	Dancing
14097	Exercise & Fitness
6946	Games
6261	Gymnastics
5277	Indoor Games
3789	Jumping
5459	Outdoor Play Equipment
16145	Outdoor Recreation
1174	Puzzles
3110	Racquet Sports
11565	Team Sports
10071	Water Sports
6145	Winter Sports
Time taken: 76.79 seconds, Fetched: 15 row(s)


hive> select count(txnno),category from txn_avro group by category;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = hduser_20220704152227_c3636660-c3e0-44c4-b6ca-7edd6b3191d4
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks not specified. Estimated from input data size: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1656670722551_0040, Tracking URL = http://Inceptez:8088/proxy/application_1656670722551_0040/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1656670722551_0040
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-07-04 15:23:11,279 Stage-1 map = 0%,  reduce = 0%
2022-07-04 15:23:28,486 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 10.15 sec
2022-07-04 15:23:41,989 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 14.34 sec
MapReduce Total cumulative CPU time: 14 seconds 340 msec
Ended Job = job_1656670722551_0040
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 14.34 sec   HDFS Read: 8485018 HDFS Write: 539 SUCCESS
Total MapReduce CPU Time Spent: 14 seconds 340 msec
OK
1933	Air Sports
3140	Combat Sports
792	Dancing
14097	Exercise & Fitness
6946	Games
6261	Gymnastics
5277	Indoor Games
3789	Jumping
5459	Outdoor Play Equipment
16145	Outdoor Recreation
1174	Puzzles
3110	Racquet Sports
11565	Team Sports
10071	Water Sports
6145	Winter Sports
Time taken: 75.978 seconds, Fetched: 15 row(s)
hive> 


```

**Benchmarking Output:**
Create a tabular column with format, HDFS of data, time for executing the above queries
Serialization Format Size occupied in HDFS Execution time of the queries

```
[hduser@localhost ~]$ hadoop fs -du -s -h /user/hive/warehouse/custdb.db/staging_txn
8.1 M  /user/hive/warehouse/custdb.db/staging_txn

[hduser@localhost ~]$ hadoop fs -du -s -h /user/hive/warehouse/custdb.db/txn_orc
943.5 K  /user/hive/warehouse/custdb.db/txn_orc

[hduser@localhost ~]$ hadoop fs -du -s -h /user/hive/warehouse/custdb.db/txn_parquet
1.3 M  /user/hive/warehouse/custdb.db/txn_parquet

[hduser@localhost ~]$ hadoop fs -du -s -h /user/hive/warehouse/custdb.db/txn_avro
8.1 M  /user/hive/warehouse/custdb.db/txn_avro

```

| Serialization Format | Size occupied in HDFS | Execution time of the queries |
|----------------------|-----------------------|-------------------------------|
| Textfile             | 8.1 M                 | 75.966 seconds                |
| ORC                  | 943.5 K               | 75.24 seconds                 |
| Parquet              | 1.3 M                 | 76.79 seconds                 |
| Avro                 | 8.1 M                 | 75.978 seconds                |


# Usecase 2: CSV Serde

Requirement:

Source provider is providing structured data from a DB or FS source, but the data may contain the
delimiter what we are going to use.

Answer – By using string/varchar type defined for the whole data set we can handle this.

DB -> sqoop import -> Hive CSVSerde -> 

FS -> Textfile table load -> join both data and load to custpayments avro table -> create view to choose
only few columns -> using the view export only the view columns into HDFS location -> Use the HDFS
location as a source to export the data to a DB using sqoop export.

1. Login to Mysql and execute the sql file to load the custpayments table:

mysql> source /home/hduser/hiveusecases/custpayments_ORIG.sql

``` 
mysql> source /home/hduser/hiveusecases/custpayments_ORIG.sql
Connection id:    450
Current database: custdb
Query OK, 0 rows affected, 1 warning (0.46 sec)
Query OK, 0 rows affected (0.00 sec)
Query OK, 1 row affected (0.12 sec)
Database changed
Query OK, 0 rows affected, 1 warning (0.02 sec)
Query OK, 0 rows affected, 2 warnings (0.15 sec)
Query OK, 122 rows affected (0.06 sec)
Records: 122  Duplicates: 0  Warnings: 0

mysql> show databases;
+--------------------+
| Database           |
+--------------------+
| custdb             |
| custpayments       |
| information_schema |
| metastore          |
| mysql              |
| performance_schema |
| sys                |
+--------------------+
7 rows in set (0.00 sec)

mysql> use custpayments
Database changed


mysql> show tables;
+------------------------+
| Tables_in_custpayments |
+------------------------+
| customers              |
+------------------------+
1 row in set (0.01 sec)

mysql> select count(*) from customers;
+----------+
| count(*) |
+----------+
|      122 |
+----------+
1 row in set (0.01 sec)

mysql> select * from customers limit 10;
+----------------+------------------------------+-----------------+------------------+-------------------+------------------------------+--------------+---------------+----------+------------+-----------+------------------------+-------------+
| customerNumber | customerName                 | contactLastName | contactFirstName | phone             | addressLine1                 | addressLine2 | city          | state    | postalCode | country   | salesRepEmployeeNumber | creditLimit |
+----------------+------------------------------+-----------------+------------------+-------------------+------------------------------+--------------+---------------+----------+------------+-----------+------------------------+-------------+
|            103 | Atelier graphique            | Schmitt         | Carine           | 40.32.2555        | 54, rue Royale               | NULL         | Nantes        | NULL     | 44000      | France    |                   1370 |    21000.00 |
|            112 | Signal Gift Stores           | King            | Jean             | 7025551838        | 8489 Strong St.              | NULL         | Las Vegas     | NV       | 83030      | USA       |                   1166 |    71800.00 |
|            114 | Australian Collectors, Co.   | Ferguson        | Peter            | 03 9520 4555      | 636 St Kilda Road            | Level 3      | Melbourne     | Victoria | 3004       | Australia |                   1611 |   117300.00 |
|            119 | La Rochelle Gifts            | Labrune         | Janine           | 40.67.8555        | 67, rue des Cinquante Otages | NULL         | Nantes        | NULL     | 44000      | France    |                   1370 |   118200.00 |
|            121 | Baane Mini Imports           | Bergulfsen      | Jonas            | 07-98 9555        | Erling Skakkes gate 78       | NULL         | Stavern       | NULL     | 4110       | Norway    |                   1504 |    81700.00 |
|            124 | Mini Gifts Distributors Ltd. | Nelson          | Susan            | 4155551450        | 5677 Strong St.              | NULL         | San Rafael    | CA       | 97562      | USA       |                   1165 |   210500.00 |
|            125 | Havel & Zbyszek Co           | Piestrzeniewicz | Zbyszek          | (26) 642-7555     | ul. Filtrowa 68              | NULL         | Warszawa      | NULL     | 01-012     | Poland    |                   NULL |        0.00 |
|            128 | Blauer See Auto, Co.         | Keitel          | Roland           | +49 69 66 90 2555 | Lyonerstr. 34                | NULL         | Frankfurt     | NULL     | 60528      | Germany   |                   1504 |    59700.00 |
|            129 | Mini Wheels Co.              | Murphy          | Julie            | 6505555787        | 5557 North Pendale Street    | NULL         | San Francisco | CA       | 94217      | USA       |                   1165 |    64600.00 |
|            131 | Land of Toys Inc.            | Lee             | Kwai             | 2125557818        | 897 Long Airport Avenue      | NULL         | NYC           | NY       | 10022      | USA       |                   1323 |   114900.00 |
+----------------+------------------------------+-----------------+------------------+-------------------+------------------------------+--------------+---------------+----------+------------+-----------+------------------------+-------------+
10 rows in set (0.00 sec)


```

2. Write sqoop command to import data from customerpayments table with 2 mappers, with enclosed
by " (As we have ',' in the data itself we are importing in sqoop using --enclosed-by option into the
location /user/hduser/custpayments).

sqoop import \
    --driver com.mysql.cj.jdbc.Driver \
    --connect jdbc:mysql://localhost/custpayments \
    --username root \
    --password Root123$ \
    -table customers \
    -m 2 \
    --split-by customernumber \
    --target-dir /user/hduser/custpayments \
    --delete-target-dir \
    --enclosed-by '\"' \
    --escaped-by '\\';

``` 
[hduser@localhost ~]$ sqoop import \
>     --driver com.mysql.cj.jdbc.Driver \
>     --connect jdbc:mysql://localhost/custpayments \
>     --username root \
>     --password Root123$ \
>     -table customers \
>     -m 2 \
>     --split-by customernumber \
>     --target-dir /user/hduser/custpayments \
>     --delete-target-dir \
>     --enclosed-by '\"' \
>     --escaped-by '\\';
Warning: /usr/local/hbase does not exist! HBase imports will fail.
Please set $HBASE_HOME to the root of your HBase installation.
Warning: /usr/local/sqoop/../hcatalog does not exist! HCatalog jobs will fail.
Please set $HCAT_HOME to the root of your HCatalog installation.
Warning: /usr/local/sqoop/../accumulo does not exist! Accumulo imports will fail.
Please set $ACCUMULO_HOME to the root of your Accumulo installation.
Warning: /usr/local/sqoop/../zookeeper does not exist! Accumulo imports will fail.
Please set $ZOOKEEPER_HOME to the root of your Zookeeper installation.
22/07/04 18:05:43 INFO sqoop.Sqoop: Running Sqoop version: 1.4.6
22/07/04 18:05:43 WARN tool.BaseSqoopTool: Setting your password on the command-line is insecure. Consider using -P instead.
22/07/04 18:05:43 WARN sqoop.ConnFactory: Parameter --driver is set to an explicit driver however appropriate connection manager is not being set (via --connection-manager). Sqoop is going to fall back to org.apache.sqoop.manager.GenericJdbcManager. Please specify explicitly which connection manager should be used next time.
22/07/04 18:05:43 INFO manager.SqlManager: Using default fetchSize of 1000
22/07/04 18:05:43 INFO tool.CodeGenTool: Beginning code generation
22/07/04 18:05:45 INFO manager.SqlManager: Executing SQL statement: SELECT t.* FROM customers AS t WHERE 1=0
22/07/04 18:05:45 INFO manager.SqlManager: Executing SQL statement: SELECT t.* FROM customers AS t WHERE 1=0
22/07/04 18:05:45 INFO orm.CompilationManager: HADOOP_MAPRED_HOME is /usr/local/hadoop
Note: /tmp/sqoop-hduser/compile/463fce5d194a28881c4a6b50d5e47e39/customers.java uses or overrides a deprecated API.
Note: Recompile with -Xlint:deprecation for details.
22/07/04 18:05:51 INFO orm.CompilationManager: Writing jar file: /tmp/sqoop-hduser/compile/463fce5d194a28881c4a6b50d5e47e39/customers.jar
22/07/04 18:05:51 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
22/07/04 18:05:53 INFO tool.ImportTool: Destination directory /user/hduser/custpayments is not present, hence not deleting.
22/07/04 18:05:53 INFO mapreduce.ImportJobBase: Beginning import of customers
22/07/04 18:05:53 INFO Configuration.deprecation: mapred.jar is deprecated. Instead, use mapreduce.job.jar
22/07/04 18:05:53 INFO manager.SqlManager: Executing SQL statement: SELECT t.* FROM customers AS t WHERE 1=0
22/07/04 18:05:53 INFO Configuration.deprecation: mapred.map.tasks is deprecated. Instead, use mapreduce.job.maps
22/07/04 18:05:53 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
22/07/04 18:05:59 INFO db.DBInputFormat: Using read commited transaction isolation
22/07/04 18:05:59 INFO db.DataDrivenDBInputFormat: BoundingValsQuery: SELECT MIN(customernumber), MAX(customernumber) FROM customers
22/07/04 18:06:00 INFO mapreduce.JobSubmitter: number of splits:2
22/07/04 18:06:00 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1656670722551_0041
22/07/04 18:06:01 INFO impl.YarnClientImpl: Submitted application application_1656670722551_0041
22/07/04 18:06:01 INFO mapreduce.Job: The url to track the job: http://Inceptez:8088/proxy/application_1656670722551_0041/
22/07/04 18:06:01 INFO mapreduce.Job: Running job: job_1656670722551_0041
22/07/04 18:06:16 INFO mapreduce.Job: Job job_1656670722551_0041 running in uber mode : false
22/07/04 18:06:16 INFO mapreduce.Job:  map 0% reduce 0%
22/07/04 18:06:33 INFO mapreduce.Job:  map 50% reduce 0%
22/07/04 18:06:34 INFO mapreduce.Job:  map 100% reduce 0%
22/07/04 18:06:34 INFO mapreduce.Job: Job job_1656670722551_0041 completed successfully
22/07/04 18:06:35 INFO mapreduce.Job: Counters: 30
	File System Counters
		FILE: Number of bytes read=0
		FILE: Number of bytes written=267950
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=245
		HDFS: Number of bytes written=17553
		HDFS: Number of read operations=8
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=4
	Job Counters 
		Launched map tasks=2
		Other local map tasks=2
		Total time spent by all maps in occupied slots (ms)=30340
		Total time spent by all reduces in occupied slots (ms)=0
		Total time spent by all map tasks (ms)=30340
		Total vcore-seconds taken by all map tasks=30340
		Total megabyte-seconds taken by all map tasks=31068160
	Map-Reduce Framework
		Map input records=122
		Map output records=122
		Input split bytes=245
		Spilled Records=0
		Failed Shuffles=0
		Merged Map outputs=0
		GC time elapsed (ms)=659
		CPU time spent (ms)=7540
		Physical memory (bytes) snapshot=339845120
		Virtual memory (bytes) snapshot=4229988352
		Total committed heap usage (bytes)=221249536
	File Input Format Counters 
		Bytes Read=0
	File Output Format Counters 
		Bytes Written=17553
22/07/04 18:06:35 INFO mapreduce.ImportJobBase: Transferred 17.1416 KB in 41.7592 seconds (420.3382 bytes/sec)
22/07/04 18:06:35 INFO mapreduce.ImportJobBase: Retrieved 122 records.

[hduser@localhost ~]$ hadoop fs -ls /user/hduser/custpayments
Found 3 items
-rw-r--r--   1 hduser hadoop          0 2022-07-04 18:06 /user/hduser/custpayments/_SUCCESS
-rw-r--r--   1 hduser hadoop       8962 2022-07-04 18:06 /user/hduser/custpayments/part-m-00000
-rw-r--r--   1 hduser hadoop       8591 2022-07-04 18:06 /user/hduser/custpayments/part-m-00001

[hduser@localhost ~]$ hadoop fs -du -s -h /user/hduser/custpayments
17.1 K  /user/hduser/custpayments

[hduser@localhost ~]$ hadoop fs -text /user/hduser/custpayments/* | head
"103","Atelier graphique","Schmitt","Carine ","40.32.2555","54, rue Royale","null","Nantes","null","44000","France","1370","21000.00"
"112","Signal Gift Stores","King","Jean","7025551838","8489 Strong St.","null","Las Vegas","NV","83030","USA","1166","71800.00"
"114","Australian Collectors, Co.","Ferguson","Peter","03 9520 4555","636 St Kilda Road","Level 3","Melbourne","Victoria","3004","Australia","1611","117300.00"
"119","La Rochelle Gifts","Labrune","Janine ","40.67.8555","67, rue des Cinquante Otages","null","Nantes","null","44000","France","1370","118200.00"
"121","Baane Mini Imports","Bergulfsen","Jonas ","07-98 9555","Erling Skakkes gate 78","null","Stavern","null","4110","Norway","1504","81700.00"
"124","Mini Gifts Distributors Ltd.","Nelson","Susan","4155551450","5677 Strong St.","null","San Rafael","CA","97562","USA","1165","210500.00"
"125","Havel & Zbyszek Co","Piestrzeniewicz","Zbyszek ","(26) 642-7555","ul. Filtrowa 68","null","Warszawa","null","01-012","Poland","null","0.00"
"128","Blauer See Auto, Co.","Keitel","Roland","+49 69 66 90 2555","Lyonerstr. 34","null","Frankfurt","null","60528","Germany","1504","59700.00"
"129","Mini Wheels Co.","Murphy","Julie","6505555787","5557 North Pendale Street","null","San Francisco","CA","94217","USA","1165","64600.00"
"131","Land of Toys Inc.","Lee","Kwai","2125557818","897 Long Airport Avenue","null","NYC","NY","10022","USA","1323","114900.00"

[hduser@localhost ~]$ hadoop fs -text /user/hduser/custpayments/* | wc -l
122

```

3. Create a hive external table and load the sqoop imported data to the hive table called custpayments.
As we have ',' in the data itself we are using quoted char option below with the csv serde option as given
below as example, create the table with all columns.

create external table custmaster (customerNumber INT, customerName STRING, contactLastName STRING, contactFirstName STRING, phone STRING, addressLine1 STRING, addressLine2 STRING, city STRING, state STRING, postalCode STRING, country STRING, salesRepEmployeeNumber  INT, creditLimit DOUBLE)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        "separatorChar" = ",",
        "quoteChar" = "\"")
    LOCATION '/user/hduser/custpayments/';

``` 
hive> create external table custmaster (customerNumber INT, customerName STRING, contactLastName STRING, contactFirstName STRING, phone STRING, addressLine1 STRING, addressLine2 STRING, city STRING, state STRING, postalCode STRING, country STRING, salesRepEmployeeNumber  INT, creditLimit DOUBLE)
    >     ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    >     WITH SERDEPROPERTIES (
    >         "separatorChar" = ",",
    >         "quoteChar" = "\"")
    >     LOCATION '/user/hduser/custpayments/';
OK
Time taken: 0.229 seconds

hive> describe custmaster;
OK
customernumber      	string              	from deserializer   
customername        	string              	from deserializer   
contactlastname     	string              	from deserializer   
contactfirstname    	string              	from deserializer   
phone               	string              	from deserializer   
addressline1        	string              	from deserializer   
addressline2        	string              	from deserializer   
city                	string              	from deserializer   
state               	string              	from deserializer   
postalcode          	string              	from deserializer   
country             	string              	from deserializer   
salesrepemployeenumber	string              	from deserializer   
creditlimit         	string              	from deserializer   
Time taken: 0.102 seconds, Fetched: 13 row(s)

hive> show create table custmaster;
OK
CREATE EXTERNAL TABLE `custmaster`(
  `customernumber` string COMMENT 'from deserializer', 
  `customername` string COMMENT 'from deserializer', 
  `contactlastname` string COMMENT 'from deserializer', 
  `contactfirstname` string COMMENT 'from deserializer', 
  `phone` string COMMENT 'from deserializer', 
  `addressline1` string COMMENT 'from deserializer', 
  `addressline2` string COMMENT 'from deserializer', 
  `city` string COMMENT 'from deserializer', 
  `state` string COMMENT 'from deserializer', 
  `postalcode` string COMMENT 'from deserializer', 
  `country` string COMMENT 'from deserializer', 
  `salesrepemployeenumber` string COMMENT 'from deserializer', 
  `creditlimit` string COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'quoteChar'='"', 
  'separatorChar'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://localhost:54310/user/hduser/custpayments'
TBLPROPERTIES (
  'transient_lastDdlTime'='1656945885')
Time taken: 0.118 seconds, Fetched: 27 row(s)
hive> 

hive> select count(*) from custmaster;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = hduser_20220704201500_7647770d-9952-406a-8431-f533fc8e4b58
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1656670722551_0045, Tracking URL = http://Inceptez:8088/proxy/application_1656670722551_0045/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1656670722551_0045
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-07-04 20:15:51,858 Stage-1 map = 0%,  reduce = 0%
2022-07-04 20:16:07,566 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 3.63 sec
2022-07-04 20:16:20,988 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 9.32 sec
MapReduce Total cumulative CPU time: 9 seconds 320 msec
Ended Job = job_1656670722551_0045
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 9.32 sec   HDFS Read: 27059 HDFS Write: 103 SUCCESS
Total MapReduce CPU Time Spent: 9 seconds 320 msec
OK
122
Time taken: 82.706 seconds, Fetched: 1 row(s)

hive> select * from custmaster limit 10;
OK
103	Atelier graphique	Schmitt	Carine 	40.32.2555	54, rue Royale	null	Nantes	null	44000	France	1370	21000.00
112	Signal Gift Stores	King	Jean	7025551838	8489 Strong St.	null	Las Vegas	NV	83030	USA	1166	71800.00
114	Australian Collectors, Co.	Ferguson	Peter	03 9520 4555	636 St Kilda Road	Level 3	Melbourne	Victoria	3004	Australia	1611117300.00
119	La Rochelle Gifts	Labrune	Janine 	40.67.8555	67, rue des Cinquante Otages	null	Nantes	null	44000	France	1370	118200.00
121	Baane Mini Imports	Bergulfsen	Jonas 	07-98 9555	Erling Skakkes gate 78	null	Stavern	null	4110	Norway	1504	81700.00
124	Mini Gifts Distributors Ltd.	Nelson	Susan	4155551450	5677 Strong St.	null	San Rafael	CA	97562	USA	1165	210500.00
125	Havel & Zbyszek Co	Piestrzeniewicz	Zbyszek 	(26) 642-7555	ul. Filtrowa 68	null	Warszawa	null	01-012	Poland	null	0.00
128	Blauer See Auto, Co.	Keitel	Roland	+49 69 66 90 2555	Lyonerstr. 34	null	Frankfurt	null	60528	Germany	1504	59700.00
129	Mini Wheels Co.	Murphy	Julie	6505555787	5557 North Pendale Street	null	San Francisco	CA	94217	USA	1165	64600.00
131	Land of Toys Inc.	Lee	Kwai	2125557818	897 Long Airport Avenue	null	NYC	NY	10022	USA	1323	114900.00
Time taken: 0.327 seconds, Fetched: 10 row(s)

```

4. Copy the payments.txt into hdfs location /user/hduser/paymentsdata/ and Create an external table
namely payments with customernumber, checknumber, paymentdate, amount columns to point the imported payments data.

create external table payments (customernumber INT, checknumber STRING, paymentdate DATE, amount DOUBLE) 
row format delimited  
fields terminated by ',' 
stored as textfile 
location '/user/hduser/paymentsdata';

``` 

[hduser@localhost ~]$ hadoop fs -mkdir -p /user/hduser/paymentsdata/

[hduser@localhost ~]$ hadoop fs -put -f /home/hduser/hiveusecases/payments.txt /user/hduser/paymentsdata/

[hduser@localhost ~]$ hadoop fs -text /user/hduser/paymentsdata/* | wc -l
272

[hduser@localhost ~]$ hadoop fs -text /user/hduser/paymentsdata/* | head
103,HQ336336,2016-10-19,6066.78
103,JM555205,2016-10-05,14571.44
103,OM314933,2016-10-18,1676.14
112,BO864823,2016-10-17,14191.12
112,HQ55022,2016-10-06,32641.98
112,ND748579,2016-10-20,33347.88
114,GG31455,2016-10-20,45864.03
114,MA765515,2016-10-15,82261.22
114,NP603840,2016-10-31,7565.08
114,NR27552,2016-10-10,44894.74

hive> create external table payments (customernumber INT, checknumber STRING, paymentdate DATE, amount DOUBLE) 
    > row format delimited  
    > fields terminated by ',' 
    > stored as textfile 
    > location '/user/hduser/paymentsdata';
OK
Time taken: 0.218 seconds

hive> select count(*) from payments;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = hduser_20220704184643_d67e8e75-9df2-42b9-a818-3950bec88432
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Starting Job = job_1656670722551_0043, Tracking URL = http://Inceptez:8088/proxy/application_1656670722551_0043/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1656670722551_0043
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
2022-07-04 18:47:31,993 Stage-1 map = 0%,  reduce = 0%
2022-07-04 18:47:47,624 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 3.48 sec
2022-07-04 18:48:02,142 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 8.45 sec
MapReduce Total cumulative CPU time: 8 seconds 450 msec
Ended Job = job_1656670722551_0043
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 8.45 sec   HDFS Read: 17613 HDFS Write: 103 SUCCESS
Total MapReduce CPU Time Spent: 8 seconds 450 msec
OK
273
Time taken: 81.176 seconds, Fetched: 1 row(s)

hive> select * from payments limit 10;
OK
103	HQ336336	2016-10-19	6066.78
103	JM555205	2016-10-05	14571.44
103	OM314933	2016-10-18	1676.14
112	BO864823	2016-10-17	14191.12
112	HQ55022	2016-10-06	32641.98
112	ND748579	2016-10-20	33347.88
114	GG31455	2016-10-20	45864.03
114	MA765515	2016-10-15	82261.22
114	NP603840	2016-10-31	7565.08
114	NR27552	2016-10-10	44894.74
Time taken: 0.336 seconds, Fetched: 10 row(s)

```

5. Create an external table called cust_payments in avro format and load data by doing inner join of
custmaster and payments tables, using insert select customernumber, contactfirstname, contactlastname, 
phone, creditlimit from custmaster and paymentdate, amount columns from payments table

**Csv serde table join payments external table -> cust_payments (avro)**

-- Create an external table called cust_payments in avro format

create table cust_payments
(customernumber INT, contactfirstname STRING, contactlastname STRING, phone STRING, 
creditlimit DOUBLE, paymentdate DATE, amount DOUBLE) 
row format delimited 
fields terminated by ',' 
lines terminated by '\n' 
stored as avrofile;

-- load data by doing inner join of custmaster and payments tables

Insert into table cust_payments 
select c.customernumber, c.contactfirstname, c.contactlastname, c.phone, c.creditlimit, p.paymentdate, p.amount  
from custmaster c JOIN payments p on (c.customernumber = p.customernumber);

``` 
hive> 
    > create table cust_payments
    > (customernumber INT, contactfirstname STRING, contactlastname STRING, phone STRING, 
    > creditlimit DOUBLE, paymentdate DATE, amount DOUBLE) 
    > row format delimited 
    > fields terminated by ',' 
    > lines terminated by '\n' 
    > stored as avrofile;
OK
Time taken: 0.484 seconds

hive> 
    > Insert into table cust_payments 
    > select c.customernumber, c.contactfirstname, c.contactlastname, c.phone, c.creditlimit, p.paymentdate, p.amount  
    > from custmaster c JOIN payments p on (c.customernumber = p.customernumber);
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = hduser_20220704203926_05ac6e4f-29a9-4f97-94bd-e9efd3bb33f0
Total jobs = 1
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/usr/local/hive/lib/log4j-slf4j-impl-2.6.2.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/usr/local/hadoop/share/hadoop/common/lib/slf4j-log4j12-1.7.10.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]
2022-07-04 20:39:47	Starting to launch local task to process map join;	maximum memory = 477626368
2022-07-04 20:39:51	Dump the side-table for tag: 1 with group count: 98 into file: file:/tmp/hduser/8030768b-c35d-4a74-8aac-49bb092f17a7/hive_2022-07-04_20-39-26_037_2432313088014818381-1/-local-10002/HashTable-Stage-4/MapJoin-mapfile31--.hashtable
2022-07-04 20:39:51	Uploaded 1 File to: file:/tmp/hduser/8030768b-c35d-4a74-8aac-49bb092f17a7/hive_2022-07-04_20-39-26_037_2432313088014818381-1/-local-10002/HashTable-Stage-4/MapJoin-mapfile31--.hashtable (6719 bytes)
2022-07-04 20:39:51	End of local task; Time Taken: 3.884 sec.
Execution completed successfully
MapredLocal task succeeded
Launching Job 1 out of 1
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1656670722551_0049, Tracking URL = http://Inceptez:8088/proxy/application_1656670722551_0049/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1656670722551_0049
Hadoop job information for Stage-4: number of mappers: 1; number of reducers: 0
2022-07-04 20:40:40,921 Stage-4 map = 0%,  reduce = 0%
2022-07-04 20:40:57,669 Stage-4 map = 100%,  reduce = 0%, Cumulative CPU 7.25 sec
MapReduce Total cumulative CPU time: 7 seconds 250 msec
Ended Job = job_1656670722551_0049
Loading data to table custdb.cust_payments
MapReduce Jobs Launched: 
Stage-Stage-4: Map: 1   Cumulative CPU: 7.25 sec   HDFS Read: 27520 HDFS Write: 15729 SUCCESS
Total MapReduce CPU Time Spent: 7 seconds 250 msec
OK
Time taken: 95.745 seconds

hive> select count(*) from cust_payments;
OK
273
Time taken: 0.502 seconds, Fetched: 1 row(s)

```

What is view? - View is a stored Query.
View Benefits:
* View doesn’t contains data, rather it stores only query.
* Reducing the complexity of writing queries, because view stores complex queries.
* Performance benefit – when create the view apply the performance accordingly (eg: use
   partition column in the filter)
* Security – Enable only the columns/rows to the intended users.

6. Create a view called custpayments_vw to only display customernumber,creditlimit,paymentdate and
amount selected from cust_payments.

Create view if not exists custpayments_vw AS select customernumber, creditlimit, paymentdate, amount from cust_payments;

``` 
hive> Create view if not exists custpayments_vw AS select customernumber, creditlimit, paymentdate, amount from cust_payments;
OK
Time taken: 1.089 seconds
hive> select count(*) from custpayments_vw;
OK
273
Time taken: 0.557 seconds, Fetched: 1 row(s)
hive> select * from custpayments_vw limit 5;
OK
103	21000.0	2016-10-19	6066.78
103	21000.0	2016-10-05	14571.44
103	21000.0	2016-10-18	1676.14
112	71800.0	2016-10-17	14191.12
112	71800.0	2016-10-06	32641.98
Time taken: 0.399 seconds, Fetched: 5 row(s)

```

7. Extract only customernumber, creditlimit,paymentdate and amount columns either using the above view/cust_payments table into 
hdfs location /user/hduser/custpaymentsexport with '|' delimiter.

Note: Achieve the above scenario using insert overwrite directory option or one more option to achieve
this, try that out and let me know what is that?

**Option 1:**

insert overwrite 
directory '/user/hduser/custpaymentsexport' 
row format delimited 
fields terminated by '|' 
select * from custpayments_vw;

hadoop fs -text /user/hduser/custpaymentsexport/* | head 

``` 
hive> insert overwrite 
    > directory '/user/hduser/custpaymentsexport' 
    > row format delimited 
    > fields terminated by '|' 
    > select * from custpayments_vw;
Query ID = hduser_20220705075348_c123cc23-2d02-4e65-8669-f6cadffe6146
Total jobs = 3
Launching Job 1 out of 3
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1656670722551_0050, Tracking URL = http://Inceptez:8088/proxy/application_1656670722551_0050/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1656670722551_0050
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 0
2022-07-05 07:55:13,235 Stage-1 map = 0%,  reduce = 0%
2022-07-05 07:55:36,765 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 7.2 sec
MapReduce Total cumulative CPU time: 7 seconds 200 msec
Ended Job = job_1656670722551_0050
Stage-3 is selected by condition resolver.
Stage-2 is filtered out by condition resolver.
Stage-4 is filtered out by condition resolver.
Moving data to directory hdfs://localhost:54310/user/hduser/custpaymentsexport/.hive-staging_hive_2022-07-05_07-53-48_721_4168516693548010840-1/-ext-10000
Moving data to directory /user/hduser/custpaymentsexport
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1   Cumulative CPU: 7.2 sec   HDFS Read: 28548 HDFS Write: 8755 SUCCESS
Total MapReduce CPU Time Spent: 7 seconds 200 msec
OK
Time taken: 109.287 seconds

[hduser@localhost ~]$ hadoop fs -text /user/hduser/custpaymentsexport/* | head 
103|21000.0|2016-10-19|6066.78
103|21000.0|2016-10-05|14571.44
103|21000.0|2016-10-18|1676.14
112|71800.0|2016-10-17|14191.12
112|71800.0|2016-10-06|32641.98
112|71800.0|2016-10-20|33347.88
114|117300.0|2016-10-20|45864.03
114|117300.0|2016-10-15|82261.22
114|117300.0|2016-10-31|7565.08
114|117300.0|2016-10-10|44894.74

[hduser@localhost ~]$ hadoop fs -text /user/hduser/custpaymentsexport/* | wc -l
273

```

**Option 2:** : CTAS

We can create external table with row format delimited as | and location as /user/hduser/custpaymentsexport.. then insert select from cust_payments table.. finally drop this external table after insert select

CREATE TABLE custpaymentsexport 
    row format delimited 
    fields terminated by '|' 
    STORED AS TEXTFILE 
    LOCATION '/user/hduser/custpaymentsexport1'
AS select * from custpayments_vw;


```  
hive> 
    > CREATE TABLE custpaymentsexport 
    >     row format delimited 
    >     fields terminated by '|' 
    >     STORED AS TEXTFILE 
    >     LOCATION '/user/hduser/custpaymentsexport1'
    > AS select * from custpayments_vw;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = hduser_20220705081421_b6b2a6b7-6ffe-4e6a-beb3-47d2198dffd8
Total jobs = 3
Launching Job 1 out of 3
Number of reduce tasks is set to 0 since there's no reduce operator
Starting Job = job_1656670722551_0051, Tracking URL = http://Inceptez:8088/proxy/application_1656670722551_0051/
Kill Command = /usr/local/hadoop/bin/hadoop job  -kill job_1656670722551_0051
Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 0
2022-07-05 08:15:19,039 Stage-1 map = 0%,  reduce = 0%
2022-07-05 08:15:43,719 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 7.34 sec
MapReduce Total cumulative CPU time: 7 seconds 340 msec
Ended Job = job_1656670722551_0051
Stage-4 is selected by condition resolver.
Stage-3 is filtered out by condition resolver.
Stage-5 is filtered out by condition resolver.
Moving data to directory hdfs://localhost:54310/user/hduser/custpaymentsexport1/.hive-staging_hive_2022-07-05_08-14-21_037_5448376573597506499-1/-ext-10002
Moving data to directory /user/hduser/custpaymentsexport1
MapReduce Jobs Launched: 
Stage-Stage-1: Map: 1   Cumulative CPU: 7.34 sec   HDFS Read: 28739 HDFS Write: 8839 SUCCESS
Total MapReduce CPU Time Spent: 7 seconds 340 msec
OK
Time taken: 87.512 seconds

hive> select count(*) from custpaymentsexport;
OK
273
Time taken: 0.356 seconds, Fetched: 1 row(s)

[hduser@localhost ~]$ hadoop fs -text /user/hduser/custpaymentsexport1/* | head 
103|21000.0|2016-10-19|6066.78
103|21000.0|2016-10-05|14571.44
103|21000.0|2016-10-18|1676.14
112|71800.0|2016-10-17|14191.12
112|71800.0|2016-10-06|32641.98
112|71800.0|2016-10-20|33347.88
114|117300.0|2016-10-20|45864.03
114|117300.0|2016-10-15|82261.22
114|117300.0|2016-10-31|7565.08
114|117300.0|2016-10-10|44894.74

[hduser@localhost ~]$ hadoop fs -text /user/hduser/custpaymentsexport1/* | wc -l
273

```

8. Export the data from the /user/hduser/custpaymentsexport location to mysql table called
cust_payments using sqoop export with staging table option using records per statement 100 and
mappers 3.

sqoop export --connect jdbc:mysql://cxln2.c.thelab-240901.internal/sqoopex 
-m 1 --table sales_sgiri --export-dir /apps/hive/warehouse/sg.db/sales_test --input-fields-terminated-by ',' 
--username sqoopuser --password NHkkP876rp;

sqoop export \
    --driver com.mysql.cj.jdbc.Driver \
    --connect jdbc:mysql://localhost/custpayments \
    --username root \
    --password Root123$ \
    --export-dir /user/hduser/custpaymentsexport \
    --input-fields-terminated-by '|' \
    -m 3 \
    --table cust_payments ;

use custpayments;

CREATE TABLE cust_payments (
  customerNumber int(11),
  creditLimit decimal(10,2),
  paymentdate date,
  amount decimal(10,2)
);

show tables;

select * from cust_payments;

``` 

mysql> use custpayments;
Database changed
mysql> CREATE TABLE cust_payments (
    ->   customerNumber int(11),
    ->   creditLimit decimal(10,2),
    ->   paymentdate date,
    ->   amount decimal(10,2)
    -> );
Query OK, 0 rows affected, 1 warning (0.08 sec)

mysql> show tables;
+------------------------+
| Tables_in_custpayments |
+------------------------+
| cust_payments          |
| customers              |
+------------------------+
2 rows in set (0.01 sec)

[hduser@localhost ~]$ sqoop export \
>     --driver com.mysql.cj.jdbc.Driver \
>     --connect jdbc:mysql://localhost/custpayments \
>     --username root \
>     --password Root123$ \
>     --export-dir /user/hduser/custpaymentsexport \
>     --input-fields-terminated-by '|' \
>     -m 3 \
>     --table cust_payments ;
Warning: /usr/local/hbase does not exist! HBase imports will fail.
Please set $HBASE_HOME to the root of your HBase installation.
Warning: /usr/local/sqoop/../hcatalog does not exist! HCatalog jobs will fail.
Please set $HCAT_HOME to the root of your HCatalog installation.
Warning: /usr/local/sqoop/../accumulo does not exist! Accumulo imports will fail.
Please set $ACCUMULO_HOME to the root of your Accumulo installation.
Warning: /usr/local/sqoop/../zookeeper does not exist! Accumulo imports will fail.
Please set $ZOOKEEPER_HOME to the root of your Zookeeper installation.
22/07/05 08:53:56 INFO sqoop.Sqoop: Running Sqoop version: 1.4.6
22/07/05 08:53:56 WARN tool.BaseSqoopTool: Setting your password on the command-line is insecure. Consider using -P instead.
22/07/05 08:53:56 WARN sqoop.ConnFactory: Parameter --driver is set to an explicit driver however appropriate connection manager is not being set (via --connection-manager). Sqoop is going to fall back to org.apache.sqoop.manager.GenericJdbcManager. Please specify explicitly which connection manager should be used next time.
22/07/05 08:53:56 INFO manager.SqlManager: Using default fetchSize of 1000
22/07/05 08:53:56 INFO tool.CodeGenTool: Beginning code generation
22/07/05 08:53:59 INFO manager.SqlManager: Executing SQL statement: SELECT t.* FROM cust_payments AS t WHERE 1=0
22/07/05 08:53:59 INFO manager.SqlManager: Executing SQL statement: SELECT t.* FROM cust_payments AS t WHERE 1=0
22/07/05 08:53:59 INFO orm.CompilationManager: HADOOP_MAPRED_HOME is /usr/local/hadoop
Note: /tmp/sqoop-hduser/compile/530bba55ebf182c03645668a5dc149f3/cust_payments.java uses or overrides a deprecated API.
Note: Recompile with -Xlint:deprecation for details.
22/07/05 08:54:04 INFO orm.CompilationManager: Writing jar file: /tmp/sqoop-hduser/compile/530bba55ebf182c03645668a5dc149f3/cust_payments.jar
22/07/05 08:54:04 INFO mapreduce.ExportJobBase: Beginning export of cust_payments
22/07/05 08:54:06 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
22/07/05 08:54:06 INFO Configuration.deprecation: mapred.jar is deprecated. Instead, use mapreduce.job.jar
22/07/05 08:54:08 INFO manager.SqlManager: Executing SQL statement: SELECT t.* FROM cust_payments AS t WHERE 1=0
22/07/05 08:54:08 INFO Configuration.deprecation: mapred.reduce.tasks.speculative.execution is deprecated. Instead, use mapreduce.reduce.speculative
22/07/05 08:54:08 INFO Configuration.deprecation: mapred.map.tasks.speculative.execution is deprecated. Instead, use mapreduce.map.speculative
22/07/05 08:54:08 INFO Configuration.deprecation: mapred.map.tasks is deprecated. Instead, use mapreduce.job.maps
22/07/05 08:54:09 INFO client.RMProxy: Connecting to ResourceManager at /0.0.0.0:8032
22/07/05 08:54:17 INFO input.FileInputFormat: Total input paths to process : 1
22/07/05 08:54:17 INFO input.FileInputFormat: Total input paths to process : 1
22/07/05 08:54:17 INFO mapreduce.JobSubmitter: number of splits:3
22/07/05 08:54:17 INFO Configuration.deprecation: mapred.map.tasks.speculative.execution is deprecated. Instead, use mapreduce.map.speculative
22/07/05 08:54:18 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1656670722551_0053
22/07/05 08:54:19 INFO impl.YarnClientImpl: Submitted application application_1656670722551_0053
22/07/05 08:54:19 INFO mapreduce.Job: The url to track the job: http://Inceptez:8088/proxy/application_1656670722551_0053/
22/07/05 08:54:19 INFO mapreduce.Job: Running job: job_1656670722551_0053
22/07/05 08:54:35 INFO mapreduce.Job: Job job_1656670722551_0053 running in uber mode : false
22/07/05 08:54:35 INFO mapreduce.Job:  map 0% reduce 0%
22/07/05 08:55:02 INFO mapreduce.Job:  map 33% reduce 0%
22/07/05 08:55:05 INFO mapreduce.Job:  map 100% reduce 0%
22/07/05 08:55:06 INFO mapreduce.Job: Job job_1656670722551_0053 completed successfully
22/07/05 08:55:06 INFO mapreduce.Job: Counters: 30
	File System Counters
		FILE: Number of bytes read=0
		FILE: Number of bytes written=400818
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=13091
		HDFS: Number of bytes written=0
		HDFS: Number of read operations=15
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=0
	Job Counters 
		Launched map tasks=3
		Data-local map tasks=3
		Total time spent by all maps in occupied slots (ms)=76592
		Total time spent by all reduces in occupied slots (ms)=0
		Total time spent by all map tasks (ms)=76592
		Total vcore-seconds taken by all map tasks=76592
		Total megabyte-seconds taken by all map tasks=78430208
	Map-Reduce Framework
		Map input records=273
		Map output records=273
		Input split bytes=508
		Spilled Records=0
		Failed Shuffles=0
		Merged Map outputs=0
		GC time elapsed (ms)=1286
		CPU time spent (ms)=10940
		Physical memory (bytes) snapshot=510136320
		Virtual memory (bytes) snapshot=6335700992
		Total committed heap usage (bytes)=358612992
	File Input Format Counters 
		Bytes Read=0
	File Output Format Counters 
		Bytes Written=0
22/07/05 08:55:06 INFO mapreduce.ExportJobBase: Transferred 12.7842 KB in 57.5763 seconds (227.3677 bytes/sec)
22/07/05 08:55:06 INFO mapreduce.ExportJobBase: Exported 273 records.

mysql> select count(*) from cust_payments;
+----------+
| count(*) |
+----------+
|      273 |
+----------+
1 row in set (0.01 sec)

mysql> select * from cust_payments limit 10;
+----------------+-------------+-------------+----------+
| customerNumber | creditLimit | paymentdate | amount   |
+----------------+-------------+-------------+----------+
|            181 |    76400.00 | 2016-10-16  | 44400.50 |
|            186 |    96500.00 | 2016-10-10  | 23602.90 |
|            186 |    96500.00 | 2016-10-27  | 37602.48 |
|            186 |    96500.00 | 2016-10-21  | 34341.08 |
|            187 |   136800.00 | 2016-10-03  | 52825.29 |
|            187 |   136800.00 | 2016-10-08  | 47159.11 |
|            187 |   136800.00 | 2016-10-27  | 48425.69 |
|            189 |    69400.00 | 2016-10-03  | 17359.53 |
|            189 |    69400.00 | 2016-10-01  | 32538.74 |
|            198 |    23000.00 | 2016-10-06  |  9658.74 |
+----------------+-------------+-------------+----------+
10 rows in set (0.00 sec)

```
