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