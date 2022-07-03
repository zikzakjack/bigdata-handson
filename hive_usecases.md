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

**Create a new Database & Table to load Customer Transactions data**

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


### Data Visualization/Analytics/Aggregation/Reporting/Discovery


