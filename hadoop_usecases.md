# HDFS Sample Usecases

___
1. Create a new directory in linux namely ~/install/hdfsusecases and create a new file inside the above directory
namely ~/install/hdfsusecases/NYSE_2020_06_20.txt copying the first 1000 lines from an existing file
~/pigdata/NYSE_daily

```
-- check the present working directory
[hduser@localhost ~]$ pwd
/home/hduser

-- Create a new directory
[hduser@localhost ~]$ mkdir ~/install/hdfsusecases

-- check if directory is created
[hduser@localhost ~]$ ls -l ~/install/hdfsusecases
total 0

-- check if the source file exists
[hduser@localhost ~]$ ls -l ~/pigdata/NYSE_daily
-rw-------. 1 hduser hduser 3194099 Jun 16  2015 /home/hduser/pigdata/NYSE_daily

-- copy first 1000 lines from ~/pigdata/NYSE_daily to ~/install/hdfsusecases/NYSE_2020_06_20.txt
[hduser@localhost ~]$ head -1000 ~/pigdata/NYSE_daily > ~/install/hdfsusecases/NYSE_2020_06_20.txt

-- verify if the new file is created
[hduser@localhost ~]$ ls -l ~/install/hdfsusecases/NYSE_2020_06_20.txt
-rw-rw-r--. 1 hduser hduser 57446 Jun 26 10:08 /home/hduser/install/hdfsusecases/NYSE_2020_06_20.txt

-- verify if the lines count is 1000
[hduser@localhost ~]$ wc -l ~/install/hdfsusecases/NYSE_2020_06_20.txt
1000 /home/hduser/install/hdfsusecases/NYSE_2020_06_20.txt

-- compare 1st line from both files 
[hduser@localhost ~]$ sed -n 1p ~/pigdata/NYSE_daily
NYSE|CLI|2009-12-31|35.39|35.70|34.50|34.57|890100|34.12

[hduser@localhost ~]$ sed -n 1p /home/hduser/install/hdfsusecases/NYSE_2020_06_20.txt
NYSE|CLI|2009-12-31|35.39|35.70|34.50|34.57|890100|34.12

-- compare 1000th line from both files 
[hduser@localhost ~]$ sed -n 1000p ~/pigdata/NYSE_daily
NYSE|CVH|2009-01-21|12.16|12.48|11.92|12.41|3220600|12.41

[hduser@localhost ~]$ sed -n 1000p /home/hduser/install/hdfsusecases/NYSE_2020_06_20.txt
NYSE|CVH|2009-01-21|12.16|12.48|11.92|12.41|3220600|12.41

```
___
2. Create another new file inside the above directory namely ~/install/hdfsusecases/NYSE_2020_06_21.txt copying
the line from 1001 to 2000 from an existing file ~/pigdata/NYSE_daily

``` 

-- copy first 1001 to 2000 lines from ~/pigdata/NYSE_daily to ~/install/hdfsusecases/NYSE_2020_06_21.txt
head -n 2000 ~/pigdata/NYSE_daily | tail -n 1000 > ~/install/hdfsusecases/NYSE_2020_06_21.txt

-- verify if the new file is created
[hduser@localhost ~]$ ls -l ~/install/hdfsusecases/NYSE_2020_06_21.txt
-rw-rw-r--. 1 hduser hduser 55610 Jun 26 10:22 /home/hduser/install/hdfsusecases/NYSE_2020_06_21.txt

-- verify if the lines count is 1000
[hduser@localhost ~]$ wc -l /home/hduser/install/hdfsusecases/NYSE_2020_06_21.txt
1000 /home/hduser/install/hdfsusecases/NYSE_2020_06_21.txt

-- compare 1001 line from source and 1st line from new file 
[hduser@localhost ~]$ sed -n 1001p ~/pigdata/NYSE_daily
NYSE|CVH|2009-01-20|12.62|12.79|11.77|11.92|3424400|11.92

[hduser@localhost ~]$ sed -n 1p /home/hduser/install/hdfsusecases/NYSE_2020_06_21.txt
NYSE|CVH|2009-01-20|12.62|12.79|11.77|11.92|3424400|11.92

-- compare 2000 line from source and 1000th line from new file 
[hduser@localhost ~]$ sed -n 2000p ~/pigdata/NYSE_daily
NYSE|CQB|2009-02-10|14.85|14.99|13.65|13.78|660900|13.78

[hduser@localhost ~]$ sed -n 1000p /home/hduser/install/hdfsusecases/NYSE_2020_06_21.txt
NYSE|CQB|2009-02-10|14.85|14.99|13.65|13.78|660900|13.78

```

___
3. Create a directory in Hadoop namely /tmp/hdfsusecases

```
-- check if the directory already exists
[hduser@localhost ~]$ hadoop fs -ls /tmp/hdfsusecases
ls: `/tmp/hdfsusecases': No such file or directory

-- create directory
[hduser@localhost ~]$ hadoop fs -mkdir -p /tmp/hdfsusecases

-- verify if the directory is created
[hduser@localhost ~]$ hadoop fs -ls -d -h /tmp/hdfsusecases
drwxr-xr-x   - hduser supergroup          0 2022-06-26 11:01 /tmp/hdfsusecases


```


___
4. Check whether the above directory is created in HDFS or not using the below command (Note: We use –test –d
option to check whether the given path is a directory or not)
hadoop fs -test -d /tmp/hdfsusecases

``` 

-- check if the path exists
[hduser@localhost ~]$ hadoop fs -test -e /tmp/hdfsusecases

-- check the exit code
[hduser@localhost ~]$ echo $?
0

-- check if the path is a directory
[hduser@localhost ~]$ hadoop fs -test -d /tmp/hdfsusecases

-- check the exit code
[hduser@localhost ~]$ echo $?
0

```

___
5. Check what is the status code of the above command using, if it shows 0 then directory is created, if shows non
zero then the directory is not created then check the step 3 again.
echo $?

``` 

-- check if the path exists
[hduser@localhost ~]$ hadoop fs -test -e /tmp/hdfsusecases

-- check the exit code
[hduser@localhost ~]$ echo $?
0

-- check if the path is a directory
[hduser@localhost ~]$ hadoop fs -test -d /tmp/hdfsusecases

-- check the exit code
[hduser@localhost ~]$ echo $?
0

```

___
6. Copy file generated only in step 1 (~/install/hdfsusecases/NYSE_2020_06_20.txt) from linux to hdfs directory
/tmp/hdfsusecases in the name of NYSE_2020_06.txt


___
7. Like step 4 and 5, check whether the above file (/tmp/hdfsusecases/NYSE_2020_06.txt) is created or not in HDFS,
using -f option and check for the status code using $? and create a zero byte file in HDFS directory
/tmp/hdfsusecases in the name of _SUCCESS


___
8. Append the file generated in step 2 in linux (~/install/hdfsusecases/NYSE_2020_06_20.txt) with the file generated
in step 6 in the hdfs directory /tmp/hdfsusecases/NYSE_2020_06.txt


___
9. Count the size of the file in HDFS /tmp/hdfsusecases/NYSE_2020_06.txt


___
10. Count the number of rows are there in the /tmp/hdfsusecases/NYSE_2020_06.txt (Which should show the total
count of the files created in step1 and 2)


___
11. Display only line 11 to 20 from the file in HDFS /tmp/hdfsusecases/NYSE_2020_06.txt


___
12. Store line 11 to 20 from the file in HDFS /tmp/hdfsusecases/NYSE_2020_06.txt into linux file namely
~/install/hdfsusecases/NYSE_sampledata1.txt


___
13. Delete the line number 1 from the HDFS file /tmp/hdfsusecases/NYSE_2020_06.txt , for example if the above file
contains 100 rows, after deletion it should have only 99 rows in HDFS
Note: we can’t do this directly because of the WORM property of HDFS data, think about the possible work
around and try to achive the result


___
14. Copy the above file /tmp/hdfsusecases/NYSE_2020_06.txt in the name of

/tmp/hdfsusecases/NYSE_2020_06_bkp.txt


___
15. Merge the files in HDFS /tmp/hdfsusecases/NYSE_2020_06.txt and /tmp/hdfsusecases/NYSE_2020_06_bkp.txt
into Linux directory namely ~/install/hdfsusecases/NYSE_2020_06_merged.txt
Note: We have to use the option called -getmerge to achieve this as given below.
hadoop fs -getmerge /tmp/hdfsusecases/NYSE_2020_06_bkp.txt /tmp/hdfsusecases/NYSE_2020_06.txt
~/install/hdfsusecases/NYSE_2020_06_merged.txt


___
16. Set the blocksize 64MB while writing the file in HDFS, check in the UI how many blocks are generated
hadoop fs -D dfs.block.size=67108864 -put /home/hduser/install/hadoop-2.7.1.tar.gz /user/hduser/


___
17. Set the blocksize 128MB (134217728) for the same file generated in step 16 and replace the existing file in HDFS.


___
18. Set the replication to 3 while writing the file in HDFS
hadoop fs -D dfs.replication=3 -put /home/hduser/install/hadoop-2.7.1.tar.gz /user/hduser/


___
19. To check the block information (In which datanode block is present,no of blocks,size,replication, etc)
hadoop fsck - -files -locations -blocks /user/hduser/hadoop-2.7.1.tar.gz


___
20. Important Command DistCp (distributed copy) is a tool used for copying data between one Hadoop cluster to
another cluster or with in the same cluster using mappers. (Interview Question – how do you copy data from
production Hadoop cluster to Dev Hadoop cluster)
hadoop distcp hdfs://localhost:54310/user/hduser/hadoop-2.7.1.tar.gz
hdfs://localhost:54310/user/hduser/hadoop/


___
21. choose to overwrite the target files unconditionally even if it exists using upto 2 mappers depends
hadoop distcp -overwrite hdfs://localhost:54310/user/hduser/hadoop-2.7.1.tar.gz
hdfs://localhost:54310/user/hduser/hadoop/


___
22. To view the content of editlog file, need to convert into xml file using editlog viewer
hdfs oev -i edits_inprogress_0000000000000009315 -o edittest.xml

___
