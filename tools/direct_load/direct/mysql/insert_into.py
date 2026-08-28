#/usr/bin/evn python
#-*- encoding:utf-8 -*-
#创建几张不同表，load data对test1导入数据，随机抽取一张表insert into，truncate test1，循环次数根据创建表的张数n*n-1决定
import csv
import re
import os
import threading
import random
import time
import sys
import commands
import argparse
import sys


def error(result, sql):
        if ("ERROR" in result[1]):
                print("ATTENTION!! excute ERROR: " + sql + result[1])
                sys.exit()
        else:
                print("excute SUCCESS: " + sql)


def success(result, sql):
        if ("ERROR" not in result[1]):
                print("ATTENTION!! excute ERROR(预期外的执行成功): " + sql)
                sys.exit()
        else:
                print("excute 预期内报错: " + sql)

def insert(host,port,tenant,database,directory):
	#conn = jaydebeapi.connect(driver, url, [user,password], jarFile)
        #curs = conn.cursor()
	cur_num=1
	next_num=1
	num_table=10
	insert_sql='insert /*+ append enable_parallel_dml parallel(5)*/ into t'
	truncate_sql='truncate table t'
	
	for i in range(1,num_table*(num_table-1)):
		while(next_num==cur_num):
			next_num=random.randint(1, num_table) 		
		insert_sql= insert_sql+str(next_num)+' select * from t'+str(cur_num)
		truncate_sql= truncate_sql+str(cur_num)
		print(insert_sql+"开始")

		cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, insert_sql)
		result = commands.getstatusoutput(cmd_str)
		error(result,insert_sql)
		cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, truncate_sql)
		result = commands.getstatusoutput(cmd_str)
		error(result,truncate_sql)
		insert_sql='insert /*+ enable_parallel_dml parallel(5) append */ into t'
        	truncate_sql='truncate table t'
		cur_num=next_num
	truncate_sql = truncate_sql + str(cur_num)
	cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, truncate_sql)
	result = commands.getstatusoutput(cmd_str)
	error(result,truncate_sql)
#加聚合函数等的insert
	

def insert_fun(host,port,tenant,database,directory):
        cur_num=1
        next_num=1
        num_table=10
        insert_sql='insert /*+ enable_parallel_dml parallel(7) append */ into t'
        truncate_sql='truncate table t'
	print("带聚合函数的insert into")
        for i in range(1,num_table*(num_table-1)):
                while(next_num==cur_num):
                        next_num=random.randint(1, num_table)

                insert_sql= insert_sql+str(next_num)+' select c1,sum(c8/2),substr(c3,1,10),c4,c5,c6,c7,SQRT(UNIX_TIMESTAMP(c5)),c9,c10,c11,c13,c14,c15,rand(),c17, CONCAT(c1,c3) from t'+str(cur_num)+' group by c2,c1 having c6>1 order by c1 limit 1000000'
                truncate_sql= truncate_sql+str(cur_num)
                print(insert_sql+"开始")
		cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, insert_sql)
		result = commands.getstatusoutput(cmd_str)
		error(result,insert_sql)
		cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, truncate_sql)
		result = commands.getstatusoutput(cmd_str)
		error(result,truncate_sql)
                insert_sql='insert /*+ enable_parallel_dml parallel(7) append */ into t'
                truncate_sql='truncate table t'
                cur_num=next_num

		
def createTable(host,port,tenant,database,directory):
	create_sql="CREATE TABLE t (  c1 varchar(10) NOT NULL,  c2 bigint(20) NOT NULL,  c3 char(10) NOT NULL,  c4 date NOT NULL,  c5 datetime NOT NULL,  c6 decimal(5,2) NOT NULL,  c7 double NOT NULL,  c8 int(11) NOT NULL,  c9 smallint(6) NOT NULL,  c10 time NOT NULL,  c11 tinyint(4) NOT NULL,   c13 json DEFAULT NULL,   c14 mediumint(9) DEFAULT NULL,   c15 mediumtext DEFAULT NULL,   c16 float DEFAULT NULL, c17 enum('M','F') DEFAULT NULL,   md5 varchar(128) DEFAULT NULL,PRIMARY KEY (c1))"
	create_sql1="CREATE TABLE t1 (  c1 varchar(10) NOT NULL,  c2 bigint(20) NOT NULL,  c3 char(10) NOT NULL,  c4 date NOT NULL,  c5 datetime NOT NULL,  c6 decimal(5,2) NOT NULL,  c7 double NOT NULL,  c8 int(11) NOT NULL,  c9 smallint(6) NOT NULL,  c10 time NOT NULL,  c11 tinyint(4) NOT NULL, c13 json DEFAULT NULL,   c14 mediumint(9) DEFAULT NULL,   c15 mediumtext DEFAULT NULL,   c16 float DEFAULT NULL, c17 enum('M','F') DEFAULT NULL,   md5 varchar(128)  DEFAULT NULL,PRIMARY KEY (c1))"
	create_sql2="CREATE TABLE t2 (  c1 varchar(10) NOT NULL,  c2 bigint(20) NOT NULL,  c3 char(10) NOT NULL,  c4 date NOT NULL,  c5 datetime NOT NULL,  c6 decimal(5,2) NOT NULL,  c7 double NOT NULL,  c8 int(11) NOT NULL,  c9 smallint(6) NOT NULL,  c10 time NOT NULL,  c11 tinyint(4) NOT NULL,  c13 json DEFAULT NULL,   c14 mediumint(9) DEFAULT NULL,   c15 mediumtext DEFAULT NULL,   c16 float DEFAULT NULL, c17 enum('M','F') DEFAULT NULL,   md5 varchar(128) DEFAULT NULL)"

	create_sql3="CREATE TABLE t3 (  c1 varchar(10) NOT NULL,  c2 bigint(20) NOT NULL,  c3 char(10) NOT NULL,  c4 date NOT NULL,  c5 datetime NOT NULL,  c6 decimal(5,2) NOT NULL,  c7 double NOT NULL,  c8 int(11) NOT NULL,  c9 smallint(6) NOT NULL,  c10 time NOT NULL,  c11 tinyint(4) NOT NULL, c13 json DEFAULT NULL,   c14 mediumint(9) DEFAULT NULL,   c15 mediumtext DEFAULT NULL,   c16 float DEFAULT NULL, c17 enum('M','F') DEFAULT NULL,   md5 varchar(128) DEFAULT NULL) partition by hash(c2) partitions 500"
    
	create_sql4="CREATE TABLE t4 (  c1 varchar(10) NOT NULL,  c2 bigint(20) NOT NULL,  c3 char(10) NOT NULL,  c4 date NOT NULL,  c5 datetime NOT NULL,  c6 decimal(5,2) NOT NULL,  c7 double NOT NULL,  c8 int(11) NOT NULL,  c9 smallint(6) NOT NULL,  c10 time NOT NULL,  c11 tinyint(4) NOT NULL,  c13 json DEFAULT NULL,   c14 mediumint(9) DEFAULT NULL,   c15 mediumtext DEFAULT NULL,   c16 float DEFAULT NULL, c17 enum('M','F') DEFAULT NULL,   md5 varchar(128) DEFAULT NULL,UNIQUE KEY (c1,c2)) partition by hash(c2) partitions 500"
    
	create_sql5="CREATE TABLE t5 (  c1 varchar(10) NOT NULL,  c2 bigint(20) NOT NULL,  c3 char(10) NOT NULL,  c4 date NOT NULL,  c5 datetime NOT NULL,  c6 decimal(5,2) NOT NULL,  c7 double NOT NULL,  c8 int(11) NOT NULL,  c9 smallint(6) NOT NULL,  c10 time NOT NULL,  c11 tinyint(4) NOT NULL,  c13 json DEFAULT NULL,   c14 mediumint(9) DEFAULT NULL,   c15 mediumtext DEFAULT NULL,   c16 float DEFAULT NULL, c17 enum('M','F') DEFAULT NULL,   md5 varchar(128) DEFAULT NULL,PRIMARY KEY (c1,c8),index k1  (c4,c2,c1) global partition by hash(c2) partitions 50) partition by hash(c8) partitions 50"
    
	create_sql6="CREATE TABLE t6 (  c1 varchar(10) NOT NULL,  c2 bigint(20) NOT NULL,  c3 char(10) NOT NULL,  c4 date NOT NULL,  c5 datetime NOT NULL,  c6 decimal(5,2) NOT NULL,  c7 double NOT NULL,  c8 int(11) NOT NULL,  c9 smallint(6) NOT NULL,  c10 time NOT NULL,  c11 tinyint(4) NOT NULL,  c13 json DEFAULT NULL,   c14 mediumint(9) DEFAULT NULL,   c15 mediumtext DEFAULT NULL,   c16 float DEFAULT NULL, c17 enum('M','F') DEFAULT NULL,   md5 varchar(128) DEFAULT NULL,PRIMARY KEY (c1,c8),UNIQUE KEY (c1,c2,c8),index k1  (c4,c2,c1) global partition by hash(c2) partitions 50) partition by hash(c8) partitions 50"
    
	create_sql7="CREATE TABLE t7 (  c1 varchar(10) NOT NULL,  c2 bigint(20) NOT NULL,  c3 char(10) NOT NULL,  c4 date NOT NULL,  c5 datetime NOT NULL,  c6 decimal(5,2) NOT NULL,  c7 double NOT NULL,  c8 int(11) NOT NULL,  c9 smallint(6) NOT NULL,  c10 time NOT NULL,  c11 tinyint(4) NOT NULL,  c13 json DEFAULT NULL,   c14 mediumint(9) DEFAULT NULL,   c15 mediumtext DEFAULT NULL,   c16 float DEFAULT NULL, c17 enum('M','F') DEFAULT NULL,   md5 varchar(128) DEFAULT NULL,PRIMARY KEY (c1,c8),UNIQUE KEY (c1,c2,c8),index k1  (c4,c2,c1) global partition by hash(c2) partitions 50) partition by hash(c8) SUBPARTITION BY key(c1)    SUBPARTITIONS 2 partitions 50"
    
	create_sql8="CREATE TABLE t8 (  c1 varchar(10) NOT NULL,  c2 bigint(20) NOT NULL,  c3 char(10) NOT NULL,  c4 date NOT NULL,  c5 datetime NOT NULL,  c6 decimal(5,2) NOT NULL,  c7 double NOT NULL,  c8 int(11) NOT NULL,  c9 smallint(6) NOT NULL,  c10 time NOT NULL,  c11 tinyint(4) NOT NULL,  c13 json DEFAULT NULL,   c14 mediumint(9) DEFAULT NULL,   c15 mediumtext DEFAULT NULL,   c16 float DEFAULT NULL, c17 enum('M','F') DEFAULT NULL,   md5 varchar(128) DEFAULT NULL,PRIMARY KEY (c1,c8),UNIQUE KEY (c1,c2,c8),index k1  (c4,c2,c1) global partition by key(c1) partitions 50) partition by hash(c8)  partitions 50"
    
	create_sql9="CREATE TABLE t9 (  c1 varchar(10) NOT NULL,  c2 bigint(20) NOT NULL,  c3 char(10) NOT NULL,  c4 date NOT NULL,  c5 datetime NOT NULL,  c6 decimal(5,2) NOT NULL,  c7 double NOT NULL,  c8 int(11) NOT NULL,  c9 smallint(6) NOT NULL,  c10 time NOT NULL,  c11 tinyint(4) NOT NULL,  c13 json DEFAULT NULL,   c14 mediumint(9) DEFAULT NULL,   c15 mediumtext DEFAULT NULL,   c16 float DEFAULT NULL, c17 enum('M','F') DEFAULT NULL,   md5 varchar(128) DEFAULT NULL,PRIMARY KEY (c1,c8),UNIQUE KEY (c1,c2,c8),index k1  (c4,c2,c1) global partition by key(c1) partitions 50) partition by range(c8) (partition p1 values less than(100),partition p2 values less than(200),partition p3 values less than(300),partition p4 values less than(400),partition p5 values less than(500),partition p6 values less than(600),partition p7 values less than(700),partition p8 values less than(800),partition p9 values less than(900),partition p10 values less than(1000),partition p11 values less than(1100),partition p12 values less than(1200),partition p13 values less than(1300),partition p14 values less than(1400),partition p15 values less than(1500),partition p16 values less than(1600),partition p17 values less than(1700),partition p18 values less than(1800),partition p19 values less than(1900),partition p20 values less than(2000),partition pmax values less than maxvalue)"
    
	create_sql10="CREATE TABLE t10 (  c1 varchar(10) NOT NULL,  c2 bigint(20) NOT NULL,  c3 char(10) NOT NULL,  c4 date NOT NULL,  c5 datetime NOT NULL,  c6 decimal(5,2) NOT NULL,  c7 double NOT NULL,  c8 int(11) NOT NULL,  c9 smallint(6) NOT NULL,  c10 time NOT NULL,  c11 tinyint(4) NOT NULL,  c13 json DEFAULT NULL,   c14 mediumint(9) DEFAULT NULL,   c15 mediumtext DEFAULT NULL,   c16 float DEFAULT NULL, c17 enum('M','F') DEFAULT NULL,   md5 varchar(128)  DEFAULT NULL,PRIMARY KEY (c1,c8),CONSTRAINT s_id FOREIGN KEY (c1) REFERENCES t (c1)) partition by hash(c8)  partitions 50"#nsert into有外键不走旁路
	cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, create_sql)
	result = commands.getstatusoutput(cmd_str)
    	error(result,create_sql)
	cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, create_sql1)
	result = commands.getstatusoutput(cmd_str)
	error(result,create_sql1)
	cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, create_sql2)
	result = commands.getstatusoutput(cmd_str)
	error(result,create_sql2)	
	cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, create_sql3)
	result = commands.getstatusoutput(cmd_str)
	error(result,create_sql3)
	cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, create_sql4)
	result = commands.getstatusoutput(cmd_str)
	error(result,create_sql4)
	cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, create_sql5)
	result = commands.getstatusoutput(cmd_str)
	error(result,create_sql5)
	cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, create_sql6)
	result = commands.getstatusoutput(cmd_str)
	error(result,create_sql6)
	cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, create_sql7)
	result = commands.getstatusoutput(cmd_str)
	error(result,create_sql7)
	cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, create_sql8)
	result = commands.getstatusoutput(cmd_str)
	error(result,create_sql8)
	cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, create_sql9)
	result = commands.getstatusoutput(cmd_str)
	error(result,create_sql9)
	cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, create_sql10)
	result = commands.getstatusoutput(cmd_str)
	error(result,create_sql10)

def load(host,port,tenant,database,directory):
	load_sql="load data /*+ parallel(10) direct(true, 0) */ infile '"+directory+"/test.1.0.csv,"+directory+"/test.2.0.csv'  into table t fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
	load_sql1="load data /*+ parallel(10) direct(true, 0) */ infile '"+directory+"/test.1.0.csv,"+directory+"/test.2.0.csv'  into table t1 fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
	print("load data")
	cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, load_sql)
	result = commands.getstatusoutput(cmd_str)
	error(result,load_sql)
	cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, load_sql1)
	result = commands.getstatusoutput(cmd_str)
	error(result,load_sql1)
	
def drop(host,port,tenant,database,directory):
	drop_sql='drop table IF EXISTS t'
	print(drop_sql)
        for i in range (1,11):
                drop_sql=drop_sql+str(i)
                print(drop_sql)
		cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, drop_sql)
		result = commands.getstatusoutput(cmd_str)
		error(result,drop_sql)
                drop_sql='drop table IF EXISTS t'
	cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, drop_sql)
	result = commands.getstatusoutput(cmd_str)
	error(result,drop_sql)




def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("-host", "-host", help=" hostname. eg: -t 127.1 ", type=str, dest="host", default='127.1')
	parser.add_argument("-P", "-port", help=" observer port. eg: -P 3952", type=str, dest="port", default=None)
	parser.add_argument("-u", help=" tenant. eg: -u root@l_mysql", type=str, dest="tenant", default=None)
	parser.add_argument("-D", "-database", help=" tenant database. eg: -D test ", type=str, dest="database",default=None)
	parser.add_argument("-directory", "-directory", help=" directory /home/. eg: -directory /home/ ", type=str,dest="directory", default=None)
	args = parser.parse_args()
	
	drop(args.host, args.port, args.tenant, args.database,args.directory)
	createTable(args.host, args.port, args.tenant, args.database,args.directory)
	load(args.host, args.port, args.tenant, args.database, args.directory)
	insert(args.host, args.port, args.tenant, args.database,args.directory)
	insert_fun(args.host, args.port, args.tenant, args.database,args.directory)
	
        
if __name__ == '__main__':
        main()


