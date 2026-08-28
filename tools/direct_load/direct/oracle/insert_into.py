#/usr/bin/evn python
#-*- encoding:utf-8 -*-
#创建几张不同表，load data对test1导入数据，随机抽取一张表insert into，truncate test1，循环次数根据创建表的张数n*n-1决定
import csv
import re
import os
import threading
import random
import commands
import argparse
import sys


def error(result, sql):
        if ("ORA" in result[1]):
                print("ATTENTION!! excute ERROR: " + sql + result[1])
                sys.exit()
        else:
                print("excute SUCCESS: " + sql)


def success(result, sql):
        if ("ORA" not in result[1]):
                print("ATTENTION!! excute ERROR(预期外的执行成功): " + sql)
                sys.exit()
        else:
                print("excute 预期内报错: " + sql)
			
def insert(host,port,user,tenant,password):

	#conn = jaydebeapi.connect(driver, url, [user,password], jarFile)
        #curs = conn.cursor()
	cur_num=1
	next_num=1
	num_table=9
	insert_sql='insert /*+ enable_parallel_dml parallel(10) append */ into t'
	truncate_sql='truncate table t'

	for i in range(1,2*(num_table-1)):
		while(next_num==cur_num):
			next_num=random.randint(1, num_table) 
		
		insert_sql= insert_sql+str(next_num)+' select * from t'+str(cur_num)
		truncate_sql= truncate_sql+str(cur_num)
		print(insert_sql+"开始")
		
		cmd_str=""" obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ %(host,port,user,tenant,password,insert_sql)
		#print cmd_str
		result = commands.getstatusoutput(cmd_str)
		error(result,insert_sql)
		
		cmd_str=""" obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ %(host,port,user,tenant,password,truncate_sql)
        	result = commands.getstatusoutput(cmd_str)
        	error(result,truncate_sql)
		insert_sql='insert /*+ enable_parallel_dml parallel(10) append */ into t'
       		truncate_sql='truncate table t'
		cur_num=next_num
	#curs.close()
        #conn.close()
		
def createTable(host,port,user,tenant,password):
    	create_sql="CREATE TABLE t(c1  char(10) NOT NULL,        c2 date NOT NULL,c3 BINARY_DOUBLE  NOT NULL,c4  date not NULL,c5 int not null,c6 number not null,c7 nvarchar2(60),c8 raw(20) not null,c9 timestamp not null,c10 varchar(10) not null,c11 varchar2(20) not null,c12 blob not null,CONSTRAINT PK PRIMARY KEY(c1))"
	cmd_str = """ obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ % (host, port,user,tenant,password, create_sql)
	result = commands.getstatusoutput(cmd_str)
	print result
	create_sql1="CREATE TABLE t1(c1  char(10) NOT NULL,        c2 date NOT NULL,c3 BINARY_DOUBLE  NOT NULL,c4  date not NULL,c5 int not null,c6 number not null,c7 nvarchar2(60),c8 raw(20) not null,c9 timestamp not null,c10 varchar(10) not null,c11 varchar2(20) not null,c12 blob not null,PRIMARY KEY(c1))"
	cmd_str = """ obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ % (host, port, user, tenant, password, create_sql1)
	result = commands.getstatusoutput(cmd_str)
	print result
    	create_sql2="CREATE TABLE t2(  c1  char(10) NOT NULL,        c2 date NOT NULL,c3 BINARY_DOUBLE  NOT NULL,c4  date not NULL,c5 int not null,c6 number not null,c7 nvarchar2(60),c8 raw(20) not null,c9 timestamp not null,c10 varchar(10) not null,c11 varchar2(20) not null,c12 blob not null)"
	cmd_str = """ obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ % (host, port,user,tenant,password, create_sql2)
	result = commands.getstatusoutput(cmd_str)
	print result
    	create_sql3="CREATE TABLE t3(  c1  char(10) NOT NULL,        c2 date NOT NULL,c3 BINARY_DOUBLE  NOT NULL,c4  date not NULL,c5 int not null,c6 number not null,c7 nvarchar2(60),c8 raw(20) not null,c9 timestamp not null,c10 varchar(10) not null,c11 varchar2(20) not null,c12 blob not null)partition by hash(c1) partitions 500"
	cmd_str = """ obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ % (host, port,user,tenant,password, create_sql3)
	result = commands.getstatusoutput(cmd_str)
	print result
    	create_sql4="CREATE TABLE t4(  c1  char(10) NOT NULL,        c2 date NOT NULL,c3 BINARY_DOUBLE  NOT NULL,c4  date not NULL,c5 int not null,c6 number not null,c7 nvarchar2(60),c8 raw(20) not null,c9 timestamp not null,c10 varchar(10) not null,c11 varchar2(20) not null,c12 blob not null)partition by hash(c1) partitions 500"        
	cmd_str = """ obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ % (host, port,user,tenant,password, create_sql4)
	result = commands.getstatusoutput(cmd_str)
	print result
	index_sql4="create unique index t4 on t4(c1,c2)"
	cmd_str = """ obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ % (host, port,user,tenant,password, index_sql4)
	result = commands.getstatusoutput(cmd_str)
	print result
    	create_sql5="CREATE TABLE t5(  c1  char(10) NOT NULL,        c2 date NOT NULL,c3 BINARY_DOUBLE  NOT NULL,c4  date not NULL,c5 int not null,c6 number not null,c7 nvarchar2(60),c8 raw(20) not null,c9 timestamp not null,c10 varchar(10) not null,c11 varchar2(20) not null ,c12 blob not null,PRIMARY KEY (c1,c8)) partition by hash(c8) partitions 50"
	index_sql5="create index t5 on t5  (c4,c2,c1) global partition by hash(c2) partitions 50"
	cmd_str = """ obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ % (host, port,user,tenant,password, create_sql5)
	result = commands.getstatusoutput(cmd_str)
	print result
	cmd_str = """ obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ % (host, port,user,tenant,password, index_sql5)
	result = commands.getstatusoutput(cmd_str)
	print result
	
    	create_sql6="CREATE TABLE t6(  c1  char(10) NOT NULL,        c2 date NOT NULL,c3 BINARY_DOUBLE  NOT NULL,c4  date not NULL,c5 int not null,c6 number not null,c7 nvarchar2(60),c8 raw(20) not null,c9 timestamp not null,c10 varchar(10) not null,c11 varchar2(20) not null ,c12 blob not null,PRIMARY KEY (c1,c8)) partition by hash(c8) partitions 50"
	index_sql61="create unique index t61 on t6(c1,c2,c8)"
	index_sql62="create index t6 on t6  (c4,c2,c1) global partition by hash(c2) partitions 50"
	cmd_str = """ obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ % (host, port,user,tenant,password, create_sql6)
	result = commands.getstatusoutput(cmd_str)
	print result
	cmd_str = """ obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ % (host, port,user,tenant,password, index_sql61)
	result = commands.getstatusoutput(cmd_str)
	print result
	cmd_str = """ obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ % (host, port,user,tenant,password, index_sql62)
	result = commands.getstatusoutput(cmd_str)
	print result
	
    	create_sql7="CREATE TABLE t7(  c1  char(10) NOT NULL,        c2 date NOT NULL,c3 BINARY_DOUBLE  NOT NULL,c4  date not NULL,c5 int not null,c6 number not null,c7 nvarchar2(60),c8 raw(20) not null,c9 timestamp not null,c10 varchar(10) not null,c11 varchar2(20) not null ,c12 blob not null,PRIMARY KEY (c1,c8))  partition by hash(c8) SUBPARTITION BY hash(c1)    SUBPARTITIONS 2 partitions 50"
	index_sql71="create unique index t71 on t7(c1,c2,c8)"
	index_sql72="create index t7 on t7  (c4,c2,c1) global partition by hash(c2) partitions 50"
	cmd_str = """ obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ % (host, port,user,tenant,password, create_sql7)
	result = commands.getstatusoutput(cmd_str)
	print result
	cmd_str = """ obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ % (host, port,user,tenant,password, index_sql71)
	result = commands.getstatusoutput(cmd_str)
	print result
	cmd_str = """ obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ % (host, port,user,tenant,password, index_sql72)
	result = commands.getstatusoutput(cmd_str)
	print result
	
    	create_sql8="CREATE TABLE t8(  c1  char(10) NOT NULL,        c2 date NOT NULL,c3 BINARY_DOUBLE  NOT NULL,c4  date not NULL,c5 int not null,c6 number not null,c7 nvarchar2(60),c8 raw(20) not null,c9 timestamp not null,c10 varchar(10) not null,c11 varchar2(20) not null ,c12 blob not null,PRIMARY KEY (c1,c5))  partition by range(c5) (partition p1 values less than(100),partition p2 values less than(200),partition p3 values less than(300),partition p4 values less than(400),partition p5 values less than(500),partition p6 values less than(600),partition p7 values less than(700),partition p8 values less than(800),partition p9 values less than(900),partition p10 values less than(1000),partition p11 values less than(1100),partition p12 values less than(1200),partition p13 values less than(1300),partition p14 values less than(1400),partition p15 values less than(1500),partition p16 values less than(1600),partition p17 values less than(1700),partition p18 values less than(1800),partition p19 values less than(1900),partition p20 values less than(2000),partition pmax values less than (maxvalue))"
	index_sql81="create unique index t81 on t8(c1,c2,c8)"
	index_sql82="create index t8 on t8  (c4,c2,c1) global partition by hash(c4) partitions 50"
	cmd_str = """ obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ % (host, port,user,tenant,password, create_sql8)
	result = commands.getstatusoutput(cmd_str)
	print result
	cmd_str = """ obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ % (host, port,user,tenant,password, index_sql81)
	result = commands.getstatusoutput(cmd_str)
	print result
	cmd_str = """ obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ % (host, port,user,tenant,password, index_sql82)
	result = commands.getstatusoutput(cmd_str)
	print result
	
    	create_sql9="CREATE TABLE t9(  c1  char(10) NOT NULL,        c2 date NOT NULL,c3 BINARY_DOUBLE  NOT NULL,c4  date not NULL,c5 int not null,c6 number not null,c7 nvarchar2(60),c8 raw(20) not null,c9 timestamp not null,c10 varchar(10) not null,c11 varchar2(20) not null ,c12 blob not null,PRIMARY KEY (c1,c5), CONSTRAINT fk_column FOREIGN KEY (c1) REFERENCES t (c1))partition by hash(c5)  partitions 50"
	cmd_str = """ obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ % (host, port,user,tenant,password, create_sql9)
	result = commands.getstatusoutput(cmd_str)
	print result

def load(host,port,user,tenant,password,directory):
    	load_sql="load data /*+ parallel(10) direct(true, 0) */ infile '"+directory+"/TEST.1.0.csv,"+directory+"/TEST.2.0.csv'  into table t fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    	load_sql1="load data /*+ parallel(10) direct(true, 0) */ infile '"+directory+"/TEST.1.0.csv,"+directory+"/TEST.2.0.csv'  into table t1 fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    	print(load_sql)
	cmd_str = """ obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ % (host, port,user,tenant,password, load_sql)
	result = commands.getstatusoutput(cmd_str)
	print("t表导入：")
	error(result,load_sql)
	cmd_str = """ obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ % (host, port,user,tenant,password, load_sql1)
	result = commands.getstatusoutput(cmd_str)
	print("t1表导入：")
	error(result,load_sql1)
		
def drop_table(host,port,user,tenant,password):
	drop_sql='drop table t'
        for i in range (1,10):
                drop_sql=drop_sql+str(i)
                print(drop_sql)
		cmd_str=""" obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ %(host,port,user,tenant,password,drop_sql)
		result = commands.getstatusoutput(cmd_str)
        	error(result,drop_sql)
		drop_sql='drop table t'
	cmd_str=""" obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ %(host,port,user,tenant,password,drop_sql)
	result = commands.getstatusoutput(cmd_str)
	error(result,drop_sql)

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("-host", "-host", help=" hostname. eg: -t 127.1 ", type=str, dest="host", default='127.1')
	parser.add_argument("-P", "-port", help=" observer port. eg: -P 3952", type=str, dest="port", default=None)
	parser.add_argument("-u", "-user", help=" oceanbase user. eg: -u test", type=str, dest="user", default=None)
	parser.add_argument("-t", help=" tenant. eg: -t tt3", type=str, dest="tenant", default=None)
	parser.add_argument("-p", "-password", help=" tenant password. eg: -p test ", type=str, dest="password",default=None)
	parser.add_argument("-directory", "-directory", help=" directory /home/. eg: -directory /home/ ", type=str,	dest="directory", default=None)
	args = parser.parse_args()
        
	#drop_table(args.host, args.port, args.user, args.tenant, args.password)
	createTable(args.host, args.port, args.user, args.tenant, args.password)
	load(args.host, args.port, args.user, args.tenant, args.password,args.directory)
        #print(locality_primary_path)
    	insert(args.host, args.port, args.user, args.tenant, args.password)
	
if __name__ == '__main__':
        main()

