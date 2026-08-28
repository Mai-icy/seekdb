#/usr/bin/evn python
#-*- encoding:utf-8 -*-
import csv
import re
import os
import threading
import random
import commands
import time
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
            
def drop_table(hostname,port,user,tenant,password,directory):	
	drop_sql='drop table t'
    	for i in range (1,10):
        	drop_sql=drop_sql+str(i)
        	print(drop_sql)
		cmd_str=""" obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ %(hostname,port,user,tenant,password,drop_sql)
		result = commands.getstatusoutput(cmd_str)
        	#error(result,drop_sql)
		print result
		drop_sql='drop table t'
	cmd_str=""" obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ %(hostname,port,user,tenant,password,drop_sql)
        result = commands.getstatusoutput(cmd_str)
        #error(result,drop_sql)
	print result
	"""
		try:
	                curs.execute(drop_sql)
        	        drop_sql='drop table IF EXISTS t'
		except Exception as result:
			print(result)
			print("drop table成功")
		else:
                        print("此次drop成功")	
	"""

def create_table(hostname,port,user,tenant,password,directory):
    create_sql="CREATE TABLE t(c1  char(10) NOT NULL,        c2 date NOT NULL,c3 BINARY_DOUBLE  NOT NULL,c4  date not NULL,c5 int not null,c6 number not null,c7 nvarchar2(60),c8 raw(20) not null,c9 timestamp not null,c10 varchar(10) not null,c11 varchar2(20) not null,c12 blob not null,CONSTRAINT PK PRIMARY KEY(c1))"
    cmd_str=""" obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ %(hostname,port,user,tenant,password,create_sql)
    result = commands.getstatusoutput(cmd_str)
    error(result,create_sql)

    create_sql1="CREATE TABLE t1(c1  char(10) NOT NULL,        c2 date NOT NULL,c3 BINARY_DOUBLE  NOT NULL,c4  date not NULL,c5 int not null,c6 number not null,c7 nvarchar2(60),c8 raw(20) not null,c9 timestamp not null,c10 varchar(10) not null,c11 varchar2(20) not null,c12 blob not null,PRIMARY KEY(c1))"
    cmd_str=""" obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ %(hostname,port,user,tenant,password,create_sql1)
    result = commands.getstatusoutput(cmd_str)
    error(result,create_sql1)       
    

    create_sql2="CREATE TABLE t2(  c1  char(10) NOT NULL,        c2 date NOT NULL,c3 BINARY_DOUBLE  NOT NULL,c4  date not NULL,c5 int not null,c6 number not null,c7 nvarchar2(60),c8 raw(20) not null,c9 timestamp not null,c10 varchar(10) not null,c11 varchar2(20) not null,c12 blob not null)"
    cmd_str=""" obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ %(hostname,port,user,tenant,password,create_sql2)
    result = commands.getstatusoutput(cmd_str)
    error(result,create_sql2)

    create_sql3="CREATE TABLE t3(  c1  char(10) NOT NULL,        c2 date NOT NULL,c3 BINARY_DOUBLE  NOT NULL,c4  date not NULL,c5 int not null,c6 number not null,c7 nvarchar2(60),c8 raw(20) not null,c9 timestamp not null,c10 varchar(10) not null,c11 varchar2(20) not null,c12 blob not null)partition by hash(c1) partitions 100"
    cmd_str=""" obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ %(hostname,port,user,tenant,password,create_sql3)
    result = commands.getstatusoutput(cmd_str)
    error(result,create_sql3)

#        create_sql4="CREATE TABLE t4(  c1  char(10) NOT NULL,        c2 date NOT NULL,c3 BINARY_DOUBLE  NOT NULL,c4  date not NULL,c5 int not null,c6 number not null,c7 nvarchar2(60),c8 raw(20) not null,c9 timestamp not null,c10 varchar(10) not null,c11 varchar2(20) not null,c12 blob not null)partition by hash(c1) partitions 5000"
    create_sql4="CREATE TABLE t4(  c1  char(10) NOT NULL,        c2 date NOT NULL,c3 BINARY_DOUBLE  NOT NULL,c4  date not NULL,c5 int not null,c6 number not null,c7 nvarchar2(60),c8 raw(20) not null,c9 timestamp not null,c10 varchar(10) not null,c11 varchar2(20) not null,c12 blob not null)partition by hash(c1) partitions 50"
    cmd_str=""" obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ %(hostname,port,user,tenant,password,create_sql4)
    result = commands.getstatusoutput(cmd_str)
    error(result, create_sql4)

    index_sql4="create unique index t4 on t4(c1,c2)"
    cmd_str=""" obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ %(hostname,port,user,tenant,password,index_sql4)
    result = commands.getstatusoutput(cmd_str)
    error(result,index_sql4)

    create_sql5="CREATE TABLE t5(  c1  char(10) NOT NULL,        c2 date NOT NULL,c3 BINARY_DOUBLE  NOT NULL,c4  date not NULL,c5 int not null,c6 number not null,c7 nvarchar2(60),c8 raw(20) not null,c9 timestamp not null,c10 varchar(10) not null,c11 varchar2(20) not null ,c12 blob not null,PRIMARY KEY (c1,c8)) partition by hash(c8) partitions 50"
    index_sql5="create index t5 on t5  (c4,c2,c1) global partition by hash(c2) partitions 50"
    cmd_str=""" obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ %(hostname,port,user,tenant,password,create_sql5)
    result = commands.getstatusoutput(cmd_str)
    error(result,create_sql5)
    cmd_str=""" obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ %(hostname,port,user,tenant,password,index_sql5)
    result = commands.getstatusoutput(cmd_str)
    error(result,index_sql5)

    create_sql6="CREATE TABLE t6(  c1  char(10) NOT NULL,        c2 date NOT NULL,c3 BINARY_DOUBLE  NOT NULL,c4  date not NULL,c5 int not null,c6 number not null,c7 nvarchar2(60),c8 raw(20) not null,c9 timestamp not null,c10 varchar(10) not null,c11 varchar2(20) not null ,c12 blob not null,PRIMARY KEY (c1,c8)) partition by hash(c8) partitions 50"
    index_sql61="create unique index t61 on t6(c1,c2,c8)"
    index_sql62="create index t6 on t6  (c4,c2,c1) global partition by hash(c2) partitions 50"
    cmd_str=""" obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;%s;%s;" """ %(hostname,port,user,tenant,password,create_sql6,index_sql61,index_sql62)
    result = commands.getstatusoutput(cmd_str)
    error(result,create_sql6)

    create_sql7="CREATE TABLE t7(  c1  char(10) NOT NULL,        c2 date NOT NULL,c3 BINARY_DOUBLE  NOT NULL,c4  date not NULL,c5 int not null,c6 number not null,c7 nvarchar2(60),c8 raw(20) not null,c9 timestamp not null,c10 varchar(10) not null,c11 varchar2(20) not null ,c12 blob not null,PRIMARY KEY (c1,c8))  partition by hash(c8) SUBPARTITION BY hash(c1)    SUBPARTITIONS 2 partitions 50"
    index_sql71="create unique index t71 on t7(c1,c2,c8)"
    index_sql72="create index t7 on t7  (c4,c2,c1) global partition by hash(c2) partitions 50"
    cmd_str=""" obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;%s;%s;" """ %(hostname,port,user,tenant,password,create_sql7,index_sql71,index_sql72)
    result = commands.getstatusoutput(cmd_str)
    error(result,create_sql7)

    create_sql8="CREATE TABLE t8(  c1  char(10) NOT NULL,        c2 date NOT NULL,c3 BINARY_DOUBLE  NOT NULL,c4  date not NULL,c5 int not null,c6 number not null,c7 nvarchar2(60),c8 raw(20) not null,c9 timestamp not null,c10 varchar(10) not null,c11 varchar2(20) not null ,c12 blob not null,PRIMARY KEY (c1,c5))  partition by range(c5) (partition p1 values less than(100),partition p2 values less than(200),partition p3 values less than(300),partition p4 values less than(400),partition p5 values less than(500),partition p6 values less than(600),partition p7 values less than(700),partition p8 values less than(800),partition p9 values less than(900),partition p10 values less than(1000),partition p11 values less than(1100),partition p12 values less than(1200),partition p13 values less than(1300),partition p14 values less than(1400),partition p15 values less than(1500),partition p16 values less than(1600),partition p17 values less than(1700),partition p18 values less than(1800),partition p19 values less than(1900),partition p20 values less than(2000),partition pmax values less than (maxvalue))"
    index_sql81="create unique index t81 on t8(c1,c2,c8)"
    index_sql82="create index t8 on t8  (c4,c2,c1) global partition by hash(c4) partitions 50"
    cmd_str=""" obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;%s;%s;" """ %(hostname,port,user,tenant,password,create_sql8,index_sql81,index_sql82)
    result = commands.getstatusoutput(cmd_str)
    error(result,create_sql8)
    create_sql9="CREATE TABLE t9(  c1  char(10) NOT NULL,        c2 date NOT NULL,c3 BINARY_DOUBLE  NOT NULL,c4  date not NULL,c5 int not null,c6 number not null,c7 nvarchar2(60),c8 raw(20) not null,c9 timestamp not null,c10 varchar(10) not null,c11 varchar2(20) not null ,c12 blob not null,PRIMARY KEY (c1,c5), CONSTRAINT fk_column FOREIGN KEY (c1) REFERENCES t (c1))partition by hash(c5)  partitions 50"
    cmd_str=""" obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ %(hostname,port,user,tenant,password,create_sql9)
    result = commands.getstatusoutput(cmd_str)
    error(result,create_sql9)

    load_data="""load data /*+ parallel(10) direct(true, 0) */ infile '"""+directory+"""/TEST.1.0.csv,"""+directory+"""/TEST.2.0.csv'  into table t fields terminated by '|' enclosed by '''' """
    cmd_str=""" obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ %(hostname,port,user,tenant,password,load_data)
    result = commands.getstatusoutput(cmd_str)
    error(result, load_data)

def pl_test1(hostname,port,user,tenant,password,directory):
	delimiter="delimiter /"
	pl1="""CREATE or replace procedure testv1 AUTHID CURRENT_USER AS 
t_exits NUMBER;
    t4_exits NUMBER;
BEGIN
    execute immediate 'set autocommit = true';
    execute immediate 'set ob_query_timeout = 86400000000';
    select count(*) into t_exits from user_tables where table_name = 'T';
    if t_exits > 0 then
        execute immediate 'truncate t9;';
    end if;
    execute immediate 'load data /*+ parallel(10) direct(true, 0) */ infile ''"""+directory+"""/TEST.1.0.csv''  into table t9 fields terminated by ''|'' enclosed by '''''''' ';
    select count(*) into t4_exits from user_tables where table_name = 'T3';
    if t4_exits > 0 then
        execute immediate 'truncate t3';
    end if;
execute immediate 'load data /*+ parallel(10) direct(true, 0) */ infile ''"""+directory+"""/TEST.2.0.csv''  into table t3 fields terminated by ''|'' enclosed by '''''''' ';

    execute immediate 'INSERT /*+ APPEND  PARALLEL(4) ENABLE_PARALLEL_DML */ INTO t3 SELECT t1.c1,t1.c2,t1.c3,t1.c4,t1.c5,t1.c6,t1.c7,t1.c8,t1.c9,t1.c10,t1.c11,t1.c12 FROM t t1 LEFT JOIN t9 t2 ON t1.c1 = t2.c1 ';

END;/"""
	#print pl1
	current_time = int(time.time())
	localtime = time.localtime(current_time)
	dt = time.strftime('%Y:%m:%d %H:%M:%S', localtime)
	#print("开始时间:")
	#print(dt) 
	cmd_str=""" obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s" """ %(hostname,port,user,tenant,password,delimiter)
    	result = commands.getstatusoutput(cmd_str)
    	print result
	cmd_str=""" obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s" """ %(hostname,port,user,tenant,password,pl1)
    	result = commands.getstatusoutput(cmd_str)
    	error(result,pl1)

def pl_test2(hostname,port,user,tenant,password,directory):
	delimiter="delimiter /"
	pl2="""CREATE or replace procedure testv2 AUTHID CURRENT_USER AS 
t_exits NUMBER;
    t4_exits NUMBER;
BEGIN
    execute immediate 'set autocommit = true';
    execute immediate 'set ob_query_timeout = 86400000000';
    select count(*) into t_exits from user_tables where table_name = 'T6';
if t_exits > 0 then
	execute immediate 'truncate t6;';
execute immediate 'load data /*+ parallel(10) direct(true, 0) */ infile ''"""+directory+"""/TEST.1.0.csv,"""+directory+"""/TEST.3.0.csv,"""+directory+"""/TEST.2.0.csv''  into table t6 fields terminated by ''|'' enclosed by '''''''' ';  
        execute immediate 'drop table t6;';
    end if;
	execute immediate 'CREATE TABLE t6(  c1  char(10) NOT NULL,        c2 date NOT NULL,c3 BINARY_DOUBLE  NOT NULL,c4  date not NULL,c5 int not null,c6 number not null,c7 nvarchar2(60),c8 raw(20) not null,c9 timestamp not null,c10 varchar(10) not null,c11 varchar2(20) not null ,c12 blob not null,PRIMARY KEY (c1,c8)) partition by hash(c8) partitions 50';

    execute immediate 'load data /*+ parallel(10) direct(false, 0) */ infile ''"""+directory+"""/TEST.1.0.csv''  into table t6 fields terminated by ''|'' enclosed by '''''''' ';

        execute immediate 'create unique index t61 on t6(c1,c2,c8)';
	execute immediate 'load data /*+ parallel(100) direct(true, 10) */ infile ''"""+directory+"""/TEST.2.0.csv,"""+directory+"""/TEST.4.0.csv,"""+directory+"""/TEST.3.0.csv''  into table t6 fields terminated by ''|'' enclosed by '''''''' ';
        execute immediate 'create index t6 on t6  (c4,c2,c1) global partition by hash(c2) partitions 50';
	execute immediate 'load data /*+ parallel(10) direct(false, 0) */ infile ''"""+directory+"""/TEST.5.0.csv''  into table t6 fields terminated by ''|'' enclosed by '''''''' ';
    select count(*) into t4_exits from user_tables where table_name = 'T2';
    if t4_exits > 0 then
        execute immediate 'ALTER TABLE T2 MODIFY c1 char(20)';
    end if;
execute immediate 'load data /*+ parallel(10) direct(true, 0) */ infile ''"""+directory+"""/TEST.7.0.csv''  into table t2 fields terminated by ''|'' enclosed by '''''''' ';

    execute immediate 'INSERT /*+ APPEND  PARALLEL(4) ENABLE_PARALLEL_DML */ INTO t4 SELECT t1.c1,t1.c2,t1.c3,t1.c4,t1.c5,t1.c6,t1.c7,t1.c8,t1.c9,t1.c10,t1.c11,t1.c12 FROM t t1 LEFT JOIN t9 t2 ON t1.c1 = t2.c1 ';
execute immediate 'truncate t2';
execute immediate 'ALTER TABLE T2 MODIFY c1 char(30)';
END;/"""
	
	current_time = int(time.time())
	"""
    localtime = time.localtime(current_time)
    dt = time.strftime('%Y:%m:%d %H:%M:%S', localtime)
    print("开始时间:")
    print(dt)
	"""
	cmd_str=""" obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s" """ %(hostname,port,user,tenant,password,delimiter)
	result = commands.getstatusoutput(cmd_str)
	print result
	cmd_str=""" obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s" """ %(hostname,port,user,tenant,password,pl2)
	result = commands.getstatusoutput(cmd_str)
	error(result,pl2)
def pl_test3(hostname,port,user,tenant,password,directory):
	delimiter="delimiter /"
	pl3="""CREATE or replace procedure testv3 AUTHID CURRENT_USER AS 
t_exits NUMBER;
    t4_exits NUMBER;
BEGIN
    execute immediate 'set autocommit = true';
    execute immediate 'set ob_query_timeout = 46400000000';
    select count(*) into t_exits from user_tables where table_name = 'T1';
    if t_exits > 0 then
	
        execute immediate 'truncate t1;';
execute immediate 'load data /*+ parallel(10) direct(false, 0) */ infile ''"""+directory+"""/TEST.1.0.csv,"""+directory+"""/TEST.2.0.csv,"""+directory+"""/TEST.3.0.csv''  into table t1 fields terminated by ''|'' enclosed by '''''''' ';
execute immediate 'drop table t1;';
    end if;
execute immediate 'CREATE TABLE t1(c1  char(10) NOT NULL,        c2 date NOT NULL,c3 BINARY_DOUBLE  NOT NULL,c4  date not NULL,c5 int not null,c6 number not null,c7 nvarchar2(60),c8 raw(20) not null,c9 timestamp not null,c10 varchar(10) not null,c11 varchar2(20) not null,c12 blob not null,PRIMARY KEY(c1))';

EXECUTE IMMEDIATE 'create index INDX_AFW_FLOW_INFO_BAK_PACK on t1 (c5) parallel 8';
    EXECUTE IMMEDIATE 'alter index INDX_AFW_FLOW_INFO_BAK_PACK  noparallel ';
    execute immediate 'load data /*+ parallel(10) direct(true, 0) */ infile ''"""+directory+"""/TEST.1.0.csv''  into table t1 fields terminated by ''|'' enclosed by '''''''' ';    
select count(*) into t4_exits from user_tables where table_name = 'T8';
    if t4_exits > 0 then
        execute immediate 'truncate t8';
    end if;
	
execute immediate 'load data /*+ parallel(10) direct(true, 0) */ infile ''"""+directory+"""/TEST.2.0.csv''  into table t8 fields terminated by ''|'' enclosed by '''''''' ';
execute immediate 'update t1 set c5= 1 where c1=''Eo92LUW[g1''';
    execute immediate 'INSERT /*+ APPEND  PARALLEL(4) ENABLE_PARALLEL_DML */ INTO t8 SELECT t1.c1,t1.c2,t1.c3,t1.c4,t1.c5,t1.c6,t1.c7,t1.c8,t1.c9,t1.c10,t1.c11,t1.c12 FROM t t1 right JOIN t1 t2 ON t1.c1 = t2.c1 ';
execute immediate 'delete from t8 where c1=''Eo92LUW[g1''';
END;/

"""

	current_time = int(time.time())
	"""    
localtime = time.localtime(current_time)
    dt = time.strftime('%Y:%m:%d %H:%M:%S', localtime)
	"""

	cmd_str=""" obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s" """ %(hostname,port,user,tenant,password,delimiter)
	result = commands.getstatusoutput(cmd_str)
	print result
	cmd_str=""" obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s" """ %(hostname,port,user,tenant,password,pl3)
	result = commands.getstatusoutput(cmd_str)
	error(result,pl3)


"""
def pl_load(host,port,user,tenant,password,directory):
	delimiter="delimiter /"
	pl4=""

        result = commands.getstatusoutput(cmd_str)
        print result
        result = commands.getstatusoutput(cmd_str)
        print result
"""

def pl_load(hostname,port,user,tenant,password,directory):
    drop_table(hostname,port,user,tenant,password,directory)
    create_table(hostname,port,user,tenant,password,directory)	
    pl_test1(hostname,port,user,tenant,password,directory)
    pl_test2(hostname,port,user,tenant,password,directory)
    pl_test3(hostname,port,user,tenant,password,directory)
    current_time = int(time.time())
    localtime = time.localtime(current_time)
    dt = time.strftime('%Y:%m:%d %H:%M:%S', localtime)
    print("testv1开始时间:")
    print(dt)
    call_test="call test.testv1()"
    cmd_str=""" obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ %(hostname,port,user,tenant,password,call_test)
    result = commands.getstatusoutput(cmd_str)
    error(result, call_test)
    current_time = int(time.time())
    localtime = time.localtime(current_time)
    dt = time.strftime('%Y:%m:%d %H:%M:%S', localtime)
    print("testv2开始时间:")
    print(dt)
    call_test="call test.testv2()"
    cmd_str=""" obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ %(hostname,port,user,tenant,password,call_test)
    result = commands.getstatusoutput(cmd_str)
    error(result, call_test)

    current_time = int(time.time())
    localtime = time.localtime(current_time)
    dt = time.strftime('%Y:%m:%d %H:%M:%S', localtime)
    print("testv3开始时间:")
    print(dt)
    call_test="call test.testv3()"
    cmd_str=""" obclient -h%s -P%s -u%s@%s -c  -p%s -e "%s;" """ %(hostname,port,user,tenant,password,call_test)
    result = commands.getstatusoutput(cmd_str)
    error(result, call_test)

	

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-host", "-host", help=" hostname. eg: -t 127.1 ", type=str, dest="host", default='127.1')
    parser.add_argument("-P", "-port", help=" observer port. eg: -P 3952", type=str, dest="port", default=None)
    parser.add_argument("-u", "-user", help=" oceanbase user. eg: -u test", type=str, dest="user", default=None)
    parser.add_argument("-t", help=" tenant. eg: -t tt3", type=str, dest="tenant", default=None)
    parser.add_argument("-p", "-password", help=" tenant password. eg: -p test ", type=str, dest="password", default=None)
    parser.add_argument("-directory", "-directory", help=" directory /home/. eg: -directory /home/ ", type=str, dest="directory", default=None)
    args = parser.parse_args()
    pl_load(args.host, args.port, args.user, args.tenant, args.password,args.directory)

    
	

if __name__ == '__main__':
        main()
