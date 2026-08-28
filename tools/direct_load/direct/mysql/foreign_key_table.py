#/usr/bin/evn python
#-*- encoding:utf-8 -*-
#测分区表，空表导入，随后导入多个文件，再依次（捕捉错误？不让程序断，捕捉到，打印出符合预期）导入null导入非空，数据类型不符合，列多，列少

import csv
import re
import os
import threading
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

def drop(host,port,tenant,database,directory):
    drop_sql = 'drop table IF EXISTS test'
    #print(drop_sql)
    for i in range(10, 0, -1):
        drop_sql = drop_sql + str(i)
        print(drop_sql)
        cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, drop_sql)
        result = commands.getstatusoutput(cmd_str)
        error(result,drop_sql)
        drop_sql = 'drop table IF EXISTS test'
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, drop_sql)
    result = commands.getstatusoutput(cmd_str)
    error(result,drop_sql)



def create_table(host,port,tenant,database,directory):
    print("空外键分区表，建表开始")
    create_table1="CREATE TABLE test1 (  c1 varchar(10) NOT NULL,  c2 bigint(20) NOT NULL,  c3 char(10) NOT NULL,  c4 date NOT NULL,  c5 datetime NOT NULL,  c6 decimal(5,2) NOT NULL,  c7 double NOT NULL,  c8 int(11) NOT NULL,  c9 smallint(6) NOT NULL,  c10 time NOT NULL,  c11 tinyint(4) NOT NULL,   c13 json DEFAULT NULL,   c14 mediumint(9) DEFAULT NULL,   c15 mediumtext DEFAULT NULL,   c16 float DEFAULT NULL, c17 enum('M','F') DEFAULT NULL,   md5 varchar(128) DEFAULT NULL,PRIMARY KEY (c1))"
    create_table2="CREATE TABLE test2 (  c1 varchar(10) NOT NULL,  c2 bigint(20) NOT NULL,  c3 char(10) NOT NULL,  c4 date NOT NULL,  c5 datetime NOT NULL,  c6 decimal(5,2) NOT NULL,  c7 double NOT NULL,  c8 int(11) NOT NULL,  c9 smallint(6) NOT NULL,  c10 time NOT NULL,  c11 tinyint(4) NOT NULL,   c13 json DEFAULT NULL,   c14 mediumint(9) DEFAULT NULL,   c15 mediumtext DEFAULT NULL,   c16 float DEFAULT NULL ,c17 enum('M','F') DEFAULT NULL,   md5 varchar(128) DEFAULT NULL,PRIMARY KEY (c1),foreign key(c1) references test1(c1))"
    create_table3="CREATE TABLE test3 (  c1 varchar(10) NOT NULL,  c2 bigint(20) NOT NULL, c3 char(10) NOT NULL,  c4 date NOT NULL,  c5 datetime NOT NULL,  c6 decimal(5,2) NOT NULL,  c7 double NOT NULL,  c8 int(11) NOT NULL,  c9 smallint(6) NOT NULL,  c10 time NOT NULL,  c11 tinyint(4) NOT NULL,   c13 json DEFAULT NULL,   c14 mediumint(9) DEFAULT NULL,   c15 mediumtext DEFAULT NULL,   c16 float DEFAULT NULL, c17 enum('M','F') DEFAULT NULL,   md5 varchar(128) DEFAULT NULL,PRIMARY KEY (c1),foreign key(c1) references test1(c1),foreign key(c1) references test2(c1))"	
    create_table4="CREATE TABLE test4 (  c1 varchar(10) NOT NULL,  c2 bigint(20) NOT NULL,  c3 char(10) NOT NULL,  c4 date NOT NULL,  c5 datetime NOT NULL,  c6 decimal(5,2) NOT NULL,  c7 double NOT NULL,  c8 int(11) NOT NULL,  c9 smallint(6) NOT NULL,  c10 time NOT NULL,  c11 tinyint(4) NOT NULL,   c13 json DEFAULT NULL,  c14 mediumint(9) DEFAULT NULL,   c15 mediumtext DEFAULT NULL,   c16 float DEFAULT NULL, c17 enum('M','F') DEFAULT NULL,   md5 varchar(128) DEFAULT NULL,PRIMARY KEY (c1),foreign key(c1) references test1(c1),foreign key(c1) references test2(c1),foreign key(c1) references test3(c1))"
    create_table5="CREATE TABLE test5 (  c1 varchar(10) NOT NULL,  c2 bigint(20) NOT NULL,  c3 char(10) NOT NULL,  c4 date NOT NULL,  c5 datetime NOT NULL,  c6 decimal(5,2) NOT NULL,  c7 double NOT NULL,  c8 int(11) NOT NULL,  c9 smallint(6) NOT NULL,  c10 time NOT NULL,  c11 tinyint(4) NOT NULL,   c13 json DEFAULT NULL,   c14 mediumint(9) DEFAULT NULL,   c15 mediumtext DEFAULT NULL,   c16 float DEFAULT NULL, c17 enum('M','F') DEFAULT NULL,   md5 varchar(128) DEFAULT NULL,PRIMARY KEY (c1),foreign key(c1) references test1(c1),foreign key(c1) references test2(c1),foreign key(c1) references test3(c1),foreign key(c1) references test4(c1))"
    create_table6="CREATE TABLE test6 (  c1 varchar(10) NOT NULL,  c2 bigint(20) NOT NULL,  c3 char(10) NOT NULL,  c4 date NOT NULL,  c5 datetime NOT NULL,  c6 decimal(5,2) NOT NULL,  c7 double NOT NULL,  c8 int(11) NOT NULL,  c9 smallint(6) NOT NULL,  c10 time NOT NULL,  c11 tinyint(4) NOT NULL,   c13 json DEFAULT NULL,   c14 mediumint(9) DEFAULT NULL,   c15 mediumtext DEFAULT NULL,   c16 float DEFAULT NULL, c17 enum('M','F') DEFAULT NULL,   md5 varchar(128) DEFAULT NULL,PRIMARY KEY (c1),foreign key(c1) references test1(c1),foreign key(c1) references test2(c1),foreign key(c1) references test3(c1),foreign key(c1) references test4(c1),foreign key(c1) references test5(c1))"
    create_table7="CREATE TABLE test7 (  c1 varchar(10) NOT NULL,  c2 bigint(20) NOT NULL,  c3 char(10) NOT NULL,  c4 date NOT NULL,  c5 datetime NOT NULL,  c6 decimal(5,2) NOT NULL,  c7 double NOT NULL,  c8 int(11) NOT NULL,  c9 smallint(6) NOT NULL,  c10 time NOT NULL,  c11 tinyint(4) NOT NULL,   c13 json DEFAULT NULL,   c14 mediumint(9) DEFAULT NULL,   c15 mediumtext DEFAULT NULL,   c16 float DEFAULT NULL, c17 enum('M','F') DEFAULT NULL,   md5 varchar(128) DEFAULT NULL,PRIMARY KEY (c1),foreign key(c1) references test1(c1),foreign key(c1) references test2(c1),foreign key(c1) references test3(c1),foreign key(c1) references test4(c1),foreign key(c1) references test5(c1),foreign key(c1) references test6(c1))"
    create_table8="CREATE TABLE test8 (  c1 varchar(10) NOT NULL,  c2 bigint(20) NOT NULL,  c3 char(10) NOT NULL,  c4 date NOT NULL,  c5 datetime NOT NULL,  c6 decimal(5,2) NOT NULL,  c7 double NOT NULL,  c8 int(11) NOT NULL,  c9 smallint(6) NOT NULL,  c10 time NOT NULL,  c11 tinyint(4) NOT NULL,   c13 json DEFAULT NULL,   c14 mediumint(9) DEFAULT NULL,   c15 mediumtext DEFAULT NULL,   c16 float DEFAULT NULL, c17 enum('M','F') DEFAULT NULL,   md5 varchar(128) DEFAULT NULL,PRIMARY KEY (c1),foreign key(c1) references test1(c1),foreign key(c1) references test2(c1),foreign key(c1) references test3(c1),foreign key(c1) references test4(c1),foreign key(c1) references test5(c1),foreign key(c1) references test6(c1),foreign key(c1) references test7(c1))"
    create_table9="CREATE TABLE test9 (  c1 varchar(10) NOT NULL,  c2 bigint(20) NOT NULL,  c3 char(10) NOT NULL,  c4 date NOT NULL,  c5 datetime NOT NULL,  c6 decimal(5,2) NOT NULL,  c7 double NOT NULL,  c8 int(11) NOT NULL,  c9 smallint(6) NOT NULL,  c10 time NOT NULL,  c11 tinyint(4) NOT NULL,   c13 json DEFAULT NULL,   c14 mediumint(9) DEFAULT NULL,   c15 mediumtext DEFAULT NULL,   c16 float DEFAULT NULL, c17 enum('M','F') DEFAULT NULL,   md5 varchar(128) DEFAULT NULL,PRIMARY KEY (c1),foreign key(c1) references test1(c1),foreign key(c1) references test2(c1),foreign key(c1) references test3(c1),foreign key(c1) references test4(c1),foreign key(c1) references test5(c1),foreign key(c1) references test6(c1),foreign key(c1) references test7(c1),foreign key(c1) references test8(c1))"
    create_table10="CREATE TABLE test10 ( c1 varchar(10) NOT NULL, c2 bigint(20) default NULL,  c3 char(10) default NULL,  c4 date default NULL,  c5 datetime default NULL,  c6 decimal(5,2) default NULL, c7 double default NULL,  c8 int(11) default NULL,  c9 smallint(6) default NULL,  c10 time default NULL,  c11 tinyint(4) default NULL,   c13 json default NULL,   c14 mediumint(9) DEFAULT NULL,   c15 mediumtext DEFAULT NULL,   c16 float DEFAULT NULL, c17 enum('M','F') DEFAULT NULL,   md5 varchar(128) DEFAULT NULL,PRIMARY KEY (c1),index name(c2),foreign key(c1) references test1(c1),foreign key(c1) references test2(c1),foreign key(c1) references test3(c1),foreign key(c1) references test4(c1),foreign key(c1) references test5(c1),foreign key(c1) references test6(c1),foreign key(c1) references test7(c1),foreign key(c1) references test8(c1),foreign key(c1) references test9(c1)) partition by key(c1)  partitions 50"    
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, create_table1)
    result = commands.getstatusoutput(cmd_str)
    error(result,create_table1)
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, create_table2)
    result = commands.getstatusoutput(cmd_str)
    error(result,create_table2)
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, create_table3)
    result = commands.getstatusoutput(cmd_str)
    error(result,create_table3)
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, create_table4)
    result = commands.getstatusoutput(cmd_str)
    error(result,create_table2)
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, create_table5)
    result = commands.getstatusoutput(cmd_str)
    error(result,create_table5)
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, create_table6)
    result = commands.getstatusoutput(cmd_str)
    error(result,create_table6)
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, create_table7)
    result = commands.getstatusoutput(cmd_str)
    error(result,create_table7)
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, create_table8)
    result = commands.getstatusoutput(cmd_str)
    error(result,create_table8)
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, create_table9)
    result = commands.getstatusoutput(cmd_str)
    error(result,create_table9)
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, create_table10)
    result = commands.getstatusoutput(cmd_str)
    error(result,create_table10)
	

def empty_load(host,port,tenant,database,directory):
    print("空外键分区表多文件导入开始：")
    load_sql1="load data /*+ parallel(10) direct(true, 0) */ infile '"+directory+"/test.1.0.csv,"+directory+"/test.2.0.csv,"+directory+"/test.3.0.csv,"+directory+"/test.4.0.csv'  into table test1 fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    load_sql2="load data /*+ parallel(10) direct(true, 0) */ infile '"+directory+"/test.1.0.csv,"+directory+"/test.2.0.csv,"+directory+"/test.3.0.csv,"+directory+"/test.4.0.csv'  into table test2 fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"	
    load_sql3="load data /*+ parallel(10) direct(true, 0) */ infile '"+directory+"/test.1.0.csv,"+directory+"/test.2.0.csv,"+directory+"/test.3.0.csv,"+directory+"/test.4.0.csv'  into table test3 fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    load_sql4="load data /*+ parallel(10) direct(true, 0) */ infile '"+directory+"/test.1.0.csv,"+directory+"/test.2.0.csv,"+directory+"/test.3.0.csv,"+directory+"/test.4.0.csv'  into table test4 fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    load_sql5="load data /*+ parallel(10) direct(true, 0) */ infile '"+directory+"/test.1.0.csv,"+directory+"/test.2.0.csv,"+directory+"/test.3.0.csv,"+directory+"/test.4.0.csv'  into table test5 fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    load_sql6="load data /*+ parallel(10) direct(true, 0) */ infile '"+directory+"/test.1.0.csv,"+directory+"/test.2.0.csv,"+directory+"/test.3.0.csv,"+directory+"/test.4.0.csv'  into table test6 fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    load_sql7="load data /*+ parallel(10) direct(true, 0) */ infile '"+directory+"/test.1.0.csv,"+directory+"/test.2.0.csv,"+directory+"/test.3.0.csv,"+directory+"/test.4.0.csv'  into table test7 fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    load_sql8="load data /*+ parallel(10) direct(true, 0) */ infile '"+directory+"/test.1.0.csv,"+directory+"/test.2.0.csv,"+directory+"/test.3.0.csv,"+directory+"/test.4.0.csv'  into table test8 fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    load_sql9="load data /*+ parallel(10) direct(true, 0) */ infile '"+directory+"/test.1.0.csv,"+directory+"/test.2.0.csv,"+directory+"/test.3.0.csv,"+directory+"/test.4.0.csv'  into table test9 fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    load_sql10="load data /*+ parallel(10) direct(true, 0) */ infile '"+directory+"/test.1.0.csv,"+directory+"/test.2.0.csv,"+directory+"/test.3.0.csv'  into table test10 fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, load_sql1)
    result = commands.getstatusoutput(cmd_str)
    error(result,load_sql1)
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, load_sql2)
    result = commands.getstatusoutput(cmd_str)
    error(result,load_sql2)
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, load_sql3)
    result = commands.getstatusoutput(cmd_str)
    error(result,load_sql3)
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, load_sql4)
    result = commands.getstatusoutput(cmd_str)
    error(result,load_sql4)
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, load_sql5)
    result = commands.getstatusoutput(cmd_str)
    error(result,load_sql5)
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, load_sql6)
    result = commands.getstatusoutput(cmd_str)
    error(result,load_sql6)
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, load_sql7)
    result = commands.getstatusoutput(cmd_str)
    error(result,load_sql7)
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, load_sql8)
    result = commands.getstatusoutput(cmd_str)
    error(result,load_sql8)
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, load_sql9)
    result = commands.getstatusoutput(cmd_str)
    error(result,load_sql9)
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, load_sql10)
    result = commands.getstatusoutput(cmd_str)
    error(result,load_sql10)


def empty_load1(host,port,tenant,database,directory):
    truncate = "truncate table test10"
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, truncate)
    # print cmd_str
    result = commands.getstatusoutput(cmd_str)
    print("truncate table：")
    error(result,truncate)
    load_sql = "load data /*+ parallel(10) direct(false, 0) */ infile '"+directory+"/test.1.0.csv'  into table test10 fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, load_sql)
    result = commands.getstatusoutput(cmd_str)
    print("空表导入单文件结果：")
    error(result,load_sql)


def noEmpty_load(host,port,tenant,database,directory):
    load_sql = "load data /*+ parallel(10) direct(true, 0) */ infile '"+directory+"/test.2.0.csv,"+directory+"/test.3.0.csv'  into table test10 fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, load_sql)
    result = commands.getstatusoutput(cmd_str)
    print("非空表导入多文件结果：")
    error(result,load_sql)


def Null_value(host,port,tenant,database,directory):
    load_sql = "load data /*+ parallel(10) direct(true, 0) */ infile '"+directory+"/null.csv' replace into table test10 fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, load_sql)
    result = commands.getstatusoutput(cmd_str)
    print("非空表带null值文件导入结果：")
    error(result,load_sql)


def FalseType(host,port,tenant,database,directory):
    load_sql = "load data /*+ parallel(10) direct(true, 10) */ infile '"+directory+"/false_type.csv' replace into table test10 fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, load_sql)
    result = commands.getstatusoutput(cmd_str)
    print("错误类型导入，预期被记入错误行不报错：")
    error(result,load_sql)
    load_sql = "load data /*+ parallel(10) direct(true, 0) */ infile '"+directory+"/false_type.csv' replace  into table test10 fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, load_sql)
    result = commands.getstatusoutput(cmd_str)
    print("错误类型导入，预期报错：")
    success(result,load_sql)


def ColumnLess_More(host,port,tenant,database,directory):
    load_sql = "load data /*+ parallel(10) direct(true, 0) */ infile '"+directory+"/col_less.csv'  into table test10 fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, load_sql)
    result = commands.getstatusoutput(cmd_str)
    print("少列，预期报错1525：")
    success(result,load_sql)
    load_sql = "load data /*+ parallel(10) direct(true, 0) */ infile '"+directory+"/col_more.csv'  into table test10 fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, load_sql)
    result = commands.getstatusoutput(cmd_str)
    print("多列，预期报错1525：")
    success(result,load_sql)


def replace_ingnore(host,port,tenant,database,directory):
    load_sql = "load data /*+ parallel(10) direct(false, 0) */ infile '"+directory+"/repetition.csv' ignore into table test10 fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, load_sql)
    result = commands.getstatusoutput(cmd_str)
    print("ignore导入：")
    error(result,load_sql)
    load_sql = "load data /*+ parallel(10) direct(true, 0) */ infile '"+directory+"/repetition.csv' replace  into table test10 fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, load_sql)
    result = commands.getstatusoutput(cmd_str)
    print("replace导入：")
    error(result,load_sql)


def load(host,port,tenant,database,directory):
    create_table(host,port,tenant,database,directory)
    empty_load(host,port,tenant,database,directory)
    empty_load1(host,port,tenant,database,directory)
    noEmpty_load(host,port,tenant,database,directory)
    Null_value(host,port,tenant,database,directory)
    FalseType(host,port,tenant,database,directory)
    ColumnLess_More(host,port,tenant,database,directory)
    replace_ingnore(host,port,tenant,database,directory)





def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-host", "-host", help=" hostname. eg: -t 127.1 ", type=str, dest="host", default='127.1')
    parser.add_argument("-P", "-port", help=" observer port. eg: -P 3952", type=str, dest="port", default=None)
    parser.add_argument("-u", help=" tenant. eg: -u root@l_mysql", type=str, dest="tenant", default=None)
    parser.add_argument("-D", "-database", help=" tenant database. eg: -D test ", type=str, dest="database", default=None)
    parser.add_argument("-directory", "-directory", help=" directory /home/. eg: -directory /home/ ", type=str,dest="directory", default=None)
    args = parser.parse_args()
    drop(args.host, args.port, args.tenant, args.database,args.directory)
    load(args.host, args.port, args.tenant, args.database,args.directory)

if __name__ == '__main__':
	main()

