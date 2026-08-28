#/usr/bin/evn python
#-*- encoding:utf-8 -*-
#测分区表，空表导入，随后导入多个文件，再依次（捕捉错误？不让程序断，捕捉到，打印出符合预期）导入null导入非空，数据类型不符合，列多，列少
import csv
import re
import os
import threading
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

def create_table(host,port,tenant,database,directory):
    create_table = "CREATE TABLE heap(c1 varchar(10) NOT NULL,c2 bigint(20) NOT NULL,  c3 char(10) NOT NULL,  c4 date NOT NULL,  c5 datetime NOT NULL,  c6 decimal(5,2) NOT NULL,  c7 double NOT NULL,  c8 int(11) NOT NULL,  c9 smallint(6) NOT NULL,  c10 time NOT NULL,  c11 tinyint(4) DEFAULT NULL,  c13 json DEFAULT NULL,   c14 mediumint(9) DEFAULT NULL,   c15 mediumtext DEFAULT NULL,   c16 float DEFAULT NULL, c17 enum('M','F') DEFAULT NULL,   md5 varchar(128) DEFAULT NULL,index k1 (c4,c2,c1) global) partition by hash(c8) partitions 200"
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, create_table)
    # print cmd_str
    result = commands.getstatusoutput(cmd_str)
    print("创表结果：")
    error(result,create_table)


def empty_load(host,port,tenant,database,directory):
    load_sql = "load data /*+ parallel(10) direct(true, 0) */ infile '"+directory+"/test.1.0.csv,"+directory+"/test.2.0.csv,"+directory+"/test.3.0.csv,"+directory+"/test.4.0.csv'  into table heap fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, load_sql)
    # print cmd_str
    result = commands.getstatusoutput(cmd_str)
    print("空表导入多文件结果：")
    error(result,load_sql)


def empty_load1(host,port,tenant,database,directory):
    truncate = "truncate table heap"
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, truncate)
    # print cmd_str
    result = commands.getstatusoutput(cmd_str)
    print("truncate table：")
    error(result, truncate)
    load_sql = "load data /*+ parallel(10) direct(false, 0) */ infile '"+directory+"/test.1.0.csv'  into table heap fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, load_sql)
    result = commands.getstatusoutput(cmd_str)
    print("空表导入单文件结果：")
    error(result,load_sql)


def noEmpty_load(host,port,tenant,database,directory):
    load_sql = "load data /*+ parallel(10) direct(true, 0) */ infile '"+directory+"/test.2.0.csv,"+directory+"/test.3.0.csv'  into table heap fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, load_sql)
    result = commands.getstatusoutput(cmd_str)
    print("非空表导入多文件结果：")
    error(result,load_sql)


def Null_value(host,port,tenant,database,directory):
    load_sql = "load data /*+ parallel(10) direct(true, 0) */ infile '"+directory+"/null.csv' replace into table heap fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, load_sql)
    result = commands.getstatusoutput(cmd_str)
    print("非空表带null值文件导入结果：")
    success(result,load_sql)


def FalseType(host,port,tenant,database,directory):
    load_sql = "load data /*+ parallel(10) direct(true, 10) */ infile '"+directory+"/false_type.csv'  into table heap fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, load_sql)
    result = commands.getstatusoutput(cmd_str)
    print("错误类型导入，预期被记入错误行不报错：")
    error(result,load_sql)
    load_sql = "load data /*+ parallel(10) direct(true, 0) */ infile '"+directory+"/false_type.csv' replace  into table heap fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, load_sql)
    result = commands.getstatusoutput(cmd_str)
    print("错误类型导入，预期报错：")
    success(result,load_sql)


def ColumnLess_More(host,port,tenant,database,directory):
    load_sql = "load data /*+ parallel(10) direct(true, 0) */ infile '"+directory+"/col_less.csv'  into table heap fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, load_sql)
    result = commands.getstatusoutput(cmd_str)
    print("少列，预期报错1525：")
    success(result,load_sql)
    load_sql = "load data /*+ parallel(10) direct(true, 0) */ infile '"+directory+"/col_more.csv'  into table heap fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, load_sql)
    result = commands.getstatusoutput(cmd_str)
    print("多列，预期报错1525：")
    success(result,load_sql)


def replace_ingnore(host,port,tenant,database,directory):
    load_sql = "load data /*+ parallel(10) direct(false, 0) */ infile '"+directory+"/repetition.csv' ignore into table heap fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
    cmd_str = """ obclient -h%s -P%s -u%s -c  -D%s -e "%s;" """ % (host, port, tenant, database, load_sql)
    result = commands.getstatusoutput(cmd_str)
    print("ignore导入：")
    error(result,load_sql)
    load_sql = "load data /*+ parallel(10) direct(true, 0) */ infile '"+directory+"/repetition.csv' replace  into table heap fields terminated by '|' enclosed by '''' lines starting by '' terminated by '\\n'"
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
    parser.add_argument("-directory", "-directory", help=" directory /home/. eg: -directory /home/ ", type=str, dest="directory", default=None)
    args = parser.parse_args()
    load(args.host, args.port, args.tenant, args.database, args.directory)


if __name__ == '__main__':
    main()


