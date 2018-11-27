#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os

MOVTO = "20180301"

#os.system("hdfs dfs -get /gpa/rawzone/stg/manual_rh")
#os.system("hdfs dfs -get /gpa/rawzone/stg/rightnow")
#os.system("hdfs dfs -get /gpa/rawzone/stg/elancers")
#os.system("hdfs dfs -get /gpa/rawzone/stg/adp")
#os.system("hdfs dfs -get /gpa/rawzone/stg/sap")

#"find /home/cloudera/maximilian_erhard/ -name *-20180301* -exec rm -rf {} \;"
base_path = "/home/cloudera/maximilian_erhard/"
maximilian_erhard = map(lambda x: base_path + x + "/",os.listdir(base_path))
files = list()
for database in maximilian_erhard:
    tables = map(lambda x: database + x + "/",os.listdir(database))

    for datafiles in tables:
        datafiles = map(lambda x: datafiles + x, os.listdir(datafiles))
        datafiles.sort(reverse=True)
        files_qtty = len(datafiles)

        while files_qtty > 1:
            os.remove(datafiles[files_qtty-1])
            del datafiles[files_qtty-1]
            files_qtty = files_qtty-1

        datafiles = [x for x in datafiles if x != []]

        #print(filter(lambda x: not x,datafiles))
        #datafiles = filter(lambda x, x is not None,datafiles)
        print(datafiles)
        for datafile in datafiles:
            files.append(datafile)

for file in files:

    if "." in file.split("/")[-1]:
        name,ext = file.split(".")
        new_file_name = name + "-" + MOVTO + "." + ext
    else:
        new_file_name = file + "-" + MOVTO

    with open(file,"r") as old_file, open(new_file_name,"w+") as new_file:
        for i in range(0,11):
            new_file.write(old_file.readline())

    print(old_file.name)
    old_file.close()
    new_file.close()
    os.remove(old_file.name)

"scp -r cloudera@10.183.36.175:/home/cloudera/maximilian_erhard/* ."

