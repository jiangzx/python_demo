#!/usr/bin/env python
# -*- coding: utf-8 -*-
''' 
    Scaning files suffix with .log_analyzed.dat and then upload to hdfs://10.224.243.130:8020/user/admin/ 
    refer documents >> https://hdfscli.readthedocs.io/en/latest/api.html#module-hdfs.client
'''
__author__ = 'zhajiang@cisco.com'

from hdfs import InsecureClient
import os, sys, time

def _getClient(hdfsRoot,hdfsUser):
    client = InsecureClient(hdfsRoot, user=hdfsUser)
    return client

def writeHdfs(client,localDir,hdfsDir,suffix):
    isDir = os.path.isdir(localDir)
    if isDir:
        files = os.listdir(localDir)
        files = filter(lambda str: str.find(suffix) != -1 , files)
        # or use >> files = my_list = [str for str in files if str.find(suffix)]
        for file in files:
            start = time.time()
            _byDate = file[4:14]
            # write to hdfs
            localFullPath = localDir + '/' + file
            hdfsFullPath = hdfsDir + '/' + _byDate + '/' + file
            with open(localFullPath) as reader, client.write(hdfsFullPath,encoding='utf-8',overwrite='true') as writer:
                for line in reader:
                    writer.write(line)
            end = time.time()
            elapsed = end - start
            print file + ' process costs %f %s'  %(elapsed,'sec')
    else:
        print dir + ' is not a vaild directory'

if __name__ =='__main__':
    hdfsRoot = 'http://10.224.243.130:50070'
    hdfsUser = 'admin'
    client = _getClient(hdfsRoot,hdfsUser)
    localDir = 'C:\work'
    hdfsDir = '/user/admin/cusp'
    suffix = '.log_analyzed.dat'
    # filePath = log_2017_03_24_005558.log_analyzed.dat
    # write to hdfs
    print 'Start to process task...'
    writeHdfs(client,localDir,hdfsDir,suffix)
    print 'Write done.'
