#!/usr/bin/env python
# -*- coding: utf-8 -*-
#__author__ = 'zhajiang'
''' 
    Scaning rolling files suffix with .log_analyzed.dat and then upload onto 
    hdfs://10.224.243.130:8020/user/admin/cusp/yyyy-mm-dd/your_file_with_timestamp.log_analyzed.dat
    Usage : sudo python dir_path/scanCuspFilesUpHdfs.py /dir/that_exist_log_analyzed/
    refer documents > https://hdfscli.readthedocs.io/en/latest/api.html#module-hdfs.client
'''

from optparse import OptionParser
from hdfs import InsecureClient
from datetime import tzinfo, timedelta, datetime
import os, sys, time, logging

###########################################################################
#save running log in avw_cusp_upload_hdfs.log
logging.basicConfig(level=logging.DEBUG,
                    #format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s'
                    format='%(asctime)s %(levelname)s %(message)s',
                    #datefmt='%a, %d %b %Y %H:%M:%S'
                    filename='avw_cusp_upload_hdfs.log',
                    filemode='w'
)
###########################################################################
#print log on console screen
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)
###########################################################################

def _getClient(hdfsRoot,hdfsUser):
    client = InsecureClient(hdfsRoot, user=hdfsUser)
    return client

def writeHdfs(client,localDir,hdfsDir,suffix):
    isDir = os.path.isdir(localDir)
    if isDir:
        files = os.listdir(localDir)
        files = filter(lambda str: str.find(suffix) != -1 , files)
        # or use > files = my_list = [str for str in files if str.find(suffix)]
        for file in files:
            # convert partion date directory to yyyy-mm-dd
            _byDate = file[4:14].replace('_','-')
            # write to hdfs
            localFullPath = localDir + '/' + file
            hdfsFullPath = hdfsDir + '/' + _byDate + '/' + file

            # check current file if exists on hdfs
            status = client.status(hdfsFullPath,False)
            # skip if exists
            if status is None:
                logging.info('upload cusp -- File is uploading %s to hdfs' %localFullPath)
                with open(localFullPath) as reader, client.write(hdfsFullPath,encoding='utf-8',overwrite='true') as writer:
                    for line in reader:
                        writer.write(line)
                logging.info('upload cusp -- File:: %s has been uploaded to hdfs' %localFullPath)
            else:
                logging.info('upload cusp -- File:: %s exists on hdfs' %localFullPath)
    else:
        logging.info('upload cusp -- ' + dir + ' is not a vaild directory')

def main():
    options = OptionParser(usage='%prog log [options]', description='select a directory as parameter')
    options.add_option('-d', '--dest', type='string', default='', help='')
    opt, args = options.parse_args()
    if len(args) < 1:
        options.print_help()
        return

    hdfsRoot = 'http://10.224.243.130:50070'
    hdfsUser = 'admin'
    client = _getClient(hdfsRoot,hdfsUser)
    # localDir = 'C:/work'
    localDir=args[0]
    hdfsDir = '/user/admin/cusp'
    suffix = '_analyzed.dat'

    # example like : filePath = log_2017_03_24_005558.log_analyzed.dat
    # write to hdfs://
    writeHdfs(client,localDir,hdfsDir,suffix)

if __name__ =='__main__':
    logging.info("upload cusp -- Getting started...")
    start = datetime.now()
    main()
    elapsed = (datetime.now() - start)
    logging.info("upload cusp -- Write Done and time used:%s"  %(elapsed.seconds))
