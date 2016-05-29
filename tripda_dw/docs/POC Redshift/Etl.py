import boto
import boto.ec2
import boto.rds
from boto.s3.connection import S3Connection
from boto.s3.key import Key

import math, os
import boto
from filechunkio import FileChunkIO

import psycopg2
import pprint

import sys
import datetime

#[Credentials]
aws_access_key_id = 'AKIAIV2RUJREUS2TLWEA'
aws_secret_access_key = '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'

def conection():
    conn_s3 = S3Connection(aws_access_key_id, aws_secret_access_key)
    b = conn_s3.get_bucket('testetripda')
    return conn_s3, b


def Load_s3_chunk(upload_file):    
    source_path = upload_file
    source_size = os.stat(source_path).st_size

    mp = b.initiate_multipart_upload(os.path.basename(source_path))

    chunk_size = 52428800
    chunk_count = int(math.ceil(source_size / float(chunk_size)))

    for i in range(chunk_count):
        offset = chunk_size * i
        bytes = min(chunk_size, source_size - offset)
        with FileChunkIO(source_path, 'r', offset=offset,bytes=bytes) as fp:
            mp.upload_part_from_file(fp, part_num=i + 1)

    mp.complete_upload()

    k = Key(b)


def Load_s3(b):
    k = Key(b) 
    k.key = 'trips.csv'
    k.set_contents_from_filename('trips.csv')
    return k


def permissions():    
    bucketname = sys.argv[1]
    dirname = sys.argv[2]
    conn_s3 = S3Connection(aws_access_key_id, aws_secret_access_key)
    b = conn_s3.get_bucket('testetripda')

    keys = b.list(dirname)

    for k in keys:
        new_grants = []
        acl = k.get_acl()
        for g in acl.acl.grants:
            if g.uri != "http://acs.amazonaws.com/groups/global/AllUsers":
                new_grants.append(g)
        acl.acl.grants = new_grants
        k.set_acl(acl)


def s3_to_redshift():  
    conn_redshift = psycopg2.connect(dbname = database, host = endpoint, port = porta, user = usr, password = pwd)
    cur = conn_redshift.cursor()

    cur.execute(Copy)
    conn_redshift.commit()

database = 'dev'
endpoint = 'examplecluster.cmtaaopdwh3l.us-west-2.redshift.amazonaws.com'
porta = 5439
usr = 'masteruser'
pwd = 'Masteruser123'

link = 's3://testetripda/sample.csv'    

Copy = 'COPY \n    example_resumable \nFROM \n'
Copy += '    \'' + link + '\'' + '\n'
Copy += 'CREDENTIALS\n    \'aws_access_key_id='
Copy += aws_access_key_id
Copy += ';aws_secret_access_key='
Copy += aws_secret_access_key + '\'\n'
Copy += 'DELIMITER' + '\n'
Copy += '    \',\';'


def main():
    conn, b = conection()
    Load_s3(b)
    s3_to_redshift()


if __name__ == '__main__':
    inicio = datetime.datetime.now().time() 
    main()
    print datetime.datetime.now().time() - inicio