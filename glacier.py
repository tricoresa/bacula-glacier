__author__ = 'matt'

import boto3
import psycopg2
from celery import Celery
from treehash import TreeHash
import hashlib


# Constants

HTTP_SUCCESS_LOW = 200
HTTP_SUCCESS_HIGH = 226

app = Celery('glacier', backend='amqp', broker='amqp://guest@localhost')
#app.conf.update( 
#    CELERY_ROUTES = { 
#	'glacier.upload_glacier': {'queue': 'upload'},
#	'glacier.upload_part': {'queue': 'multi_upload'},
#	'glacier.hash_file': {'queue': 'hashtree'},
#    },
#)

app.control.rate_limit('glacier.upload_part', '1/h')
@app.task
def hash_file(fname):
	treehash = TreeHash(algo=hashlib.sha256)
    with open(fname, 'rb') as f:
        while True:
            buf = f.read(1024 * 1024)
            if not buf:
                break
            treehash.update(buf)
    return treehash.hexdigest()

@app.task
def get_vols(jobid):
    SQL = """SELECT volumename from media where mediaid in (select mediaid from jobmedia where jobid=%(job)s)""" % {'job':jobid}

    try:
        conn = psycopg2.connect("dbname='bacula' user='bacula' host='localhost' password='bacula'")
    except:
        return "Failed"

    cur = conn.cursor()
    cur.execute(SQL)
    rows = cur.fetchall()
    return rows


@app.task
def upload_glacier(filename, vault, description):
    glacier_conn = boto3.client('glacier')
    with open(filename, 'rb') as f:
        upload_arc = glacier_conn.upload_archive(vaultName=vault, archiveDescription=description, body=f)
    return upload_arc

@app.task
def upload_multi_init(filename, vault, chunksize):
    g = boto3.client('glacier')
    try:
        r = g.initiate_multipart_upload(vaultName=vault,archiveDescription=filename,partSize=chunksize)
    except:
        raise
    return r['uploadId']

@app.task
def upload_multi_exec(fname, fsize, vault, uid, chunksize):
    g = boto3.client('glacier')
    p = 1

    try:
        with open (fname, 'rb') as f:
            r = 0
            failed_parts = {}
            for chunk in iter(lambda: f.read(chunksize), b''):
                part = "part_"+str(p)
                if r + chunksize > fsize:
                    incr = fsize -r
                else:
                    incr = chunksize
                    part_range = "bytes " + str(r) + "-" + str(str(r + int(incr) -1)) + "/*"
                    g = boto3.client('glacier')
                try:
                    response = g.upload_multipart_part(vaultName=vault,uploadId=uid,range=part_range,body=chunk)
                except:
                    failed_parts[part] = str(str(r + int(incr) -1))
                    pass
                g = None

                                #if not HTTP_SUCCESS_LOW <= response['status'] <= HTTP_SUCCESS_HIGH:
                                #        success = False

                                r = r + chunksize
                                p = p + 1

    except:
        failed_parts = 'all'

    return failed_parts



@app.task
def upload_part(vault,uid,range,body):
    g = boto3.client('glacier')
    try:
        r = g.upload_multipart_part(vaultName=vault,uploadId=uid,range=range,body=body)
    except:
        raise
    return r

@app.task
def complete_multi(vault,uid,size,check):
    g = boto3.client('glacier')
    try:
        r = g.complete_multipart_upload(vaultName=vault,uploadId=uid,archiveSize=size,checksum=check)
    except:
        raise
    return r

@app.task
def update_db(SQL, query):
    dbconn = "dbname='bacula' user='bacula' password='bacula' host='localhost'"
    conn=psycopg2.connect(dbconn)
    cur = conn.cursor()
    try:
        cur.execute(SQL)
    except:
        raise
    if query == 'select':
        rows = cur.fetchall()
        return rows
    conn.commit()
    conn.close()

def get_hash():
        SQL = """SELECT volumename,comment FROM media WHERE comment->>'state'='treehash' AND comment->>'status'='in-progress'"""
        volumes = glacier.update_db(SQL, 'select')
        for vol in volumes:
                r = AsyncResult(vol[1]['celery_id'])
                if r.ready() == True:
                        vol[1]['hash'] = r.result
                        vol[1]['status'] = "complete"
                        d = json.dumps(vol[1])
                        SQL = """UPDATE media SET comment='%(data)s' WHERE volumename='%(vol)s'""" % {'data': d, 'vol': vol[0]}
                        glacier.update_db(SQL,'update')


Class db_data():
    def __init__():
        dbconn = "dbname='bacula' user='bacula' password='bacula' host='localhost'"
        conn=psycopg2.connect(dbconn)
        cur = conn.cursor()

    def select(columns,table,clause):
        SQL = """SELECT %(columns)s from %(table)s where %(condition)s""" % {'columns': columns, 'table': table, 'condition':clause}
        cur.execute(SQL)
        rows = cur.fetchall()
        return rows

    def update_comment(data,volume):
        SQL = """UPDATE media SET comment='%(data)s' where volumename='%(vol)s'""" % {'data': data, 'vol': volume}
