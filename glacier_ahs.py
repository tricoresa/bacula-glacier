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
    glacier_conn = None

@app.task
def upload_multi_init(filename, vault, chunksize):
    g = boto3.client('glacier')
    try:
        r = g.initiate_multipart_upload(vaultName=vault,archiveDescription=filename,partSize=chunksize)
    except:
        raise
    return r['uploadId']
    g = None

@app.task
def upload_multi_exec(fname, fsize, vault, uid, chunksize):
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
                try:
                    g = boto3.client('glacier')
                    response = g.upload_multipart_part(vaultName=vault,uploadId=uid,range=part_range,body=chunk)
                    g = None
                except:
                    failed_parts[part] = part_range
                    pass

                r = r + chunksize
                p = p + 1

    except:
        failed_parts = 'all'

    return failed_parts



#@app.task
#def upload_part(vault,uid,range,body):
#    g = boto3.client('glacier')
#    try:
#        r = g.upload_multipart_part(vaultName=vault,uploadId=uid,range=range,body=body)
#    except:
#        raise
#    return r

@app.task
def complete_multi(vault,uid,size,check):
    g = boto3.client('glacier')
    try:
        r = g.complete_multipart_upload(vaultName=vault,uploadId=uid,archiveSize=size,checksum=check)
    except:
        raise
    return r

#@app.task
#def update_db(SQL, query):
#    dbconn = "dbname='bacula' user='bacula' password='bacula' host='localhost'"
#    conn=psycopg2.connect(dbconn)
#    cur = conn.cursor()
#    try:
#        cur.execute(SQL)
#    except:
#        raise
#    if query == 'select':
#        rows = cur.fetchall()
#        return rows
#    conn.commit()
#    conn.close()


@app.task
def submit_request(type, vault, archiveid=""):
    g = boto3.client('glacier')

    result = "failed"
    params = None
    try:
        if type.lower() == "inventory":
            params={
                "Format": "JSON",
                "Type": "inventory-retrieval"
            }
        elif (type.lower() == "archive") and archiveid:
            params={
                "Type": "archive-retrieval",
                "ArchiveId": archiveid
            }

        if params:
            response = g.initiate_job(
                vaultName=vault,
                jobParameters=params
            )

            if (HTTP_SUCCESS_LOW <= response['ResponseMetadata']['HTTPStatusCode'] <= HTTP_SUCCESS_HIGH):
                result = response['jobId']

    except:
        raise

    return result


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


class db_data:
    dbconn = "dbname='bacula' user='bacula' password='bacula' host='localhost'"
    conn=psycopg2.connect(dbconn)
    cur = conn.cursor()
 
    def job_vols(self, jobid):
        SQL = """SELECT volumename from media where mediaid in (select mediaid from jobmedia where jobid=%(job)s)""" % {'job':jobid}        
        self.cur.execute(SQL)
        rows = self.cur.fetchall()
        return rows        

    def get_vol_hash(self):
        SQL="""SELECT volumename,comment FROM media WHERE comment->>'state'='treehash' AND comment->>'status'='in-progress'"""
        self.cur.execute(SQL)
        rows = self.cur.fetchall()
        return rows

    def get_upload_vols(self):
        SQL = """SELECT  volumename,comment FROM media WHERE comment->>'state'='treehash' AND comment->>'status'='complete'"""
        self.cur.execute(SQL)
        rows = self.cur.fetchall()
        return rows


    def get_single_uploads(self):
        SQL = """SELECT volumename,comment FROM media WHERE comment->>'state'='single-upload' AND comment->>'status'='in-progress'"""
        self.cur.execute(SQL)
        rows = self.cur.fetchall()
        return rows

    def get_multi_uploads(self):
        SQL = """SELECT volumename,comment FROM media WHERE comment->>'state'='multi-upload' AND comment->>'status'='in-progress'"""
        self.cur.execute(SQL)
        rows = self.cur.fetchall()
        return rows

    def update_comment(self,data,volume):
        SQL = """UPDATE media SET comment='%(data)s' where volumename='%(vol)s'""" % {'data': data, 'vol': volume}
        try:
            self.cur.execute(SQL)
        except: 
            return 'comment update failed'
        self.conn.commit()

    def get_completed_volumes(self):
        SQL="""SELECT mediaid,comment,volumename FROM media WHERE comment->>'state'='multi-upload' AND comment->>'status'='complete' OR (comment->>'state'='single-upload' AND comment->>'status'='complete')"""
        try:
            self.cur.execute(SQL)
        except:
            return 'failed'
        rows = self.cur.fetchall()
        return rows

    def get_purged_volumes(self):
        SQL="""SELECT mediaid,comment,volumename FROM media WHERE comment->>'state'='cleanedup' AND comment->>'status'='complete' AND volstatus like '%Purged%';"""
        try:
            self.cur.execute(SQL)
        except:
            return 'failed'
        rows = self.cur.fetchall()
        return rows

    def select_volumes(self,clause):
        SQL="""SELECT mediaid,comment,volumename FROM media WHERE {0}""".format(clause)
        try:
            self.cur.execute(SQL)
        except:
            return 'failed'
        rows = self.cur.fetchall()
        return rows
