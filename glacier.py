__author__ = 'matt'

import boto3
import psycopg2
from celery import Celery
from treehash import TreeHash
import hashlib
import math
import os


# Constants

HTTP_SUCCESS_LOW = 200
HTTP_SUCCESS_HIGH = 226
DEFAULT_HASH_CHUNK_SIZE = pow(1024, 2)


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
        failed_parts = 'failed'

    return failed_parts

@app.task
def upload_failed_parts(fname,vault,uid,part_range,chunksize):
    try:
        with open(fname, 'rb') as f:
            p = part_range.split('-')
            part_range = "bytes " + part_range
            start = int(p[0])
            f.seek(start)
            body = f.read(chunksize) 
            g = boto3.client('glacier')
            response = g.upload_multipart_part(vaultName=vault,uploadId=uid,range=part_range,body=body)
            g = None
    except:
        raise
    return response
    

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


# convenience functions for boundary sanity checks
def next_power_of_2(num):
    return int(pow(2, round(math.log(num)/math.log(2))))

def is_power_of_2(num):
    return num != 0 and ((num & (num - 1)) == 0)

def response_success(response):
    if 'status' in response:
        status_code = response['status']
    elif 'ResponseMetadata' in response:
        status_code = response['ResponseMetadata']['HTTPStatusCode']
    else:
        status_code = -1

    return (HTTP_SUCCESS_LOW <= status_code <= HTTP_SUCCESS_HIGH)

def treehash_on_file_range(treehash, filename, start, end, hash_chunk_size=DEFAULT_HASH_CHUNK_SIZE):

    treehash_local = TreeHash(algo=hashlib.sha256)

    try:
        infile = open(filename, "rb")
        infile.seek(start)

        current_pos = start
        end += 1
        while current_pos < end:
            read_size = end - current_pos
            if read_size > hash_chunk_size:
                read_size = hash_chunk_size

            chunk = infile.read(read_size)
            if treehash:
                treehash.update(chunk)
            treehash_local.update(chunk)

            current_pos += read_size
        infile.close()

    except:
        raise

    return treehash_local.hexdigest()


@app.task
def submit_request(type, vault, archiveid=""):
    client = boto3.client('glacier')

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
            response = client.initiate_job(
                vaultName=vault,
                jobParameters=params
            )

            if response_success(response):
                result = response['jobId']

    except:
        raise

    return result

def process_archive_retrieval_range(job,output_path,start,end,friendly_name,treehash=None):

    range_string = "bytes=" + str(start) + "-" + str(end)

    try:
        response = job.get_output(
            range=range_string)

        if response_success(response):

            if friendly_name:
                filename = output_path + "/" + os.path.basename(response['archiveDescription'])
            else:
                filename = output_path + "/" + job.id + ".archive"

            if os.path.exists(filename):
                statinfo = os.stat(filename)
                if(start > statinfo.st_size):
                    # chunk range is beyond end of file
                    return False
                else:
                    archive_file = open(filename, "r+b")
            else:
                archive_file = open(filename, "wb")

            archive_file.seek(start)
            archive_file.write(response['body'].read())
            archive_file.close()
            section_hash = treehash_on_file_range(treehash, filename, start, end)

            if section_hash != response['checksum']:
                # Failed checksum
                return False

        else:
            return False
    except:
        raise

    return True


def process_archive_retrieval_job(job,chunk_size,output_path,friendly_name=False):
    global chunk_count

    filepos_limit = job.archive_size_in_bytes - 1
    pad_length = len(str(job.archive_size_in_bytes // chunk_size)) + 1
    current_pos = 0
    job_archive_hash = job.archive_sha256_tree_hash
    chunk_count = 0
    failed_parts = {}
    running_treehash = TreeHash(algo=hashlib.sha256)
    try:
        while current_pos < filepos_limit:
            chunk_count += 1
            end_pos = current_pos + (chunk_size - 1)
            if end_pos > filepos_limit:
                end_pos = filepos_limit

            if not process_archive_retrieval_range(job, output_path, current_pos, end_pos, friendly_name, running_treehash):
                failed_parts["part_" + str(chunk_count).zfill(pad_length)]=[current_pos, end_pos]
            current_pos = end_pos + 1

        if (not failed_parts) and (running_treehash.hexdigest() != job.archive_sha256_tree_hash):
            failed_parts = None
            failed_parts["all"] = True
    except:
        failed_parts["all"] = True
        raise

    return failed_parts


def process_inventory_retrieval_job(job,output_path,friendly_name=False):

    failed_parts = {}

    try:
        response = job.get_output()
        if response_success(response):
            if friendly_name:
                output_name = "Inventory_completed_" + job.completion_date + ".json"
            else:
                output_name = job.id + ".inventory.json"
            inventory_file = open(output_path + "/" + output_name, "wb")
            inventory_file.write(response['body'].read())
            inventory_file.close
        else:
            failed_parts["all"] = True
    except:
        failed_parts["all"] = True 
        raise

    return failed_parts

@app.task
def process_failed_job(vault, jobid, archive_file, failed_list, accountid="-"):
    try:
        glacier = boto3.resource('glacier')

        job = glacier.Job(
            account_id=accountid,
            vault_name=vault,
            id=jobid
        )

        new_failed_list = {}
        for part in sorted(failed_list):
           if not process_archive_retrieval_range(job, archive_file, failed_list[part][0], failed_list[part][1],False):
               new_failed_list[part]=[failed_list[part][0], failed_list[part][1]]

        if (not new_failed_list) and (hash_file(archive_file) != job.archive_sha256_tree_hash):
            new_failed_list = None
            new_failed_list["all"] = True
    except:
        new_failed_list["all"] = True
        raise
    return new_failed_list


@app.task
def process_job(job,chunk_size,output_path,friendly_name=False):
    try:
        job.load()

        failed_parts = {}
        if job.status_code == "Succeeded":
            if job.action == "InventoryRetrieval":
                failed_parts = process_inventory_retrieval_job(job,output_path,friendly_name)
            else:
                failed_parts = process_archive_retrieval_job(job,chunk_size,output_path,friendly_name)
        else:
            failed_parts["all"] = True
    except:
        raise

    return failed_parts



@app.task
def process_request(vault, jobid, chunksize, outputpath, friendlyname=False, accountid="-"):

    
    failed_collection = {}
    try:
        client = boto3.client('glacier')
        glacier = boto3.resource('glacier')
    
        if (jobid.lower() == "any") or (jobid.lower() == "inventory") or (jobid.lower() == "archive"):
            response = client.list_jobs(
                vaultName=vault,
                statuscode="Succeeded"
            )

            if response_success(response):
                for jobitem in response['JobList']:
                    job = glacier.Job(
                        account_id=accountid,
                        vault_name=vault,
                        id=jobitem['JobId']
                    )
                    job.load()

                    if (jobid.lower() == "inventory") and (job.action == "InventoryRetrieval"):
                        failed_collection[job.id] = process_job(job, chunksize, outputpath, friendlyname)
                    elif (jobid.lower() == "archive") and (job.action == "ArchiveRetrieval"):
                        failed_collection[job.id] = process_job(job, chunksize, outputpath, friendlyname)
                    elif (jobid.lower() == "any"):
                        failed_collection[job.id] = process_job(job, chunksize, outputpath, friendlyname)
            else:
                failed_collection["all"] = True
        else:
            job = glacier.Job(
                account_id=accountid,
                vault_name=vault,
                id=jobid
            )
            job.load()
            failed_collection[job.id] = process_job(job, chunksize, outputpath, friendlyname)
    except:
        raise

    return failed_collection

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
