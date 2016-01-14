#!/u01/Djracula/.virtualenvs/bacula-glacier/bin/python
import sys,argparse
import glacier
from datetime import datetime
import psycopg2
import os
import json
from celery.result import AsyncResult

parser = argparse.ArgumentParser()
parser.add_argument("-j", "--job", help="The Bacula Job ID of the completed job", required=False)
parser.add_argument("-s", "--store", help="The storage directory for the job", required=False)
parser.add_argument("-c", "--command", help="submit or get", required=True)
parser.add_argument("-v", "--vault", help="The Glacier Vault name", required=True)

args = parser.parse_args()

jobid = args.job
storage = args.store
command = args.command
vault = args.vault
chunksize = 1073741824
#chunksize = 4294967296
#chunksize = 2147483648

n = datetime.now()
time = n.strftime('%Y-%m-%d %H:%M:%S')

def submit_hash():
	d = glacier.db_data()
	try:
    		v = d.job_vols(jobid)
	except:
    		raise

        for vol in v:
                fname = storage+'/'+vol[0]
                thash = glacier.hash_file.delay(fname)
                data = {'state': 'treehash', 'status': 'in-progress', 'celery_id': thash.task_id, 'path': fname, 'hash': '', 'failed_parts': '', 'upload_id': '', 'job_id': '', 'error_id': '', 'date': time, 'archiveId': ''}
                s = json.dumps(data)
                d.update_comment(s, vol[0])

def get_hash():
	d = glacier.db_data()
	volumes = d.get_vol_hash()
	for vol in volumes:	
		r = AsyncResult(vol[1]['celery_id'])
		if r.ready() == True:
			vol[1]['hash'] = r.result
			vol[1]['status'] = "complete"
			s = json.dumps(vol[1])
                        d.update_comment(s, vol[0])	


def upload_volume():
	d = glacier.db_data()
	volumes = d.get_upload_vols()
	for vol in volumes:
		fname = vol[1]['path']
		vol[1]['status'] = 'in-progress'
		vol[1]['state'] = 'multi-upload'

		fsize = os.stat(fname).st_size
		if fsize > chunksize:
			print "Working on %s" % fname
			r = 0
			try:
				init_multi = glacier.upload_multi_init(fname,vault,str(chunksize))
				vol[1]['upload_id'] = init_multi
				s = json.dumps(vol[1])
				d.update_comment(s, vol[0])

			except:
				raise
			
                        try:
                                up = glacier.upload_multi_exec.delay(fname,fsize,vault,init_multi,chunksize)
                        except:
                                raise
				

		else:
			try:
				u = glacier.upload_glacier.delay(fname,vault,fname)
				vol[1]['status'] = 'in-progress'
				vol[1]['state'] = 'single-upload'
				vol[1]['single_upload_id'] = u.id
				s = json.dumps(vol[1])
				d.update_comment(s, vol[0])
			except:
				raise


def complete_single():
    d = glacier.db_data()
    volumes = d.get_single_uploads()
    for vol in volumes:
        r = AsyncResult(vol[1]['celery_id'])
	if r.ready() == True:
            vol[1]['archiveId'] = r.result
            vol[1]['status'] = 'complete'
            s = json.dumps(vol[1])
            d.update_comment(s,vol[0])  

def complete_multi():
    d = glacier.db_data()
    volumes = d.get_multi_uploads()
    for vol in volumes:
        r = AsyncResult(vol[1]['celery_id'])
        if r.ready() == True:
            if 'archiveId' in r.result:
	        vol[1]['archiveid'] = r.result
	    else:
                vol[1]['failed_parts'] = r.result

	
#def complete_multi():

if command == 'submit':
    submit_hash()
elif command == 'get':
    get_hash()
elif command == 'upload':
    upload_volume()     
elif command == 'complete':
    complete_single()
    complete_multi()
else:
    print("Should be either submit or get")

