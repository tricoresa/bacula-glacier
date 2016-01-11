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
#chunksize = 4194304


n = datetime.now()
time = n.strftime('%Y-%m-%d %H:%M:%S')

def submit_hash():
	try:
    		v = glacier.get_vols(jobid)
	except:
    		raise

        for vol in v:
                fname = storage+'/'+vol[0]
                thash = glacier.hash_file.delay(fname)
                data = {'state': 'treehash', 'status': 'in-progress', 'celery_id': thash.task_id, 'path': fname, 'hash': '', 'failed_parts': '', 'upoad_id': '', 'job_id': '', 'error_id': '', 'date': time, 'archiveId': ''}
                d = json.dumps(data)
                SQL = """UPDATE media SET comment='%(data)s' where volumename='%(vol)s'""" % {'data': d, 'vol': vol[0]}
                glacier.update_db(SQL,'update')

def get_hash():
        SQL = """SELECT volumename,comment FROM media WHERE comment->>'state'='treehash' AND comment->>'status'='in-progress'"""
	volumes = glacier.update_db(SQL,'select')
	for vol in volumes:	
		r = AsyncResult(vol[1]['celery_id'])
		if r.ready() == True:
			vol[1]['hash'] = r.result
			vol[1]['status'] = "complete"
			d = json.dumps(vol[1])
                        SQL = """UPDATE media SET comment='%(data)s' WHERE volumename='%(vol)s'""" % {'data': d, 'vol': vol[0]}
                        glacier.update_db(SQL,'update')	


def upload_volume():
	SQL = """SELECT volumename,comment FROM media WHERE comment->>'state'='treehash' AND comment->>'status'='complete'"""
	volumes = glacier.update_db(SQL,'select')
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
				SQL = """UPDATE media SET comment='%(status)s' WHERE volumename='%(vol)s'""" % {'status': s, 'vol': vol[0]}
				glacier.update_db(SQL, 'update')

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
				SQL="""UPDATE media SET comment='%(status)s' WHERE volumename='%(vol)s'""" % {'status': s, 'vol': vol[0]}
				glacier.update_db(SQL, 'update')
			except:
				raise


if command == 'submit':
    submit_hash()
elif command == 'get':
    get_hash()
elif command == 'upload':
    upload_volume()     
else:
    print("Should be either submit or get")

