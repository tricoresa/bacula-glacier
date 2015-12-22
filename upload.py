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
chunksize = 8388608


n = datetime.now()
time = n.strftime('%Y-%m-%d %H:%M:%S')

def submit_hash():
	try:
    		v = glacier.get_vols(jobid)
	except:
    		print "Failed to get volumes"
    		raise
        for vol in v:
                fname = storage+'/'+vol[0]
                thash = glacier.hash_file.delay(fname)
                data = {'state': 'treehash', 'status': 'in-progress', 'celery_id': thash.task_id, 'path': fname, 'hash': '', 'failed_parts': '', 'upoad_id': '', 'job_id': '', 'error_id': '', 'date': time}
                d = json.dumps(data)
                SQL = """UPDATE media SET comment='%(data)s' where volumename='%(vol)s'""" % {'data': d, 'vol': vol[0]}
                glacier.update_db(SQL,'update')

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


def upload_volume(fname):
	SQL = """SELECT volumename,comment FROM media WHERE comment->>'state'='treehash' AND comment->>'status'='complete'"""
	volumes = glacier.update_db(SQL, 'select')
	for vol in volumes:
		fname = vol[1]['path']
		if os.stat(fname).st_size > chunksize:
			r = 0
			try:
				init_multi = glacier.upload_multi_init(fname,vault,str(chunksize))
			except:
				raise
			p = 1
			with open (fname, 'rb') as f:
				status = {}
				status['file_loc'] = fname
 				for chunk in iter(lambda: f.read(chunksize), b''):
					part = "part_"+str(p)	
					if r + chunksize > fsize:
						incr = fsize -r
					else:
						incr = chunksize
					try:
						up = glacier.upload_part(vault,init_multi,"bytes " + str(r) + "-" + str(str(r + int(incr) -1)) + "/*",body=chunk)
					except:
						raise

					r = r + chunksize
					p = p + 1
					if up['ResponseMetadata']['HTTPStatusCode'] != 204:
						status['failures'][part] = '%(start)s-%(end)s' % {'start': r, 'end': r+incr}
				
			try:
				com = glacier.complete_multi(vault,init_multi,str(fsize))# complete multipart upload
				if com['ResponseMetadata']['HTTPStatusCode'] == 201:
					status['glacier_data'] = {'archiveId': com['archiveId'], 'checksum': com['checksum']}
			except:
				raise

			s =  json.dumps(status)
			SQL = """UPDATE media SET comment='%(status)s' WHERE volumename='%(vol)s'""" % {'status': s, 'vol': row[0]}
			glacier.update_db(SQL, 'update')

		else:
			try:
				u = glacier.upload_glacier.delay(fname,vault,fname)
				SQL="""UPDATE media SET comment='{"celery_id": "%(cid)s"}' WHERE volumename='%(vol)s'""" % {'cid': u.id, 'vol': row[0]}
				glacier.update_db(SQL, 'update')
			except:
				raise


if command == 'submit':
    submit_hash()
elif command == 'get':
    get_hash()
elif command == 'upload':
    upload_file(fname)     
else:
    print("Should be either submit or get")

