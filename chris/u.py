#!/u01/Djracula/.virtualenvs/bacula-glacier/bin/python
import sys,argparse
import glacier
import time
import psycopg2
import os
import json
#import hashlib
from treehash import TreeHash

parser = argparse.ArgumentParser()
parser.add_argument("-j", "--job", help="The Bacula Job ID of the completed job", required=True)
parser.add_argument("-s", "--store", help="The storage directory for the job", required=True)
args = parser.parse_args()

jobid = args.job
storage = args.store

try:
    v = glacier.get_vols(jobid)
except:
    print "Failed to get volumes"
    raise


vault = 'BaculaTest001'
dbconn = "dbname='bacula' user='bacula' password='bacula' host='localhost'"
chunksize = 8388608
for row in v:
	fname = storage+'/'+row[0]
	fsize = os.stat(fname).st_size
	#treehash = TreeHash(algo=hashlib.sha256)
	#treehash.update(open(fname, "rb").read())
	#thash = treehash.hexdigest()
	if fsize > chunksize:
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
