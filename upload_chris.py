#!/u01/Djracula/.virtualenvs/bacula-glacier/bin/python
import sys,argparse
import glacier
import time
import psycopg2
import os
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
	fsize = os.stat(fname).st_stat
	if fsize > chunksize:
		r = 0
		init_multi = glacier.upload_multi_init(fname,vault,chunksize)
		with open (fname, 'rb') as f:
 			for chunk in iter(lambda: f.read(chunksize), b''):
				if r + chunksize > fsize:
					incr = fsize -r
				else:
					incr = chunksize
				up = glacier.upload_part(vault,init_multi,"bytes " + str(r) + "-" + str(str(r + int(incr) -1) + "/*",body=chunk)
				r = r + chunksize
					
				
		# complete multipart upload
	else:
		try:
			u = glacier.upload_glacier.delay(fname,vault,fname)
			conn=psycopg2.connect(dbconn)
			cur = conn.cursor()
			SQL="""UPDATE media SET comment='{"celery_id": "%(cid)s"}' WHERE volumename='%(vol)s'""" % {'cid': u.id, 'vol': row[0]}
			cur.execute(SQL)
			conn.commit()
			conn.close()	
		except:
			raise
