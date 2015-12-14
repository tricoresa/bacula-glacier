#!/u01/Djracula/.virtualenvs/bacula-glacier/bin/python
import sys,argparse
import glacier
import time
import psycopg2

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
print v
dbconn = "dbname='bacula' user='bacula' password='bacula' host='localhost'"
for row in v:
	file = storage+'/'+row[0]
	print file
	try:
		u = glacier.upload_glacier.delay(file,vault,file)
		print u
		conn=psycopg2.connect(dbconn)
		cur = conn.cursor()
		SQL="""UPDATE media SET comment='{"celery_id": "%(cid)s"}' WHERE volumename='%(vol)s'""" % {'cid': u.id, 'vol': row[0]}
		cur.execute(SQL)
		conn.commit()
		conn.close()	
	except:
		raise
		#print "Upload failed"