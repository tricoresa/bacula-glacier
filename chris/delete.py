#!/u01/Djracula/.virtualenvs/bacula-glacier/bin/python
import boto3
import json
from collections import namedtuple
import psycopg2
import subprocess

vault = 'BaculaTest001'
account = '117778811131'

def get_purged():
  # This will be really "select media,comment from media where volstatus like '%Purged%';"
  SQL="""select mediaid,comment from media where mediaid=12;"""
  dbconn = "dbname='bacula' user='bacula' password='bacula' host='localhost'"

  try:
    conn=psycopg2.connect(dbconn)
    cur = conn.cursor()
    cur.execute(SQL)
    rows = cur.fetchall()
    conn.commit()
    conn.close()	
    return rows
  except:
    #print "delete failed"
    raise

def get_bacula_vol(mediaId):
  SQL="""select volumename from media where mediaid={0};""".format(mediaId)
  dbconn = "dbname='bacula' user='bacula' password='bacula' host='localhost'"

  try:
    conn=psycopg2.connect(dbconn)
    cur = conn.cursor()
    cur.execute(SQL)
    name = cur.fetchall()
    conn.commit()
    conn.close()	
    return name
  except:
    raise 

def delete_bacula_vol(mediaId):
  vol_names = get_bacula_vol(mediaId)
  for vol in vol_names:
    copy_cmd = 'delete yes volume={0}'.format(vol[0])
    print copy_cmd
    echo = subprocess.Popen(["echo", copy_cmd], stdout=subprocess.PIPE)
    run_copy = subprocess.Popen(["/opt/bacula/bin/bconsole"], stdin=echo.stdout, stdout=subprocess.PIPE)
    run_copy.communicate()


def delete_archive_vol(mediaId):
  try:
    glacier = boto3.resource('glacier')
    archive = glacier.Archive(account, vault, id)
    delete = archive.delete()
    # Log the return code, and requestID
    print delete
  except:
    #delete failed
    raise

rows = get_purged()
for row in rows:
  mediaId = row[0]
  delete_bacula_vol(mediaId)
  glacierId = row[1]['archiveId']
  delete_glacier_vol(archiveId)

