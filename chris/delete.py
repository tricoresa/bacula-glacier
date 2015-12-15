#!/u01/Djracula/.virtualenvs/bacula-glacier/bin/python
import boto3
import json
from collections import namedtuple
import psycopg2

def get_purged():
  # This will be really "select comment from media where volstatus like '%Purged%';"
  SQL="""select comment from media where mediaid=12;"""
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


vault = 'BaculaTest001'
account = '117778811131'

rows = get_purged()
for row in rows:
  try:
    id = row[0]['archiveId']
    print id
    glacier = boto3.resource('glacier')
    archive = glacier.Archive(account, vault, id)
    delete = archive.delete()
    print delete
  except:
    #delete failed
    raise
