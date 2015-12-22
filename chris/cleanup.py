#!/u01/Djracula/.virtualenvs/bacula-glacier/bin/python
import boto3
import json
from collections import namedtuple
import glacier
import os

vault = 'BaculaTest001'
account = '117778811131'

def get_uploaded():
  # This will be really "select mediaid from media where comment->>stage='Upload' and comment->>status='Completed';"
  SQL="""select mediaid from media where mediaid=12;"""
  uploaded = glacier.update_db(SQL, 'select')
  return uploaded

def delete_disk_vol(mediaId):
  # This will be SQL="""select comment->>description,volume from media where mediaid={0};""".format(mediaId)
  SQL="""select comment->>'celery_id',volumename from media where mediaid={0};""".format(mediaId)
  row = glacier.update_db(SQL, 'select')
  if row[0][0] != None:
    full_path = '{0}/{1}'.format (row[0], row[1])
    print full_path
    try:
      os.remove(full_path)
    except OSError:
      pass
  else:
    print 'Incorrect file/path, please check if this volume {0} has been uploaded correctly'.format (row[0][1])

def update_status(mediaId):
  stage = 'cleanedup'
  SQL = """UPDATE media SET comment->>'status'='%(status)s' WHERE mediaid='%(id)s'""" % {'status': stage, 'id': mediaId}
  glacier.update_db(SQL, 'update')
  

rows = get_uploaded()
for row in rows:
  mediaId = row[0]
  delete_disk_vol(mediaId)
  update_status(mediaId)
