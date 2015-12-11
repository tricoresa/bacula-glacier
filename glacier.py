__author__ = 'matt'

import boto3
import psycopg2
from celery import Celery

app = Celery('tasks', backend='amqp', broker='amqp://guest@localhost//')


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

