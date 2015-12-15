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


def upload_multi_init(filename, vault, chunksize):
	g = boto3.client('glacier')
	try:
		r = g.initiate_multipart_upload(vaultName=vault,archiveDescription=filename,partSize=chunksize)
	except:
		raise
	return r['uploadId']


def upload_part(vault,uid,range,body):
	g = boto3.client('glacier')
	try:
		r = g.upload_multipart_part(vaultName=vault,uploadId=uid,range=range,body=body)
	except:
		raise
	return r

def complete_multi(vault,uid,size,tree):
	g = boto3.client('glacier')
	try:
		r = g.complete_multipart_upload(vaultName=vault,uploadId=uid,archiveSize=size,checksum=tree)
	except: 
		raise
	return r
	
