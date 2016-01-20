#!/u01/Djracula/.virtualenvs/bacula-glacier/bin/python
import argparse
import glacier_ahs
from celery.result import AsyncResult

parser = argparse.ArgumentParser()
parser.add_argument("-v", "--vault", help="The Glacier Vault name", required=True)
parser.add_argument("-t", "--type", help="inventory or archive", required=True)
parser.add_argument("-a", "--archiveid", help="archiveid", default="")

args = parser.parse_args()

jobid = glacier_ahs.submit_request(args.type, args.vault, args.archiveid)

if jobid.lower() == "failed":
    print("Request submission failed.")
else:
    print("Request submitted successfully. Jobid: " + jobid)

