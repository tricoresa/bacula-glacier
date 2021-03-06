#!/usr/bin/env python

import boto3
import argparse
import json
import StringIO
import sys
import os
import math
import hashlib
from treehash import TreeHash


# Constants

HTTP_SUCCESS_LOW = 200
HTTP_SUCCESS_HIGH = 226
DEFAULT_ACCOUNT_ID = "-"
DEFAULT_CHUNK_SIZE = 1024 ** 3
DEFAULT_OUTPUT_PATH = "."
DEFAULT_HASH_CHUNK_SIZE = 512 ** 2


# Global Variables

Debug = False

chunk_count = 0



class JobInfo:
    __job = None
    __archive_name = ""
    __archive_description = ""
    __archive_treehash = ""
    def __init__(self, job=None):
        __job = job

    



def set_debug(flag):
    global Debug
    Debug = flag

def next_power_of_2(num):
    return int(pow(2, round(math.log(num)/math.log(2))))

def is_power_of_2(num):
    return num != 0 and ((num & (num - 1)) == 0)

def treehash_on_file_range(treehash, filename, start, end, hash_chunk_size=DEFAULT_HASH_CHUNK_SIZE):

    infile = open(filename, "rb")

    infile.seek(start)

    if Debug:
        print("Treehash: Start: " + str(start) + ", End: " + str(end))

    treehash_local = TreeHash(algo=hashlib.sha256)
    current_pos = start
    end += 1
    while current_pos < end:
        read_size = end - current_pos
        if read_size > hash_chunk_size:
            read_size = hash_chunk_size

        chunk = infile.read(read_size)
        if treehash:
            treehash.update(chunk)
        treehash_local.update(chunk)

        current_pos += read_size
    infile.close()

    return treehash_local.hexdigest()


def process_archive_retrieval_range(job,output_path,start,end,friendly_name,treehash=None):

    range_string = "bytes=" + str(start) + "-" + str(end)
    response = job.get_output(
        range=range_string
    )

    if Debug:
        print("process_archive_retrieval_job: job.get_output() response: " + str(response))

    if HTTP_SUCCESS_LOW <= response['status'] <= HTTP_SUCCESS_HIGH:

        if Debug:
            #print("Writing chunk " + str(chunk_count) + " " + range_string + " Checksum: " + response['checksum'] + " ContentRange: " + response['contentRange'] + " AcceptRanges: " + response['acceptRanges'] + " ContentType: " + response['contentType'] + " ArchiveDescription: " + response['archiveDescription'])
            print("Writing chunk " + range_string + " Checksum: " + response['checksum'])

        if friendly_name:
            filename = output_path + "/" + os.path.basename(response['archiveDescription'])
        else:
            filename = output_path + "/" + job.id + ".archive"

        if os.path.exists(filename):
            statinfo = os.stat(filename)
            if Debug:
                print("File size: " + str(statinfo.st_size) + " start: " + str(start) + " end: " + str(end))
            if(start > statinfo.st_size):
                print("Range specified is non-contiguous with the end of file. Aborting.")
                return False
            else:
                archive_file = open(filename, "r+b")
        else:
            archive_file = open(filename, "wb")

        archive_file.seek(start)
        archive_file.write(response['body'].read())
        #archive_file.flush()
        archive_file.close()
        section_hash = treehash_on_file_range(treehash, filename, start, end)

        if section_hash != response['checksum']:
            if Debug:
                print("Checkum failed for " + range_string + " Checkum expected: " + response['checksum'] + " locally calculated: " + section_hash)

            return False

        if Debug:
            if treehash:
                print("Current running treehash is  " + treehash.hexdigest())

    else:
        print("Retrieval unsuccessful: " + str(response['status']))
        return False

    return True


def process_archive_retrieval_job(job,chunk_size,output_path,friendly_name=False):
    global chunk_count 

    filepos_limit = job.archive_size_in_bytes - 1
    current_pos = 0
    job_archive_hash = job.archive_sha256_tree_hash
    chunk_count = 0
    failed_parts = {}
    running_treehash = TreeHash(algo=hashlib.sha256)
    while current_pos < filepos_limit:
        chunk_count += 1
        end_pos = current_pos + (chunk_size - 1)
        #end_pos = current_pos + chunk_size
        if end_pos > filepos_limit:
            end_pos = filepos_limit
        
        if not process_archive_retrieval_range(job, output_path, current_pos, end_pos, friendly_name, running_treehash):
        #if True:
            if Debug:
                print("Section " + str(current_pos) + "-" + str(end_pos) + " failed.")
            failed_parts[chunk_count]=[current_pos, end_pos]
        current_pos = end_pos + 1

    return failed_parts
    #if failed_parts:
        #failed_parts_file = open(output_path + "/" + job.id + ".failed_list", "w")
        #json.dump(failed_parts, failed_parts_file, sort_keys=True)
        #failed_parts_file.close()
        


def process_inventory_retrieval_job(job,output_path,friendly_name=False):
    response = job.get_output()

    failed_parts = {}

    if Debug:
        print("process_inventory_retrieval_job: job.get_output() response: " + str(response))

    if HTTP_SUCCESS_LOW <= response['status'] <= HTTP_SUCCESS_HIGH:
        if friendly_name:
            output_name = "Inventory_completed_" + job.completion_date + ".json"
        else:
            output_name = job.id + ".inventory.json"
        inventory_file = open(output_path + "/" + output_name, "wb")
        inventory_file.write(response['body'].read())
        inventory_file.close
    else:
        failed_parts = 'all'
return failed_parts


def process_failed_job(job, chunk_size, archive_file, failed_list,friendly_name=False):
    failed_list_file = open(failed_file, "r")
    failed_list = json.load(failed_list_file)

    for part in sorted(failed_list):
       if not process_archive_retrieval_range(job, archive_file, failed_list[part][0], failed_list[part][1],friendly_name):
           print("Something screwey")

def process_job(job,chunk_size,output_path,friendly_name=False):
    job.load()

    if Debug:
        print("Job Action: " + str(job.action))
        print("Job Archive ID: " + str(job.archive_id))
        print("Job Archive SHA256 Tree Hash: " + str(job.archive_sha256_tree_hash))
        print("Job Archive Size in Bytes: " + str(job.archive_size_in_bytes))
        print("Job Completed: " + str(job.completed))
        print("Job Completion Date: " + str(job.completion_date))
        print("Job Creation Date: " + str(job.creation_date))
        print("Job Inventory Retrieval Parameters: " + str(job.inventory_retrieval_parameters))
        print("Job Inventory Size in Bytes: " + str(job.inventory_size_in_bytes))
        print("Job Description: " + str(job.job_description))
        print("Job ID: " + str(job.job_id))
        print("Job Retrieval Byte Range: " + str(job.retrieval_byte_range))
        print("Job SHA256 Tree Hash: " + str(job.sha256_tree_hash))
        print("Job SNS Topic: " + str(job.sns_topic))
        print("Job Status Code: " + str(job.status_code))
        print("Job Status Message: " + str(job.status_message))
        print("Job Vault ARN: " + str(job.vault_arn))

    if job.status_code == "Succeeded":
        if job.action == "InventoryRetrieval":
            process_inventory_retrieval_job(job,output_path,friendly_name)
        else:
            failed_parts = process_archive_retrieval_job(job,chunk_size,output_path,friendly_name)
    else:
        print("Fatal error, job status is " + job.status_code + ". Exiting.")
        sys.exit(1)



# Main loop

def main():

    global Debug

    # Parse command line options
    parser = argparse.ArgumentParser()
    parser.add_argument("--account", help="Account ID", default=DEFAULT_ACCOUNT_ID)
    parser.add_argument("--vault", help="Vault Name", required=True)
    parser.add_argument("--jobid", help="Job ID. Supplying a Job ID of \"Any\" will process all completed jobs with status code \"Succeeded\".", required=True)
    parser.add_argument("--outputpath", help="Path to store output", default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--chunksize", help="Size of the chunks to use for download. Only valid of the job is ArchiveRetrieval.", default=DEFAULT_CHUNK_SIZE)
    parser.add_argument("--debug", help="Print Debug messages", action="store_true")
    parser.add_argument("--friendlyname", help="Use friendly names", action="store_true")
    parser.add_argument("--processfailed", help="Re-download failed portions", action="store_true")
    parser.add_argument("--failedlist", help="File containing failed parts list")
    parser.add_argument("--localfile", help="Path to local file")
    args = parser.parse_args()

    Debug = args.debug
    chunksize = int(args.chunksize)

    if not is_power_of_2(chunksize):
        print("Chunksize " + str(chunksize) + " is not a power of two. The next closest power of two is " + str(next_power_of_2(chunksize)))
        print("Exiting.")
        sys.exit(1)

    client = boto3.client('glacier')
    glacier = boto3.resource('glacier')

    if (args.jobid.lower() == "any") or (args.jobid.lower() == "inventory") or (args.jobid.lower() == "archive") :
        response = client.list_jobs(
            vaultName = args.vault,
            statuscode="Succeeded"
        )

        if Debug:
            print("client.list_jobs() response: " + str(response))

        for jobitem in response['JobList']:
            job = glacier.Job(
                account_id=args.account,
                vault_name=args.vault,
                id=jobitem['JobId']
            )
            if (args.jobid.lower() == "inventory") and (job.action == "InventoryRetrieval"):
                process_job(job, chunksize, args.outputpath, args.friendlyname)
            elif (args.jobid.lower() == "archive") and (job.action == "ArchvieRetrieval"):
                process_job(job, chunksize, args.outputpath, args.friendlyname)
            elif (args.jobid.lower() == "any"):
                process_job(job, chunksize, args.outputpath, args.friendlyname)
            
           
    else:
        job = glacier.Job(
            account_id=args.account, 
            vault_name=args.vault, 
            id=args.jobid
        )
        if args.processfailed:
            failed_list_file = open(failed_file, "r")
            failed_list = json.load(failed_list_file)
            failed_list_file.close()
            process_failed_job(job, chunksize, args.localfile, failed_list)
        else:
            process_job(job, chunksize, args.outputpath, args.friendlyname)



if __name__ == "__main__":
    main()



