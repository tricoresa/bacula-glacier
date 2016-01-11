#!/usr/bin/env python

import boto3
import argparse
import json
import StringIO
import sys
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


def next_power_of_2(num):
    return int(pow(2, round(math.log(num)/math.log(2))))

def is_power_of_2(num):
    return num != 0 and ((num & (num - 1)) == 0)


def running_treehash_on_file_range(treehash, filename, start, end, hash_chunk_size=DEFAULT_HASH_CHUNK_SIZE):

    infile = open(filename, "rb")

    if Debug:
        outfile = open(filename + ".treehash.debug", "wb")

    infile.seek(start)

    print("Treehash: Start: " + str(start) + ", End: " + str(end))
    current_pos = start
    while current_pos < end:
        read_size = end - current_pos
        if read_size > hash_chunk_size:
            read_size = hash_chunk_size
        if Debug:
            print("Reading from " + str(current_pos) + " to " + str(current_pos + read_size - 1) + ", Readsize: " + str(read_size))
        if Debug:
            chunk = infile.read(read_size)
            treehash.update(chunk)
            outfile.write(chunk)
        else:
            treehash.update(infile.read(read_size))
        if Debug:
            print("Current treehash for  " + str(current_pos) + " to " + str(current_pos + read_size - 1) + " is " + treehash.hexdigest())
        current_pos += read_size
    infile.close()
    if Debug:
        print("TreeHash for this section (" + str(start) + " to " + str(end) + ") is " + treehash.hexdigest())
        outfile.close()

def sha256_on_file_range(filename, start, end, hash_chunk_size=DEFAULT_HASH_CHUNK_SIZE):

    sha256 = hashlib.sha256()
    infile = open(filename, "rb")

    if Debug:
        outfile = open(filename + ".debug", "wb")

    infile.seek(start)
    current_pos = start
    read_count = 1
    while current_pos < end:
        read_size = end - current_pos
        if read_size > hash_chunk_size:
            read_size = hash_chunk_size
        if Debug:
            print("Reading (" + str(read_count) + ") from " + str(current_pos) + " to " + str(current_pos + read_size - 1))
        if Debug:
            chunk = infile.read(read_size)
            sha256.update(chunk)
            outfile.write(chunk)
        else:
            sha256.update(infile.read(read_size))
        current_pos += read_size
        read_count += 1
    infile.close()
    if Debug:
        outfile.close()
    return sha256.hexdigest()


def process_archive_retrieval_job(job,chunk_size,output_path):
    global chunk_count 

    filepos_limit = job.archive_size_in_bytes - 1
    current_pos = 0
    job_archive_hash = job.archive_sha256_tree_hash
    chunk_count = 0
    archive_file_name = output_path + "/" + job.id + ".archive"
    archive_file = open(archive_file_name, "wb")
    treehash = TreeHash(algo=hashlib.sha256)
    while current_pos < filepos_limit:
        end_pos = current_pos + (chunk_size - 1)
        if end_pos > filepos_limit:
            end_pos = filepos_limit

        range_string = "bytes=" + str(current_pos) + "-" + str(end_pos)

        response = job.get_output(
            range=range_string
        )

        if Debug:
            print("process_archive_retrieval_job: job.get_output() response: " + str(response))

        if HTTP_SUCCESS_LOW <= response['status'] <= HTTP_SUCCESS_HIGH:
            chunk_count += 1

            if Debug:
                #print("Writing chunk " + str(chunk_count) + " " + range_string + " Checksum: " + response['checksum'] + " ContentRange: " + response['contentRange'] + " AcceptRanges: " + response['acceptRanges'] + " ContentType: " + response['contentType'] + " ArchiveDescription: " + response['archiveDescription'])
                print("Writing chunk " + str(chunk_count) + " " + range_string + " Checksum: " + response['checksum'])

            #archive_file.write(response['body'].read())
            chunk_bytes=response['body'].read()
            archive_file.write(chunk_bytes)

            if Debug:
                chunk_file = open(archive_file_name + ".chunk." + str(chunk_count), "wb")
                chunk_file.write(chunk_bytes)
                chunk_file.close

            #section_hash = sha256_on_file_range(archive_file_name, current_pos, end_pos+1)
            section_hash = sha256_on_file_range(archive_file_name, current_pos, end_pos)
            running_treehash_on_file_range(treehash, archive_file_name, current_pos, end_pos)

            if Debug:
                print("Local checksum of chunk " + str(chunk_count) + ": " + section_hash)
                print("Current running treehash is  " + treehash.hexdigest())
            
            current_pos = end_pos + 1
        else:
            print("Response unsuccessful. Retrying")

    archive_file.close


def process_inventory_retrieval_job(job,output_path):
    response = job.get_output()

    if Debug:
        print("process_inventory_retrieval_job: job.get_output() response: " + str(response))

    if HTTP_SUCCESS_LOW <= response['status'] <= HTTP_SUCCESS_HIGH:
        inventory_file = open(output_path + "/" + job.id + ".inventory", "wb")
        inventory_file.write(response['body'].read())
        inventory_file.close
    else:
        print("HTTP Return code " + str(response['status']) + " indicates unsuccessful retrieval of job output for Job ID " + args.jobid + ". Please try again.")
        sys.exit(1)


def process_job(job,chunk_size,output_path):
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
            process_inventory_retrieval_job(job,output_path)
        else:
            process_archive_retrieval_job(job,chunk_size,output_path)
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
    args = parser.parse_args()

    Debug = args.debug
    chunksize = int(args.chunksize)

    if not is_power_of_2(chunksize):
        print("Chunksize " + str(chunksize) + " is not a power of two. The next closest power of two is " + str(next_power_of_2(chunksize)))
        print("Exiting.")
        sys.exit(1)

    client = boto3.client('glacier')
    glacier = boto3.resource('glacier')

    if args.jobid.lower() == "any":
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
            process_job(job, chunksize, args.outputpath)
    else:
        job = glacier.Job(
            account_id=args.account, 
            vault_name=args.vault, 
            id=args.jobid
        )
        process_job(job, chunksize, args.outputpath)



if __name__ == "__main__":
    main()



