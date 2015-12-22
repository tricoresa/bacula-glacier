#!/usr/bin/env python

import boto3
import argparse
import json
import sys

# GLobal Vars

inventory_json = None
HTTPSuccessLow = 200
HTTPSuccessHigh = 226

# Main loop


def main():
    # Parse command line options
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", help="File containing inventory data. If this is specified --vault and --jobid are ignored")
    parser.add_argument("--account", help="Account ID")
    parser.add_argument("--vault", help="Vault Name")
    parser.add_argument("--jobid", help="Inventory Job ID")
    parser.add_argument("--details", help="Print Inventory details", action="store_true")
    args = parser.parse_args()

    if args.account:
        account = args.account
    else:
        account = "-"

    if args.file:
        with open(args.file) as input_file:
            inventory_json = json.load(input_file)
        input_file.close

    else:
        if args.vault and args.jobid:
            glacier = boto3.resource('glacier')
            job = glacier.Job(
                account_id=account,
                vault_name=args.vault,
                id=args.jobid
            )
            job.load()
            if job.action == "InventoryRetrieval":
                if job.status_code == "Succeeded":
                    response = job.get_output()
                    if HTTPSuccessLow <= response['status'] <= HTTPSuccessHigh:
                        inventory_json = json.loads(str(response['body'].read()))
                    else:
                        print("HTTP Return code " + str(response['status']) + " indicates unsuccessful retrieval of job output for Job ID " + args.jobid + ". Please try again.")
                        sys.exit(1)
                else:
                  print("Job's status is " + str(job.status_code) + " for Job ID " + args.jobid + ". Exiting.")
                  sys.exit(1)
            else:
                print("Job ID " + args.jobid + " is not an inventory retrieval job.")
                sys.exit(1)
        else:
            print("--vault and --jobid must both be specified for retrieving inventory information from a job if --file is not specified")
            sys.exit(1)

    print("Vault ARN: " + inventory_json['VaultARN'])
    print("Inventory Date: " + inventory_json['InventoryDate'])

    archive_count = 0
    archive_count_unique = 0
    archive_size_in_bytes = 0
    archive_size_in_bytes_unique = 0
    hash_list = []
    for archive in inventory_json['ArchiveList']:
        if args.details:
            print(str(archive))
        archive_count += 1
        archive_size_in_bytes += int(archive['Size'])
        if not (archive['SHA256TreeHash'] in hash_list):
            hash_list.append(archive['SHA256TreeHash'])
            archive_count_unique += 1
            archive_size_in_bytes_unique += int(archive['Size'])

    print("Total number of archives: " + str(archive_count))
    print("Total number of unique archives: " + str(archive_count_unique))
    print("Total size in bytes: " + str(archive_size_in_bytes))
    print("Total unique size in bytes: " + str(archive_size_in_bytes_unique))

if __name__ == "__main__":
    main()


