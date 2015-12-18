#!/usr/bin/env python

import boto3
import argparse
import json
import StringIO

# Main loop


def main():
    # Parse command line options
    parser = argparse.ArgumentParser()
    parser.add_argument("--account", help="Account ID", required=True)
    parser.add_argument("--vault", help="Vault Name", required=True)
    parser.add_argument("--jobid", help="Vault Name", required=True)
    args = parser.parse_args()

    glacier = boto3.resource('glacier')
    job = glacier.Job(args.account, args.vault, args.jobid)

    job.load()
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
            response = job.get_output()
            print("Job get_output response content: " + str(response))
            print("Job get_output response status: " + str(response['status']))
            print("Job get_output response contentType: " + str(response['contentType']))

            inventory_body = response['body'].read()
            print("Outputing Raw Inventory to Data: " + args.jobid + ".output")
            inventory_file_raw = open(args.jobid + ".output", "wb")
            inventory_file_raw.write(inventory_body)
            inventory_file_raw.close

            print("Outputing Pretty Inventory to Data: " + args.jobid + ".output.pretty")
            inventory_file_pretty = open(args.jobid + ".output.pretty", "w")
            #inventoryio = StringIO.StringIO(inventory_body)
            #inventoryJSON = json.load(inventoryio)
            inventoryJSON = json.loads(str(inventory_body))
            #print("Inventory: " + str(inventoryJSON))
            inventory_file_pretty.write(json.dumps(inventoryJSON, indent=4))
            inventory_file_pretty.close

if __name__ == "__main__":
    main()



