#!/usr/bin/env python

import boto3
import argparse

# Main loop


def main():
    # Parse command line options
    parser = argparse.ArgumentParser()
    parser.add_argument("--account", help="Account ID", required=True)
    parser.add_argument("--vault", help="Vault Name", required=True)
    args = parser.parse_args()

    client = boto3.client('glacier')

    response = client.initiate_job(
            vaultName=args.vault,
            jobParameters={
                "Format": "JSON",
                "Type": "inventory-retrieval"
            }
    )

    print("Job submitted.")
    print("Job Location: " + response['location'])
    print("Job ID: " + response['jobId'])

if __name__ == "__main__":
    main()


