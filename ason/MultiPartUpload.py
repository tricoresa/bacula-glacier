#!/usr/bin/env python

import boto3
import argparse
import os
import hashlib
from treehash import TreeHash


# Global Variables
min_chunk_size = 1048576


# Main loop


def main():

    # Parse command line options
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", help="File to upload", required=True)
    parser.add_argument("--size", help="Chunk size in bytes", required=True)
    parser.add_argument("--account", help="Account ID", required=True)
    parser.add_argument("--vault", help="Vault Name", required=True)
    args = parser.parse_args()

    in_file = open(args.file, "rb")
    in_file_size = os.path.getsize(args.file)
    in_file_sha256 = hashlib.sha256(open(args.file, "rb").read()).hexdigest()

    treehash = TreeHash(algo=hashlib.sha256)
    treehash.update(open(args.file, "rb").read())
    in_file_tree_sha256 = treehash.hexdigest()

    chunk_size = int(args.size)
    if chunk_size < min_chunk_size:
        print("Supplied chunk size (" + args.size + ") is less than minimum. Setting chunk size to " + str(min_chunk_size))
        chunk_size = min_chunk_size

    glacier = boto3.resource('glacier')
    client = boto3.client('glacier')
    #

    multiupload_request = client.initiate_multipart_upload(
        vaultName="TestVault",
        archiveDescription=args.file,
        partSize=str(chunk_size)
    )

    multipart_upload = glacier.MultipartUpload(args.account, args.vault, multiupload_request['uploadId'])

    print("MultiUpload ID: " + multiupload_request['uploadId'])
    print("Size: " + str(in_file_size))
    print("Hash: " + in_file_sha256)
    print("Tree Hash: " + in_file_tree_sha256)

    position = 0
    chunk = in_file.read(chunk_size)
    while chunk:
        print("Length: " + str(len(chunk)))
        print("Current range: bytes " + str(position) + "-" + str(position + len(chunk) - 1) + "/*")
        response = multipart_upload.upload_part(
                range="bytes " + str(position) + "-" + str(position + len(chunk) - 1) + "/*",
                body=chunk
        )
        print("Uploaded Checksum: " + response['checksum'])

        position += len(chunk)
        chunk = in_file.read(chunk_size)

    response = multipart_upload.complete(
        archiveSize=str(in_file_size),
        checksum=in_file_tree_sha256
    )

    print("Upload Complete.")
    print("Location: " + response['location'])
    print("Checksum: " + response['checksum'])
    print("Archive ID: " + response['archiveId'])

    print("Account ID: " + multipart_upload.account_id)
    print("Vault name: " + multipart_upload.vault_name)
    print("ID: " + multipart_upload.id)


if __name__ == "__main__":
    main()

