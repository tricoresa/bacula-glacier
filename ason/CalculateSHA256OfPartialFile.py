#!/usr/bin/env python

import boto3
import argparse
import json
import StringIO
import sys
import hashlib


# Constants

HTTP_SUCCESS_LOW = 200
HTTP_SUCCESS_HIGH = 226
DEFAULT_ACCOUNT_ID = "-"
DEFAULT_CHUNK_SIZE = 1024 ** 2
DEFAULT_OUTPUT_PATH = "."


# Global Variables

Debug = False

def sha256_on_file_range(filename, start, end, chunk_size):
    global Debug

    sha256 = hashlib.sha256()
    infile = open(filename, "rb")

    if Debug:
        outfile = open(filename + ".debug", "wb")

    infile.seek(start)
    current_pos = start
    while current_pos < end:
        read_size = end - current_pos
        if read_size > chunk_size:
            read_size = chunk_size
        if Debug:
            print("Reading from " + str(current_pos) + " to " + str(current_pos + read_size - 1))
        if Debug:
            chunk = infile.read(read_size)
            sha256.update(chunk)
            outfile.write(chunk)
        else:
            sha256.update(infile.read(read_size))
        current_pos += read_size
    infile.close()
    if Debug:
        outfile.close()
    return sha256.hexdigest()
        

# Main loop

def main():
    global Debug
    # Parse command line options
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", help="File Name", required=True)
    parser.add_argument("--start", help="Start position", required=True)
    parser.add_argument("--end", help="End position", required=True)
    parser.add_argument("--chunksize", help="Size of the chunks to use for checksumming.", default=DEFAULT_CHUNK_SIZE)
    parser.add_argument("--debug", help="Print Debug messages", action="store_true")
    args = parser.parse_args()

    Debug = args.debug

    print("Hash for " + args.file + " (" + args.start + " - " + args.end + ") is " + sha256_on_file_range(args.file, int(args.start), int(args.end), int(args.chunksize)))



if __name__ == "__main__":
    main()



