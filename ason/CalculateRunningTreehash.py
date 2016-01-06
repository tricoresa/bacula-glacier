#!/usr/bin/env python

import boto3
import argparse
import json
import StringIO
import sys
import os
import hashlib
from treehash import TreeHash


# Constants

HTTP_SUCCESS_LOW = 200
HTTP_SUCCESS_HIGH = 226
DEFAULT_ACCOUNT_ID = "-"
DEFAULT_CHUNK_SIZE = 1024 ** 2
DEFAULT_OUTPUT_PATH = "."


# Global Variables

Debug = False

def running_treehash_on_file_range(treehash, filename, start, end, chunk_size):
    global Debug

    infile = open(filename, "rb")

    infile.seek(start)
    current_pos = start
    while current_pos < end:
        read_size = end - current_pos
        if read_size > chunk_size:
            read_size = chunk_size
        if Debug:
            print("Reading from " + str(current_pos) + " to " + str(current_pos + read_size - 1))
        treehash.update(infile.read(read_size))
        if Debug:
            print("Current treehash for  " + str(current_pos) + " to " + str(current_pos + read_size - 1) + " is " + treehash.hexdigest())
        current_pos += read_size
    infile.close()
    if Debug:
        print("TreeHash for this section (" + str(start) + " to " + str(end) + ") is " + treehash.hexdigest())
        

# Main loop

def main():
    global Debug
    # Parse command line options
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", help="File Name", required=True)
    parser.add_argument("--rangesize", help="Size of the range", required=True)
    parser.add_argument("--chunksize", help="Size of the chunks to use for checksumming.", default=DEFAULT_CHUNK_SIZE)
    parser.add_argument("--debug", help="Print Debug messages", action="store_true")
    args = parser.parse_args()

    Debug = args.debug

    treehash = TreeHash(algo=hashlib.sha256)

    statinfo = os.stat(args.file)
    end_pos = statinfo.st_size 
    range = int(args.rangesize)
    chunksize = int(args.chunksize)

    current_pos = 0
    while current_pos < end_pos:
        current_end_pos = current_pos + range
        if current_end_pos > end_pos:
            current_end_pos = end_pos

        running_treehash_on_file_range(treehash, args.file, current_pos, current_end_pos, chunksize)
        current_pos = current_end_pos 

    print("TreeHash for " + args.file + " is " + treehash.hexdigest())



if __name__ == "__main__":
    main()



