#!/bin/env python

import ProcessJob_old
import argparse
import os
import sys
import hashlib
from treehash import TreeHash

DEFAULT_OUTPUT_PATH="."
DEFAULT_CHUNK_SIZE=1024 ** 3

# Main loop

def main():

    global Debug

    # Parse command line options
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", help="file to use for hash tests", required=True)
    parser.add_argument("--outputpath", help="Path to store output", default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--chunksize", help="Size of the chunks to use for download. Only valid of the job is ArchiveRetrieval.", default=DEFAULT_CHUNK_SIZE)
    parser.add_argument("--debug", help="Print Debug messages", action="store_true")
    args = parser.parse_args()

    Debug = args.debug
    ProcessJob_old.set_debug(Debug)
    chunksize = int(args.chunksize)
    statinfo = os.stat(args.file)

    if not ProcessJob_old.is_power_of_2(chunksize):
        print("Chunksize " + str(chunksize) + " is not a power of two. The next closest power of two is " + str(ProcessJob_old.next_power_of_2(chunksize)))
        print("Exiting.")
        sys.exit(1)

    if chunksize > statinfo.st_size:
        chunksize = statinfo.st_size


    current_pos = 0

    chunk_count = 0
    treehash = TreeHash(algo=hashlib.sha256)
    while current_pos < statinfo.st_size:
        chunk_count += 1
        end_pos = current_pos + chunksize - 1
        if end_pos > statinfo.st_size:
            end_pos = statinfo.st_size
        if Debug:
            print("Processing chunk " + str(chunk_count) + " range " + str(current_pos) + " to " + str(end_pos) )
        section_hash = ProcessJob_old.sha256_on_file_range(args.file, current_pos, end_pos)
        ProcessJob_old.running_treehash_on_file_range(treehash, args.file, current_pos, end_pos)

        print("Range: " + str(current_pos) + " to " + str(end_pos))
        print("Local checksum of chunk " + str(chunk_count) + ": " + section_hash)
        print("Current running treehash is " + treehash.hexdigest())
        
        current_pos = end_pos + 1
        
    


if __name__ == "__main__":
    main()

