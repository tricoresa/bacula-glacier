#!/u01/Djracula/.virtualenvs/bacula-glacier/bin/python
import argparse
import glacier
from celery.result import AsyncResult
import json

DEFAULT_CHUNK_SIZE = pow(1024, 3)
DEFAULT_OUTPUT_PATH = "."

parser = argparse.ArgumentParser()
parser.add_argument("-v", "--vault", help="The Glacier Vault name", required=True)
parser.add_argument("-j", "--jobid", help="Job ID. Supplying a Job ID of \"Any\" will process all completed jobs with status code \"Succeeded\". Similary, supplying a Job ID of \"inventory\" or \"archive\" will process any successfully completed job that are InventoryRerieval or ArchiveRetrieval respectively.", required=True)
parser.add_argument("-o", "--outputpath", help="Path to store output", default=DEFAULT_OUTPUT_PATH)
parser.add_argument("-c", "--chunksize", help="Size of the chunks to use for download. Only valid of the job is ArchiveRetrieval.", default=DEFAULT_CHUNK_SIZE)
parser.add_argument("-f", "--friendlyname", help="Use friendly name for output file", action="store_true")
parser.add_argument("-r", "--reprocessfailed", help="Reprocess a failed download (reqires a failed parts list and the source failed file)", action="store_true")
parser.add_argument("-s", "--failedsourcefile", help="Path to the download failed file")
parser.add_argument("-l", "--failedlistfile", help="Path to the file containing the failed parts list")

args = parser.parse_args()

chunksize = int(args.chunksize)

if not glacier.is_power_of_2(chunksize):
    print("Chunksize " + str(chunksize) + " is not a power of two. The next closest power of two is " + str(glacier.next_power_of_2(chunksize)))

else:
    if args.reprocessfailed:
         if (not args.failedsourcefile) or (not args.failedlistfile):
             print(" a failed source file and a failed list file is required to reprocess a failed download")
         else:
             if (args.jobid.lower() == "any") or (args.jobid.lower() == "inventory" ) or (args.jobid.lower() == "archive"):
                 print("reprocessing requires a specific jobid")
             else:
                 try:
                     with open(args.failedlistfile, "r") as retry_failed_list_file:
                         retry_failed_list = json.load(retry_failed_list_file)

                     if 'all' in retry_failed_list:
                         print("The failed list indicates that all parts failed. Please re-download the entire file instead.")
                     else:
                         new_failed_list = glacier.process_failed_job(args.jobid, chunksize, args.failedsourcefile, retry_failed_list)
                         if new_failed_list:
                             print("Errors detected in re-processing the file. Overwriting old failed list file with new failed list. Please try again with this list")
                             with open(args.failedlistfile, "w") as retry_failed_list_file:
                                 json.dump(new_failed_list, retry_failed_list_file)
                         else:
                             print(args.failedsourcefile + " reprocessed successfully.")
                 except:
                     raise
                         
    else:
        failures = glacier.process_request(args.vault, args.jobid, chunksize, args.outputpath, args.friendlyname)
        if "all" in failures:
            print("Failed at processing multiple jobs. Please try again.")
        else:
            for job in failures:
                if failures[job]:
                    print("Failures detected for jobid " + job + ". Writing failure list to file.")
                    with open(args.outputpath + "/" + job + ".failed_parts.json", "w") as failurefile:
                        try:
                            json.dump(failures[job], failurefile)
                        except:
                            raise
                else:
                    print(job + " downloaded successfully.")
