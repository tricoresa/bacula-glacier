#!/usr/bin/env python

import argparse
import math

def next_power_of_2(num):
    return int(pow(2, round(math.log(num)/math.log(2))))

def is_power_of_2(num):
    return num != 0 and ((num & (num - 1)) == 0)

# Main loop


def main():

    # Parse command line options
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", help="Chunk size in bytes", required=True)
    args = parser.parse_args()

    if is_power_of_2(int(args.size)):
        print(args.size + " is a power of 2")
    else:
        print(args.size + " is NOT a power of 2")
        print("The nearest power of 2 is " + str(next_power_of_2(int(args.size))))


if __name__ == "__main__":
    main()

