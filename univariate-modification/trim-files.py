#! /usr/bin/python3

"""
This script remove the Output and Summary files that have a number higher or equal to remove_ge.
This is to make sure all analysed output have the same amount of repetitions.
"""

import os, sys

remove_ge = int(sys.argv[1])
files_amount = 100

for subdir, dirs, files in os.walk("."):
    for directory in dirs:
        os.chdir(directory)

        # Compact every files
        for name in ["_Output.gz", "_Summary.gz"]:
            for i in range(remove_ge, files_amount):
                current_file = str(i) + name
                if os.path.exists(current_file):  # Find first missing file
                    os.remove(current_file)

        os.chdir("..")

