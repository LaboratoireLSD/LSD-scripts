#! /usr/bin/python3

"""
This scripts rename from the last the output file to make sure all the one in the folder are contiguous.
This is required by Analyse.sh
"""

import os

src = reversed(range(100))
dest = range(100)
file_amount = 100

for subdir, dirs, files in os.walk("."):
    for directory in dirs:
        os.chdir(directory)

        # Compact every files
        for name in ["_Output.gz", "_Summary.gz"]:
            current_src_num = file_amount

            for i in range(file_amount):
                if i >= current_src_num - 1:    #Stop if we reached the next file to be used for compacting
                    break
                current_destination = str(i) + name
                if not os.path.exists(current_destination):  # Find first missing file
                    for j in reversed(range(i, current_src_num)):
                        current_src = str(j) + name
                        if os.path.exists(current_src):
                            os.rename(current_src, current_destination)
                            current_src_num = j
                            break

        os.chdir("..")

