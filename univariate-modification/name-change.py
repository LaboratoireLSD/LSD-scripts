#! /usr/bin/python3

"""
This script let you rename, in order, the files from src to dest. It will overwrite files so be careful not to erase
valuable data.
"""

import os

src = [99,98,97,96]
dest = [0,3,9,13]

for subdir, dirs, files in os.walk("."):
    for dir in dirs:
        os.chdir(dir)

        #Compact every files
        for name in ["_Output.gz", "_Summary.gz"]:
            for i in range(len(src)):
                current_src = str(src[i]) + name
                current_dest = str(dest[i]) + name
                if os.path.exists(current_src):
                    os.rename(current_src, current_dest)

        os.chdir("..")
