import tarfile
import os
import sys
import shutil
from os.path import join

configFile = "configs.tar.gz"

def tar(path):
    for root, subdirs, files in os.walk(path):
        if "Environment" in subdirs and "Libraries" in subdirs and "Populations" in subdirs:
            #Move parameters in zip file
            with tarfile.open(join(root, configFile), mode='w:gz') as archive:
                for subdir in subdirs:
                    if subdir != "Results" and subdir != "Analyse" and subdir != "colosse_output":
                        archive.add(join(root, subdir), arcname=subdir)
                        shutil.rmtree(join(root, subdir))
                for file in files:
                    if file != ".meta":
                        archive.add(join(root, file), arcname=file)
                        os.remove(join(root, file))
        
tar(sys.argv[1:][0])