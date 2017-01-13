import subprocess
import getopt
import datetime
import sys
from os.path import join

def showHelp():
    print("Possible arguments : \n")
    print("-p, --path             Path of the folder to calculate meta file.")
    print("[-m, --meta]           Name of the meta file. Default = '.meta'")

def folderSize(folderName, metaFile):
    today = datetime.datetime.now().strftime("%d-%m-%Y")
    proc = subprocess.Popen(["du", "-h", folderName], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    
    if not stdout:
        return
        
    for line in stdout.split("\n"):
        try:
            size, folder = line.split("\t")
            with open(join(folder, metaFile), "w") as file:
                file.write("size:" + size + "\n")
                file.write("creation date:" + today + "\n")
        except:
            pass
        
def main(args):
    folderPath = ""
    metaFile = ".meta"    
    
    try:
        options, arguments = getopt.getopt(args, "hp:m:", ["help", "path=", "meta="])
    except getopt.GetoptError as error:
        print (error)
        sys.exit(1)
    
    if not options: #If no options given
        print("No argument given")
        sys.exit(1)
    
    #Parsing all the options
    for opt, arg in options:
        if (opt in ('-h', '--help')):
            sys.exit()
        elif (opt in ('-p', '--path')):
            folderPath = arg
        elif (opt in ('-m', '--meta')):
            metaFile = arg
      
    if not folderPath:
        showHelp()
        sys.exit(1)
    
    folderSize(folderPath, metaFile)
        
main(sys.argv[1:])