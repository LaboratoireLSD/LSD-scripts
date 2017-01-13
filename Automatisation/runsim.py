#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 27 15:31:59 2016

@author: mathieu boily

This script is the sum of 3 scripts. This way, it's easier to keep all of 
them up to date and to use them.

The "launcher" is the only script directly used by the user.
The "runner" is placed on the supercalculator and does the simulation.
The "fetcher" can be placed on a server. It retrieves the simulation once done.
Everything is automatised, so you don't need to copy the script on other computers.

If comments are not enough to understand properly, see the documentation.
"""

import sys, os

launcherScript = "launch"
fetcherScript = "fetch"
runnerScript = "run"
runSimScript = os.path.basename(__file__)
koksoakScriptLocation = "/home/lsdadmin/scripts/"

"""
Shows help page for the launcher script.
Call it with arguments : launch --help|-h
"""
def showHelpLauncher():
    print("\n")
    print("Possible arguments :\n")
    
    print("     -p, --project <path>               Complete path to the project (folder's name)")
    print("     -u, --username <name>              Username on Colosse for the ssh connection")
    print("     [-l, --log]                        Tells schnaps to print logs")
    print("     [-e, --email <email>]              Koksoak will send an email to this address when the simulation will be retrieved")
    print("     [-m, --mode <1-4>]                 1 : One job per file. 2 : One job per iteration. 3 : One job per simulation. 4 : One job for all. Default = 2")
    print("     [-d, --duration <HH:MM:SS>]        Maximum duration of the simulation. Default = 24:00:00. Cannot exceed 48h")
    print("     [-o, --options <option>]           Options for SCHNAPS. See its doc for more information.")
    print("     [-h, --help]                       Shows the help page\n")
    print("For help about the RSA key : http://doc.fedora-fr.org/wiki/SSH_:_Authentification_par_cl%C3%A9")
    
"""
Shows help page for the fetcher script.
Call it with arguments : fetch --help|-h
"""
def showHelpFetcher():
    print("\n")
    print("Possible arguments :\n")
    print("     -u, --username <name>              Username on Colosse for the ssh connection")
    print("     -p, --project <name>               Project's name")
    print("     -i, --id <job's id>                Job's id given by Colosse")
    print("     [-e, --email <address@ulaval.ca>]  Person to join when the simulation is done")
    print("     [-c]                               Create a new cron job")

"""
Shows help page for the runner script.
Call it with arguments : run --help|-h
"""
def showHelpRunner():
    print("\n")
    print("Possible arguments :\n")
    print("     -p, --project <name>               Project's name")
    print("     -m, --mode <1-4>                   1 : One job per file. 2 : One job per iteration. 3 : One job per simulation. 4 : One job for all.")
    print("     -t, --task                         Index of the jobs' array. Represents the Xe job.")
    print("     -r, --rap-id                       Rap id.")
    print("     -s, --scenario                     List of scenarios splitted by the argument.")
    print("     [-o, --options <option>]           Options for SCHNAPS. See its doc for more information.")

"""
Shows the general help page.
It contains the help pages of all 3 scripts.
Call it without passing parameter, or with : -h|--help
"""
def showHelpGeneral():
    print("Possible arguments :\n")
    
    print("     [" + launcherScript + ", " + fetcherScript + ", " + runnerScript + "]               Which mode to use. Must be the first argument. If omitted = " + launcherScript)
    print("\n" + launcherScript + " :")
    showHelpLauncher()
    print("\n" + fetcherScript + " :")
    showHelpFetcher()
    print("\n" + runnerScript + " :")
    showHelpRunner()

def main(args):
    if not args or args[0] in ["-h", "--help", "-help", "help"]:
        showHelpGeneral()
        sys.exit(0)
    
    if args[0] == launcherScript:
        launcher(args[1:])
    elif args[0] == fetcherScript:
        fetcher(args[1:])
    elif args[0] == runnerScript:
        runner(args[1:])
    else:
        launcher(args)

"""
First script.
Used by the user.
It starts the chain by creating a job on the supercalculator, executing it and
calling the 3rd script on the recieving server.
"""        
def launcher(args):
    import os, getopt, ntpath, re, time
    from lxml import etree as ET
    from os.path import basename, isdir, join, isfile, realpath, dirname

    try:
        import paramiko
    except:
        print("Error : Paramiko isn't installed on your system.")
        print("Before installing it, make sure you have the correct dependencies with : 'sudo apt-get install build-essential libssl-dev libffi-dev python-dev'")
        print("Then, install pip with 'sudo apt-get install python-pip' and paramiko with 'sudo pip install paramiko'\n")
        sys.exit(1)
        
    submitScriptName = "generated_submit.pbs"
    homeUserPath = "/home/"
    emailTo = ""
    duration = "24:00:00"
    rapId = "wny-790-aa"
    username = ""
    projectPath = ""
    projectName = ""
    projectNameWithDatetime = ""
    scenarios = []
    scenariosToString = "" #Will be used to pass all the scenario to the execution script
    advParameters = " -o " #Advanced parameters - Schnaps
    jobId = ""
    mode = 2 # How jobs will be created 
    nbTasks = 0 # Tasks in the array of jobs
    nbIterations = 0
    now = time.strftime("%d-%m-%Y_%H-%M")
    log = False
    givenParameters = False
    
    #Accepted arguments
    try:
        options, arguments = getopt.getopt(args, "hlp:u:e:d:o:m:", ["help", "log", "project=", "username=", "email=", "duration=", "options=", "mode="])
    except getopt.GetoptError as error:
        print (error)
        showHelpLauncher()
        sys.exit(1)
        
    if not options: #If no options given
        showHelpLauncher()
        sys.exit(1)
    
    #Parsing all the options
    for opt, arg in options:
        if (opt in ('-h', '--help')):
            showHelpLauncher()
            sys.exit()
        elif (opt in ('-p', '--project')):
            if isdir(arg):
                projectPath = arg
                if " " in projectPath:
                    print("Project path must not contain space. Exiting...")
                    sys.exit(1)
                if projectPath.endswith("/"):
                    projectPath = projectPath[:-1]
                projectName = ntpath.basename(projectPath)
                projectNameWithDatetime = projectName + "_" + now
            else:
                print("Invalid project's folder")
                showHelpLauncher()
                sys.exit(1)
        elif (opt in ("-u", "--username")):
            username = arg
        elif (opt in ("-e", "--email")):
            emailTo = " -e " + arg
        elif (opt in ("-d", "--duration")):
            if re.match("([0-9]+):([0-5][0-9]):([0-5][0-9])", arg):
                duration = arg
            else:
                print("Error : Duration must be HH:MM:SS\n")
                showHelpLauncher()
                sys.exit(1)
        elif (opt in ("-o", "--options")):
            advParameters += arg
            givenParameters = True
        elif (opt in ("-m", "--mode")):
            mode = int(arg)
        elif (opt in ("-l", "--log")):
            log = True
    
    # Adding the log parameter
    if (givenParameters):
        advParameters += ","
    advParameters += "print.log=" + str(log).lower()
            
    # Getting scenarios. Find the first "parameters_x.xml" in project to retrieve scenarios
    try:
        parameterName = [fileName for fileName in os.listdir(projectPath) if isfile(join(projectPath, fileName)) and fileName.startswith("parameters_")]
        parametersFile = ET.parse(join(projectPath, parameterName[0]))
        nbIterations = len(parameterName) - 1 # Number of parameters_x.xml files = number of iterations
        for scenario in parametersFile.xpath("/Simulator/Simulation/Scenarios/Scenario"):
            scenarios.append(scenario.get("label"))
            scenariosToString += " -s " + scenarios[-1]
    except IndexError:
        print("Error while getting scenarios. Make sure your project has iterations. No 'parameters_X.xml' found.")
    except:
        print("Error occurred while retrieving the scenarios.")
        sys.exit(1)
        
    if not username or not scenarios or not projectName:
        #Project, scenario and username are necessary
        showHelpLauncher()
        sys.exit(1)
       
    # Getting the number of jobs depending on the chosen mode
    if mode == 1:
        # 1 job per simulation (Ex. 5 scenarios with 100 simulations = 500 jobs)
        nbTasks = len(scenarios) * nbIterations
    elif mode == 2:
        # 1 job per iteration
        nbTasks = nbIterations
    elif mode == 3:
        # 1 job per scenario
        nbTasks = len(scenarios)
    else:
        # 1 job for all
        nbTasks = 1
    print(projectName)
    print(projectPath)
    sys.exit(1)
    # Creating the folder that will contain colosse's outputs
    if not os.path.exists(join(projectPath, "colosse_output")):
        os.makedirs(join(projectPath, "colosse_output"))
        
    homeUserPath += username
    standardOutputFolder = join("/scratch", rapId, projectNameWithDatetime, "colosse_output")
    errorOutputFolder = join("/scratch", rapId, projectNameWithDatetime, "colosse_output")
    #Submission script content
    submitScriptContent = ("#!/bin/bash\n"
                           "#PBS -A " + rapId + "\n" #Rap ID
                           "#PBS -l walltime=" + duration + "\n" #Max duration HH:MM:SS
                           "#PBS -l nodes=1:ppn=8\n" #Total nodes and hearts
                           "#PBS -N " + projectName + "\n" #Job's name
                           "#PBS -o " + standardOutputFolder + "/" + projectName + "_%I.out\n" #Standard output
                           "#PBS -e " + errorOutputFolder + "/" + projectName + "_%I.err\n" #Error output
                           "#PBS -t [0-" + str(nbTasks) + "]%100\n" # Array of jobs. Max 50 jobs at the same time. Can be anything else than 50 (don't know the max)
                           
                           "python " + runSimScript + " " + runnerScript + " -p " + projectNameWithDatetime + " -m " + str(mode) + " -t $MOAB_JOBARRAYINDEX -i " + str(nbIterations) + scenariosToString + advParameters + " -r " + rapId + "\n" #Executing the 2nd script
                        )
    
    print("Connection to Colosse by ssh.")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    k = paramiko.RSAKey.from_private_key_file(os.path.expanduser("~/.ssh/id_rsa"))
    ssh.connect(hostname="colosse.calculquebec.ca", username=username, pkey=k)
    
    print("Sending this script to user's home folder.")
    os.system("scp " + join(dirname(realpath(__file__)), basename(__file__)) + " " + username + "@colosse.calculquebec.ca:")

    print("Generating the submit script on Colosse.")
    ssh.exec_command("echo '" + submitScriptContent + "' > " + join(homeUserPath, submitScriptName) + "\n")
    
    print("Sending the project folder to : " + username + "@colosse.calculquebec.ca:/scratch/" + rapId + "/" + projectNameWithDatetime)
    os.system("scp -r " + projectPath + "/ " + username + "@colosse.calculquebec.ca:/scratch/" + rapId + "/" + projectNameWithDatetime + "/")
    
    print("Launching the submit script.")
    stin, stout, sterr = ssh.exec_command("msub " + join(homeUserPath, submitScriptName) + "\n")
    sterrRead = sterr.readlines() #If ssh returns an error
    stoutRead = stout.readlines() #If the ssh returns a normal output
    
    if sterrRead:
        #An error has occurred
        print(sterrRead)
    if stoutRead:
        #Printing the return of the previous commands.
        if type(stoutRead) is list and str.isdigit(str(stoutRead[1]).strip()):
            print("Job's id : " + stoutRead[1])
            jobId = stoutRead[1].strip()
        else:
            print(str(stoutRead))
            print("Couldn't get the job's id. Exiting without creating a cron job on Koksoak.")
            sys.exit(1)
    
    ssh.close()
   
    print("-------------------------------")
    print("Connection to Koksoak by ssh.")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    k = paramiko.RSAKey.from_private_key_file(os.path.expanduser("~/.ssh/id_rsa"))
    ssh.connect(hostname="koksoak.gel.ulaval.ca", username="lsdadmin", pkey=k)
    
    print("Sending this script to Koksoak.")
    os.system("scp " + join(dirname(realpath(__file__)), basename(__file__)) + " lsdadmin@koksoak.gel.ulaval.ca:" + koksoakScriptLocation)
    
    print("Creating a cron job on Koksoak.")
    stin, stout, sterr = ssh.exec_command("python " + koksoakScriptLocation + runSimScript + " " + fetcherScript + " -u " + username + " -i " + jobId + " -p " + projectNameWithDatetime + emailTo + " -c\n")
    sterrRead = sterr.readlines() #If ssh returns an error
    stoutRead = stout.readlines() #If the ssh returns a normal output
    
    if sterrRead:
        #An error has occurred
        if type(sterrRead) is list:
            for line in sterrRead:
                print(line)
        else:
            print(sterrRead)
    if stoutRead:
        #Printing the return of the previous commands.
        if type(stoutRead) is list:
            for line in stoutRead:
                print(line)
        else:
            print(stoutRead)
    
    ssh.close()
    print("Done")
    
def runner(args):
    import getopt
    import sys
    import subprocess
    import datetime
    import math
    from os.path import join, exists

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
    
    def getNextHundred(number):
        return number if number % 100 == 0 else number + 100 - number % 100
            
    metaFile = ".meta"
    projectName = ""
    projectPath = ""
    advParameters = ""
    rapId = ""
    scenarios = []
    mode = 0
    task = 0 # Running task. Equivalent of the index in the jobs' list
    iterations = 0
    startJob = datetime.datetime.now()
    
    try:
        #Accepted arguments
        options, arguments = getopt.getopt(args, "p:m:t:o:r:i:s:", ["project=", "mode=", "task=", "iterations=", "options=", "rap-id=", "scenario="])
    except getopt.GetoptError as error:
        print(error)
        sys.exit(1)
    
    #Parsing all the options
    for opt, arg in options:
        if opt in ('-p', '--project'):
            projectName = arg
        elif opt in ("-t", "--task"):
            task = int(arg)
        elif opt in ("-m", "--mode"):
            mode = int(arg)
        elif opt in ("-o", "--options"):
            advParameters = arg
        elif opt in ("-r", "--rap-id"):
            rapId = arg
        elif opt in ("-s", "--scenario"):
            scenarios.append(arg)
        elif opt in ("-i", "--iterations"):
            iterations = int(arg)
            
    # Required fields
    if not projectName or not mode or not rapId or task < 0 or not scenarios or not iterations:
        print("Missing arguments. Received : " + str(options))
        sys.exit(1)
        
    projectPath = join("/scratch", rapId, projectName)
    
    # Creating the scenarios' Results folder
    if not exists(join(projectPath, "Results")):
        try:
            os.mkdir(join(projectPath, "Results"))
        except:
            print("Error while creating Results folder : " + str(sys.exc_info()[0]))
    for scenario in scenarios:
        if not exists(join(projectPath, "Results", scenario)):
            try:
                os.mkdir(join(projectPath, "Results", scenario))
            except:
                print("Error while creating Results/" + scenario + " folder : " + str(sys.exc_info()[0]))

    # Make sure that the schnaps' parameters are well-written
    if advParameters:
        advParameters = "," + advParameters
    
    if mode == 1:
        # 1 job per simulation (Ex. 5 scenarios with 100 simulations = 500 jobs)
        scenario = scenarios[getNextHundred(task) / 100]
        iteration = str(task % iterations)
        configFile = "parameters_" + iteration + ".xml"
        outputPrefix = "Results/" + scenario + "/" + iteration + "_"
        proc = subprocess.Popen(["schnaps", "-c", configFile, "-d", projectPath, "-s", scenario, "-p", "print.prefix=" + outputPrefix + advParameters], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()        
        
        if stdout:
            print("Scenario " + scenario + " : " + stdout)
        if stderr:
            print("Scenario " + scenario + " : " + stderr)
            return
    elif mode == 2:
        # 1 job per iteration
        for scenario in scenarios:
            configFile = "parameters_" + str(task) + ".xml"
            outputPrefix = "Results/" + scenario + "/" + str(task) + "_"
            proc = subprocess.Popen(["schnaps", "-c", configFile, "-d", projectPath, "-s", scenario, "-p", "print.prefix=" + outputPrefix + advParameters], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = proc.communicate()
        
            if stdout:
                print("Scenario " + scenario + " : " + stdout)
            if stderr:
                print("Scenario " + scenario + " : " + stderr)
                return
    elif mode == 3:
        # 1 job per scenario
        for i in range(0, iterations):
            scenario = scenarios[task]
            configFile = "parameters_" + str(i) + ".xml"
            outputPrefix = "Results/" + scenario + "/" + str(i) + "_"
            proc = subprocess.Popen(["schnaps", "-c", configFile, "-d", projectPath, "-s", scenario, "-p", "print.prefix=" + outputPrefix + advParameters], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = proc.communicate()        
        
            if stdout:
                print("Scenario " + scenario + " : " + stdout)
            if stderr:
                print("Scenario " + scenario + " : " + stderr)
                return
    else:
        # 1 job for all
        for scenario in scenarios:
            for j in range(0, iterations):
                configFile = "parameters_" + str(j) + ".xml"
                outputPrefix = "Results/" + scenario + "/" + str(j) + "_"
                proc = subprocess.Popen(["schnaps", "-c", configFile, "-d", projectPath, "-s", scenario, "-p", "print.prefix=" + outputPrefix + advParameters], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                stdout, stderr = proc.communicate()        
        
                if stdout:
                    print("Scenario " + scenario + " : " + stdout)
                if stderr:
                    print("Scenario " + scenario + " : " + stderr)
                    return
    
    #Creates the metadata file in each directory of the project.
    #Do not modify the metadata's filename, unless you modify it also in the configuration file of Koksoak's website (/media/safe/www/html/conf.php)
    try:
        folderSize(os.path.join("/scratch", rapId, projectName), metaFile)
    except:
        pass
    
    # Count how much time the job took
    endJob = datetime.datetime.now()
    delta = endJob - startJob
    minutes, seconds = divmod(delta.days * 86400 + delta.seconds, 60)
    hours = 0
    if minutes >= 60:
        hours = int(math.ceil(minutes / 60))
        minutes = int(minutes - (minutes * hours))
    print("Total time of job (HH:MM:SS) : " + str(hours) + ":" + str(minutes) + ":" + str(seconds))
    
def fetcher(args):
    import paramiko
    import getopt
    import smtplib
    import tarfile
    from email.mime.text import MIMEText
    from crontab import CronTab
    from os.path import join
    
    rapId = "wny-790-aa"
    username = ""
    jobId = ""
    projectName = ""
    projectPath = "/media/safe/Results"
    configFile = "configs.tar.gz"
    create = False
    email = ""
    
    try:
        options, arguments = getopt.getopt(args, "hcu:i:p:e:", ["help", "create", "username=", "id=", "project=", "email="])
    except getopt.GetoptError as error:
        print (error)
        showHelpFetcher()
        sys.exit(1)
    
    if not options: #If no options given
        showHelpFetcher()
        sys.exit(1)
    
    #Parsing all the options
    for opt, arg in options:
        if (opt in ('-h', '--help')):
            showHelpFetcher()
            sys.exit()
        elif (opt in ("-u", "--username")):
            username = arg
        elif (opt in ("-i", "--id")):
            jobId = arg
        elif (opt in ("-p", "--project")):
            projectName = arg
        elif (opt in ("-c", "--create")):
            create = True
        elif (opt in ("-e", "--email")):
            email = arg
    
    if not username or not jobId or not projectName:
        #Username, project's name and job's id are required
        showHelpFetcher()
        sys.exit(1)
        
    if create:
        #Creates a cron job
        try:
            cron = CronTab(user="lsdadmin")
            cronJob = cron.new("/usr/bin/python " + koksoakScriptLocation + runSimScript + " " + fetcherScript + " -u " + username + " -i " + jobId + " -p " + projectName + " -e " + email, comment=jobId)
            cronJob.minute.every(15)
            cron.write()
            print("Cron job with job id " + jobId + " created successfully")
            sys.exit(0)
        except Exception as e:
            print("An error has occured while creating the cron job : " + str(e))
            sys.exit(0)
    
    
    #Setting up the ssh
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    k = paramiko.RSAKey.from_private_key_file(os.path.expanduser("/home/lsdadmin/.ssh/id_rsa"))
    ssh.connect(hostname="colosse.calculquebec.ca", username=username, pkey=k)
    
    #Looking for active jobs
    stin, stout, sterr = ssh.exec_command("showq -u $USER\n")
    
    #Getting the output from the last command
    output = stout.readlines()
    
    category = "" #Used to know if we are reading active, eligible or blocked jobs
    active = False
    eligible = False
    blocked = False
    for line in output:
        if "active jobs-----" in line:
            category = "active"
            continue
        elif "eligible jobs-----" in line:
            category = "eligible"
            continue
        elif "blocked jobs-----" in line:
            category = "blocked"
            continue
        
        if category == "active":
            if line.startswith(jobId):
                active = True
        elif category == "eligible":
            if line.startswith(jobId):
                eligible = True
        elif category == "blocked":
            if line.startswith(jobId):
                blocked = True
        
    if not active and not eligible and not blocked:
        #Simulation is done
        configFile = join(projectPath, projectName, configFile)
        try:            
            #Getting the project
            os.system("scp -r " + username + "@colosse.calculquebec.ca:" + join("/scratch", rapId, projectName) + " " + join(projectPath, projectName))
            #Set permission to group for reading
            os.system("find " + join(projectPath, projectName) + " -type f -exec chmod +r {} \;")
            #Move parameters in zip file
            with tarfile.open(configFile, mode='w:gz') as archive:
                archive.add(join(projectPath, projectName, "Environment"))
                archive.add(join(projectPath, projectName, "Libraries"))
                archive.add(join(projectPath, projectName, "Populations"))
                archive.add(join(projectPath, projectName, "Processes"))
                archive.add(join(projectPath, projectName, "XSD"))
                for file in os.listdir(join(projectPath, projectName)):
                    if file.startswith("parameter"):
                        archive.add(join(projectPath, projectName, file))
            
        except:
            print("An error has occurred while retrieving the results : " + str(sys.exc_info()[0]))
        
        #Now that the simulation is done, we remove the cron job.
        cron = CronTab(user="lsdadmin")
        cron.remove_all(comment=jobId)
        cron.write()
        print("Cron job with job id " + jobId + " removed successfully")
        
        #Sending an email to the user
        if email:
            try:
                server = "smtp.ulaval.ca"
    
                msg = MIMEText("Simulation called '" + projectName + "' is done and now on Koksoak")
                msg["Subject"] = "Simulation '" + projectName + "' is done"
                msg["From"] = "no-reply@ulaval.ca"
                msg["To"] = email
                
                smtp = smtplib.SMTP(server)
                smtp.sendmail("no-reply@ulaval.ca", [email], msg.as_string())
                smtp.quit()
            except:
                print("Email not send. Error occurred :", sys.exc_info()[0])
    
    ssh.close() 

main(sys.argv[1:])