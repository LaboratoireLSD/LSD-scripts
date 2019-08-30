import os
import sys
import time
from supercomputer import ComputeServer
import getopt
import ntpath

from os.path import isdir, join, isfile, realpath, dirname


def show_help_launcher():
    """
    Shows help page for the launcher script.
    Call it with arguments : launch --help|-h
    """
    compute = ComputeServer()
    print("\n")
    print("Possible arguments :\n")

    print("     -p, --project <path>               Complete path to the project (folder's name)")
    print("     -u, --username <name>              Username on supercomputer for the ssh connection")
    print("     [-l, --log]                        Tells schnaps to print logs")
    print("     [-e, --email <email>]              Koksoak will send an email to this address when the simulation will "
          "be retrieved")
    print("     [-m, --mode <1-4>]                 1 : One job per file. 2 : One job per iteration. 3 : One job per "
          "simulation. 4 : One job for all. Default = 2")
    print("     [-s, --supercomputer <name>]       Supercomputer system name. Defalt = Colosse\n"
          "                                        Options: ", compute.systems)
    print("     [-d, --duration <HH:MM:SS>]        Maximum duration of the simulation. Default = 24:00:00\n"
          "                                        (Colosse:cannot exceed 48h) \n"
          "                                        (Guillimin: cannot exced 30 days\n)"
          "                                        (Cedar and Graham: cannot exceed 28 days\n)"  # A shorter job will 
          # have more scheduling opportunities than an otherwise-identical longer job.
          "                                        (Optional format: <DD:HH:MM:SS> for Colosse and Guillimin"
          "                                                          <DD-HH:MM> for Cedar and Graham.)")
    print("     [-r, --ram <memory>]               Expected memory consumption.\n"
          "                                        Cedar and Graham: default = 15000Mn (per noeud).")
    print("     [-o, --options <option>]           Options for SCHNAPS. See its doc for more information.")
    print("     [-h, --help]                       Shows the help page\n")
    print("For help about the RSA key : https://docs.fedoraproject.org/en-US/Fedora/15/html/Deployment_Guide/"
          "s2-ssh-configuration-keypairs.html")


class Launcher:
    """
    First script.
    Used by the user.
    It starts the chain by creating a job on the supercalculator, executing it and
    calling the 3rd script on the receiving server.
    """

    def __init__(self, args):
        self.run_sim_script = "run.py "
        self.runner_script = "run"
        self.koksoak_script_location = "/home/lsdadmin/scripts"
        self.fetcher_script = "main"
        self.home_user_path = "/home/"
        self.email_to = "no-reply@ulaval.ca"
        self.duration = "24:00:00"
        self.username = ""
        self.project_path = ""
        self.project_name = ""
        self.project_name_with_datetime = ""
        self.scenarios = []
        self.scenarios_to_string = ""  # Will be used to pass all the scenario to the execution script
        self.adv_parameters = " -o "  # Advanced parameters - Schnaps
        self.mode = 2  # How jobs will be created
        self.nb_tasks = 0  # Tasks in the array of jobs
        self.nb_iterations = 0
        self.now = time.strftime("%d-%m-%Y_%H-%M")
        self.log = False
        self.given_parameters = False
        self.ram_usage = 0  # Expected memory consumption per job in Mib (1024Mib in 1 Gib)
        self.server_name = 'colosse'
        self.scratch_path = ''
        self.submit_script_content = ''

        self.check_paramiko()
        self.parse_args(args)
        self.add_log_parameter()
        self.get_scenarios()

        self.supercomputer = ComputeServer(name=self.server_name, username=self.username,
                                           project_name=self.project_name,
                                           project_name_with_datetime=self.project_name_with_datetime,
                                           duration=self.duration,
                                           nb_tasks=self.nb_tasks, ram_usage=self.ram_usage).create_supercomputer()

        self.supercomputer.nb_tasks = self.get_nb_jobs()
        self.home_user_path += self.username
        self.simulation_folders = self.create_output_folder(self.project_path, self.supercomputer.server_output)
        self.send_project()
        self.create_job_script()
        # 2nd script
        self.submit_script(self.submit_script_content)
        # 3rd script
        self.collect_script()
        print("Done")

    def create_job_script(self):
        count = 1
        self.scratch_path = self.supercomputer.scratch_folder
        self.supercomputer.create_submission_script()
        self.submit_script_content = (self.supercomputer.script_content + " " + self.run_sim_script + " " +
                                      self.runner_script +
                                      ' -p ' + self.supercomputer.project_name_with_datetime +
                                      ' -m ' + str(self.mode) +
                                      ' -i ' + str(self.nb_iterations) +
                                      self.scenarios_to_string + self.adv_parameters +
                                      ' -d ' + self.scratch_path)  # Executing the 2nd script
        if int(self.supercomputer.nb_tasks) >= 1:
            self.submit_script_content += ' -t $' + self.supercomputer.task_job_array
        count += 1

    def send_project(self):
        # Creating the folder that will contain supercomputer's outputs
        ssh = self.supercomputer.connect_ssh()
        print("Sending the project folder to : " + self.username + "@" + self.supercomputer.login_node + ":" +
              self.supercomputer.scratch_folder)
        os.system("scp -r " + self.project_path + "/ " + self.username + "@" + self.supercomputer.login_node + ":" +
                  self.supercomputer.scratch_folder + "/")
        ssh.close()

    def get_nb_jobs(self):
        # Getting the number of jobs depending on the chosen mode
        if self.mode == 1:
            # 1 job per simulation (Ex. 5 scenarios with 100 simulations = 500 jobs)
            return str(len(self.scenarios) * (self.nb_iterations + 1) - 1)
        elif self.mode == 2:
            # 1 job per iteration (Ex. 5 scenarios with 100 iterations = 100 jobs)
            return str(self.nb_iterations)
        elif self.mode == 3:
            # 1 job per scenario (Ex. 5 scenarios with 100 iterations = 5 jobs)
            return str(len(self.scenarios) - 1)
        else:
            # 1 job for all
            return str(1)

    def add_log_parameter(self):
        # Adding the log parameter
        if self.given_parameters:
            self.adv_parameters += ","
        self.adv_parameters += "print.log=" + str(self.log).lower()

    def check_paramiko(self):
        try:
            import paramiko

        except:
            print("Error : Paramiko isn't installed on your system.")
            print("Before installing it, make sure you have the correct dependencies with : "
                  "'sudo apt-get install build-essential libssl-dev libffi-dev python-dev'")
            print("Then, install pip with 'sudo apt-get install python-pip' and paramiko with "
                  "'sudo pip install paramiko'\n")
            sys.exit(1)

    def create_output_folder(self, path, server_output):
        """Creates the folder that will contain supercomputer's outputs and returns a list with the project names
        Args:
            path: The project path
            server_output: The folder that contains supercomputer's outputs
        Returns:
            list: A list that will hold the simulation folders' name
        """

        if not os.path.exists(os.path.join(path, server_output)):
            os.makedirs(os.path.join(path, server_output))

    def submit_script(self, submit_script_content):
        # Executing the 2nd script
        ssh = self.supercomputer.connect_ssh()

        print("Generating the submit script on " + self.server_name + ".")

        ssh.exec_command("echo '" + submit_script_content + "' > " + join(self.supercomputer.group_space + '/scripts/',
                                                                          self.supercomputer.submit_script_name) + "\n")
        print("Sending script to user's group project/script folder.")
        os.system("scp " + join(dirname(realpath(__file__)), "run.py") + " " + self.username + "@" +
                  self.supercomputer.login_node + ":" + self.supercomputer.group_space + '/scripts/')
        ssh.exec_command("chmod g+rwx " + join(self.supercomputer.group_space + '/scripts/',
                                               self.supercomputer.submit_script_name) + " " +
                         join(self.supercomputer.group_space + '/scripts/run.py' + "\n"))
        print("Launching the submit script.")
        stin, stout, sterr = ssh.exec_command('cd ' + self.supercomputer.group_space + '/scripts\n' +
                                              self.supercomputer.command_job_submission +
                                              self.supercomputer.submit_script_name + "\n")
        sterr_read = sterr.readlines()  # If ssh returns an error
        stout_read = stout.readlines()  # If the ssh returns a normal output

        if sterr_read:
            # An error has occurred
            print(sterr_read)
        if stout_read:
            # Get job id
            self.supercomputer.get_job_id(stout_read)
            # Printing job id
            if type(stout_read) is list:
                print("Job's id : " + self.supercomputer.job_id)
            else:
                print(str(stout_read))
                print("Couldn't get the job's id. Exiting without creating a cron job on Koksoak.")
                sys.exit(1)
        ssh.close()

    def collect_script(self):
        import paramiko
        print("-------------------------------")
        print("Connection to Koksoak by ssh.")
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        k = paramiko.RSAKey.from_private_key_file(os.path.expanduser("~/.ssh/id_rsa"))
        ssh.connect(hostname="koksoak.gel.ulaval.ca", username="lsdadmin", pkey=k)

        print("Sending this script to Koksoak.")

        os.system("scp -r " + dirname(realpath(__file__)) + " lsdadmin@koksoak.gel.ulaval.ca:" +
                  self.koksoak_script_location)

        print("Creating a cron job on Koksoak.\n")

        # Executing the 3rd script
        stin, stout, sterr = ssh.exec_command('python3 ' + self.koksoak_script_location + "/runsim/main.py fetch" +
                                              ' -u ' + self.supercomputer.username +
                                              ' -i ' + self.supercomputer.job_id +
                                              ' -p ' + self.supercomputer.project_name_with_datetime +
                                              ' -s ' + self.supercomputer.name +
                                              ' -d ' + self.scratch_path +
                                              ' -e ' + self.email_to +
                                              ' -c \n')
        sterr_read = sterr.readlines()  # If ssh returns an error
        stout_read = stout.readlines()  # If the ssh returns a normal output

        if sterr_read:
            # An error has occurred
            if type(sterr_read) is list:
                for line in sterr_read:
                    print(line)
            else:
                print(sterr_read)
        if stout_read:
            # Printing the return of the previous commands.
            if type(stout_read) is list:
                for line in stout_read:
                    print(line)
            else:
                print(stout_read)
        ssh.close()

    def parse_args(self, args):
        # Accepted arguments
        try:
            options, arguments = getopt.getopt(args, "hlp:u:s:d:e:o:m:r:",
                                               ["help", "log", "project=", "username=", "compute-server=", "duration=",
                                                "email=""options=", "mode=", "ram="])
        except getopt.GetoptError:
            show_help_launcher()
            sys.exit(1)

        if not options:  # If no options given
            show_help_launcher()
            sys.exit(1)

        for opt, arg in options:
            if opt in ('-h', '--help'):
                show_help_launcher()
                sys.exit()
            if opt in ("-s", "--compute-server"):
                self.server_name = arg
            elif opt in ('-p', '--project'):
                if isdir(arg):
                    self.project_path = arg
                    if " " in self.project_path:
                        print("Project path must not contain space. Exiting...")
                        sys.exit(1)
                    if self.project_path.endswith("/"):
                        self.project_path = self.project_path[:-1]
                    self.project_name = ntpath.basename(self.project_path)
                    self.project_name_with_datetime = self.project_name + "_" + self.now
                else:
                    print("Invalid project's folder")
                    show_help_launcher()
                    sys.exit(1)
            elif opt in ('-u', "--username"):
                self.username = arg
            elif opt in ("-o", "--options"):
                self.adv_parameters += arg
                self.given_parameters = True
            elif opt in ("-m", "--mode"):
                self.mode = int(arg)
                if self.mode < 1 or self.mode > 4:
                    print("Mode unsupported (" + str(self.mode) + "). See the help page for more information.")
                    sys.exit(1)
            elif opt in ("-l", "--log"):
                self.log = True
            elif opt in ("-r", "--ram"):
                self.ram_usage = int(arg)
            elif opt in ("-e", "--email"):
                self.email_to = arg
            elif opt in ("-d", "--duration"):
                self.duration = arg

    def get_scenarios(self):
        from lxml import etree as ET
        # Getting scenarios. Find the first "parameters_x.xml" in project to retrieve scenarios
        try:
            parameter_name = [fileName for fileName in os.listdir(self.project_path) if
                              isfile(join(self.project_path, fileName))
                              and fileName.startswith("parameters_")]
            parameters_file = ET.parse(join(self.project_path, parameter_name[0]))
            self.nb_iterations = len(parameter_name) - 1  # Number of parameters_x.xml files = number of iterations
            for scenario in parameters_file.xpath("/Simulator/Simulation/Scenarios/Scenario"):
                self.scenarios.append(scenario.get("label"))
                self.scenarios_to_string += " -s " + self.scenarios[-1]
        except IndexError:
            print("Error while getting scenarios. Make sure your project has iterations. No 'parameters_X.xml' found.")
        except:
            print("Error occurred while retrieving the scenarios.")
            sys.exit(1)

        if not self.username or not self.scenarios or not self.project_name:
            # Project, scenario and username are necessary
            show_help_launcher()
            sys.exit(1)

        return self.scenarios
