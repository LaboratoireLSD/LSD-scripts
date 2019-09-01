import sys
import os
import re


class ComputeServer:
    """A base class that represents a supercomputer
    Attributes:
        name(str): The name of an available supercomputer
        username(str): User id on Compute Canada default account
        rap_id(str):  L'identificateur de projet
        account(str): Research Allocation Project ID or Compute Canada Role Identifier (CCRI)
        project_name(str): Project name
        project_name_with_datetime(str): Project name with datetime
        scratch_folder(str): Scratch folder path
        server_output(str): Output folder name
        output_folder(str): Standard output folder path
        error_folder(str): Standard error folder path
        script_content(str): Job script
        login_node(str): Login node
        task_job_array(str): Environment variable that represents the number of the task job array (multiple jobs
                             to be executed with identical parameters)
        command_job_submission(str): Command job submission
        command_job_status (str): The command that lists pending and running jobs.
        submit_script_name(str): Job script name
        ram_usage(str): Expected memory consumption per job
        nb_tasks(str): Tasks in the array of jobs
        job_id(str): The base job ID
        mode(str): How jobs will be created <1-4>
    Args:
        name(str): The name of the supercomputer
        username(str): The username
        project_name(str): The project name
        project_name_with_datetime(str): The project name with datetime
        duration(str): Time limit for the job
        nb_tasks(int): The array index value of the jobs
        ram_usage(int): The expected memory consumption per job
    """

    def __init__(self, **kwargs):
        self.name = ''
        self.username = ''
        self.rap_id = 'wny-790-aa'
        self.account = 'def-reinharz'
        self.project_name = ''
        self.project_name_with_datetime = ''
        self.scratch_folder = ''
        self.server_output = ''
        self.output_folder = ''
        self.error_folder = ''
        self.script_content = ''
        self.login_node = ''
        self.task_job_array = ''
        self.command_job_submission = ''
        self.command_job_status = ''
        self.submit_script_name = 'generated_submit'
        self.ram_usage = '0'
        self.nb_tasks = '0'
        self.job_id = ''
        self.nb_cpu = 1
        self.group_space = '/rap/' + self.rap_id

        # systems available for use
        self.systems = ["colosse", "cedar", "graham", "beluga"]

        # Parsing the options
        for opt, arg in kwargs.items():
            if opt == 'name':
                try:
                    assert kwargs['name'] in self.systems
                    self.name = arg
                    self.server_output = self.name + '_output'
                except AssertionError:
                    print('\nERROR: "-s ' + kwargs['name'] + '" not valide. Try ' + str(self.systems))
                    sys.exit(1)
            elif opt == 'username':
                self.username = arg
            elif opt == 'project_name':
                self.project_name = arg
            elif opt == 'project_name_with_datetime':
                self.project_name_with_datetime = arg
            elif opt == 'duration':
                self.duration = arg
            elif opt == 'nb_tasks':
                self.nb_tasks = str(arg)
            elif opt == 'ram_usage':
                self.ram_usage = str(arg)

    def create_supercomputer(self):
        if self.name == 'colosse':
            return Colosse(self.username, self.project_name, self.project_name_with_datetime, self.nb_tasks,
                           self.duration, self.ram_usage)
        elif self.name == 'cedar':
            return Cedar(self.username, self.project_name, self.project_name_with_datetime, self.nb_tasks,
                         self.duration, self.ram_usage)
        elif self.name == 'graham':
            return Graham(self.username, self.project_name, self.project_name_with_datetime, self.nb_tasks,
                          self.duration, self.ram_usage)
        elif self.name == 'beluga':
            return Beluga(self.username, self.project_name, self.project_name_with_datetime, self.nb_tasks,
                          self.duration, self.ram_usage)

    def connect_ssh(self):
        """Connects to supercomputer and authenticates to it
        Returns:
            SSHClient: The client
        """
        import paramiko
        print("Connection to " + self.name + " by ssh.")
        try:
            # Setting up the ssh
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            k = paramiko.RSAKey.from_private_key_file(os.path.expanduser("~/.ssh/id_rsa"))
            ssh.connect(hostname=self.login_node, username=self.username, pkey=k)
            return ssh

        except:
            print("\nERROR:Authentication failed.\n")
            print("For help about the RSA key : https://docs.fedoraproject.org/en-US/Fedora/15/html/Deployment_Guide/"
                  "s2-ssh-configuration-keypairs.html\n")
            exit(1)

    def get_job_id(self, stout_read):
        """Gets the job identification number when a job is submitted.
        Args:
            stout_read(list): The message printed once the job has been successfully put into the queue.
        Warning:
            Method redefined in the derived classes.
        """
        raise NotImplementedError

    @staticmethod
    def get_job_status(output, job_id):
        """ Checks the status of the job
        Args:
            job_id (string): job id
            output (list): list of jobs output
        Warning:
            Method redefined in the derived classes
        Returns:
            bool: True if job is completed (or canceled), and False otherwise
        """
        raise NotImplementedError

    def create_submission_script(self):
        """Create the batch job script content
        Warning:
            Method redefined in the derived classes
        """
        raise NotImplementedError


class CalculQuebec(ComputeServer):
    def __init__(self, name, username, project_name, project_name_with_datetime, nb_tasks):
        super().__init__(name=name, username=username, project_name=project_name,
                         project_name_with_datetime=project_name_with_datetime)

        self.submit_script_name = "generated_submit.pbs"
        self.nb_tasks = str(nb_tasks)

        if self.name == 'colosse':
            self.scratch_folder = "/scratch/" + self.rap_id + "/" + project_name_with_datetime
            self.login_node = self.name + ".calculquebec.ca"

        self.output_folder = self.scratch_folder + "/" + self.name + '_output'
        self.error_folder = self.output_folder

    def get_job_id(self, stout_read):
        pass

    def create_submission_script(self):
        pass

    @staticmethod
    def get_job_status(output, job_id):
        pass


class Colosse(CalculQuebec):
    def __init__(self, username, project_name, project_name_with_datetime, nb_tasks, duration, ram_usage):
        super().__init__("colosse", username, project_name, project_name_with_datetime, nb_tasks)

        self.command_job_submission = "msub "
        self.command_job_status = "showq -u " + username + "\n"
        self.task_job_array = "MOAB_JOBARRAYINDEX "
        self.ram_usage = ram_usage

        # Acceptable time format "hours:minutes:seconds"
        if duration == "":
            self.duration = "24:00:00"
        elif re.match("([0-9]+):([0-5][0-9]):([0-5][0-9])", duration):  # cannot exceed 48 hours
            self.duration = duration
        else:
            print("ERROR : -d " + duration + ". Duration must be HH:MM:SS (max 30 days)")
            sys.exit(1)

    def create_submission_script(self):
        # Submission script content
        self.script_content = ('#!/bin/bash\n'
                               '#PBS -N ' + self.project_name + '\n'  # Job\'s name
                               '#PBS -l walltime=' + self.duration + '\n'  # Max duration HH:MM:SS
                               '#PBS -o ' + self.output_folder + '/' + self.project_name + '_%I.out\n'  # Standard output  
                               '#PBS -e ' + self.error_folder + '/' + self.project_name + '_%I.err\n'  # Error output
                               '#PBS -A ' + self.rap_id + '\n'  # Rap ID
                               '#PBS -l nodes=1:ppn=8\n'  # Total nodes and hearts
                               )
        if int(self.nb_tasks) >= 1:  # mode 1
            self.script_content += '#PBS -t 0-' + self.nb_tasks + '\n'  # Array of jobs.
        self.script_content += 'module load apps/python/3.5.0\npython3 '  # Loads python 3 version

    def get_job_id(self, stout_read):
        if str.isdigit(str(stout_read[1]).strip()):
            self.job_id = stout_read[1].strip()

    @staticmethod
    def get_job_status(output, job_id):
        category = ""  # Used to know if we are reading active, eligible or blocked jobs
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
                if line.startswith(job_id):
                    active = True
            elif category == "eligible":
                if line.startswith(job_id):
                    eligible = True
            elif category == "blocked":
                if line.startswith(job_id):
                    blocked = True
        if not active and not eligible and not blocked:
            return True

    @staticmethod
    def get_job_status(output, job_id):
        if not output:
            # Job completed
            return True

        for line in output:
            if job_id in line:
                # Job not completed
                if " C " not in line:
                    return False
        return True


class NationalSystem(ComputeServer):
    def __init__(self, name, username, project_name, project_name_with_datetime, nb_tasks, duration, ram_usage):
        super().__init__(name=name, username=username, project_name=project_name,
                         project_name_with_datetime=project_name_with_datetime)

        self.login_node = self.name + ".computecanada.ca"
        self.scratch_folder = "/scratch/" + self.username + "/" + project_name_with_datetime
        self.output_folder = self.scratch_folder + "/" + self.name + '_output'
        self.error_folder = self.scratch_folder + "/" + self.name + '_output'
        self.command_job_submission = "sbatch "
        self.command_job_status = "squeue -u " + self.username + "\n"
        self.task_job_array = "SLURM_ARRAY_TASK_ID"
        self.submit_script_name = "generated_submit.sbatch"
        self.ram_usage = "15000"
        self.nb_tasks = nb_tasks
        self.home_path = "/home/" + self.username
        self.schnaps_path = self.home_path + "/project/init/bin/schnaps"
        self.group_space = '/project/' + self.account

        if ram_usage != str(0):
            # Minimum required memory for the job, in MB.
            # Units can be specified: [K|M|G|T]
            # 'c' - Memory Per CPU, 'n'- Memory Per Node
            self.ram_usage = ram_usage

        # Acceptable time format "days-hours:minutes" or "hour:minutes:seconds" (cannot exceed 28 days)
        try:
            if duration == "":
                self.duration = "1-00:00"
            elif re.match("([0-9]+):([0-5][0-9]):([0-5][0-9])", duration):
                self.duration = duration
            elif re.match("([0-9]+)-([0-5][0-9]):([0-5][0-9])", duration):
                self.duration = duration
            else:
                self.duration = duration
                raise ValueError('"-d ' + duration + '" Duration must be <JJ-HH:MM> or <HH:MM:SS>')
        except ValueError as e:
            print("\nERROR : {}\n".format(e))
            sys.exit(1)

    def create_submission_script(self):
        if int(self.nb_tasks) >= 999:
            print("\nERROR : The maximum size of a job array in Cedar/Graham can't exceed 999 jobs.\n"
                  "Try option -m 2 (or plus)")
            exit(1)
        self.script_content = ('#!/bin/bash\n'
                               # Compute Canada Resource Allocation Project's account
                               '#SBATCH --account=' + self.account + '\n'  # Specific account

                               # Maximum walltime per job (DD-HH:MM or HH:MM:SS)
                               '#SBATCH --time=' + self.duration + '\n'

                               # Job's name
                               '#SBATCH --job-name=' + self.project_name + '\n'

                               # Standard output. "%A" for job ID and "%a for array index
                               '#SBATCH -o ' + self.output_folder + '/' + self.project_name + '_%A_%a.out\n'

                               # Standard error      
                               '#SBATCH -e ' + self.error_folder + '/' + self.project_name + '_%A_%a.err\n'

                               '#SBATCH --mem-per-cpu=' + self.ram_usage + "Mn\n" +
                               '#SBATCH --ntasks=1\n' +
                               '#SBATCH --cpus-per-task=2\n')

        if int(self.nb_tasks) >= 1:  # mode 1
            self.script_content += '#SBATCH --array=0-' + self.nb_tasks + '\n'

        self.script_content += ('module load python/3.6\n' + 'python')

    def get_job_id(self, stout_read):
        if str.isdigit(str(stout_read[0].strip("Submitted batch job \n"))):
            self.job_id = stout_read[0].strip("Submitted batch job \n")

    @staticmethod
    def get_job_status(output, job_id):
        for line in output:
            if job_id in line:
                return False
        return True


class Cedar(NationalSystem):
    def __init__(self, username, project_name, project_name_with_datetime, nb_tasks, duration, mem_req):
        super().__init__("cedar", username, project_name, project_name_with_datetime, nb_tasks, duration, mem_req)
        self.nb_cpu = 32


class Graham(NationalSystem):
    def __init__(self, username, project_name, project_name_with_datetime, nb_tasks, duration, mem_req):
        super().__init__("graham", username, project_name, project_name_with_datetime, nb_tasks, duration, mem_req)
        self.nb_cpu = 32


class Beluga(NationalSystem):
    def __init__(self, username, project_name, project_name_with_datetime, nb_tasks, duration, mem_req):
        super().__init__("beluga", username, project_name, project_name_with_datetime, nb_tasks, duration, mem_req)
        self.nb_cpu = 32
