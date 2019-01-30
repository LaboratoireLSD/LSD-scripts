import sys
import os
from supercomputer import ComputeServer


def show_help_fetcher():
    """
    Shows help page for the fetcher script.
    Call it with arguments : fetch --help|-h
    """
    print("\n")
    print("Possible arguments :\n")
    print("     -u, --username <name>              Username on supercomputer for the ssh connection")
    print("     -p, --project <name>               Project's name")
    print("     -i, --id <job's id>                Job's id given by supercomputer")
    print("     -s, --supercomputer <name>         Supercomputer system name")
    print("     -d, --scratch-path <path>          Full path to the supercomputer's scratch storage")
    print("     [-e, --email <address@ulaval.ca>]  Person to contact when the simulation is done")
    print("     [-c]                               Create a new cron job")


def fetcher(args):
    import paramiko
    import getopt
    import smtplib
    import tarfile
    import shutil
    from email.mime.text import MIMEText
    from crontab import CronTab
    import subprocess
    import fileinput

    meta_file = ".meta"
    fetcher_script = "fetch"
    koksoak_script_location = "/home/lsdadmin/scripts/runsim/"
    meta_file = ".meta"

    def update_size(proj_path):
        """Replace the size of the project's directory in an existing meta file
        Args:
            proj_path (str): The project path
        """
        meta_path = os.path.join(proj_path, meta_file)
        size = subprocess.check_output(['du', '-sh', proj_path]).split()[0].decode('utf-8')

        if os.path.isfile(meta_path):
            with open(meta_path, "r") as f:
                old_line = f.readline()

        for line in fileinput.input(meta_path, inplace=True):
            if old_line in line:
                line = line.replace(old_line, "size:" + size + "\n")
            sys.stdout.write(line)

    username = ""
    job_id = ""
    project_name = ""
    project_path = "/media/safe/Results"
    config_file = "configs.tar.gz"
    create = False
    email_to = ""
    system_name = ""
    scratch_path = ""

    try:
        options, arguments = getopt.getopt(args, "hcu:i:p:s:d:e:",
                                           ["help", "create", "username=", "id=", "project=",
                                            "supercomputer=", "--scratch-path", "email="])
    except getopt.GetoptError as error:
        print(error)
        show_help_fetcher()
        sys.exit(1)

    if not options:  # If no options given
        show_help_fetcher()
        sys.exit(1)

    # Parsing all the options
    for opt, arg in options:
        if opt in ('-h', '--help'):
            show_help_fetcher()
            sys.exit()
        elif opt in ("-u", "--username"):
            username = arg
        elif opt in ("-i", "--id"):
            job_id = arg
        elif opt in ("-p", "--project"):
            project_name = arg
        elif opt in ("-s", "--supercomputer"):
            system_name = arg
        elif opt in ("-d", "--scratch-path"):
            scratch_path = arg
        elif opt in ("-e", "--email"):
            email_to = arg
        elif opt in ("-c", "--create"):
            create = True
    server_output = system_name + "_output"

    if not username or not job_id or not project_name:
        # Username, project's name and job's id are required
        show_help_fetcher()
        sys.exit(1)

    supercomputer = ComputeServer(name=system_name, username=username, project_name="",
                                  project_name_with_datetime=project_name, duration="").create_supercomputer()

    if create:
        # Creates a cron job
        try:
            cron = CronTab(user="lsdadmin")
            cron_job = cron.new("python3 " + koksoak_script_location + "main.py " + fetcher_script +
                                " -u " + username +
                                " -i " + job_id +
                                " -p " + project_name +
                                " -s " + system_name +
                                " -d " + scratch_path +
                                " -e " + email_to,
                                comment=job_id)
            cron_job.minute.every(15)
            cron.write()
            print("Cron job with job id " + job_id + " created successfully")
            sys.exit(0)
        except Exception as e:
            print("An error has occured while creating the cron job : " + str(e))
            sys.exit(0)

    # Setting up the ssh
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    k = paramiko.RSAKey.from_private_key_file(os.path.expanduser("/home/lsdadmin/.ssh/id_rsa"))
    ssh.connect(hostname=supercomputer.login_node, username=username, pkey=k)

    # Looking for active jobs
    stin, stout, sterr = ssh.exec_command(supercomputer.command_job_status)

    # Getting the output from the last command
    output = stout.readlines()

    if supercomputer.get_job_status(output, job_id):
        project_path = os.path.join(project_path, project_name)
        try:
            # Getting the project
            os.system("scp -r " + username + "@" + supercomputer.login_node + ":" + scratch_path +
                      " " + project_path)
            # Set permission to group for reading
            os.system("find " + project_path + " -type f -exec chmod +r {} \;")

            for root, subdirs, files in os.walk(project_path):
                if "Environment" in subdirs and "Populations" in subdirs and "Libraries" in subdirs:
                    # Move parameters in zip file
                    with tarfile.open(os.path.join(root, config_file), mode='w:gz') as archive:
                        for subdir in subdirs:
                            if subdir != "Results" and subdir != "Analyse" and subdir != server_output:
                                archive.add(os.path.join(root, subdir), arcname=subdir)
                                shutil.rmtree(os.path.join(root, subdir))
                        for file in files:
                            if file != meta_file:
                                archive.add(os.path.join(root, file), arcname=file)
                                os.remove(os.path.join(root, file))

        except:
            print("An error has occurred while retrieving the results : " + str(sys.exc_info()[0]))

        # Now that the simulation is done, we remove the cron job.
        cron = CronTab(user="lsdadmin")
        cron.remove_all(comment=job_id)
        cron.write()
        print("Cron job with job id " + job_id + " removed successfully")

        # Sending an email to the user
        if email_to != "no-reply@ulaval.ca":
            try:
                server = "smtp.ulaval.ca"

                msg = MIMEText("Simulation called '" + project_name + "' is done and now on Koksoak")
                msg["Subject"] = "Simulation '" + project_name + "' is done"
                msg["From"] = "no-reply@ulaval.ca"
                msg["To"] = email_to

                smtp = smtplib.SMTP(server)
                smtp.sendmail("no-reply@ulaval.ca", [email_to], msg.as_string())
                smtp.quit()
            except smtplib.SMTPException:
                print("Email not send. Error occurred :", sys.exc_info()[0])

    ssh.close()
