import datetime
import time
import os
import sys

def runner(args):
    import getopt
    import subprocess
    from os.path import join, exists

    meta_file = ".meta"

    def get_change_time(file_path):
        """Returns the last time the directory entry changed.
        Args:
            file_path (str): a file path
        Returns:
             str: ctime <DD-MM-YYYY>
        """

        ctime = (datetime.datetime.strptime(time.ctime(os.path.getctime(file_path)), "%a %b %d %H:%M:%S %Y"))
        return str(ctime.day) + "-" + str(ctime.month) + "-" + str(ctime.year)

    def create_meta_file(path):
        """Creates the meta file writing on it the size and the last changed date of the project's directory
        Args:
            path: The (scratch) path of the  project
        Returns: ???
        """
        today = get_change_time(os.path.join(path, "Results"))

        proc = subprocess.Popen(["du", "-h", path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        if not stdout:
            return

        for line in stdout.decode('utf-8').split("\n"):
            try:
                size, folder = line.split("\t")
                if os.path.isfile(os.path.join(folder, meta_file)):
                    with open(os.path.join(folder, meta_file)) as file:
                        for info in file:
                            if info.startswith("creation date:"):
                                today = info.split(":")[1]
                with open(os.path.join(folder, meta_file), "w") as file:
                    file.write("size:" + size + "\n")
                    file.write("creation date:" + today + "\n")
            except:
                pass

    project_name = ""
    scratch_path = ""
    adv_parameters = ""
    scenarios = []
    mode = 0
    task = 0  # Running task. Equivalent of the index in the jobs' list
    iterations = 0
    start_job = datetime.datetime.now()

    try:
        options, arguments = getopt.getopt(args, "p:m:t:i:s:o:d:",
                                           ["project=", "mode=", "task=", "iterations=", "scenario=", "options=",
                                            "scratch-path="])
    except getopt.GetoptError as error:
        print(error)
        sys.exit(1)
    # Parsing all the options
    for opt, arg in options:
        if opt in ('-p', '--project'):
            project_name = arg
        elif opt in ("-t", "--task"):
            task = int(arg)
        elif opt in ("-m", "--mode"):
            mode = int(arg)
        elif opt in ("-o", "--options"):
            adv_parameters = arg
        elif opt in ("-s", "--scenario"):
            scenarios.append(arg)
        elif opt in ("-i", "--iterations"):
            iterations = arg
        elif opt in ("-d", "--scratch_path"):
            scratch_path = arg
    # Required fields
    # if not project_name or not mode or not rap_id or task < 0 or not scenarios or not iterations:
    if not project_name or not mode or not scenarios or not iterations and (mode != 4 and task < 0):
        print("Missing arguments. Received : " + str(options))
        sys.exit(1)

    # Creating the scenarios' Results folder
    if not exists(join(scratch_path, "Results")):
        try:
            os.mkdir(join(scratch_path, "Results"))
        except OSError:
            print("Error while creating Results folder : " + str(sys.exc_info()[0]))
    for scenario in scenarios:
        if not exists(join(scratch_path, "Results", scenario)):
            try:
                os.mkdir(join(scratch_path, "Results", scenario))
            except OSError:
                print("Error while creating Results/" + scenario + " folder : " + str(sys.exc_info()[0]))

    # Make sure that the schnaps' parameters are well-written
    if adv_parameters:
        adv_parameters = "," + adv_parameters

    if mode == 1:
        # 1 job per simulation (Ex. 5 scenarios with 100 simulations = 500 jobs)
        scenario = scenarios[task // (int(iterations) + 1)]
        iteration = str(task % (int(iterations) + 1))
        config_file = "parameters_" + iteration + ".xml"
        output_prefix = "Results/" + scenario + "/" + iteration + "_"
        proc = subprocess.Popen(["schnaps", "-c", config_file, "-d", scratch_path, "-s", scenario, "-p",
                                 "print.prefix=" + output_prefix + adv_parameters], stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()

        if stdout:
            print("Scenario " + scenario + " : " + stdout.decode('utf-8'))
        if stderr:
            print("Scenario " + scenario + " : " + stderr.decode('utf-8'))
            return

    elif mode == 2:
        # 1 job per iteration
        for scenario in scenarios:
            config_file = "parameters_" + str(task) + ".xml"
            output_prefix = "Results/" + scenario + "/" + str(task) + "_"
            proc = subprocess.Popen(["schnaps", "-c", config_file, "-d", scratch_path, "-s", scenario, "-p",
                                     "print.prefix=" + output_prefix, adv_parameters], stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
            stdout, stderr = proc.communicate()

            if stdout:
                print("Scenario " + scenario + " : " + stdout.decode('utf-8'))
            if stderr:
                print("Scenario " + scenario + " : " + stderr.decode('utf-8'))
                return

    elif mode == 3:
        # 1 job per scenario
        for i in range(0, int(iterations) + 1):
            scenario = scenarios[task]
            config_file = "parameters_" + str(i) + ".xml"
            output_prefix = "Results/" + scenario + "/" + str(i) + "_"
            proc = subprocess.Popen(["schnaps", "-c", config_file, "-d", scratch_path, "-s", scenario, "-p",
                                     "print.prefix=" + output_prefix + adv_parameters], stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
            stdout, stderr = proc.communicate()
            if stdout:
                print("Scenario " + scenario + " : " + stdout.decode('utf-8'))
            if stderr:
                print("Scenario " + scenario + " : " + stderr.decode('utf-8'))
                return

    elif mode == 4:
        def mode4():
            config_file = "parameters_" + str(j) + ".xml"
            output_prefix = "Results/" + scenario + "/" + str(j) + "_"
            proc = subprocess.Popen(["schnaps", "-c", config_file, "-d", scratch_path, "-s", scenario, "-p",
                                     "print.prefix=" + output_prefix + adv_parameters], stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)

            stdout, stderr = proc.communicate()
            if stdout:
                print("Scenario " + scenario + " : " + stdout.decode('utf-8'))
            if stderr:
                print("Scenario " + scenario + " : " + stderr.decode('utf-8'))

        # 1 job for all
        for scenario in scenarios:
            if "-" in iterations:
                it = iterations.split("-")
                for j in range(int(it[0]), int(it[-1]) + 1):
                    mode4()
            else:
                for j in range(0, int(iterations) + 1):
                    mode4()
        return

    else:
        print("Mode unsupported (" + str(mode) + "). See the help page for more information.")

    # Creates the metadata file in each directory of the project.
    # Do not modify the metadata's filename, unless you modify it also in the configuration file of Koksoak's website
    # (/media/safe/www/html/conf.php)
    try:
        create_meta_file(scratch_path)

    except:
        pass

    # Count how much time the job took
    end_job = datetime.datetime.now()
    delta = end_job - start_job

    job_time = ''
    for i in str(delta):
        if i == ".":
            break
        job_time += i
    print("Total time of job (HH:MM:SS) : ", job_time)


def main(args):
    if args[0] == "run":
        runner(args[1:])


if __name__ == '__main__':
    main(sys.argv[1:])
