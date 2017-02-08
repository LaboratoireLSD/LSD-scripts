
"""
Created on Wed Feb 1 09:52:59 2017

@author: Jean-Alexandre Beaumont

This script let a user change the values of the configuration files produced by LSD-GUI.

It can be useful when we want to have univariate simulation but want to tie multiple values together.
"""

import sys
import subprocess



def show_help_general():
    print("Arguments :\n")

    print("First argument : folder in which to change the parameters_XX.xml\n")
    print("Second argument : config file which have the list of key-value to change. One key-value per line.\n")


def extract_configs(config_file):
    result = []
    with open(config_file) as file:
        for line in file:
            result.append(line.strip())
    return result



def uni_mod(args):
    target_folder = args[0]
    config_file = args[1]

    config_array = extract_configs(config_file)

    for param_pair in config_array:
        parameter, value = param_pair.split(":")
        grep_args = "grep -rl 'Entry label=\"" + parameter + "\"' " + target_folder



        # Do a grep to find the list of file to modify
        subproc_grep = subprocess.Popen(["grep", "-rl", "Entry label=\"" + parameter + "\"", target_folder],
                                        shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        file_name_raw, null = subproc_grep.communicate()
        file_name_str = []
        file_name_1 = file_name_raw.decode().split("\n")

        print(file_name_str)


def main(args):
    if not args or args[0] in ["-h", "--help", "-help", "help"]:
        show_help_general()
        sys.exit(0)

    else:
        uni_mod(args)
        sys.exit(0)

main(sys.argv[1:])
