#! /usr/bin/python3

"""
Created on Wed Feb 1 09:52:59 2017

@author: Jean-Alexandre Beaumont

This script let a user change the values of the configuration files produced by LSD-GUI.

It can be useful when we want to have univariate simulation but want to tie multiple values together.
"""

import sys
import subprocess
import re



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
    import mmap
    target_folder = args[0]
    config_file = args[1]

    config_array = extract_configs(config_file)

    for param_pair in config_array:
        parameter, value = param_pair.split(":")
        grep_args = "grep -rl 'Entry label=\"" + parameter + "\"' " + target_folder
        xml_parameter = "Entry label=\"" + parameter + "\""



        # Do a grep to find the list of file to modify
        subproc_grep = subprocess.Popen(["grep", "-rl", xml_parameter, target_folder],
                                        shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        file_names_raw, null = subproc_grep.communicate()
        file_names_str = file_names_raw.decode().split("\n")[:-1]

        for file_path in file_names_str:
            if not re.search('parameters\.xml', file_path):
                file = open(file_path)
                file_str = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
                index = file_str.find(xml_parameter.encode('utf-8'))
                substr = file_str[index:index+120]
                pattern = re.compile(b'="\d+\.?\d*"')
                regex_span_found = pattern.search(substr).span()
                length_span = regex_span_found[1]-regex_span_found[0]
                number_index = regex_span_found[0] + index
                file.close()
                file = open(file_path, "r+b")

                file.seek(number_index)
                # print(file.read(40))
                file.seek(number_index + 2)
                str2 = b'"/>\n'
                if len(value) < length_span:
                    file.write(value.encode() + str2 + b' ' * (length_span - len(value)))
                else:
                    file.write(value.encode() + str2)
                file.seek(number_index)
                # print(file.read(40))
                file.close()



def main(args):
    if not args or args[0] in ["-h", "--help", "-help", "help"]:
        show_help_general()
        sys.exit(0)

    else:
        uni_mod(args)
        sys.exit(0)

main(sys.argv[1:])
