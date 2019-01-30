#! /usr/bin/env python3
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

import sys
import os
from fetcher import show_help_fetcher, fetcher
from launch import show_help_launcher, Launcher
from run import runner

launcher_script = "launch"
fetcher_script = "fetch"
runner_script = "run"
run_sim_script = os.path.basename(__file__)
koksoak_script_location = "/home/lsdadmin/scripts/"
meta_file = ".meta"


def show_help_runner():
    """
    Shows help page for the runner script.
    Call it with arguments : run --help|-h
    """

    print("\n")
    print("Possible arguments :\n")
    print("     -p, --project <name>               Project's name")
    print("     -m, --mode <1-4>                   1 : One job per file. 2 : One job per iteration. "
          "3 : One job per scenario. 4 : One job for all.")
    print("     -t, --task                         Index of the jobs' array. Represents the Xe job.")
    print("     -r, --rap-id                       Rap id.")
    print("     -s, --scenario                     List of scenarios splitted by the argument.")
    print("     -i, --iterations                   Number of iterations")
    print("     -d, --scratch-path                 Scratch storage")
    print("     [-o, --options <option>]           Options for SCHNAPS. For more information : "
          "https://github.com/audurand/schnaps/wiki/Usage.")


def show_help_general():
    """
    Shows the general help page.
    It contains the help pages of all 3 scripts.
    Call it without passing parameter, or with : -h|--help
    """
    print("Possible arguments :\n")

    print("     [" + launcher_script + ", " + fetcher_script + ", " + runner_script +
          "]               Which mode to use. Must be the first argument. If omitted = " + launcher_script)
    print("\n" + runner_script + " :")
    show_help_runner()
    print("\n" + fetcher_script + " :")
    show_help_fetcher()
    print("\n" + launcher_script + " :")
    show_help_launcher()


def main(args):
    if not args or args[0] in ["-h", "--help", "-help", "help"]:
        show_help_general()
        sys.exit(0)
    if args[0] == launcher_script:
        Launcher(args[1:])
    elif args[0] == fetcher_script:
        fetcher(args[1:])
    elif args[0] == runner_script:
        runner(args[1:])
    else:
        Launcher(args)


main(sys.argv[1:])
