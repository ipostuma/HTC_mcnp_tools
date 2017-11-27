#!/usr/bin/python
import argparse
import os
import fileinput
import re

######################################
#    HTCondor Managing Functions     #
######################################

# Function that generates the bash input file
# that HTCondor executes on a certain node.
def BashFileGen(BASH_FILE,MCNP_CODE,MCNP_DATA,INPUT):
    text_file = open(BASH_FILE, "w")
    text_file.write("#!/bin/sh \n#PATH for MCNP executables\n")
    text_file.write("echo \"PATH for MCNP executables - set to :%s\"\n" % MCNP_CODE)
    text_file.write("export PATH=$PATH\":%s\"\n" % MCNP_CODE)
    text_file.write("\n# Increase the stacksize \nulimit -s unlimited\n")
    text_file.write("\n# DATAPATH for MCNP cross-section data \necho \"DATAPATH for MCNP cross-section data - set to %s\"\n" % MCNP_DATA)
    text_file.write("export DATAPATH=\"%s\"\n" % MCNP_DATA)
    text_file.write("\nmcnp6 n=%s\n" % INPUT)
    text_file.close()

# Function that generates the HTCondor Submission
# file for MCNP execution
def HTCsubFileGen(SUB_FILE,BASH_FILE, INPUT, LOG="HTC_mcnp.log", OUT="HTC_mcnp.out"):
    text_file = open(SUB_FILE, "w")
    text_file.write("Universe = vanilla\nExecutable = %s\nInput = %s\nOutput = %s\nLog = %s\nshould_transfer_files = IF_NEEDED\nwhen_to_transfer_output = ON_EXIT\nQueue\n" % (BASH_FILE,INPUT,OUT,LOG))
    text_file.close()

# Function that generates a new mcnp input file
def newMCNPinput(INPUT,Fnumber,NPS=1e6):
    NewFileName = INPUT+"%03d"%Fnumber
    fs          = open(INPUT,"r")
    ft          = open(NewFileName,"w")
    nps_bool    = True
    dbcn_bool   = True
    prdmp_bool  = True
    for line in fs:
        if re.search('nps', line, re.IGNORECASE):
            new_line = "nps %1.2e $ HTC_mcnp mod\n"%(NPS)
            ft.write(new_line)
            nps_bool  = False
        elif re.search('dbcn', line, re.IGNORECASE):
            new_line = "dbcn %d $ HTC_mcnp mod\n"%(Fnumber*2+1001)
            ft.write(new_line)
            dbcn_bool = False
        elif re.search('prdmp', line, re.IGNORECASE):
            new_line = "prdmp j j 1 $ HTC_mcnp mod\n"
            ft.write(new_line)
            prdmp_bool = False
        else:
            ft.write(line)
    if nps_bool:
        new_line = "nps %1.2e $ HTC_mcnp mod\n"%(NPS)
        ft.write(new_line)
    if dbcn_bool:
        new_line = "dbcn %d $ HTC_mcnp mod\n"%(Fnumber*2+1001)
        ft.write(new_line)
    if prdmp_bool:
        new_line = "prdmp j j 1 $ HTC_mcnp mod\n"
        ft.write(new_line)
    ft.close()
    fs.close()
    return NewFileName

# Function that splits the mcnp calculation in multiple
# files before Submission
def SplitMCNP(MCNP_CODE,MCNP_DATA,INPUT,CORE,NPS):
    HTC_files = []
    for i in range(CORE):
        INPUT_i        = newMCNPinput(INPUT,i,NPS)
        HTC_mcnp_sh_i  = "HTC_mcnp_%03d.sh"%(i)
        HTC_mcnp_sub_i = "HTC_mcnp_%03d.sub"%(i)
        HTC_mcnp_log_i = "HTC_mcnp_%03d.log"%(i)
        HTC_mcnp_out_i = "HTC_mcnp_%03d.out"%(i)
        BashFileGen(HTC_mcnp_sh_i,MCNP_CODE,MCNP_DATA,INPUT_i)
        HTCsubFileGen(HTC_mcnp_sub_i,HTC_mcnp_sh_i, INPUT_i, OUT=HTC_mcnp_out_i, LOG=HTC_mcnp_log_i )
        HTC_files.append(HTC_mcnp_sub_i)
    return HTC_files

# Function that generates the bash file to launch
# mcnp calculations of the cluster
def SubmitJob(SUB_FILES):
    text_file = open("HTC_submit.sh","w")
    text_file.write("#!/bin/bash\n")
    for subfile in SUB_FILES:
        text_file.write("condor_submit " + subfile + " &\n")
    text_file.close()
    # start execution
    os.system("bash HTC_submit.sh")

######################################
#     Argument check Functions       #
######################################

# Function that checks if the passed argument is
# a positive integer
def check_positive(value):
    ivalue = int(value)
    if ivalue <= 0:
         raise argparse.ArgumentTypeError("%s is not a positive int value" % value)
    return ivalue

# Function that checks if PATH_TO_MCNP is correct
def check_MCNP(value):
    PATH_TO_MCNP = value
    MCNP_CODE = os.path.join(PATH_TO_MCNP,"MCNP_CODE/bin")
    MCNP_DATA = os.path.join(PATH_TO_MCNP,"MCNP_DATA")
    # Check if MCNP exits
    if(os.path.isdir(MCNP_CODE)==False):
        raise argparse.ArgumentTypeError("%s does not contain MCNP."%(MCNP_CODE))
    if(os.path.isdir(MCNP_DATA)==False):
        raise argparse.ArgumentTypeError("%s does not contain MCNP."%(MCNP_DATA))
    return PATH_TO_MCNP

# Function that checks if input file exists
def check_INPUT(value):
    INPUT = value
    # Check if input file exists
    INPUT_FILE = os.path.join(os.getcwd(),INPUT)
    if(os.path.isfile(INPUT_FILE)==False):
        raise argparse.ArgumentTypeError("%s file not found"%(INPUT_FILE))
    return INPUT

######################################
#       Argument definitions         #
######################################

parser = argparse.ArgumentParser(description='Create multiple jobs to launch on an HTCondor infrastructure')
parser.add_argument('PATH_TO_MCNP', type=check_MCNP , help='Path to MCNP directory to set env variables for execution.')
parser.add_argument('INPUT', type=check_INPUT, help='MCNP intput file.')
parser.add_argument('CORE', type=check_positive, help='Number of cores needed for the simulation.')
parser.add_argument('NPS', type=check_positive, help='Nuber of particles on each core.')
parser.add_argument('-k','--KCODE', action='store_true',
                    help='This parameter is needed to activate KCODE calculations -- STILL NOT ACTIVE.')
parser.add_argument('-s','--HTCondor_submit', action='store_true',
                    help='Launch HTCondor batch-system after splitting files.')
parser.add_argument('-m','--HTCondor_merge', action='store_true',
                    help='Once the simulation has finished, you may use this parameter to create a merget MCTAL.')

######################################
#         Program execution          #
######################################

if __name__ == '__main__':
    args         = parser.parse_args()
    PATH_TO_MCNP = args.PATH_TO_MCNP
    MCNP_CODE    = os.path.join(PATH_TO_MCNP,"MCNP_CODE/bin")
    MCNP_DATA    = os.path.join(PATH_TO_MCNP,"MCNP_DATA")
    INPUT        = args.INPUT
    CORE         = args.CORE
    NPS          = args.NPS

    if args.HTCondor_merge == False :
        HTC_files = SplitMCNP(MCNP_CODE,MCNP_DATA,INPUT,CORE,NPS)
        if(args.HTCondor_submit):
            SubmitJob(HTC_files)
    else: os.system("%s %s???m"%(os.path.join(MCNP_CODE,"merge_mctal"),INPUT))
