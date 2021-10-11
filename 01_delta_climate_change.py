#!/usr/bin/env python


import os
import stat
import re
import fnmatch
# source:        https://pypi.org/project/cdo/
# documentation: https://code.mpimet.mpg.de/projects/cdo/embedded/cdo.pdf
from cdo import Cdo
from shutil import copyfile
from shutil import copy

#-----------------------------------------------------------------------------------------#

# Search for User_Input in all file.

# User_Input: update the source and dest paths. update the time ranges.

rcm_model=['CLMcom','CLMcom-ETH','CNRM','DHMZ','DMI','GERICS','ICTP','IPSL','IPSL-INERIS','KNMI','MOHC','MPI-CSC','RMIB-UGent','SMHI','UHOH']
sel_rcm_model=rcm_model[14]

print(type(sel_rcm_model))

if (sel_rcm_model=='CNRM' or sel_rcm_model=='ICTP' or sel_rcm_model=='MOHC' or sel_rcm_model=='RMIB-UGent' ):
   inputpath = '/p/largedata/slts/ramod_CORDEX_ESGF/p.data.regridded_to_EUR11/CORDEX/output/EUR-11/'+sel_rcm_model+'/'             # path suffix should be '/'
else:
   inputpath = '/p/largedata/slts/ramod_CORDEX_ESGF/o.data/CORDEX/output/EUR-11/'+sel_rcm_model+'/'             # path suffix should be '/'


#outputpath = '/p/scratch/cjibg35/eltahan1/Tahan_manuscript_2/ex_data/'+sel_rcm_model+'/' # path suffix should be '/'

outputpath='/p/scratch/cjibg35/eltahan1/Tahan_manuscript_2/ex_data/'

years_A = '1971/2000'  # 1971/2000 : for historical
years_B = '2021/2050'  # 2021/2050 : for RCP
years_C = '2070/2099'  # 2071/2100 : for RCP

var_names = ['pr', 'mrro','evspsbl']
#-----------------------------------------------------------------------------------------#

cdo = Cdo()

#----------------------------- copy_dirs_struc function ----------------------------------#

def copy_dirs_struc(inpath, outpath):
# This function copies a directory from inpath to outpath discarding files

    dirsfilsdict = {}
    for dirpath, _, filesnames in os.walk(inpath):
        structure = os.path.join(outpath, dirpath[len(inpath):])
        if not os.path.isdir(structure):
            os.mkdir(structure)
            os.chmod(structure, stat.S_IRWXU)    # give write access to owner
        else:
            print("Folder does already exist!")

        cmpstr = dirpath + '/'
        if (re.match(r".*\/mrro\/.*", cmpstr)):     # User_Input: specify var name to be analyzed
            dirsfilsdict[dirpath] = filesnames

    return (dirsfilsdict) # dict with each dir and its files names

#--------------------------------- clean_dict function ----------------------------------#

def clean_dict(indict):
# This function returns a dict with no empty val entries

    outdict = {}
    for key,val in indict.items():
        if len(val):
            outdict[key] = val

    return (outdict)

#--------------------------------- extract_yrs function ---------------------------------#

def extract_yrs(mergdirpathlist):
# This function extracts year ranges (selA, selB, selC) in historical and rcp
#       mergdirpathlist: is a list with paths of merged_data dirs

    for outputdir in mergdirpathlist:
        for file in os.listdir(outputdir):
            if(re.match(r".*\/historical.*",outputdir)):
                cdo.selyear(years_A, input = (outputdir + '/' + file), output = (outputdir + '/' + file[:-3] + '_selA.nc'))
            elif(re.match(r".*\/rcp.*",outputdir)):
                cdo.selyear(years_B, input = (outputdir + '/' + file), output = (outputdir + '/' + file[:-3] + '_selB.nc'))
                cdo.selyear(years_C, input = (outputdir + '/' + file), output = (outputdir + '/' + file[:-3] + '_selC.nc'))

#------------------------------ extract_info_str function -------------------------------#

def extract_info_str(outpath, instring):
    # TODO: Add check on length of instring before every step
    # outpath:  example: /media/sf_sharedfolder/paper_2/EUR-11_new/
    # instring: example: /media/sf_sharedfolder/paper_2/EUR-11_new/CLMcom/CNRM-CERFACS-CNRM-CM5/rcp45/r1xz/xx/v1/day/pr/merged_data

    modelname    = instring[len(outpath):instring.find('/', len(outpath))]
    # instring update. example: /CNRM-CERFACS-CNRM-CM5/rcp45/r1xz/xx/v1/day/pr/merged_data
    instring     = instring[instring.find('/', len(outpath))+1:]
    submodelname = instring[:instring.find('/')]
    # instring update. example: /rcp45/r1xz/xx/v1/day/pr/merged_data
    instring     = instring[instring.find('/')+1:]
    rcpname      = instring[:instring.find('/')]
    # instring update. example: /r1xz/xx/v1/day/pr/merged_data
    instring     = instring[instring.find('/')+1:]
    rcpversion   = instring[:instring.find('/')]
    # instring update. example: /xx/v1/day/pr/merged_data
    instring        = instring[instring.find('/')+1:]
    subsubmodelname = instring[:instring.find('/')]
    # instring update. example: /v1/day/pr/merged_data
    instring           = instring[instring.find('/')+1:]
    subsubmodelversion = instring[:instring.find('/')]
    # instring update. example: /day/pr/merged_data
    instring           = instring[instring.find('/')+1:]
    # instring update. example: /pr/merged_data
    instring           = instring[instring.find('/')+1:]
    varname            = instring[:instring.find('/')]

    return (modelname, submodelname, rcpname, rcpversion, subsubmodelname, subsubmodelversion, varname)    

#---------------------------------- gen_monsum function ---------------------------------#

def gen_monsum(infiles, inpath, outpath):
# This function makes monsum operation on .*day.nc files at inpath and writes them to outpath
#       infiles: is a dict{key="dirpath", val="files"}
#       inpath:  path of source files
#       outpath: path of dest. files

    for dirpath, filenames in infiles.items():
        for f in filenames:
            if (re.match(r".*\.nc",f)):         # assuming only needed files are present
                outputdir = os.path.join(outpath, dirpath[len(inpath):])
                outfile = re.sub('day','mon',f) # rename file from source
                cdo.monsum(input = (dirpath + '/' + f), output = (outputdir + '/' + outfile))
                print("cdo monsum done")
                print(outputdir + '/' + outfile)


#----------------------------- gen_mergetime_yrsum function -----------------------------#

def gen_mergetime_yrsum(infiles, inpath, outpath):
# This function makes merge time for all months and then executes year sum on the output to years.
#       infiles: is a dict{key="dirpath", val="files"}
#       inpath:  path of source files
#       outpath: path of dest. files
#       return mergdirpath: a list with paths of all merged_data dirs

    mergdirpath = []
    for dirpath in infiles:
        outputdir = os.path.join(outpath, dirpath[len(inpath):])
        structure = outputdir + '/merged_data'
        if not os.path.isdir(structure):
            os.mkdir(structure)
            mergdirpath.append(structure + '/')
            cdo.mergetime(input = (outputdir + '/*.nc'), output = (structure + '/months.nc'))
            cdo.yearsum(input = (structure + '/months.nc'), output = (structure + '/years.nc'))

    return (mergdirpath)

#--------------------------------- gen_timemean function --------------------------------#

def gen_timemean(mergdirpathlist):
# This function takes input years_selx.nc (the extracted time range file)
#       mergdirpathlist: is a list with paths of merged_data dirs
    for outputdir in mergdirpathlist:
        for file in os.listdir(outputdir):
            if(re.match(r"years_sel.*",file)):
                cdo.timmean(input = (outputdir + '/' + file), output = (outputdir + '/' + file[:-3] + '_timemean.nc'))

#--------------------------------- gen_yrmonavg function --------------------------------#

def gen_yrmonavg(mergdirpathlist):
# This function takes input months_selx.nc (the extracted time range file)
#       mergdirpathlist: is a list with paths of merged_data dirs
    for outputdir in mergdirpathlist:
        for file in os.listdir(outputdir):
            if(re.match(r"months_sel.*",file)):
                cdo.ymonavg(input = (outputdir + '/' + file), output = (outputdir + '/' + file[:-3] + '_yrmonavg.nc'))

#--------------------------------- gen_rcpdelta function --------------------------------#

def gen_rcpdelta(outpath):
# This function generates rcp delta between each rcp../ and its corresponding historical../ file
# First it creates lists with current architecture for hist and rcps. Then it loops on each rcpxx to find its
# match in the historical, once found, subtraction is done and delta is generated.

    histdirslist  = []
    rcp26dirslist = []
    rcp45dirslist = []
    rcp85dirslist = []

    for dirpath, _, _ in os.walk(outpath):
        if(re.match(r".*historical.*merged_data.*",dirpath)):
            histdirslist.append(dirpath)
        elif(re.match(r".*rcp26.*merged_data.*",dirpath)):
            rcp26dirslist.append(dirpath)
        elif(re.match(r".*rcp45.*merged_data.*",dirpath)):
            rcp45dirslist.append(dirpath)
        elif(re.match(r".*rcp85.*merged_data.*",dirpath)):
            rcp85dirslist.append(dirpath)

    # Example for input paths:
    # EUR-11_new/CLMcom/CNRM-CERFACS-CNRM-CM5/historical/r1xz/xx/v1/day/pr/merged_data/
    # EUR-11_new/CLMcom/CNRM-CERFACS-CNRM-CM5/rcp45/r1xz/xx/v1/day/pr/merged_data/

    for rcpdirpath in rcp26dirslist:
        rcpdirpathcmp = rcpdirpath.replace('rcp26', '')  # remove rcp26 word from path
        for histdirpath in histdirslist:
            histdirpathcmp = histdirpath.replace('historical', '')  # remove historical word from path
            if (rcpdirpathcmp == histdirpathcmp): # match found. rcp found corresponding hist
                modelname, submodelname, rcpname, rcpversion, subsubmodelname, subsubmodelversion, varname = extract_info_str(outpath, rcpdirpath)
                dirstruct = rcpdirpath + '/delta/'
                #deltadirpath.append(dirstruct) # list of delta folders
                if not os.path.isdir(dirstruct):
                    os.mkdir(dirstruct)
                infile1 = histdirpath + '/years_selA_timemean.nc'
                infile2 = rcpdirpath + '/years_selB_timemean.nc'
                cdo.sub(input = infile1 + ' ' + infile2, output = (dirstruct + modelname + '_' + submodelname + '_' + rcpname + '_' + rcpversion + '_' + subsubmodelname + '_' + subsubmodelversion + '_' + varname + '_yrdeltaBA.nc'))
                infile2 = rcpdirpath + '/years_selC_timemean.nc'
                cdo.sub(input = infile1 + ' ' + infile2, output = (dirstruct + modelname + '_' + submodelname + '_' + rcpname + '_' + rcpversion + '_' + subsubmodelname + '_' + subsubmodelversion + '_' + varname +  '_yrdeltaCA.nc'))
                # copy historical file
                copyfile((histdirpath + '/years_selA_timemean.nc'), (dirstruct + modelname + '_' + submodelname + '_' + rcpname + '_' + rcpversion + '_' + subsubmodelname + '_' + subsubmodelversion + '_' + varname + '_hist.nc'))
                               
                break # exit for loop since match for rcp is found in hist. no need to check remain hist list.

    for rcpdirpath in rcp45dirslist:
        rcpdirpathcmp = rcpdirpath.replace('rcp45', '')  # remove rcp45 word from path
        for histdirpath in histdirslist:
            histdirpathcmp = histdirpath.replace('historical', '')  # remove historical word from path
            if (rcpdirpathcmp == histdirpathcmp): # match found. rcp found corresponding hist
                modelname, submodelname, rcpname, rcpversion, subsubmodelname, subsubmodelversion, varname = extract_info_str(outpath, rcpdirpath)
                # do the subtraction
                dirstruct = rcpdirpath + '/delta/'
                #deltadirpath.append(dirstruct) # list of delta folders
                if not os.path.isdir(dirstruct):
                    os.mkdir(dirstruct)
                infile1 = histdirpath + '/years_selA_timemean.nc'
                infile2 = rcpdirpath + '/years_selB_timemean.nc'
                cdo.sub(input = infile1 + ' ' + infile2, output = (dirstruct + modelname + '_' + submodelname + '_' + rcpname + '_' + rcpversion + '_' + subsubmodelname + '_' + subsubmodelversion + '_' + varname + '_yrdeltaBA.nc'))
                infile2 = rcpdirpath + '/years_selC_timemean.nc'
                cdo.sub(input = infile1 + ' ' + infile2, output = (dirstruct + modelname + '_' + submodelname + '_' + rcpname + '_' + rcpversion + '_' + subsubmodelname + '_' + subsubmodelversion + '_' + varname + '_yrdeltaCA.nc'))
                # copy historical file
                copyfile((histdirpath + '/years_selA_timemean.nc'), (dirstruct + modelname + '_' + submodelname + '_' + rcpname + '_' + rcpversion + '_' + subsubmodelname + '_' + subsubmodelversion + '_' + varname + '_hist.nc'))
                
                break # exit for loop since match for rcp is found in hist. no need to check remain hist list.

    for rcpdirpath in rcp85dirslist:
        rcpdirpathcmp = rcpdirpath.replace('rcp85', '')  # remove rcp85 word from path
        for histdirpath in histdirslist:
            histdirpathcmp = histdirpath.replace('historical', '')  # remove historical word from path
            if (rcpdirpathcmp == histdirpathcmp): # match found. rcp found corresponding hist
                modelname, submodelname, rcpname, rcpversion, subsubmodelname, subsubmodelversion, varname = extract_info_str(outpath, rcpdirpath)
                # do the subtraction
                dirstruct = rcpdirpath + '/delta/'
                #deltadirpath.append(dirstruct) # list of delta folders
                if not os.path.isdir(dirstruct):
                    os.mkdir(dirstruct)
                infile1 = histdirpath + '/years_selA_timemean.nc'
                infile2 = rcpdirpath + '/years_selB_timemean.nc'
                cdo.sub(input = infile1 + ' ' + infile2, output = (dirstruct + modelname + '_' + submodelname + '_' + rcpname + '_' + rcpversion + '_' + subsubmodelname + '_' + subsubmodelversion + '_' + varname + '_yrdeltaBA.nc'))
                infile2 = rcpdirpath + '/years_selC_timemean.nc'
                cdo.sub(input = infile1 + ' ' + infile2, output = (dirstruct + modelname + '_' + submodelname + '_' + rcpname + '_' + rcpversion + '_' + subsubmodelname + '_' + subsubmodelversion + '_' + varname + '_yrdeltaCA.nc'))
                # copy historical file
                copyfile((histdirpath + '/years_selA_timemean.nc'), (dirstruct + modelname + '_' + submodelname + '_' + rcpname + '_' + rcpversion + '_' + subsubmodelname + '_' + subsubmodelversion + '_' + varname + '_hist.nc'))
                
                break # exit for loop since match for rcp is found in hist. no need to check remain hist list.

#------------------------------- gen_merg_delta function --------------------------------#

def gen_merg_delta(outpath):
# This function generates merged delta files for different time ranges for easier analysis.
# i.e: it merges all rcp26 from all models (and submodels) into one file.

    rcpdirs   = ['rcp26', 'rcp45', 'rcp85']
    deltadirs = ['yrdeltaBA', 'yrdeltaCA', 'hist']

    # create output dirs
    if not os.path.isdir(outpath + 'merged_deltas/'):
        for rcpdir in rcpdirs:
            for deltadir in deltadirs:
                for vardir in var_names:
                    os.makedirs(outpath + 'merged_deltas/' + rcpdir + '/' + deltadir + '/' + vardir)

    # collect delta files into one location
    for dirpath, _, files in os.walk(outpath):
        # make sure we are not in the output folder
        if(not(re.match(r".*merged_deltas/", dirpath))):

            if(re.match(r".*rcp26.*delta.*", dirpath)):
                for filename in files:
                    for deltadir in deltadirs:
                        if fnmatch.fnmatch(filename, '*_' + deltadir + '.nc'):
                            copy((dirpath + '/' + filename), (outpath + 'merged_deltas/rcp26/' + deltadir))

            elif(re.match(r".*rcp45.*delta.*", dirpath)):
                for filename in files:
                    for deltadir in deltadirs:
                        if fnmatch.fnmatch(filename, '*_' + deltadir + '.nc'):
                            copy((dirpath + '/' + filename), (outpath + 'merged_deltas/rcp45/' + deltadir))

            elif(re.match(r".*rcp85.*delta.*", dirpath)):
                for filename in files:
                    for deltadir in deltadirs:
                        if fnmatch.fnmatch(filename, '*_' + deltadir + '.nc'):
                            copy((dirpath + '/' + filename), (outpath + 'merged_deltas/rcp85/' + deltadir))

    # move variable delta files to corresponding folder
    for rcpdir in rcpdirs:
        for deltadir in deltadirs:
            for var in var_names:
                for _, _, files in os.walk(outputpath + 'merged_deltas/' + rcpdir + '/' + deltadir):
                    for filename in files:
                        if fnmatch.fnmatch(filename, '*' + var + '*'):
                            copy(outputpath + 'merged_deltas/' + rcpdir + '/' + deltadir + '/' + filename, outputpath + 'merged_deltas/' + rcpdir + '/' + deltadir + '/' + var)
                            os.remove(outputpath + 'merged_deltas/' + rcpdir + '/' + deltadir + '/' + filename)
                    break # break the loop. we only need files at first dir not recursivly.

    for rcpdir in rcpdirs:
        for deltadir in deltadirs:
            for var in var_names:
                if (len(os.listdir(outpath + 'merged_deltas/' + rcpdir + '/' + deltadir + '/' + var)) != 0):
                    cdo.merge(input = (outpath + 'merged_deltas/' + rcpdir + '/' + deltadir + '/' + var + '/*.nc'), output = (outpath + 'merged_deltas/' + rcpdir + '/merged_' + deltadir + '_' + var + '.nc'))
                    
#----------------------------------- MAIN function ---------------------------------------#

def main():
    # Caveat: Sequence must be respected
#    dirsfilesdict = clean_dict(copy_dirs_struc(inputpath, outputpath))
#    print("Archeticture copied sucessfully ")
#    gen_monsum(dirsfilesdict, inputpath, outputpath)
#    mergdirpathlist = gen_mergetime_yrsum(dirsfilesdict, inputpath, outputpath)
#    print("I will start extracting selected years")
#    extract_yrs(mergdirpathlist)
#    print("I will start calculating multi year average")
#    gen_yrmonavg(mergdirpathlist)
#    print("I will start time mean")
#    gen_timemean(mergdirpathlist)
#    print("---I generated time mean and waiting for calculating delta----")
    print("I will start calculating delta")
    gen_rcpdelta(outputpath)
    print("I will start merge delta")
    gen_merg_delta(outputpath)

#-----------------------------------------------------------------------------------------#

if __name__ == "__main__":
    main()
