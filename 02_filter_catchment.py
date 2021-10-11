#!/usr/bin/env python

import os
import numpy as np
import re
from shutil import copy
import fnmatch
from cdo import Cdo
from netcdflib import *         #private lib
from strmanplib import *        #private lib
import pickle

#-----------------------------------------------------------------------------------------#

inputpath = '/p/scratch/cjibg35/eltahan1/Tahan_manuscript_2/heat_ex_data/'        # path suffix should be '/'
varnames = ['pr','evspsbl','mrro']
#varnames = ['mrro']
Basins_Masks='/p/project/cslts/eltahan1/TAHAN_SCRIPTS/EXTRACTED_3D_MASK_ARRAY_FOR_BASINS_EURO_11_CORDEX'

## input indicies to generate mask from it. These are the indicies for points to be taken. Others will be ignored.
#indexlist = [
#[[2,3],[1,1],[9,5],[9,19]], # Filter one
#[[1,3],[2,1],[9,5],[9,19]] #Filter two
#]


#-----------------------------------------------------------------------------------------#

cdo = Cdo()

#-------------------- Construct indexlist which contains catchment filter ----------------#

def Construct_catchmnet_filters(Basins_Masks):

    # open a file, where you stored the pickled data
    Data_lons_lat_mask= open(Basins_Masks,'rb')
    
    # dump information to that file
#    data = pickle.load(Data_lons_lat_mask,encoding='latin1')
    data = pickle.load(Data_lons_lat_mask)

    # Store the lons,lat and MASk
    lons=data[0]
    lats=data[1]
    Mask=data[2]

    # close the file
    Data_lons_lat_mask.close()

    #BASIN_NAME=["Guadalquivir (56408.3)","Guadiana (66927.7)","Tagus (70719.8) ","Douro (97145.0)","Ebro (84619.2)","Garonne (55858.0)","Rhone (96015.5)","Po (73276.3)","Seine (73090.1)","Rhine (163028.7)","Loire (116624.11)","Maas (32733.1)","Weser (44758.6)","Elbe (138369.7)","Oder (118760.9)","Vistuala (192634.7)","Danube (786433.7)","Dniester (73194.9)","Dnieper (509776.0)","Neman (95662.6)"]

    index_i=[]
    index_j=[]
    mask_value=[]
    indexlist=[]

    for idx_basin in range(0,20):
        indexlist_filter=[]
        for mm in range(412):
    #for mm in range(len(Mask[1])): #412
       # for nn in range(len(Mask[1][1,:])): #424
            for nn in range(424):
                if Mask[idx_basin][mm][nn]==1:
                   index_i.append(mm)
                   index_j.append(nn)
                   mask_value.append(Mask[idx_basin][mm,nn])
                  # print([mm,nn])
                   indexlist_filter.append([mm,nn])
        indexlist.append(indexlist_filter)

    print("------ This is number of catchment----")
    print(len(indexlist))
    print(len(indexlist[0]))

    return indexlist



#---------------------------------- create_mask function ---------------------------------#

def create_mask(inindexlist, masksize):
# This function creates mask
#   inindexlist: the indices for the values to be TAKEN.
#   masksize:    size of mask
    x, y = masksize[1], masksize[2]
    arrmask = np.ones((x,y))

    for index in inindexlist:
        arrmask[index[0]][index[1]] = 0         # set mask values with input indices

    return (arrmask)

#---------------------------------- extract_catch function ---------------------------------#

def extract_catch(inmask, indataset):
# This function extracts catchment from dataset.
#   inmask: This is mask value where 0 values are taken and 1 values are ignored
#   indataset: This is the dataset in 3D format, where z index indicates months
    catchmean = []
    datasetsize = indataset.shape               # z,x,y

    for zindex in range(datasetsize[0]):
        dataset2d = indataset[zindex][:][:]          # process one month at a time
        catchment = np.ma.array(dataset2d, mask=inmask)
        catchmean.append(np.nanmean(catchment))
        # catchmean.append(catchment.mean())

    return (catchmean)

#---------------------------------- filter_months function ---------------------------------#

def filter_months(inindexlist, inputfile, invarname):
# This function masks input files and outputs the mean value for each one.
    meanvallist = []    # Each list entry represents one filter applied to a months_selx.nc
                        
    dataset, datasetsize = extract_dataset(inputfile, invarname)
    for i in range(len(inindexlist)):
        mask = create_mask(inindexlist[i], datasetsize)
        meanvallist.append(extract_catch(mask, dataset))
    
    meanvalarr = np.array(meanvallist)      # convert list to array to use later with netcdf format

    return (meanvalarr)

#---------------------------------- loop_and_filter function -------------------------------#

def loop_and_filter(inpath, invarname,indexlist):
# This function loops on all months_selX.nc files and generates filtered files based on given mask.
    for dirpath, _, files in os.walk(inpath):
        if fnmatch.fnmatch(dirpath, '*' + invarname + '*'):
           for filename in files:
               if (re.match(r"months_sel..nc",filename)):
                  meanvalarr = filter_months(indexlist, dirpath + '/' + filename, invarname)
                  save_netcdf4_file(invarname, dirpath + '/filtered_' + filename, meanvalarr)

#--------------------------------- collect_and_merge function ------------------------------#

def collect_and_merge(inpath):
# This function collects all filtered_months_selX.nc from historical and rcps and copies them to output folder
# taking into consideration their rename to match model-submodel .. notation. Then performs merge on them.
# output: there's a merged file for each months_selx.nc per variable.
    rcpdirs   = ['rcp26', 'rcp45', 'rcp85']

    if not os.path.isdir(inpath + 'merged_filters/'):
        # rcps
        for rcpdir in rcpdirs:
            for vardir in varnames:
                os.makedirs(inpath + 'merged_filters/' + rcpdir + '/' + vardir)
        # historical
        for vardir in varnames:
                    os.makedirs(inpath + 'merged_filters/hist/' + vardir)

    # collect filtered files into one location
    for dirpath, _, files in os.walk(inpath):
        # make sure we are not in the output folders
        if( (not(re.match(r".*merged_deltas/", dirpath))) and (not(re.match(r".*merged_filters/", dirpath))) ):

            if(re.match(r".*historical.*merged_data", dirpath)):
                for varname in varnames:
                    for filename in files:  # filtered_months_selA.nc
                        if fnmatch.fnmatch(filename, 'filtered_months_selA.nc'):
                            if fnmatch.fnmatch(dirpath, '*' + varname + '*'):  # which variable are we currently in
                                modelname, submodelname, rcpname, rcpversion, subsubmodelname, subsubmodelversion, _ = extract_info_str(inpath, dirpath)
                                copy((dirpath + '/' + filename), (inpath + 'merged_filters/hist/' + varname + '/' + modelname + '_' + submodelname + '_' + rcpname + '_' + rcpversion + '_' + subsubmodelname + '_' + subsubmodelversion + '_' + varname + '_' + filename))

            for rcpdir in rcpdirs:
                if fnmatch.fnmatch(dirpath, '*' + rcpdir + '*merged_data'):
                    for varname in varnames:
                        for filename in files:  # filtered_months_selB.nc and filtered_months_selC.nc
                            if fnmatch.fnmatch(filename, 'filtered_months_sel' + '*'):
                                if fnmatch.fnmatch(dirpath, '*' + varname + '*'):
                                    modelname, submodelname, rcpname, rcpversion, subsubmodelname, subsubmodelversion, _ = extract_info_str(inpath, dirpath)
                                    copy((dirpath + '/' + filename), (inpath + 'merged_filters/' + rcpdir + '/' + varname + '/' + modelname + '_' + submodelname + '_' + rcpname + '_' + rcpversion + '_' + subsubmodelname + '_' + subsubmodelversion + '_' + varname + '_' + filename))

    # Go to output folder merged_filters and get its path
    for dirpath, dirnames, files in os.walk(inpath):
        for dirname in dirnames:
            if fnmatch.fnmatch(dirname, 'merged_filters'): # if inside merged_filters dir
                mergedfiltersdir = dirpath + '/' + dirname
                break # if found no need to complete remaining os.walk
        break
    
    # historical
    for var in varnames:
        if (len(os.listdir(mergedfiltersdir + '/hist/' + var)) != 0):
            cdo.merge(input = (mergedfiltersdir + '/hist/' + var + '/*A.nc'), output = (mergedfiltersdir + '/hist/merged_filtered_selA_' + var + '.nc'))

    # rcps
    for rcpdir in rcpdirs:
        for var in varnames:
            if (len(os.listdir(mergedfiltersdir + '/' + rcpdir + '/' + var)) != 0):
                cdo.merge(input = (mergedfiltersdir + '/' + rcpdir + '/' + var + '/*B.nc'), output = (mergedfiltersdir + '/' + rcpdir + '/merged_filtered_selB_' + var + '.nc'))
                cdo.merge(input = (mergedfiltersdir + '/' + rcpdir + '/' + var + '/*C.nc'), output = (mergedfiltersdir + '/' + rcpdir + '/merged_filtered_selC_' + var + '.nc'))


#----------------------------------- MAIN function ---------------------------------------#

def main():
    for varname in varnames:
        print("------ This is variable name :" + varname)
    #    indexlist=Construct_catchmnet_filters(Basins_Masks)
        print("--- finish filter extraction------")
    #    loop_and_filter(inputpath, varname,indexlist)
        print("----- finish load and filter-----")
        collect_and_merge(inputpath)
        print("----- collect and merge-----")
#-----------------------------------------------------------------------------------------#

if __name__ == "__main__":
    main()

#-----------------------------------------------------------------------------------------#
