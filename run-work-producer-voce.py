#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Tommaso Stella <tommaso.stella@zalf.de>
# Michael Berg-Mohnicke <michael.berg@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# This file has been created at the Institute of
# Landscape Systems Analysis at the ZALF.
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

#import copy
import csv
import json
#import os
import sqlite3
#import types

import sys
#####sys.path.insert(0, "C:\\Users\\berg.ZALF-AD\\GitHub\\monica\\project-files\\Win32\\Release")
####sys.path.insert(0, "C:\\Users\\berg.ZALF-AD\\GitHub\\monica\\src\\python")
#print sys.path

import time
#from datetime import date, datetime, timedelta
#from StringIO import StringIO

import zmq

#sys.path.append('C:/Users/berg.ZALF-AD/GitHub/util/soil')
#from soil_conversion import *
#import monica_python
import monica_io
import soil_io
import ascii_io
from datetime import date, timedelta
import copy

#print "pyzmq version: ", zmq.pyzmq_version()
#print "sys.path: ", sys.path
#print "sys.version: ", sys.version
USER = "berg"

PATHS = {
    "stella": {
        "INCLUDE_FILE_BASE_PATH": "C:/Users/stella/Documents/GitHub/monica-parameters/",
    },
    "berg": {
        "INCLUDE_FILE_BASE_PATH": "C:/Users/berg.ZALF-AD/GitHub/monica-parameters/"
    }
}

PATH_TO_CLIMATE_DATA_DIR = "/archiv-daten/md/data/climate/isimip/csvs/earth/"
LOCAL_PATH_TO_CLIMATE_DATA_DIR = "d:/climate/isimip/csvs/earth/"
#PATH_TO_CLIMATE_DATA_DIR ="/archiv-daten/md/projects/sustag/MACSUR_WP3_NRW_1x1/" #"Z:/projects/sustag/MACSUR_WP3_NRW_1x1/"

LOCAL_RUN = False #True
RUN_ON_CLUSTER = 1

def main():
    "main function"

    config = {
        "port": 6666,
    }
    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            k,v = arg.split("=")
            if k in config:
                config[k] = int(v) 

    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    if LOCAL_RUN:
        socket.connect("tcp://localhost:" + str(config["port"]))
    else:
        socket.connect("tcp://cluster" + str(RUN_ON_CLUSTER) + ":" + str(config["port"]))

    soil_db_con = sqlite3.connect("soil.sqlite")

    with open("sim-voce.json") as _:
        sim = json.load(_)

    with open("site-voce.json") as _:
        site = json.load(_)

    with open("crop-voce.json") as _:
        crop = json.load(_)

    with open("rotations-voce.json") as _:
        rotations = json.load(_)


    sim["include-file-base-path"] = PATHS[USER]["INCLUDE_FILE_BASE_PATH"]

    def read_general_metadata(path_to_file):
        "read metadata file"
        with open(path_to_file) as file_:
            data = {}
            reader = csv.reader(file_, delimiter="\t")
            reader.next()
            for row in reader:
                if int(row[1]) != 1:
                    continue
                data[(int(row[2]), int(row[3]))] = {
                    "subpath-climate.csv": row[9],
                    "latitude": float(row[13]),
                    "elevation": float(row[14])
                }
            return data

    general_metadata = read_general_metadata("NRW_General_Metadata.csv")


    def load_mapping(row_offset=0, col_offset=0):
        to_climate_index = {}
        with(open("working_resolution_to_climate_lat_lon_indices.json")) as _:
            l = json.load(_)
            for i in xrange(0, len(l), 2):
                cell = (row_offset + l[i][0], col_offset + l[i][1])
                to_climate_index[cell] = tuple(l[i+1])
            return to_climate_index

    def read_orgN_kreise(path_to_file):
        "read organic N info for kreise"
        with open(path_to_file) as file_:
            data = {}
            reader = csv.reader(file_, delimiter=",")
            reader.next()
            reader.next()
            for row in reader:
                for kreis_code in row[1].split("|"):
                    if kreis_code != "":
                        data[int(kreis_code)] = float(row[8])
        return data

    orgN_kreise = read_orgN_kreise("NRW_orgN_balance.csv")
    #print orgN_kreise.keys()

    def update_soil_crop_dates(row, col):
        "in place update the env"
        #startDate = date(1980, 1, 1)# + timedelta(days = p["sowing-doy"])
        #sim["start-date"] = startDate.isoformat()
        #sim["end-date"] = date(2010, 12, 31).isoformat()
        #sim["debug?"] = True

        site["Latitude"] = general_metadata[(row, col)]["latitude"]
        site["HeightNN"] = [general_metadata[(row, col)]["elevation"], "m"]
        site["SiteParameters"]["SoilProfileParameters"] = soil_io.soil_parameters(soil_db_con, soil_ids[(row, col)])
        for layer in site["SiteParameters"]["SoilProfileParameters"]:
            layer["SoilBulkDensity"][0] = max(layer["SoilBulkDensity"][0], 600)
    
    def read_ascii_grid(path_to_file, include_no_data=False, row_offset=0, col_offset=0):
        "read an ascii grid into a map, without the no-data values"
        def int_or_float(s):
            try:
                return int(s)
            except ValueError:
                return float(s)

        with open(path_to_file) as file_:
            data = {}
            #skip the header (first 6 lines)
            for _ in range(0, 6):
                file_.next()
            row = -1
            for line in file_:
                row += 1
                col = -1
                for col_str in line.strip().split(" "):
                    col += 1
                    if not include_no_data and int_or_float(col_str) == -9999:
                        continue
                    data[(row_offset+row, col_offset+col)] = int_or_float(col_str)

            return data

    #offset is used to match info in general metadata and soil database
    soil_ids = read_ascii_grid("soil-profile-id_nrw_gk3.asc", row_offset=282)
    bkr_ids = read_ascii_grid("bkr_nrw_gk3.asc", row_offset=282)
    lu_ids = read_ascii_grid("lu_resampled.asc", row_offset=282)
    kreise_ids = read_ascii_grid("kreise_matrix.asc", row_offset=282)
    meteo_ids = load_mapping(row_offset=282)

    #counter = 0
    #for k, v in bkr_ids.iteritems():
    #    if v == 134:
    #        if k in lu_ids:
    #            counter += 1
    #print counter

    i = 0
    start_send = time.clock()

    def calculate_orgfert_amount(N_applied, fert_type):
        "convert N applied in amount of fresh org fert"
        AOM_DryMatterContent = fert_type["AOM_DryMatterContent"][0]
        AOM_NH4Content = fert_type["AOM_NH4Content"][0]
        AOM_NO3Content = fert_type["AOM_NO3Content"][0]
        CN_Ratio_AOM_Fast = fert_type["CN_Ratio_AOM_Fast"][0]
        CN_Ratio_AOM_Slow = fert_type["CN_Ratio_AOM_Slow"][0]
        PartAOM_to_AOM_Fast = fert_type["PartAOM_to_AOM_Fast"][0]
        PartAOM_to_AOM_Slow = fert_type["PartAOM_to_AOM_Slow"][0]
        AOM_to_C = 0.45

        AOM_fast_factor = 1/(CN_Ratio_AOM_Fast/(AOM_to_C * PartAOM_to_AOM_Fast))
        AOM_slow_factor = 1/(CN_Ratio_AOM_Slow/(AOM_to_C * PartAOM_to_AOM_Slow))

        conversion_coeff = AOM_NH4Content + AOM_NO3Content + AOM_fast_factor + AOM_slow_factor

        AOM_dry = N_applied / conversion_coeff
        AOM_fresh = AOM_dry / AOM_DryMatterContent

        return AOM_fresh

    simulated_cells = 0
    no_kreis = 0
    for (row, col), gmd in general_metadata.iteritems():

        if (row, col) in soil_ids and (row, col) in bkr_ids and (row, col) in lu_ids:

            bkr_id = bkr_ids[(row, col)]
            #if bkr_id != 143:
            #    continue
            soil_id = soil_ids[(row, col)]
            meteo_id = meteo_ids[(row, col)]
            if (row, col) in kreise_ids:
                kreis_id = kreise_ids[(row, col)]
            else:
                no_kreis += 1
                print "-----------------------------------------------------"
                print "kreis not found for calculation of organic N" #TODO find out a solution
                print "-----------------------------------------------------"

            simulated_cells += 1
            update_soil_crop_dates(row, col)

            for rot_id, rotation in rotations.iteritems():

                crop["cropRotation"] = rotation

                env = monica_io.create_env_json_from_json_config({
                    "crop": crop,
                    "site": site,
                    "sim": sim,
                    "climate": ""
                })

                #assign amount of organic fertilizer
                #for cultivation_method in env["cropRotation"]:
                #    for workstep in cultivation_method["worksteps"]:
                #        if workstep["type"] == "OrganicFertilization":
                #            workstep["amount"] = calculate_orgfert_amount(orgN_kreise[kreis_id], workstep["parameters"])

                #climate is read by the server
                env["csvViaHeaderOptions"] = sim["climate.csv-options"]
                if LOCAL_RUN:
                    env["pathToClimateCSV"] = LOCAL_PATH_TO_CLIMATE_DATA_DIR + "row-" + str(meteo_id[0]) + "/col-" + str(meteo_id[1]) + ".csv"
                else:
                    env["pathToClimateCSV"] = PATH_TO_CLIMATE_DATA_DIR + "row-" + str(meteo_id[0]) + "/col-" + str(meteo_id[1]) + ".csv"
                #env["pathToClimateCSV"] = PATH_TO_CLIMATE_DATA_DIR + gmd["subpath-climate.csv"]

                env["customId"] = rot_id \
                                + "|" + str(soil_id) \
                                + "|(" + str(row) + "/" + str(col) + ")" \
                                + "|" + str(bkr_id)
                socket.send_json(env) 
                print "sent env ", i, " customId: ", env["customId"]
                #if i > 10:
                #    exit()
                i += 1


    stop_send = time.clock()

    print "sending ", i, " envs took ", (stop_send - start_send), " seconds"
    print "simulated cells: ", simulated_cells, "; not found kreise for org N: ", no_kreis


main()
