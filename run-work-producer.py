#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
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
USER = "stella"

PATHS = {
    "stella": {
        "INCLUDE_FILE_BASE_PATH": "C:/Users/stella/Documents/GitHub",
    },
    "berg": {
        "INCLUDE_FILE_BASE_PATH": "C:/Users/berg.ZALF-AD/MONICA"        
    }
}

PATH_TO_CLIMATE_DATA_DIR = "/archiv-daten/md/data/climate/isimip/csvs/earth/"
#PATH_TO_CLIMATE_DATA_DIR ="/archiv-daten/md/projects/sustag/MACSUR_WP3_NRW_1x1/" #"Z:/projects/sustag/MACSUR_WP3_NRW_1x1/"

def main():
    "main function"

    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    port = 6666 if len(sys.argv) == 1 else sys.argv[1]
    socket.connect("tcp://cluster2:" + str(port))

    soil_db_con = sqlite3.connect("soil.sqlite")

    with open("sim.json") as _:
        sim = json.load(_)

    with open("site.json") as _:
        site = json.load(_)

    with open("crop.json") as _:
        crop = json.load(_)

    with open("cover-crop.json") as _:
        cover_crop = json.load(_)

    with open("sims.json") as _:
        sims = json.load(_)

    with open("rotations.json") as _:
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
            row = 0
            for line in file_:
                col = 0
                for col_str in line.strip().split(" "):
                    if not include_no_data and int_or_float(col_str) == -9999:
                        continue
                    data[(row_offset+row, col_offset+col)] = int_or_float(col_str)
                    col += 1
                row += 1
            return data
    
    #offset is used to match info in general metadata and soil database
    soil_ids = read_ascii_grid("soil-profile-id_nrw_gk3.asc", row_offset=282)
    bkr_ids = read_ascii_grid("bkr_nrw_gk3.asc", row_offset=282)
    lu_ids = read_ascii_grid("lu_resampled.asc", row_offset=282)
    kreise_ids = read_ascii_grid("kreise_matrix.asc", row_offset=282)
    meteo_ids = load_mapping(row_offset=282)

    def rotate(crop_rotation):
        "rotate the crops in the rotation"
        crop_rotation.insert(0, crop_rotation.pop())
    
    def insert_cc(crop_rotation):
        "insert cover crops in the rotation"
        insert_cover_before = ["maize", "spring barley", "potato", "sugar beet"]
        insert_cover_here = []
        for cultivation_method in range(len(crop_rotation)):
            for workstep in crop_rotation[cultivation_method]["worksteps"]:
                if workstep["type"] == "Sowing":
                    if workstep["crop"]["cropParams"]["species"]["SpeciesName"] in insert_cover_before or workstep["crop"]["cropParams"]["cultivar"]["CultivarName"] in insert_cover_before:
                        insert_cover_here.append((cultivation_method, workstep["date"]))
                        break
        for position, mydate in reversed(insert_cover_here): 
            mydate = mydate.split("-")
            main_crop_sowing = date(2017, int(mydate[1]), int(mydate[2]))
            latest_harvest_cc = main_crop_sowing - timedelta(days = 10)
            latest_harvest_cc = unicode("0001-" + str(latest_harvest_cc.month).zfill(2) + "-" + str(latest_harvest_cc.day).zfill(2))
            crop_rotation.insert(position, copy.deepcopy(cc_data))
            crop_rotation[position]["worksteps"][1]["latest-date"] = latest_harvest_cc

    #def remove_cc(crop_rotation):
    #    "remove cover crops from the rotation"
    #    for cultivation_method in reversed(range(len(crop_rotation))):
    #        if "is-cover-crop" in crop_rotation[cultivation_method]:
    #            del crop_rotation[cultivation_method]

    #env built only to have structured data for cover crop
    cover_env = monica_io.create_env_json_from_json_config({
        "crop": cover_crop,
        "site": site,
        "sim": sim,
        "climate": ""
        })
    cc_data = cover_env["cropRotation"][0]

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

    for (row, col), gmd in general_metadata.iteritems():

        if (row, col) in soil_ids and (row, col) in bkr_ids and (row, col) in lu_ids:
            update_soil_crop_dates(row, col)

            bkr_id = bkr_ids[(row, col)]
            soil_id = soil_ids[(row, col)]
            kreis_id = kreise_ids[(row, col)]
            meteo_id = meteo_ids[(row, col)]

            if bkr_id != 142:
                continue

            for rot_id, rotation in rotations[str(bkr_id)].iteritems():

                crop["cropRotation"] = rotation

                env = monica_io.create_env_json_from_json_config({
                    "crop": crop,
                    "site": site,
                    "sim": sim,
                    "climate": ""
                })

                #with open("test_crop.json", "w") as _:
                #    _.write(json.dumps(crop))

                #with open("test_site.json", "w") as _:
                #    _.write(json.dumps(site))

                #with open("test_sim.json", "w") as _:
                #    _.write(json.dumps(sim))

                insert_cc(env["cropRotation"])

                #assign amount of organic fertilizer
                for cultivation_method in env["cropRotation"]:
                    for workstep in cultivation_method["worksteps"]:
                        if workstep["type"] == "OrganicFertilization":
                            workstep["amount"] = calculate_orgfert_amount(orgN_kreise[kreis_id], workstep["parameters"])

                #climate is read by the server
                env["csvViaHeaderOptions"] = sim["climate.csv-options"]
                env["csvViaHeaderOptions"]["start-date"] = sim["start-date"]
                env["csvViaHeaderOptions"]["end-date"] = sim["end-date"]
                env["pathToClimateCSV"] = PATH_TO_CLIMATE_DATA_DIR + "row-" + str(meteo_id[0]) + "/col-" + str(meteo_id[1]) + ".csv"
                #env["pathToClimateCSV"] = PATH_TO_CLIMATE_DATA_DIR + gmd["subpath-climate.csv"]

                for sim_id, sim_ in sims.iteritems():
                    if sim_id != "WL.NL.rain":
                        continue
                    #sim_id, sim_ = ("potential", sims["potential"])
                    env["events"] = sim_["output"]
                    env["params"]["simulationParameters"]["NitrogenResponseOn"] = sim_["NitrogenResponseOn"]
                    env["params"]["simulationParameters"]["WaterDeficitResponseOn"] = sim_["WaterDeficitResponseOn"]
                    env["params"]["simulationParameters"]["UseAutomaticIrrigation"] = sim_["UseAutomaticIrrigation"]
                    env["params"]["simulationParameters"]["UseNMinMineralFertilisingMethod"] = sim_["UseNMinMineralFertilisingMethod"]

                    for rot in range(0, len(env["cropRotation"])):
                        env["customId"] = rot_id \
                                        + "|" + sim_id \
                                        + "|" + str(soil_id) \
                                        + "|(" + str(row) + "/" + str(col) + ")" \
                                        + "|" + str(bkr_id) \
                                        + "|" + str(rot)
                        socket.send_json(env) 
                        print "sent env ", i, " customId: ", env["customId"]
                        i += 1                        
                        rotate(env["cropRotation"]) 


    stop_send = time.clock()

    print "sending ", i, " envs took ", (stop_send - start_send), " seconds"



main()
