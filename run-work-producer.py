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

PATH_TO_NRW_METADATA_CSV_FILE = "NRW_General_Metadata.csv"
PATH_TO_CLIMATE_DATA_DIR = "/archiv-daten/md/projects/sustag/MACSUR_WP3_NRW_1x1/"

def main():
    "main function"

    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    port = 6666 if len(sys.argv) == 1 else sys.argv[1]
    socket.bind("tcp://*:" + str(port))

    soil_db_con = sqlite3.connect("soil.sqlite")

    with open("sim.json") as _:
        sim = json.load(_)

    with open("site.json") as _:
        site = json.load(_)

    with open("crop.json") as _:
        crop = json.load(_)

    with open("sims.json") as _:
        sims = json.load(_)

    with open("rotations.json") as _:
        rotations = json.load(_)

    sim["include-file-base-path"] = PATHS[USER]["INCLUDE_FILE_BASE_PATH"]
    #sim["climate.csv"] = "35_120_v1.csv"
    #sim["climate.csv"] = "C:/Users/stella/MONICA/Examples/Hohenfinow2/climate.csv"

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

    general_metadata = read_general_metadata(PATH_TO_NRW_METADATA_CSV_FILE)

    def update_soil_crop_dates(row, col):
        "in place update the env"
        #startDate = date(1980, 1, 1)# + timedelta(days = p["sowing-doy"])
        #sim["start-date"] = startDate.isoformat()
        #sim["end-date"] = date(2010, 12, 31).isoformat()
        #sim["debug?"] = True

        site["Latitude"] = general_metadata[(row, col)]["latitude"]
        site["HeightNN"] = [general_metadata[(row, col)]["elevation"], "m"]
        site["SiteParameters"]["SoilProfileParameters"] = soil_io.soil_parameters(soil_db_con, soil_ids[(row, col)])

    soil_ids = ascii_io.read_ascii_grid("soil-profile-id_nrw_gk3.asc", row_offset=282)
    bkr_ids = ascii_io.read_ascii_grid("bkr_nrw_gk3.asc", row_offset=282)

    def rotate(crop_rotation):
        "rotate the crops in the rotation"
        crop_rotation.insert(0, crop_rotation.pop())

    read_climate_data_locally = False
    i = 0
    start_send = time.clock()
    for (row, col), gmd in general_metadata.iteritems():
        #if not (row == 365 and col == 120):
        #    continue

        if (row, col) in soil_ids and (row, col) in bkr_ids:
            update_soil_crop_dates(row, col)

            bkr_id = bkr_ids[(row, col)]
            #if bkr_id != 191:
            #    continue
            soil_id = soil_ids[(row, col)]

            for rot_id, rotation in rotations[str(bkr_id)].iteritems():
                #if rot_id != "6130":
                #    continue
                #rot_id, rotation = ("9110", rotations["9110"])
                crop["cropRotation"] = rotation

                env = monica_io.create_env_json_from_json_config({
                    "crop": crop,
                    "site": site,
                    "sim": sim,
                    "climate": ""
                })

                #read climate data on client and send them with the env
                if read_climate_data_locally:
                    print PATH_TO_CLIMATE_DATA_DIR + gmd["subpath-climate.csv"]
                    with open(PATH_TO_CLIMATE_DATA_DIR + gmd["subpath-climate.csv"]) as _:
                        climate_data = _.read()
                    monica_io.add_climate_data_to_env(env, sim, climate_data)
                else:
                    env["csvViaHeaderOptions"] = sim["climate.csv-options"]
                    env["csvViaHeaderOptions"]["start-date"] = sim["start-date"]
                    env["csvViaHeaderOptions"]["end-date"] = sim["end-date"]
                    env["pathToClimateCSV"] = PATH_TO_CLIMATE_DATA_DIR + gmd["subpath-climate.csv"]

                for sim_id, sim_ in sims.iteritems():
                    #if sim_id != "WL.NL.rain":
                    #    continue
                    #sim_id, sim_ = ("potential", sims["potential"])
                    env["events"] = sim_["output"]
                    env["params"]["simulationParameters"]["NitrogenResponseOn"] = sim_["NitrogenResponseOn"]
                    env["params"]["simulationParameters"]["WaterDeficitResponseOn"] = sim_["WaterDeficitResponseOn"]
                    env["params"]["simulationParameters"]["UseAutomaticIrrigation"] = sim_["UseAutomaticIrrigation"]
                    env["params"]["simulationParameters"]["UseNMinMineralFertilisingMethod"] = sim_["UseNMinMineralFertilisingMethod"]

                    for rot in range(0, len(rotation)):
                        env["customId"] = rot_id \
                                        + "|" + sim_id \
                                        + "|" + str(soil_id) \
                                        + "|(" + str(row) + "/" + str(col) + ")" \
                                        + "|" + str(bkr_id) \
                                        + "|" + str(rot)
                        socket.send_json(env) #TODO uncomment this
                        print "sent env ", i, " customId: ", env["customId"]
                        i += 1
                        rotate(env["cropRotation"])


    stop_send = time.clock()

    print "sending ", i, " envs took ", (stop_send - start_send), " seconds"



main()
