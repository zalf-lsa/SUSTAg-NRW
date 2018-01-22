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
import sys
import time
import zmq
import monica_io
import soil_io
import ascii_io
from datetime import date, timedelta
import copy
import os
from collections import defaultdict

USER = "stella"

PATHS = {
    "stella": {
        "INCLUDE_FILE_BASE_PATH": "C:/Users/stella/Documents/GitHub",
    },
    "berg": {
        "INCLUDE_FILE_BASE_PATH": "C:/Users/berg.ZALF-AD/MONICA"        
    }
}

timeframes = {
    "historical": {
        "start-date": "1975-01-01",
        "end-date": "2005-12-31",
        "cluster-path-to-climate": "/archiv-daten/md/data/climate/isimip/csvs/earth/",
        "local-path-to-climate": "z:/data/climate/isimip/csvs/earth/"
    },
    "2030": {
        "start-date": "2006-01-01",
        "end-date": "2030-12-31",
        "cluster-path-to-climate": "/archiv-daten/md/data/climate/isimip/csvs/germany-nrw/", #rcp 2.6
        "local-path-to-climate": "z:/data/climate/isimip/csvs/germany-nrw/" #rcp 2.6
    }
}
#macsur climate data:
#PATH_TO_CLIMATE_DATA_DIR ="/archiv-daten/md/projects/sustag/MACSUR_WP3_NRW_1x1/" #"Z:/projects/sustag/MACSUR_WP3_NRW_1x1/"

#Configure producer
tf = "2030"
LOCAL_RUN = False
FERT_STRATEGY = "BASE" #options: "NDEM", "NMIN", "BASE"
residues_exported = True
if residues_exported:
    export_rate = "custom"
    export_presets = {
        "all": "export all residues available according to MONICA secondary yield params",
        "custom": 0.33 #33% exported, 67% left on the field
    }
#end of user configuration

if FERT_STRATEGY == "NMIN":
    rotations_file = "rotations_dynamic_harv.json"
elif FERT_STRATEGY == "NDEM":
    rotations_file = "rotations_dynamic_harv_Ndem.json"
elif FERT_STRATEGY == "BASE":
    rotations_file = "rotations_dynamic_harv_Nbaseline.json"

PATH_TO_CLIMATE_DATA_DIR = timeframes[tf]["cluster-path-to-climate"]
LOCAL_PATH_TO_CLIMATE_DATA_DIR = timeframes[tf]["local-path-to-climate"]

def main():
    "main function"

    context = zmq.Context()
    socket = context.socket(zmq.PUSH)
    port = 6666 if len(sys.argv) == 1 else sys.argv[1]
    if LOCAL_RUN:
        socket.connect("tcp://localhost:" + str(port))
    else:
        socket.connect("tcp://cluster2:" + str(port))

    soil_db_con = sqlite3.connect("soil.sqlite")

    with open("sim.json") as _:
        sim = json.load(_)
        sim["start-date"] = timeframes[tf]["start-date"]
        sim["end-date"] = timeframes[tf]["end-date"]

    with open("site.json") as _:
        site = json.load(_)

    with open("crop.json") as _:
        crop = json.load(_)

    with open("cover-crop.json") as _:
        cover_crop = json.load(_)

    with open("sims.json") as _:
        sims = json.load(_)
        if FERT_STRATEGY != "NMIN":
            #turn off Nmin automatic fertilization
            for setting in sims.iteritems():
                setting[1]["UseNMinMineralFertilisingMethod"] = False

    with open(rotations_file) as _:
        rotations = json.load(_)        
        if FERT_STRATEGY == "BASE":            
            #identify rotations with codes
            rots_info = defaultdict(lambda: defaultdict(lambda: defaultdict()))
            for bkr, rots in rotations.iteritems():
                for rot in rots.iteritems():
                    rot_code = int(rot[0])
                    my_rot = []
                    for cm in rot[1]:#["worksteps"]:
                        for ws in range(len(cm["worksteps"])):
                            if cm["worksteps"][ws]["type"] == "Sowing":
                                my_rot.append(cm["worksteps"][ws]["crop"][2])
                    rots_info[rot_code]["full_rotation"] = my_rot
                    #for each crop, identify previous one (needed to determine expected N availability)
                    for i in range(len(rots_info[rot_code]["full_rotation"])):
                        current_cp = rots_info[rot_code]["full_rotation"][i]
                        if i != 0:
                            previous_cp = rots_info[rot_code]["full_rotation"][i-1]
                        else:
                            previous_cp = rots_info[rot_code]["full_rotation"][-1]
                        rots_info[rot_code][i]["current"] = current_cp
                        rots_info[rot_code][i]["previous"] = previous_cp
    
    if FERT_STRATEGY == "BASE":
        #read additional info required for baseline fert strategy:
        #1. expected mineral N availability
        expected_N_availability = defaultdict(lambda: defaultdict())
        with open ("expected_N_availability.csv") as _:
            reader = csv.reader(_)
            reader.next()
            for row in reader:
                cp_sequence = (row[0], row[1])
                soil_type = row[2]
                expected_N_value = float(row[3])
                expected_N_availability[cp_sequence][soil_type] = expected_N_value
                expected_N_availability[cp_sequence]["target_depth"] = float(row[4])
        #2. rules to split mineral fertilization
        mineralN_split = defaultdict(lambda: defaultdict())
        with open ("MineralN_topdressing.csv") as _:
            reader = csv.reader(_)
            reader.next()
            for row in reader:
                cp = row[0]
                mineralN_split[cp]["target"] = float(row[2])
                for i in range(4, 7):
                    if row[i] != "":
                         mineralN_split[cp][i-4] = float(row[i])                  

    
    sim["UseSecondaryYields"] = residues_exported
    if residues_exported:
        if export_rate == "custom":
            for cp in crop["crops"].iteritems():
                for organ in cp[1]["cropParams"]["cultivar"]["OrganIdsForSecondaryYield"]:
                    organ["yieldPercentage"] *= export_presets["custom"]

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
        
        site["Latitude"] = general_metadata[(row, col)]["latitude"]
        site["HeightNN"] = [general_metadata[(row, col)]["elevation"], "m"]
        site["SiteParameters"]["SoilProfileParameters"] = soil_io.soil_parameters(soil_db_con, soil_ids[(row, col)])
        KA5_txt = soil_io.sand_and_clay_to_ka5_texture(site["SiteParameters"]["SoilProfileParameters"][0]["Sand"][0], site["SiteParameters"]["SoilProfileParameters"][0]["Clay"][0])
        for layer in site["SiteParameters"]["SoilProfileParameters"]:
            layer["SoilBulkDensity"][0] = max(layer["SoilBulkDensity"][0], 600)
            layer["SoilOrganicCarbon"][0] = layer["SoilOrganicCarbon"][0] * 0.6 #correction factor suggested by TGaiser
        
        return KA5_txt
    
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
                    if workstep["crop"]["cropParams"]["species"]["SpeciesName"] in insert_cover_before or workstep["crop"]["cropParams"]["cultivar"]["="]["CultivarName"] in insert_cover_before:
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

    def calculate_orgfert_amount(N_applied, fert_type, soilCN=10):
        "convert N applied in amount of fresh org fert"
        AOM_DryMatterContent = fert_type["AOM_DryMatterContent"][0]
        AOM_NH4Content = fert_type["AOM_NH4Content"][0]
        AOM_NO3Content = fert_type["AOM_NO3Content"][0]
        CN_Ratio_AOM_Fast = fert_type["CN_Ratio_AOM_Fast"][0]
        CN_Ratio_AOM_Slow = fert_type["CN_Ratio_AOM_Slow"][0]
        PartAOM_to_AOM_Fast = fert_type["PartAOM_to_AOM_Fast"][0]
        PartAOM_to_AOM_Slow = fert_type["PartAOM_to_AOM_Slow"][0]
        AOM_to_C = 0.45

        AOM_fast_factor = (AOM_to_C * PartAOM_to_AOM_Fast)/CN_Ratio_AOM_Fast
        AOM_slow_factor = (AOM_to_C * PartAOM_to_AOM_Slow)/CN_Ratio_AOM_Slow
        AOM_SOM_factor = (1- (PartAOM_to_AOM_Fast + PartAOM_to_AOM_Slow)) * AOM_to_C / soilCN

        conversion_coeff = AOM_NH4Content + AOM_NO3Content + AOM_fast_factor + AOM_slow_factor + AOM_SOM_factor

        AOM_dry = N_applied / conversion_coeff
        AOM_fresh = AOM_dry / AOM_DryMatterContent

        return AOM_fresh
    
    def update_fert_values(rotation, rot_id, rots_info, expected_N_availability, mineralN_split, KA5_class, orgN_applied):
        "function to mimic baseline fertilization"
        
        light_soils = ["Ss", "Su2", "Su3", "Su4", "St2", "Sl3", "Sl2"]
        heavy_soils = ["Tu3", "Tu4", "Lt3", "Ts2", "Tl", "Tu2", "Tt"]
        soil_type = "medium"
        if KA5_class in light_soils:
            soil_type = "light"
        elif KA5_class in heavy_soils:
            soil_type = "heavy"

        has_cover = ["SM", "GM", "SB", "PO", "SBee"] #same as "insert_cover_before"
        cc_effect = { #temporary values, ask TGaiser
            "present": {
                60: +5,
                90: -10
            },
            "absent": {
                60: -5,
                90: +10
            }
        }

        orgN = "medium"
        if orgN_applied > 120:
            orgN = "high"
        elif orgN_applied < 80:
            orgN = "low"

        orgN_effect = { #temporary values, ask TGaiser
            "low": {
                60: -6,
                90: -9
            },
            "medium":{
                60: 0,
                90: 0
            },
            "high": {
                60: +6,
                90: +9
            },
        }
        
        for cm in range(len(rotation)):
            current_cp = rots_info[rot_id][cm]["current"]
            previous_cp = rots_info[rot_id][cm]["previous"]

            N_target = mineralN_split[current_cp]["target"]
            expected_Nmin = expected_N_availability[(current_cp, previous_cp)][soil_type]
            
            #modify expected Nmin depending on livestock pressure and presence of cover crop
            target_depth = expected_N_availability[(current_cp, previous_cp)]["target_depth"]
            if current_cp in has_cover:
                expected_Nmin += cc_effect["present"][target_depth]
            else:
                expected_Nmin += cc_effect["absent"][target_depth]
            expected_Nmin += orgN_effect[orgN][target_depth]
            
            #calculate N to be applied with mineral fertilization
            sum_Nfert = max(N_target - expected_Nmin, 0)
            
            #map the fertilization worksteps
            ref_fert = 0
            for ws in range(len(rotation[cm]["worksteps"])):
                workstep = rotation[cm]["worksteps"][ws]
                if workstep["type"] == "MineralFertilization" and workstep["amount"][0] == 0:
                    workstep["amount"][0] = sum_Nfert * mineralN_split[current_cp][ref_fert]
                    ref_fert += 1

            #print (rot_id, cm, current_cp, previous_cp)
        return rotation

    simulated_cells = 0
    no_kreis = 0    

    for (row, col), gmd in general_metadata.iteritems():

        if (row, col) in soil_ids and (row, col) in bkr_ids and (row, col) in lu_ids:

            bkr_id = bkr_ids[(row, col)]
            
            ########for testing
            #if bkr_id != 129:
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

            KA5_txt = update_soil_crop_dates(row, col)

            #row_col = "{}{:03d}".format(row, col)
            #topsoil_carbon[row_col] = site["SiteParameters"]["SoilProfileParameters"][0]["SoilOrganicCarbon"][0]
            #continue

            for rot_id, rotation in rotations[str(bkr_id)].iteritems():
                
                ########for testing
                #if rot_id != "6110":
                #    continue
                if FERT_STRATEGY == "BASE":
                    updated_rot = update_fert_values(copy.deepcopy(rotation), int(rot_id), rots_info, expected_N_availability, mineralN_split, KA5_txt, orgN_kreise[kreis_id])
                    crop["cropRotation"] = updated_rot

                else:
                    crop["cropRotation"] = rotation

                env = monica_io.create_env_json_from_json_config({
                    "crop": crop,
                    "site": site,
                    "sim": sim,
                    "climate": ""
                })

                #with open("test_crop.json", "w") as _:
                #    _.write(json.dumps(crop, indent=4))

                insert_cc(env["cropRotation"])

                #assign amount of organic fertilizer
                for cultivation_method in env["cropRotation"]:
                    for workstep in cultivation_method["worksteps"]:
                        if workstep["type"] == "OrganicFertilization":
                            workstep["amount"] = calculate_orgfert_amount(orgN_kreise[kreis_id], workstep["parameters"]) #TODO: assign soilCN param dynamically

                #climate is read by the server
                env["csvViaHeaderOptions"] = sim["climate.csv-options"]
                env["csvViaHeaderOptions"]["start-date"] = sim["start-date"]
                env["csvViaHeaderOptions"]["end-date"] = sim["end-date"]
                if LOCAL_RUN:
                    env["pathToClimateCSV"] = LOCAL_PATH_TO_CLIMATE_DATA_DIR + "row-" + str(meteo_id[0]) + "/col-" + str(meteo_id[1]) + ".csv"
                else:
                    env["pathToClimateCSV"] = PATH_TO_CLIMATE_DATA_DIR + "row-" + str(meteo_id[0]) + "/col-" + str(meteo_id[1]) + ".csv"

                for sim_id, sim_ in sims.iteritems():
                    if sim_id != "WL.NL.rain":
                        continue
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
                                        + "|" + str(rot) \
                                        + "|" + str(sim["UseSecondaryYields"])
                        ##TEST
                        #my_rot = []
                        #for cm in env["cropRotation"]:
                        #    if "is-cover-crop" in cm.keys():
                        #        my_rot.append("cover")
                        #    else:
                        #        for ws in cm["worksteps"]:
                        #            if ws["type"] == "Sowing":
                        #                cv_params = ws["crop"]["cropParams"]["cultivar"]
                        #                my_rot.append(cv_params["="]["CultivarName"])
                        #print my_rot

                        socket.send_json(env) 
                        print "sent env ", i, " customId: ", env["customId"]
                        i += 1
                        rotate(env["cropRotation"])


    stop_send = time.clock()

    print "sending ", i, " envs took ", (stop_send - start_send), " seconds"
    print "simulated cells: ", simulated_cells, "; not found kreise for org N: ", no_kreis


#topsoil_carbon = {}
main()

#with open("topsoilC.csv", "wb") as _:
#    writer = csv.writer(_, delimiter=",")
#    header = ["IDcell", "ini_SOCtop"]
#    writer.writerow(header)
#    for row_col in topsoil_carbon.keys():
#        row = [row_col, topsoil_carbon[row_col]]
#        writer.writerow(row)
