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

import sys
#sys.path.insert(0, "C:\\Users\\berg.ZALF-AD\\GitHub\\monica\\project-files\\Win32\\Release")
#sys.path.insert(0, "C:\\Users\\berg.ZALF-AD\\GitHub\\monica\\src\\python")
#print sys.path

#import ascii_io
#import json
import csv
import types
import os
from datetime import datetime
from collections import defaultdict

import zmq
#print zmq.pyzmq_version()
import monica_io
import re

start_recording_out = 1975

LOCAL_RUN = False #True
RUN_ON_CLUSTER = 1

def create_crop_output(oids, row, col, rotation, values):
    "create crop output lines"
    row_col = "{}{:03d}".format(row, col)
    out = []
    if len(values) > 0:
        for kkk in range(0, len(values[0])):
            vals = {}
            for iii in range(0, len(oids)):
                oid = oids[iii]
                val = values[iii][kkk]

                name = oid["name"] if len(oid["displayName"]) == 0 else oid["displayName"]

                if isinstance(val, types.ListType):
                    for val_ in val:
                        vals[name] = val_
                else:
                    vals[name] = val

            if vals.get("Year", 0) > start_recording_out:
                out.append([
                    row_col,
                    rotation,
                    vals.get("Crop", "NA").replace("/", "_").replace(" ", "-"),
                    vals.get("Year", "NA"),
                    vals.get("cycleLength", "NA"),
                    vals.get("abBiom", "NA"),
                    vals.get("yield", "NA"),
                    vals.get("maxLAI", "NA"),
                    vals.get("sumNup", "NA"),
                    vals.get("sumTra", "NA"),
                    vals.get("sumGIso", "NA"),
                    vals.get("sumGMono", "NA"),
                    vals.get("sumJIso", "NA"),
                    vals.get("sumJMono", "NA"),
                    vals.get("sumGlobrad", "NA"),
                    vals.get("avgGlobrad", "NA"),
                    vals.get("sumTavg", "NA"),
                    vals.get("avgTmin", "NA"),
                    vals.get("avgTavg", "NA"),
                    vals.get("avgTmax", "NA"),
                    vals.get("sumPrecip", "NA")
                ])

    return out


def write_data(region_id, crop_data):
    "write data"

    path_to_crop_file = "out/" + str(region_id) + "_crop.csv"

    if not os.path.isfile(path_to_crop_file):
        with open(path_to_crop_file, "w") as _:
            _.write("IDcell,rotation,crop,year,cycleLength,abBiom,yield,maxLAI,sumNup,sumTra,sumGIso,sumGMono,sumJIso,sumJMono,sumGlobrad,avgGlobrad,sumTavg,avgTmin,avgTavg,avgTmax,sumPrecip\n")

    with open(path_to_crop_file, 'ab') as _:
        writer = csv.writer(_, delimiter=",")
        for row_ in crop_data[region_id]:
            writer.writerow(row_)
        crop_data[region_id] = []


def main():
    "consume data from workers"

    config = {
        "port": 7777,
    }
    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            k,v = arg.split("=")
            if k in config:
                config[k] = int(v) 

    year_data = defaultdict(list)
    crop_data = defaultdict(list)
    pheno_data = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))

    i = 0
    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    if LOCAL_RUN:
        socket.connect("tcp://localhost:" + str(config["port"]))
    else:
        socket.connect("tcp://cluster" + str(RUN_ON_CLUSTER) + ":" + str(config["port"]))

    socket.RCVTIMEO = 1000
    leave = False
    write_normal_output_files = False
    start_writing_lines_threshold = 1000
    while not leave:

        try:
            result = socket.recv_json()
        except:
            for region_id in crop_data.keys():
                if len(crop_data[region_id]) > 0:
                    write_data(region_id, crop_data)
            continue

        if result["type"] == "finish":
            print "received finish message"
            leave = True

        elif not write_normal_output_files:
            print "received work result ", i, " customId: ", result.get("customId", ""), " len(year_data): ", len((year_data.values()[:1] or [[]])[0])

            custom_id = result["customId"]
            ci_parts = custom_id.split("|")
            rotation = ci_parts[0]
            soil_id = ci_parts[1]
            row, col = map(int, ci_parts[2][1:-1].split("/"))
            region_id = ci_parts[3]

            for data in result.get("data", []):
                results = data.get("results", [])
                orig_spec = data.get("origSpec", "")
                output_ids = data.get("outputIds", [])
                if len(results) > 0:
                    if orig_spec == '"crop"':
                        res = create_crop_output(output_ids, row, col, rotation, results)
                        crop_data[region_id].extend(res)


            for region_id in crop_data.keys():
                if len(crop_data[region_id]) > start_writing_lines_threshold:
                    write_data(region_id, crop_data)

            i = i + 1

        elif write_normal_output_files:
            print "received work result ", i, " customId: ", result.get("customId", "")

            with open("out/out-" + str(i) + ".csv", 'wb') as _:
                writer = csv.writer(_, delimiter=",")

                for data in result.get("data", []):
                    results = data.get("results", [])
                    orig_spec = data.get("origSpec", "")
                    output_ids = data.get("outputIds", [])

                    if len(results) > 0:
                        writer.writerow([orig_spec])
                        for row in monica_io.write_output_header_rows(output_ids,
                                                                      include_header_row=True,
                                                                      include_units_row=True,
                                                                      include_time_agg=False):
                            writer.writerow(row)

                        for row in monica_io.write_output(output_ids, results):
                            writer.writerow(row)

                    writer.writerow([])

            i = i + 1


main()

