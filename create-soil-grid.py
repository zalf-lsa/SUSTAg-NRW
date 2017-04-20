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

import sqlite3

def create():
    "create soil grid"

    with open("soil-profile-id_nrw_gk3.asc", mode="w") as fff:
        fff.write(
"""ncols        250
nrows        241
xllcorner    3280914.799999999800
yllcorner    5580000.500000000000
cellsize     1000.000000000000
NODATA_value  -9999
""")

        query = "select row, column, grid_id from MACSUR_WP3_soil_r1 order by row, column"
        con = sqlite3.connect("soil.sqlite")
        con.row_factory = sqlite3.Row
        values = {}
        for row in con.cursor().execute(query):
            values[(int(row["row"]), int(row["column"]))] = int(row["grid_id"])
        con.close()

        for row in range(0, 241):
            for col in range(0, 250):
                rowcol = ((row + 282), col)
                fff.write(str(values[rowcol] if rowcol in values else -9999) + " " if col < 250 else "")
            fff.write("\n")


#con = sqlite3.connect("soil.sqlite")
#x = soil_parameters(con, 197595)
#print x

create()
