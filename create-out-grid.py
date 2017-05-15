import numpy as np
import pandas as pd
from collections import defaultdict

def map_indexes(aggr_data, cells, rotations=None):
    r_loc = {}
    c_loc = {}    
    #rows
    if rotations:
        for i in range(len(cells)):
            r_loc[(cells[i], rotations[i])] = i
    else:
        for i in range(len(cells)):
            r_loc[cells[i]] = i
    #cols
    for i in range(len(aggr_data.columns)):
        c_loc[aggr_data.columns[i]] = i
    
    return r_loc, c_loc

def update_grid_data(aggr_data, grid_data, row, col, r_loc, c_index, rot=None, offset_row=282, offset_col=0):
    row_col = "{}{:03d}".format(row + offset_row, col + offset_col)
    grid_data[row][col] = -9999
    if rot:
        if (int(row_col), int(rot)) in r_loc:
            r_index =  r_loc[(int(row_col), int(rot))]
            grid_data[row][col] = aggr_data.iloc[(r_index,c_index)]
    else:
        if int(row_col) in r_loc:
            r_index =  r_loc[int(row_col)]
            grid_data[row][col] = aggr_data.iloc[(r_index,c_index)]

def write_grid_file(grid_data, cp_yr_tag, out_var, rot=""):
        out_file = "out/grids/" + str(cp_yr_tag) + "_" + str(out_var) + "_" + str(rot) + ".asc"
        with(open(out_file, "w")) as _:    
            #header
            _.write(
"""ncols        250
nrows        241
xllcorner    3280914.799999999800
yllcorner    5580000.500000000000
cellsize     1000.000000000000
NODATA_value  -9999
""")
            #grid
            for row in range(n_rows):
                for col in range(n_cols):
                    _.write(str(grid_data[row][col]) + " ")
                _.write("\n")

def crop_rotation_grids(plot_vars, n_rows, n_cols):

    #identify available crops
    crops = set(df.crop)

    #subset for crop
    for cp in crops:
        crop_df = df[df.crop == cp]

        #aggregate per cell and rotation
        aggr_data = crop_df.groupby(["IDcell", "rotation"]).mean()
        #print aggr_data

        #identify cells/rotations
        cells = aggr_data.index.get_level_values('IDcell').tolist()
        rotations = aggr_data.index.get_level_values('rotation').tolist()
        
        #map indexes
        r_loc, c_loc = map_indexes(aggr_data, cells, rotations)
        
        for rot in set(rotations):
            for out_var in plot_vars:
                grid_data = defaultdict(lambda: defaultdict(dict)) #dict row, dict col
                c_index = c_loc[out_var]
                for row in range(n_rows):
                    for col in range(n_cols):
                        update_grid_data(aggr_data, grid_data, row, col, r_loc, c_index, rot)
                write_grid_file(grid_data, cp, out_var, rot)
                print("out grid for crop: " + str(cp) + " var: " + str(out_var) + " and rotation: " + str(rot) + " ready!")

def crop_grids(plot_vars, n_rows, n_cols):
    
    #identify available crops
    crops = set(df.crop)

    #subset for crop
    for cp in crops:
        crop_df = df[df.crop == cp]

        #aggregate per cell and rotation
        aggr_data = crop_df.groupby(["IDcell"]).mean()
        #print aggr_data

        #retrieve values per cell/rotation
        cells = aggr_data.index.get_level_values('IDcell').tolist()        

        #map indexes
        r_loc, c_loc = map_indexes(aggr_data, cells)

        for out_var in plot_vars:
            grid_data = defaultdict(lambda: defaultdict(dict)) #dict row, dict col
            c_index = c_loc[out_var]
            for row in range(n_rows):
                for col in range(n_cols):
                    update_grid_data(aggr_data, grid_data, row, col, r_loc, c_index)
            write_grid_file(grid_data, cp, out_var)
            print("out grid for crop: " + str(cp) + " var: " + str(out_var) + " ready!")

def year_rotation_grids(plot_vars, n_rows, n_cols):
    
    #aggregate per cell and rotation
    aggr_data = df.groupby(["IDcell", "rotation"]).mean()
    #print aggr_data

    #retrieve values per cell/rotation
    cells = aggr_data.index.get_level_values('IDcell').tolist()
    rotations = aggr_data.index.get_level_values('rotation').tolist()

    #map indexes
    r_loc, c_loc = map_indexes(aggr_data, cells, rotations)

    for rot in set(rotations):
        for out_var in plot_vars:
            grid_data = defaultdict(lambda: defaultdict(dict)) #dict row, dict col
            c_index = c_loc[out_var]
            for row in range(n_rows):
                for col in range(n_cols):
                    update_grid_data(aggr_data, grid_data, row, col, r_loc, c_index, rot)
            write_grid_file(grid_data, "yearly", out_var, rot)
            print("out grid for yearly out var: " + str(out_var) + " and rotation: " + str(rot) + " ready!")

def year_grids(plot_vars, n_rows, n_cols):
    
    #aggregate per cell and rotation
    aggr_data = df.groupby(["IDcell"]).mean()
    #print aggr_data

    #retrieve values per cell/rotation
    cells = aggr_data.index.get_level_values('IDcell').tolist()

    #map indexes
    r_loc, c_loc = map_indexes(aggr_data, cells)

    for out_var in plot_vars:
        grid_data = defaultdict(lambda: defaultdict(dict)) #dict row, dict col
        c_index = c_loc[out_var]
        for row in range(n_rows):
            for col in range(n_cols):
                update_grid_data(aggr_data, grid_data, row, col, r_loc, c_index)
        write_grid_file(grid_data, "yearly", out_var)
        print("out grid for yearly out var: " + str(out_var) + " ready!")

#read file(s)
df_cp_129 = pd.read_csv("out/129_crop.csv")
df_cp_142 = pd.read_csv("out/142_crop.csv")

frames = [df_cp_129, df_cp_142]
df = pd.concat(frames)

plot_vars = ["agb"]
n_rows = 241
n_cols = 250

#crop_rotation_grids(plot_vars, n_rows, n_cols)
#crop_grids(plot_vars, n_rows, n_cols)
year_rotation_grids(plot_vars, n_rows, n_cols)
year_grids(plot_vars, n_rows, n_cols)

print "finished!"