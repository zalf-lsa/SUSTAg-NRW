import numpy as np
import pandas as pd
from collections import defaultdict

#read file
df = pd.read_csv("out/129_crop.csv")

#df.append(df2, ignore_index=True)

#subset for crop
crop_df = df[df.crop == "mustard_"]

#calculate summary out
#average_LAI = crop_df["LAImax"].mean()

#aggregate per cell and rotation
cell_rotation = crop_df.groupby(["IDcell", "rotation"]).mean()

#print cell_rotation.index
print cell_rotation

#retrieve values per cell/rotation
cells = cell_rotation.index.get_level_values('IDcell').tolist()
rotations = cell_rotation.index.get_level_values('rotation').tolist()

#TODO find a better solution to get the single value of interest
r_loc = {}
for i in range(len(cells)):
    r_loc[(cells[i], rotations[i])] = i

c_loc = {}
for i in range(len(cell_rotation.columns)):
    c_loc[cell_rotation.columns[i]] = i

plot_vars = ["cyclelength", "agb"]

def update_grid_data(row, col, rot, out_var, offset_row = 282, offset_col = 0):
    row_col = "{}{:03d}".format(row + offset_row, col + offset_col)
    if (int(row_col), int(rot)) in r_loc:
        r_index =  r_loc[(int(row_col), int(rot))]
        grid_data[row][col] = cell_rotation.iloc[(r_index,c_index)]
    else:
        grid_data[row][col] = -9999

def write_grid_file(grid_data, out_var, rot):
    out_file = "out/grids/" + str(out_var) + "_" + str(rot) + ".asc"
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

n_rows = 241
n_cols = 250

for rot in set(rotations):
    for out_var in plot_vars:
        grid_data = defaultdict(lambda: defaultdict(dict)) #dict row, dict col
        c_index = c_loc[out_var]
        for row in range(n_rows):
            for col in range(n_cols):
                update_grid_data(row, col, rot, out_var)
        write_grid_file(grid_data, out_var, rot)
        print("out grid for var: " + str(out_var) + " and rotation: " + str(rot) + " ready!")


#test4 = cell_rotation.loc[cell_rotation.index['IDcell'] == '353141']
#test3 = cell_rotation.loc[cell_rotation['year'] < 3531.41, 'agb']
#test = cell_rotation.iloc[(0,1)]
#print test

#agb = cell_rotation[(353141, 6110), 'agb']#[cell_rotation['IDcell'] == '353141', 'agb']

