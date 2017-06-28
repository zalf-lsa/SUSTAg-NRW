import csv
from collections import defaultdict
import pandas as pd
import json

n_rows = 241
n_cols = 250

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

def read_ascii_grid(path_to_file, include_no_data=False, row_offset=0, col_offset=0):
    "read an ascii grid into a map, without the no-data values"
    def int_or_float(s):
        try:
            return int(s)
        except ValueError:
            return float(s)
    
    with open(path_to_file) as file_:
        data = defaultdict(lambda: defaultdict(dict)) #dict row, dict col
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
                data[row_offset+row][col_offset+col] = int_or_float(col_str)

        return data

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

def kreise_N_amount():
    orgN_kreise = read_orgN_kreise("NRW_orgN_balance.csv")
    kreise_ids = read_ascii_grid("kreise_matrix.asc", include_no_data=True)

    for row in range(n_rows):
        for col in range(n_cols):
            kreis = kreise_ids[row][col]
            if kreis in orgN_kreise:
                kreise_ids[row][col] = orgN_kreise[kreis]

    write_grid_file(kreise_ids, "Kreise", "N_amount")

def soc_trajectories():
    df_yr_129 = pd.read_csv("out/splitted-out/129_year.csv")
    df_yr_134 = pd.read_csv("out/splitted-out/134_year.csv")
    df_yr_141 = pd.read_csv("out/splitted-out/141_year.csv")
    df_yr_142 = pd.read_csv("out/splitted-out/142_year.csv")
    df_yr_143 = pd.read_csv("out/splitted-out/143_year.csv")
    df_yr_146 = pd.read_csv("out/splitted-out/146_year.csv")
    df_yr_147 = pd.read_csv("out/splitted-out/147_year.csv")
    df_yr_148 = pd.read_csv("out/splitted-out/148_year.csv")
    df_yr_191 = pd.read_csv("out/splitted-out/191_year.csv")

    yr_frames = [df_yr_129, df_yr_134, df_yr_141, df_yr_142, df_yr_143, df_yr_146, df_yr_147, df_yr_148, df_yr_191]
    yr_df = pd.concat(yr_frames)

    with open("rotations.json") as _:
        rotations = json.load(_)
    
    #summary_data = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
    
    with open("SOC_trajectories.csv", "wb") as _:
        writer = csv.writer(_, delimiter=",")
        header = ["rotation", "year", "dSOCavg", "dSOCstd", "dSOCmin", "dSOCmax"]
        writer.writerow(header)
        for bkr, rot_info in rotations.iteritems():
            for rot_id, crop_data in rot_info.iteritems():
                rot_data = yr_df.loc[yr_df['rotation'] == int(rot_id)]
                #print rot_data
                for year in range(1975,2006):
                    line = []
                    if year == 1975:
                        line.append(rot_id)
                        line.append(year)
                        line.append(100)
                        line.append(0)
                        line.append(0)
                        line.append(0)
                    else:                        
                        year_data = rot_data.loc[rot_data["year"] == year]
                        #print year_data
                        line.append(rot_id)
                        line.append(year)
                        line.append(year_data["deltaOC"].mean())
                        line.append(year_data["deltaOC"].std())
                        line.append(year_data["deltaOC"].min())
                        line.append(year_data["deltaOC"].max())
                    writer.writerow(line)
    

#kreise_N_amount()
soc_trajectories()

print "finished!"