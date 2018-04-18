import csv
from collections import defaultdict
import pandas as pd
import numpy as np
import json
import os
import matplotlib.pyplot as plt
from matplotlib import colors

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

def soc_trajectories(start_year, end_year):
    print("reading files...")
    df_yr_129 = pd.read_csv("out/splitted-out/129_year.csv")
    df_yr_134 = pd.read_csv("out/splitted-out/134_year.csv")
    df_yr_141 = pd.read_csv("out/splitted-out/141_year.csv")
    df_yr_142 = pd.read_csv("out/splitted-out/142_year.csv")
    df_yr_143 = pd.read_csv("out/splitted-out/143_year.csv")
    df_yr_146 = pd.read_csv("out/splitted-out/146_year.csv")
    df_yr_147 = pd.read_csv("out/splitted-out/147_year.csv")
    df_yr_148 = pd.read_csv("out/splitted-out/148_year.csv")
    df_yr_191 = pd.read_csv("out/splitted-out/191_year.csv")

    print("concatenating data frames...")
    yr_frames = [df_yr_129, df_yr_134, df_yr_141, df_yr_142, df_yr_143, df_yr_146, df_yr_147, df_yr_148, df_yr_191]
    yr_df = pd.concat(yr_frames)

    with open("rotations_dynamic_harv.json") as _:
        rotations = json.load(_)

    #summary_data = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
    
    print("writing output file...")
    with open("SOC_trajectories.csv", "wb") as _:
        writer = csv.writer(_, delimiter=",")
        header = ["rotation", "year", "dSOCavg", "dSOCstd", "dSOCmin", "dSOCmax"]
        writer.writerow(header)
        for bkr, rot_info in rotations.iteritems():
            for rot_id, crop_data in rot_info.iteritems():
                rot_data = yr_df.loc[yr_df['rotation'] == int(rot_id)]
                #print rot_data
                for year in range(start_year, end_year + 1):
                    line = []
                    if year == start_year:
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
#soc_trajectories(2006, 2030)

def produce_plot(filename, x_vals, y_dict, deltaSOC_dict, title):
    colors = {
        "light": "silver",
        "medium": "gray",
        "heavy": "black"
    }
    plt.clf()
    plt.title(title)
    plt.ylim(70, 110)
    for soil_type, relSOC_serie in y_dict.items():
        avg_deltaSOC = round(np.mean(deltaSOC_dict[soil_type]),2)
        plt.plot(x_vals, relSOC_serie, "-", color=colors[soil_type], label=soil_type + ", avg deltaOC: " + str(avg_deltaSOC))
    plt.xlabel('year')
    plt.ylabel('rel SOC')
    plt.legend()
    plt.savefig(filename)
    text = 'A figure has been saved as ' + filename
    print(text)

def soc_trajectories_plus(start_year, end_year):

    base_dir = "C:/Users/stella/Documents/GitHub/SUSTAg-NRW/out/id23/splitted/23/"

    ids_sim = ["23"]
    soil_types = ["heavy", "medium", "light"]

    for f_type in ["_year"]:
        dframes = []
        merged_df = None
        for f in os.listdir(base_dir):
            if ".csv" not in f:
                continue
            fname = f.split("_")
            id_sim = fname[1][2:]
            if f_type in f and id_sim in ids_sim:
                print(" appending " + f)
                dframes.append(pd.read_csv(base_dir + "/" + f))
        print("concatenating data frames...")
        merged_df = pd.concat(dframes)

    with open("rotations_dynamic_harv.json") as _:
        rotations = json.load(_)

    print("writing output file...")
    with open(base_dir + "/SOC/SOC_trajectories.csv", "wb") as _:
        writer = csv.writer(_, delimiter=",")
        header = ["rotation", "year", "dSOCavg", "dSOCstd", "dSOCmin", "dSOCmax", "soiltype", "relSOC"]
        writer.writerow(header)
        for bkr, rot_info in rotations.iteritems():
            for rot_id, crop_data in rot_info.iteritems():
                
                #initialize data structures for plotting
                years = []
                for year in range(start_year, end_year + 1):
                    years.append(year)
                SOC_traj = defaultdict(list)
                deltaSOCs = defaultdict(list)

                for soil_t in soil_types:
                    rot_data = merged_df.loc[(merged_df['rotation'] == int(rot_id)) & (merged_df['soiltype'] == soil_t)]
                    if rot_data.empty:
                        continue
                    #print rot_data
                    relSOC = 100
                    for year in range(start_year, end_year + 1):
                        line = []                        
                        if year == start_year:
                            line.append(rot_id)
                            line.append(year)
                            line.append(0)
                            line.append(0)
                            line.append(0)
                            line.append(0)
                            line.append(soil_t)
                            line.append(relSOC)
                        else: 
                            year_data = rot_data.loc[rot_data["year"] == year]
                            avg_deltaOC = year_data["deltaOC"].mean()
                            relSOC += relSOC * avg_deltaOC/100
                            #print year_data
                            line.append(rot_id)
                            line.append(year)
                            line.append(avg_deltaOC)
                            line.append(year_data["deltaOC"].std())
                            line.append(year_data["deltaOC"].min())
                            line.append(year_data["deltaOC"].max())
                            line.append(soil_t)
                            line.append(relSOC)
                            deltaSOCs[soil_t].append(avg_deltaOC)
                        SOC_traj[soil_t].append(relSOC)                        
                        writer.writerow(line)
                produce_plot(base_dir + "/SOC/" + str(rot_id) + ".png", years, SOC_traj, deltaSOCs, title=str(rot_id))

#soc_trajectories_plus(2011, 2030)

def aggregate_results():

    def one_liner(df):
        line = []
        no_avg = ["IDcell", "crop", "rotation", "soiltype", "id", "bkr", "tf", "fert", "res", "cc", "pl"]
        for column in df:
            if column in no_avg:
                line.append(df[column].iloc[0])
            else:
                line.append(round(df[column].mean(), 2))
        return line
    
    def prepare_header(df):
        header = []
        for column in df:
            header.append(column)
        return header

    base_dir = "C:/Users/stella/Documents/GitHub/SUSTAg-NRW/out/out-2018-04-16-EUBCE-processed/splitted/"
    for dirName, subdirList, fileList in os.walk(base_dir):
        #print('Found directory: %s' % dirName)
        for fname in fileList:
            print("reading " + fname)
            towrite = []
            header = []
            #print('\t%s' % fname)
            my_df = pd.read_csv(dirName + "/" + fname)
            #print my_df.head()
            id_cells = set(my_df["IDcell"])

            for cell in id_cells:
                cell_data = my_df.loc[(my_df["IDcell"] == cell)]
                rotations = set(cell_data["rotation"])

                for rot in rotations:
                    cell_rot_data = cell_data.loc[(cell_data["rotation"] == rot)]

                    if len(header) == 0:
                        header = prepare_header(cell_rot_data)
                        towrite.append(header)

                    if "_year" in fname:
                        towrite.append(one_liner(cell_rot_data))

                    if "_crop" in fname:
                        crops = set(cell_rot_data["crop"])
                        for cp in crops:
                            cell_rot_crop_data = cell_rot_data.loc[(cell_rot_data["crop"] == cp)]
                            towrite.append(one_liner(cell_rot_crop_data))
            
            out_dir = dirName + "/aggregated/"
            if not os.path.exists(out_dir):
                os.makedirs(out_dir)

            with open(out_dir + "/" + fname, "wb") as _:
                writer = csv.writer(_)
                for line in towrite:
                    writer.writerow(line)        

#aggregate_results()

def concatenate_files():    
    base_dir = "C:/Users/stella/Documents/GitHub/SUSTAg-NRW/out/out-2018-04-16-EUBCE-processed/splitted/"
    id_sim = ""
    for dirName, subdirList, fileList in os.walk(base_dir):
        print('Found directory: %s' % dirName)
        if "aggregated" in dirName:
            for f_type in ["_year", "_crop"]:
                dframes = []
                for fname in fileList:
                    fn = fname.split("_")
                    id_sim = fn[1][2:]
                    if f_type in fname:
                        print(" appending " + fname)
                        dframes.append(pd.read_csv(dirName + "/" + fname))
                print("concatenating data frames...")
                merged_df = pd.concat(dframes)
                merged_df.to_csv("C:/Users/stella/Documents/GitHub/SUSTAg-NRW/out/EUBCE/" + id_sim + f_type + ".csv")
                print("id sim " + id_sim + f_type + " processed!")

concatenate_files()

print "finished!"