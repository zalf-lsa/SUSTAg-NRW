import os
import csv

basepath = os.path.dirname(os.path.abspath(__file__))
dir_name = basepath + "/out/out-020218-baseline-POT/"
out_dir = basepath + "/out/splitted-out/"


#extract_vars_cp = ["IDcell", "crop", "rotation", "yield", "Nleach", "Nminfert", "RemovalRate", "FertMethod"]
extract_vars_cp = ["crop", "rotation", "yield", "ReturnResidues"]
extract_vars_yr = ["IDcell", "rotation", "Nleach", "deltaOC"]

def split(suffix, extract_vars, tag_bkr=True, calc_hi=False, pot_cp_residue=False, excludecc=False):
    for filename in os.listdir(dir_name):
        if ".csv" in filename and suffix in filename:
            print("opening " + filename)
            bkr = filename.split("_")[0]
            towrite = []
            with open(dir_name + filename) as file_:
                reader = csv.reader(file_, delimiter=",")
                header = reader.next()
                field_map = {}
                for i in range(len(header)):
                    field_map[header[i]] = i
                for row in reader:
                    if suffix == "_crop" and excludecc:
                        if "mustard" in row[field_map["crop"]]:
                            continue
                    line = []
                    for v in extract_vars:
                        line.append(row[field_map[v]])                        
                    if tag_bkr:
                        line.append(bkr)
                    if suffix == "_crop" and calc_hi:
                        hi = str(float(row[field_map["yield"]]) / float(row[field_map["agb"]]))
                        line.append(hi)
                    if suffix == "_crop" and pot_cp_residue:
                        if "potato" in row[field_map["crop"]] or "beet" in row[field_map["crop"]]:
                            cp_res = row[field_map["agb"]]
                        else:
                            cp_res = str(float(row[field_map["agb"]]) - float(row[field_map["yield"]]))
                        line.append(cp_res)
                    towrite.append(line)
            out_name = out_dir + filename
            with open(out_name, 'wb') as _:
                writer = csv.writer(_, delimiter=",")
                out_header = []
                for ev in extract_vars:
                    out_header.append(ev)
                if tag_bkr:
                    out_header.append("bkr")
                if suffix == "_crop" and calc_hi:
                    out_header.append("hi")
                if suffix == "_crop" and pot_cp_residue:
                    out_header.append("pot_residues")
                writer.writerow(out_header)
                for row_ in towrite:
                    writer.writerow(row_)
            print(filename + " done!")

split("_crop", extract_vars_cp, tag_bkr=True, calc_hi=False, pot_cp_residue=False, excludecc=False)
#split("_year", extract_vars_yr, tag_bkr=True)

def add_out_colums(col_names, col_vals, directory):
    #create a new file with added cols
    for filename in os.listdir(dir_name):
        with open(dir_name + filename) as file_:
            with open(dir_name + "_new_" + filename, 'wb') as _:
                writer = csv.writer(_, delimiter=",")
                print("opening " + filename)
                reader = csv.reader(file_, delimiter=",")
                header = reader.next()
                for var in col_names:
                    header.append(var)
                writer.writerow(header)
                for row in reader:
                    for val in col_vals:
                        row.append(val)
                    writer.writerow(row)
    #delete old files
    for filename in os.listdir(dir_name):
        if "_new_" not in filename:
            os.remove(dir_name + filename)
    #rename new files
    for filename in os.listdir(dir_name):
        new_name = filename.replace("_new_", "")
        os.rename(dir_name + filename, dir_name + new_name)

#col_names = ["RemovalRate", "FertMethod"]
#col_vals = ["0", "Nmin"]

#add_out_colums(col_names, col_vals, dir_name)

print("finished")