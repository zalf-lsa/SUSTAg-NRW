import os
import csv

basepath = os.path.dirname(os.path.abspath(__file__))
dir_name = basepath + "/out/"
out_dir = dir_name + "/splitted-out/"

#extract_vars_cp = ["IDcell", "rotation", "crop", "agb", "yield"]
extract_vars_cp = ["rotation", "crop", "Nminfert"]
extract_vars_yr = ["year", "rotation", "deltaOC"]

def split(suffix, extract_vars, tag_bkr=True):
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
                    line = []
                    for v in extract_vars:
                        line.append(row[field_map[v]])
                    if tag_bkr:
                        line.append(bkr)
                    towrite.append(line)
            out_name = out_dir + filename
            with open(out_name, 'wb') as _:
                writer = csv.writer(_, delimiter=",")
                out_header = []
                for ev in extract_vars:
                    out_header.append(ev)
                if tag_bkr:
                    out_header.append("bkr")
                writer.writerow(out_header)
                for row_ in towrite:
                    writer.writerow(row_)
            print(filename + " done!")

split("_crop", extract_vars_cp)
#split("_year", extract_vars_yr, True)

print("finished")