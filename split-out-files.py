import os
import csv

basepath = os.path.dirname(os.path.abspath(__file__))
dir_name = basepath + "/out/"
out_dir = dir_name + "/splitted-out/"

extract_vars_cp = ["IDcell", "rotation", "crop", "agb", "yield"]
extract_vars_yr = ["IDcell", "rotation", "deltaOC", "waterperc", "Nleach"]

def split(suffix, extract_vars):
    for filename in os.listdir(dir_name):
        if ".csv" in filename and suffix in filename:
            print("opening " + filename)
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
                    towrite.append(line)
            out_name = out_dir + filename
            with open(out_name, 'wb') as _:
                writer = csv.writer(_, delimiter=",")
                writer.writerow(extract_vars)
                for row_ in towrite:
                    writer.writerow(row_)
            print(filename + " done!")

split("_crop", extract_vars_cp)
#split("_year", extract_vars_yr)

print("finished")