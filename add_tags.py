import os
import csv

dir_name = "Z:/projects/sustag/test/"

for filename in os.listdir(dir_name):
    print("opening " + filename)
    towrite = []
    
    with open(dir_name + filename) as file_:
        name_parts = filename.split(".")[0].split("_")
        bkr = name_parts[0]
        tf = name_parts[2]
        fert = name_parts[3].split("-")[1]
        res = name_parts[4].split("-")[1]
        cc =  name_parts[5].split("-")[1]
        pl =  name_parts[6].split("-")[1]
        
        reader = csv.reader(file_, delimiter=",")
        
        header = reader.next()
        header.append("bkr")
        header.append("tf")
        header.append("fert")
        header.append("res")
        header.append("cc")
        header.append("pl")
        towrite.append(header)
        
        for row in reader:
            row.append(bkr)
            row.append(tf)
            row.append(fert)
            row.append(res)
            row.append(cc)
            row.append(pl)

            towrite.append(row)
    
    with open(dir_name + filename, 'wb') as _:
        writer = csv.writer(_, delimiter=",")
        for row_ in towrite:
            writer.writerow(row_)
        print(filename + " done!")
        
