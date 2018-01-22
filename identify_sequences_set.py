import json
import csv

rotations_file = "rotations_dynamic_harv.json"

with open(rotations_file) as _:
    rotations = json.load(_)

all_rotations = []
all_sequences = set() #set of sequences (crop, previous crop)

#collect all the available rotations
for bkr in rotations.keys():
    for rot_id in rotations[bkr].keys():
        rot = []
        for cm in rotations[bkr][rot_id]:
            for ws in cm["worksteps"]:
                if ws["type"] == "Sowing":
                    rot.append(ws["crop"][2])
        all_rotations.append(rot)

#find all the available sequences
def rotate(crop_rotation):
    crop_rotation.insert(0, crop_rotation.pop())

for rot in all_rotations:
    for i in range(len(rot)):
        #print((rot[1], rot[0]))
        all_sequences.add((rot[1], rot[0]))
        rotate(rot)

#write output
with open("crop_sequences.csv", "wb") as outfile:
    writer = csv.writer(outfile)
    for seq in all_sequences:
        outrow = []
        outrow.append(seq[0])
        outrow.append(seq[1])
        writer.writerow(outrow)

print("finished!")
