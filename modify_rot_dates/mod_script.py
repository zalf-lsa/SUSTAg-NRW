import json

with open("rotations_template.json") as _:
    rots = json.load(_)

with open("sow_harv_dates.json") as _:
    ref_dates = json.load(_)

for bkr in rots:
    for rot in rots[bkr]:
        for cp in rots[bkr][rot]:
            for ws in cp["worksteps"]:
                if "crop" in ws.keys():
                    cp_name = ws["crop"][2]
                    if cp_name == "WTr":
                        #use wheat data for triticale
                        cp_name = "WW"
                    break
            if cp_name != "PO": #no potato data was provided by TGaiser
                for ws in cp["worksteps"]:
                    if ws["type"] in ref_dates[bkr][cp_name].keys():
                        ws["date"] = ref_dates[bkr][cp_name][ws["type"]]

with open("rotations.json", "w") as _:
    _.write(json.dumps(rots, indent=4))

print "done!"
