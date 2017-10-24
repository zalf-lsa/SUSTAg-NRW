import json
import copy
from datetime import date, timedelta

def fixed_sowing_harvest():
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

    with open("rotations_fixed_harv.json", "w") as _:
        _.write(json.dumps(rots, indent=4))

def dynamic_harvest():
    with open("rotations_template.json") as _:
        rots = json.load(_)

    with open("sow_harv_dates.json") as _:
        ref_dates = json.load(_)

    with open("automatic_harvest.json") as _:
        harv_template = json.load(_)

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
                for i in range(len(cp["worksteps"])):
                    ws = cp["worksteps"][i]
                    if ws["type"] in ref_dates[bkr][cp_name].keys():
                        if ws["type"] == "Harvest":
                            new_harv = copy.deepcopy(harv_template)
                            ref_harv = ref_dates[bkr][cp_name][ws["type"]]
                            ref_harv = ref_harv.split("-")
                            h_date = date(2017, int(ref_harv[1]), int(ref_harv[2]))
                            h_date = h_date + timedelta(days = 15)
                            new_harv["latest-date"] = unicode(ref_harv[0] + "-" + str(h_date.month).zfill(2) + "-" + str(h_date.day).zfill(2))
                            cp["worksteps"][i] = new_harv
                        else:
                            ws["date"] = ref_dates[bkr][cp_name][ws["type"]]

    with open("rotations_dynamic_harv.json", "w") as _:
        _.write(json.dumps(rots, indent=4))


#fixed_sowing_harvest()
dynamic_harvest()

print "done!"
