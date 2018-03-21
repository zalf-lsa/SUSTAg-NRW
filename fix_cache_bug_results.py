import sys
import os

def main():

    #path = "P:/sustag/out-NRW-2018-02-22/"
    #path = "P:/sustag/bla/"
    path = "C:/Users/berg.ZALF-AD/GitHub/SUSTAg-NRW/out/"

    files = {}
    for filename in os.listdir(path):

        parts = filename.split("_")
        bkr_id = int(parts[0])
        setup_id = int(parts[1][2:])
        crop_or_year = parts[7][:4]

        files[(bkr_id, setup_id, crop_or_year)] = [path, filename]

    def move_lines(crop_or_year, first_id_cell, from_setup_id, to_setup_id):
        "move lines from one file to other"
        fp21 = files[(bkr_id, from_setup_id, crop_or_year)]
        fp20 = files[(bkr_id, to_setup_id, crop_or_year)] 
        
        os.rename("".join(fp21), fp21[0] + "_" + fp21[1])

        f_21 = open(fp21[0] + "_" + fp21[1])
        f21 = open("".join(fp21), "w")
        f20 = open("".join(fp20), "a")

        f21.write(f_21.readline())
        #move lines to other file while the first id cell has not been reached
        moved_lines = 0
        while True:
            l = f_21.readline()
            id_cell = int(l[:6])
            if id_cell != first_id_cell:
                f20.write(l)
                moved_lines += 1
            else:
                f21.write(l)
                break
        
        #copy the rest to the clean file
        while True:
            l = f_21.readline()
            if l:
                f21.write(l)
            else:
                break  

        f_21.close()
        f21.close()
        f20.close()

        print "moved", moved_lines, "from", fp21, "to", fp20


    for bkr_id, first_id_cell in [(129, 505058), (134, 402211), (141, 479045), (142, 360157), (143, 392152), (146, 309204), (147, 333197), (148, 369081), (191, 303128)]:
        #move_lines("crop", first_id_cell, 21, 20)
        move_lines("year", first_id_cell, 21, 20)

        #move_lines("crop", first_id_cell, 11, 10)
        #move_lines("crop", first_id_cell, 10, 9)

        move_lines("year", first_id_cell, 11, 10)
        move_lines("year", first_id_cell, 10, 9)
 
        #move_lines("crop", first_id_cell, 5, 4)
        #move_lines("crop", first_id_cell, 4, 3)

        move_lines("year", first_id_cell, 5, 4)
        move_lines("year", first_id_cell, 4, 3)


if __name__ == "__main__":
    main()



