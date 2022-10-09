from preprocessing import *
import os

filedir = "datafiles"
filename = "16.txt"
processed_data = "".join(preprocessing(filedir + "/" + filename))[9:]
if not os.path.exists("processd"):
    os.mkdir("processd")
f = open(r"processed/" + filename, "w")
f.write(processed_data)
f.close()
