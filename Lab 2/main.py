from preprocessing import *
import os

filedir = "datafiles"
filename = "16.txt"
processed_data = "".join(preprocessing(filedir + "/" + filename))[9:]
if not os.path.exists("processed"):
    os.mkdir("processed")
f = open(os.path.abspath("processed/" + filename), "w")
f.writelines(processed_data)
f.close()
