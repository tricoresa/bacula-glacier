#!/usr/bin/env python
import json

partlist = {}
data = {}

for index in range(1,30):
    partlist[index] = [index, index]

data["partlist"]=partlist
outfile = open("quicktest.out.json", "w")
json.dump(data, outfile)
outfile.close()
