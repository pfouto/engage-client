#!/usr/bin/env python3

import json

f = open('config/tree_16_4zones.json')

data = json.load(f)

nodes = data['nodes']

output = open("config/latency_map", "w")
for i in nodes:
    for j in nodes:
        if j == i:
            output.write("-1  ")
        elif nodes[i]['region'] == nodes[j]['region']:
            output.write("35  ")
        else:
            output.write("100 ")
    output.write("\n")
