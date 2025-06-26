# matplotlib code written by Nils Blach

import re
import matplotlib
import matplotlib.pyplot as plt
import os
import statistics

bfs_xvalues = []
khop_xvalues = []
gda_bfs_time = []
g500_bfs_time = []
khop_latency = [[],[],[]]

# BFS: GDA
scale=23
for i in ("0001", "0002", "0004", "0008", "0016", "0032", "0064", "0128", "0256", "0512", "1024", "2048"):
  bfs_xvalues.append(scale)
  fp = open( "results/n" + i + "_s" + str(scale) + "_e16.bfs/lsb.gdi_bfs.r0", "rt" )
  rdr = [list(filter(None, i.split(' '))) for i in list(filter(lambda row: row[0]!='#', fp))[1:]]
  times = []
  for row in rdr:
    times.append(float(row[2]))
  gda_bfs_time.append(statistics.median(times))
  fp.close()
  scale+= 1

# BFS: Graph500
scale=23
for i in ("0001", "0002", "0004", "0008", "0016", "0032", "0064", "0128", "0256", "0512", "1024", "2048"):
  fp = open( "results/n" + i + "_s" + str(scale) + "_e16.bfs/bfs_times.txt", "rt" )
  rdr = [list(filter(None, re.split(r'[\t\n]', i))) for i in list(fp)]
  times = []
  for row in rdr:
    times.append(float(row[1]))
  g500_bfs_time.append(statistics.median(times))
  fp.close()
  scale+= 1

# k-Hop: GDA
scale=23
for i in ("0001", "0002", "0004", "0008", "0016", "0032", "0064", "0128", "0256", "0512", "1024"):
  cmp_root = -1;
  khop_xvalues.append(scale)
  fp = open( "results/n" + i + "_s" + str(scale) + "_e16.khop/lsb.gdi_k_hop.r0", "rt" )
  rdr = [list(filter(None, i.split(' '))) for i in list(filter(lambda row: row[0]!='#', fp))[1:]]
  latency = [[],[],[]]
  for row in rdr:
    if cmp_root == -1:
      cmp_root = int(row[0])
    if int(row[0]) == cmp_root and int(row[1]) > 1:
      latency[int(row[1])-2].append(float(row[2]))
  for j in range(3):
    khop_latency[j].append(statistics.median(latency[j]))
  fp.close()
  scale+= 1

# time in seconds
gda_bfs_time = [val / 1000000 for val in gda_bfs_time]
for i in range(3):
  khop_latency[i] = [val / 1000000 for val in khop_latency[i]]

matplotlib.rc('pdf')
fig, ax = plt.subplots(1,1, figsize = (7.25,3.5))
plt.subplots_adjust(left=0.2, right = 0.95, top = 0.95, bottom = 0.15)

linestyle = "-"

marker = "o"
color = "#009900"
label = "2-Hop GDA"

ax.plot(khop_xvalues, khop_latency[0], fillstyle='none', ls=linestyle, marker = marker, color = color, label = label)

marker = "x"
color = "#000099"
label = "3-Hop GDA"

ax.plot(khop_xvalues, khop_latency[1], fillstyle='none', ls=linestyle, marker = marker, color = color, label = label)

marker = "^"
color = "#990000"
label = "4-Hop GDA"

ax.plot(khop_xvalues, khop_latency[2], fillstyle='none', ls=linestyle, marker = marker, color = color, label = label)

marker = "d"
color = "#990099"
label  = "BFS GDA"

ax.plot(bfs_xvalues, gda_bfs_time, fillstyle='none', ls=linestyle, marker = marker, color = color, label = label)

marker = "*"
color = "#999900"
label = "BFS Graph500"

ax.plot(bfs_xvalues, g500_bfs_time, fillstyle='none', ls=linestyle, marker = marker, color = color, label = label)

# rows = ["#servers", "#vertices", "#edges"]
# data = [[    "1",     "2",     "4",     "8",   "16",   "32",   "64",   "128",   "256",   "512", "1024",  "2048"],
#         [ "8.4M", "16.8M", "33.6M", "67.1M", "134M", "268M", "537M",  "1.1B",  "2.1B",  "4.3B", "8.6B", "17.2B"],
#         [ "134M",  "268M",  "537M",  "1.1B", "2.1B", "4.3B", "8.6B", "17.2B", "34.4B", "68.7B", "137B",  "275B"]]

# add a table at the bottom of the axes
# the_table = plt.table(cellText=data, rowLabels=rows, cellLoc='center', loc='bottom', edges='open')
# the_table.auto_set_font_size(False)
# the_table.set_fontsize(9)

# set ticks for the x axis, so that the grid works, but don't display them
plt.xticks(bfs_xvalues)
# ax.tick_params(axis='x', which='both', bottom=False, labelbottom=False)
plt.xlabel( "Scale", fontsize=12, labelpad=1)

plt.yticks(fontsize=12)
ax.tick_params(axis='y', pad=2)
plt.ylabel( "Runtime [s]", fontsize=12, labelpad=-1)
plt.yscale("log")

# grid
ax.grid(True, which="both")
box = ax.get_position()
ax.set_position([box.x0, box.y0, box.width, box.height*0.8])

# legend
ax.legend( loc="upper center", bbox_to_anchor=(0.5,1.02), fontsize = 8.2, columnspacing = 0.4, labelspacing = 0.2, ncol=3 )

plt.savefig("bfs_khop_comb_weak_scaling.pdf", bbox_inches='tight')
