# matplotlib code written by Nils Blach

import re
import matplotlib
import matplotlib.pyplot as plt
import statistics

nnodes = []
g500_nnodes = []

gda_bfs_time = []
g500_bfs_time = []

latency_khop = [[],[],[]]

# BFS: GDA
for i in ("008", "016", "024", "032", "040", "048", "056", "064"):
  fp = open( "results/n" + i + "_s26_e16.bfs/lsb.gdi_bfs.r0", "rt" )
  rdr = [list(filter(None, i.split(' '))) for i in list(filter(lambda row: row[0]!='#', fp))[1:]]
  times = []
  for row in rdr:
    times.append(float(row[2]))
  nnodes.append(int(i));
  gda_bfs_time.append(statistics.median(times))
  fp.close()

# BFS: Graph500
for i in ("008", "016", "032", "064"):
  fp = open( "results/n" + i + "_s26_e16.bfs/bfs_times.txt", "rt" )
  rdr = [list(filter(None, re.split(r'[\t\n]', i))) for i in list(fp)]
  times = []
  for row in rdr:
    times.append(float(row[1]))
  g500_nnodes.append(int(i));
  g500_bfs_time.append(statistics.median(times))
  fp.close()

# k-Hop: GDA
cmp_root = -1
for i in ("008", "016", "024", "032", "040", "048", "056", "064"):
  fp = open( "results/n" + i + "_s26_e16.khop/lsb.gdi_k_hop.r0", "rt" )
  rdr = [list(filter(None, i.split(' '))) for i in list(filter(lambda row: row[0]!='#', fp))[1:]]
  latency = [[],[],[]]
  for row in rdr:
    if cmp_root == -1:
      cmp_root = int(row[0])
    if int(row[0]) == cmp_root and int(row[1]) > 1:
      latency[int(row[1])-2].append(float(row[2]))
  for j in range(3):
    latency_khop[j].append(statistics.median(latency[j]))
  fp.close()


# time in seconds
gda_bfs_time = [val / 1000000 for val in gda_bfs_time]
for i in range(3):
  latency_khop[i] = [val / 1000000 for val in latency_khop[i]]

matplotlib.rc('pdf')
fig, ax = plt.subplots(1,1, figsize = (7.25,3.5))
plt.subplots_adjust(left=0.2, right = 0.95, top = 0.95, bottom = 0.15)

linestyle = "-"

marker = "o"
color = "#009900"
label = "2-Hop GDA"

ax.plot(nnodes, latency_khop[0], fillstyle='none', ls=linestyle, marker = marker, color = color, label = label)

marker = "x"
color = "#000099"
label = "3-Hop GDA"

ax.plot(nnodes, latency_khop[1], fillstyle='none', ls=linestyle, marker = marker, color = color, label = label)

marker = "^"
color = "#990000"
ax.plot(nnodes, latency_khop[2], fillstyle='none', ls=linestyle, marker = marker, color = color, label = label)

marker = "d"
color = "#990099"
label = "BFS GDA"

ax.plot(nnodes, gda_bfs_time, fillstyle='none', ls=linestyle, marker = marker, color = color, label = label)

marker = "*"
color = "#999900"
label = "BFS Graph500"

ax.plot(g500_nnodes, g500_bfs_time, fillstyle='none', ls=linestyle, marker = marker, color = color, label = label)

plt.xticks(ticks=[8, 16, 24, 32, 40, 48, 56, 64], fontsize=9)
plt.yticks(fontsize=12)
ax.tick_params(axis='x', pad=2)
ax.tick_params(axis='y', pad=2)
plt.xlabel( "Servers", fontsize=9, labelpad=1)
plt.ylabel( "Runtime [s]", fontsize=12, labelpad=-1)
plt.yscale("log")

# grid
ax.grid(True, which="both")
box = ax.get_position()
ax.set_position([box.x0, box.y0, box.width, box.height*0.8])

# legend
ax.legend( loc="upper center", bbox_to_anchor=(0.717,1.02), fontsize = 8.2, columnspacing = 0.4, labelspacing = 0.2, ncol=2 )

plt.savefig("bfs_khop_comb_strong_scaling.pdf", bbox_inches='tight')
