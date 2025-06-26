# GNN: graph with scale = 25 and edge factor = 16 (strong scaling)

# matplotlib code written by Nils Blach

import matplotlib
import matplotlib.pyplot as plt
import statistics

nnodes = []
time_v004 = []
time_v016 = []
time_v064 = []
time_v256 = []
time_v500 = []

for i in ("008", "016", "024", "032", "040", "048", "056", "064"):
  # feature vector size = 4
  nnodes.append(int(i))
  fp = open( "results/n" + i + "_s25_e16_v004.gnn/lsb.gdi_gnn.r0", "rt" )
  rdr = [list(filter(None, i.split(' '))) for i in list(filter(lambda row: row[0]!='#', fp))[1:]]
  times = []
  for row in rdr:
    times.append(float(row[3]))
  time_v004.append(statistics.median(times))
  fp.close()

  # feature vector size = 16
  fp = open( "results/n" + i + "_s25_e16_v016.gnn/lsb.gdi_gnn.r0", "rt" )
  rdr = [list(filter(None, i.split(' '))) for i in list(filter(lambda row: row[0]!='#', fp))[1:]]
  times = []
  for row in rdr:
    times.append(float(row[3]))
  time_v016.append(statistics.median(times))
  fp.close()

  # feature vector size = 64
  fp = open( "results/n" + i + "_s25_e16_v064.gnn/lsb.gdi_gnn.r0", "rt" )
  rdr = [list(filter(None, i.split(' '))) for i in list(filter(lambda row: row[0]!='#', fp))[1:]]
  times = []
  for row in rdr:
    times.append(float(row[3]))
  time_v064.append(statistics.median(times))
  fp.close()

  # feature vector size = 256
  fp = open( "results/n" + i + "_s25_e16_v256.gnn/lsb.gdi_gnn.r0", "rt" )
  rdr = [list(filter(None, i.split(' '))) for i in list(filter(lambda row: row[0]!='#', fp))[1:]]
  times = []
  for row in rdr:
    times.append(float(row[3]))
  time_v256.append(statistics.median(times))
  fp.close()

  # feature vector size = 500
  fp = open( "results/n" + i + "_s25_e16_v500.gnn/lsb.gdi_gnn.r0", "rt" )
  rdr = [list(filter(None, i.split(' '))) for i in list(filter(lambda row: row[0]!='#', fp))[1:]]
  times = []
  for row in rdr:
    times.append(float(row[3]))
  time_v500.append(statistics.median(times))
  fp.close()

# time in seconds
time_v004 = [val / 1000000 for val in time_v004]
time_v016 = [val / 1000000 for val in time_v016]
time_v064 = [val / 1000000 for val in time_v064]
time_v256 = [val / 1000000 for val in time_v256]
time_v500 = [val / 1000000 for val in time_v500]

matplotlib.rc('pdf')
fig, ax = plt.subplots(1,1, figsize = (7.25,3.5))
plt.subplots_adjust(left=0.2, right = 0.95, top = 0.95, bottom = 0.15)

linestyle = "-"
marker = "o"
color = "#009900"
label = "GDA k=4"

ax.plot(nnodes, time_v004, fillstyle='none', ls=linestyle, marker = marker, color = color, label = label)

marker = "x"
color = "#000099"
label = "GDA k=16"

ax.plot(nnodes, time_v016, fillstyle='none', ls=linestyle, marker = marker, color = color, label = label)

marker = "^"
color = "#990000"
label = "GDA k=64"

ax.plot(nnodes, time_v064, fillstyle='none', ls=linestyle, marker = marker, color = color, label = label)

marker = "d"
color = "#990099"
label = "GDA k=256"

ax.plot(nnodes, time_v256, fillstyle='none', ls=linestyle, marker = marker, color = color, label = label)

marker = "*"
color = "#999900"
label = "GDA k=500"

ax.plot(nnodes, time_v500, fillstyle='none', ls=linestyle, marker = marker, color = color, label = label)

plt.xticks(ticks=[8, 16, 24, 32, 40, 48, 56, 64], fontsize=9)
plt.yticks(fontsize=12)
ax.tick_params(axis='x', pad=2)
ax.tick_params(axis='y', pad=2)
plt.xlabel( "Servers", fontsize=9, labelpad=1)
plt.ylabel( "Runtime [s]", fontsize=12, labelpad=-1)

# grid
ax.grid(True, which="both")
box = ax.get_position()
ax.set_position([box.x0, box.y0, box.width, box.height*0.8])

# legend
ax.legend( loc="upper center", bbox_to_anchor=(0.55,1.0), fontsize = 8.2, columnspacing = 0.4, labelspacing = 0.2, ncol=3 )

plt.savefig("gnn_strong_scaling.pdf", bbox_inches='tight')
