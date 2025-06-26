# GNN: graph with edge factor = 16 (weak scaling)

# matplotlib code written by Nils Blach

import matplotlib
import matplotlib.pyplot as plt
import statistics

xvalues = []
time_v004 = []
time_v016 = []
time_v064 = []
time_v256 = []
time_v500 = []

scale = 22
for i in ("0001", "0002", "0004", "0008", "0016", "0032", "0064", "0128", "0256", "0512", "1024"):
  # feature vector size = 4
  xvalues.append(scale)
  fp = open( "results/n" + i + "_s" + str(scale) + "_e16_v004.gnn/lsb.gdi_gnn.r0", "rt" )
  rdr = [list(filter(None, i.split(' '))) for i in list(filter(lambda row: row[0]!='#', fp))[1:]]
  times = []
  for row in rdr:
     times.append(float(row[3]))
  time_v004.append(statistics.median(times))
  fp.close()

  # feature vector size = 16
  fp = open( "results/n" + i + "_s" + str(scale) + "_e16_v016.gnn/lsb.gdi_gnn.r0", "rt" )
  rdr = [list(filter(None, i.split(' '))) for i in list(filter(lambda row: row[0]!='#', fp))[1:]]
  times = []
  for row in rdr:
     times.append(float(row[3]))
  time_v016.append(statistics.median(times))
  fp.close()

  # feature vector size = 64
  fp = open( "results/n" + i + "_s" + str(scale) + "_e16_v064.gnn/lsb.gdi_gnn.r0", "rt" )
  rdr = [list(filter(None, i.split(' '))) for i in list(filter(lambda row: row[0]!='#', fp))[1:]]
  times = []
  for row in rdr:
     times.append(float(row[3]))
  time_v064.append(statistics.median(times))
  fp.close()

  # feature vector size = 256
  fp = open( "results/n" + i + "_s" + str(scale) + "_e16_v256.gnn/lsb.gdi_gnn.r0", "rt" )
  rdr = [list(filter(None, i.split(' '))) for i in list(filter(lambda row: row[0]!='#', fp))[1:]]
  times = []
  for row in rdr:
     times.append(float(row[3]))
  time_v256.append(statistics.median(times))
  fp.close()

  # feature vector size = 500
  fp = open( "results/n" + i + "_s" + str(scale) + "_e16_v500.gnn/lsb.gdi_gnn.r0", "rt" )
  rdr = [list(filter(None, i.split(' '))) for i in list(filter(lambda row: row[0]!='#', fp))[1:]]
  times = []
  for row in rdr:
     times.append(float(row[3]))
  time_v500.append(statistics.median(times))
  fp.close()
  scale+= 1

# time in minutes
time_v004 = [val / (60 * 1000000) for val in time_v004]
time_v016 = [val / (60 * 1000000) for val in time_v016]
time_v064 = [val / (60 * 1000000) for val in time_v064]
time_v256 = [val / (60 * 1000000) for val in time_v256]
time_v500 = [val / (60 * 1000000) for val in time_v500]

matplotlib.rc('pdf')
fig, ax = plt.subplots(1,1, figsize = (7.25,3.5))
plt.subplots_adjust(left=0.2, right = 0.95, top = 0.95, bottom = 0.15)

linestyle = "-"

marker = "o"
color = "#009900"
label = "GDA k=4"

ax.plot(xvalues, time_v004, fillstyle='none', ls=linestyle, marker = marker, color = color, label = label)

marker = "x"
color = "#000099"
label = "GDA k=16"

ax.plot(xvalues, time_v016, fillstyle='none', ls=linestyle, marker = marker, color = color, label = label)

marker = "^"
color = "#990000"
label = "GDA k=64"

ax.plot(xvalues, time_v064, fillstyle='none', ls=linestyle, marker = marker, color = color, label = label)

marker = "d"
color = "#990099"
label = "GDA k=256"

ax.plot(xvalues, time_v256, fillstyle='none', ls=linestyle, marker = marker, color = color, label = label)

marker = "*"
color = "#999900"
label = "GDA k=500"

ax.plot(xvalues, time_v500, fillstyle='none', ls=linestyle, marker = marker, color = color, label = label)

# rows = ["#servers", "#vertices", "#edges"]
# data = [[    "1",    "2",     "4",     "8",    "16",   "32",   "64",  "128",   "256",   "512",  "1024"],
#         [ "4.2M", "8.4M", "16.8M", "33.6M", "67.1M", "134M", "268M", "537M",  "1.1B",  "2.1B",  "4.3B"],
#         ["67.1M", "134M",  "268M",  "537M",  "1.1B", "2.1B", "4.3B", "8.6B", "17.2B", "34.4B", "68.7B"]]

# add a table at the bottom of the axes
# the_table = plt.table(cellText=data, rowLabels=rows, cellLoc='center', loc='bottom', edges='open')
# the_table.auto_set_font_size(False)
# the_table.set_fontsize(9)

# set ticks for the x axis, so that grid works, but don't show them
plt.xticks(xvalues)
# ax.tick_params(axis='x', which='both', bottom=False, labelbottom=False)
plt.xlabel( "Scale", fontsize=12, labelpad=1)

plt.yticks(fontsize=12)
ax.tick_params(axis='y', pad=2)
plt.ylabel( "Runtime [min]", fontsize=12, labelpad=-1)

# grid
ax.grid(True, which="both")
box = ax.get_position()
ax.set_position([box.x0, box.y0, box.width, box.height*0.8])

# legend
ax.legend( loc="upper center", bbox_to_anchor=(0.41,1.01), fontsize = 8.2, columnspacing = 0.4, labelspacing = 0.2, ncol=3 )

plt.savefig("gnn_weak_scaling.pdf", bbox_inches='tight')
