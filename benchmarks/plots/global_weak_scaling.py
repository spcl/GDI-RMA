# median on graph with edge factor = 16 (weak scaling)

# matplotlib code written by Nils Blach

import matplotlib
import matplotlib.pyplot as plt
import statistics

xvalues = []
cdlp_time = []
pr_time = []
wcc_time = []

# WCC
scale = 23
for i in ("0001", "0002", "0004", "0008", "0016", "0032", "0064", "0128", "0256", "0512", "1024"):
  xvalues.append(scale)
  fp = open( "results/n" + i + "_s" + str(scale) + "_e16.wcc/lsb.gdi_wcc.r0", "rt" )
  rdr = [list(filter(None, i.split(' '))) for i in list(filter(lambda row: row[0]!='#', fp))[1:]]
  times = []
  for row in rdr:
    times.append(float(row[2]))
  wcc_time.append(statistics.median(times))
  fp.close()
  scale+= 1

# CDLP
scale = 23
for i in ("0001", "0002", "0004", "0008", "0016", "0032", "0064", "0128", "0256", "0512", "1024"):
  fp = open( "results/n" + i + "_s" + str(scale) + "_e16.cdlp/lsb.gdi_cdlp.r0", "rt" )
  rdr = [list(filter(None, i.split(' '))) for i in list(filter(lambda row: row[0]!='#', fp))[1:]]
  times = []
  for row in rdr:
    times.append(float(row[2]))
  cdlp_time.append(statistics.median(times))
  fp.close()
  scale+= 1

# PageRank
scale = 23
for i in ("0001", "0002", "0004", "0008", "0016", "0032", "0064", "0128", "0256", "0512", "1024"):
  fp = open( "results/n" + i + "_s" + str(scale) + "_e16.pr/lsb.gdi_pr.r0", "rt" )
  rdr = [list(filter(None, i.split(' '))) for i in list(filter(lambda row: row[0]!='#', fp))[1:]]
  times = []
  for row in rdr:
    times.append(float(row[3]))
  pr_time.append(statistics.median(times))
  fp.close()
  scale+= 1

# time in seconds
wcc_time = [val / 1000000 for val in wcc_time]
cdlp_time = [val / 1000000 for val in cdlp_time]
pr_time = [val / 1000000 for val in pr_time]

matplotlib.rc('pdf')
fig, ax = plt.subplots(1,1, figsize = (7.25,3.5))
plt.subplots_adjust(left=0.2, right = 0.95, top = 0.95, bottom = 0.15)

linestyle = "-"

marker = "o"
color = "#009900"
label = "WCC (i=5)"

ax.plot(xvalues, wcc_time, fillstyle='none', ls=linestyle, marker = marker, color = color, label = label)

marker = "x"
color = "#000099"
label = "CDLP (i=5)"

ax.plot(xvalues, cdlp_time, fillstyle='none', ls=linestyle, marker = marker, color = color, label = label)

marker = "^"
color = "#990000"
label = "PageRank (i=10, df=0.85)"

ax.plot(xvalues, pr_time, fillstyle='none', ls=linestyle, marker = marker, color = color, label = label)

# rows = ["#servers", "#vertices", "#edges"]
# data = [[    "1",     "2",     "4",     "8",   "16",   "32",   "64",   "128",   "256",   "512", "1024"],
#         [ "8.4M", "16.8M", "33.6M", "67.1M", "134M", "268M", "537M",  "1.1B",  "2.1B",  "4.3B", "8.6B"],
#         [ "134M",  "268M",  "537M",  "1.1B", "2.1B", "4.3B", "8.6B", "17.2B", "34.4B", "68.7B", "137B"]]

# add a table at the bottom of the axes
# the_table = plt.table(cellText=data, rowLabels=rows, cellLoc='center', loc='bottom', edges='open')
# the_table.auto_set_font_size(False)
# the_table.set_fontsize(9)

# set ticks for the x axis, so that the grid works, but don't display them
plt.xticks(xvalues)
# ax.tick_params(axis='x', which='both', bottom=False, labelbottom=False)
plt.xlabel( "Scale", fontsize=12, labelpad=1)

plt.yticks(fontsize=12)
ax.tick_params(axis='y', pad=2)
plt.ylabel( "Runtime [s]", fontsize=12, labelpad=-1)

# grid
ax.grid(True, which="both")
box = ax.get_position()
ax.set_position([box.x0, box.y0, box.width, box.height*0.8])

# legend
ax.legend( loc="upper center", bbox_to_anchor=(0.25,1.0), fontsize = 8.2, columnspacing = 0.4, labelspacing = 0.2, ncol=1 )

plt.savefig("global_weak_scaling.pdf", bbox_inches='tight')
