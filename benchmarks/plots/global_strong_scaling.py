# median on graph with scale = 26 and edge factor = 16 (strong scaling)
# LCC has a different scale: scale = 23

# matplotlib code written by Nils Blach

import matplotlib
import matplotlib.pyplot as plt
import statistics

nnodes = []
lcc_nnodes = []
cdlp_time = []
pr_time = []
wcc_time = []
lcc_time = []
bi2_time = []

# WCC
for i in ("008", "016", "024", "032", "040", "048", "056", "064"):
  nnodes.append(int(i))
  fp = open( "results/n" + i + "_s26_e16.wcc/lsb.gdi_wcc.r0", "rt" )
  rdr = [list(filter(None, i.split(' '))) for i in list(filter(lambda row: row[0]!='#', fp))[1:]]
  times = []
  for row in rdr:
    times.append(float(row[2]))
  wcc_time.append(statistics.median(times))
  fp.close()

# CDLP
for i in ("008", "016", "024", "032", "040", "048", "056", "064"):
  fp = open( "results/n" + i + "_s26_e16.cdlp/lsb.gdi_cdlp.r0", "rt" )
  rdr = [list(filter(None, i.split(' '))) for i in list(filter(lambda row: row[0]!='#', fp))[1:]]
  times = []
  for row in rdr:
    times.append(float(row[2]))
  cdlp_time.append(statistics.median(times))
  fp.close()

# PageRank
for i in ("008", "016", "024", "032", "040", "048", "056", "064"):
  fp = open( "results/n" + i + "_s26_e16.pr/lsb.gdi_pr.r0", "rt" )
  rdr = [list(filter(None, i.split(' '))) for i in list(filter(lambda row: row[0]!='#', fp))[1:]]
  times = []
  for row in rdr:
    times.append(float(row[3]))
  pr_time.append(statistics.median(times))
  fp.close()

# LCC
for i in ("008", "016", "024", "032", "040", "048", "056", "064"):
  fp = open( "results/n" + i + "_s23_e16.lcc/lsb.gdi_lcc.r0", "rt" )
  rdr = [list(filter(None, i.split(' '))) for i in list(filter(lambda row: row[0]!='#', fp))[1:]]
  times = []
  for row in rdr:
    times.append(float(row[1]))
  lcc_time.append(statistics.median(times))
  fp.close()

# BI
for i in ("008", "016", "024", "032", "040", "048", "056", "064"):
  fp = open( "results/n" + i + "_s26_e16.bi/lsb.gdi_bi.r0", "rt" )
  rdr = [list(filter(None, i.split(' '))) for i in list(filter(lambda row: row[0]!='#', fp))[1:]]
  times = []
  for row in rdr:
    times.append(float(row[1]))
  bi2_time.append(statistics.median(times))
  fp.close()

# time in seconds
wcc_time = [val / 1000000 for val in wcc_time]
cdlp_time = [val / 1000000 for val in cdlp_time]
pr_time = [val / 1000000 for val in pr_time]
lcc_time = [val / 1000000 for val in lcc_time]
bi2_time = [val / 1000000 for val in bi2_time]

matplotlib.rc('pdf')
fig, ax = plt.subplots(1,1, figsize = (7.25,3.5))
plt.subplots_adjust(left=0.2, right = 0.95, top = 0.95, bottom = 0.15)

linestyle = "-"

marker = "o"
color = "#009900"
label = "WCC (i=5) GDA"

ax.plot(nnodes, wcc_time, fillstyle='none', ls=linestyle, marker = marker, color = color, label = label)

marker = "x"
color = "#000099"
label = "CDLP (i=5) GDA"

ax.plot(nnodes, cdlp_time, fillstyle='none', ls=linestyle, marker = marker, color = color, label = label)

marker = "^"
color = "#990000"
label = "PageRank (i=10, df=0.85) GDA"

ax.plot(nnodes, pr_time, fillstyle='none', ls=linestyle, marker = marker, color = color, label = label)

marker = "d"
color = "#990099"
label = "LCC GDA"

ax.plot(nnodes, lcc_time, fillstyle='none', ls=linestyle, marker = marker, color = color, label = label)

marker = "*"
color = "#999900"
label = "BI2 GDA"

ax.plot(nnodes, bi2_time, fillstyle='none', ls=linestyle, marker = marker, color = color, label = label)

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
ax.legend( loc="upper center", bbox_to_anchor=(0.65,0.55), fontsize = 8.2, columnspacing = 0.4, labelspacing = 0.2, ncol=2 )

plt.savefig("global_strong_scaling.pdf", bbox_inches='tight')
