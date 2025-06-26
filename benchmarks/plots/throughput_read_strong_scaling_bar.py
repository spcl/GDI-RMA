# OLTP throughput for read intensive and read mostly workloads on graph with scale = 26 and edge factor = 16 (strong scaling)

# matplotlib code written by Nils Blach

import matplotlib
import matplotlib.pyplot as plt
import os

ri_xvalues = []
ri_tp = []
ri_failed = []

rm_xvalues = []
rm_tp = []
rm_failed = []

# read intensive workload
for i in ("008", "016", "024", "032", "040", "048", "056", "064"):
  ri_xvalues.append(int(i))
  directory = "results/n" + i + "_s26_e16.oltp.ri/"
  onlyfiles = [directory+f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f)) and f.startswith("lsb")]
  barrier_time = []
  tp_time = []
  num_queries = 0
  failed_queries = 0
  for filepath in onlyfiles:
    fp = open( filepath, "rt" )
    rdr = [list(filter(None, i.split(' '))) for i in list(filter(lambda row: row[0]!='#', fp))[1:]]
    barrier_time.append(float(rdr[0][4]))
    tp_time.append(float(rdr[1][4]))
    num_queries += int(rdr[1][0]) - int(rdr[1][2])
    failed_queries += int(rdr[1][1])
    fp.close()
  ri_failed.append(float(failed_queries/num_queries*100)) # convert to percent
  ri_tp.append(num_queries/(max(tp_time) - min(barrier_time)))

# read mostly workload
for i in ("008", "016", "024", "032", "040", "048", "056", "064"):
  rm_xvalues.append(int(i))
  directory = "results/n" + i + "_s26_e16.oltp.rm/"
  onlyfiles = [directory+f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f)) and f.startswith("lsb")]
  barrier_time = []
  tp_time = []
  num_queries = 0
  failed_queries = 0
  for filepath in onlyfiles:
    fp = open( filepath, "rt" )
    rdr = [list(filter(None, i.split(' '))) for i in list(filter(lambda row: row[0]!='#', fp))[1:]]
    barrier_time.append(float(rdr[0][4]))
    tp_time.append(float(rdr[1][4]))
    num_queries += int(rdr[1][0]) - int(rdr[1][2])
    failed_queries += int(rdr[1][1])
    fp.close()
  rm_failed.append(float(failed_queries/num_queries*100)) # convert to percent
  rm_tp.append(num_queries/(max(tp_time) - min(barrier_time)))

matplotlib.rc('pdf')
fig, ax = plt.subplots(1,1, figsize = (7.25,3.5))
plt.subplots_adjust(left=0.2, right = 0.95, top = 0.95, bottom = 0.15)

width = 2

color = "#009900"
label = "read mostly"

rm_xvalues = [val-0.5*width for val in rm_xvalues]
ax.bar(rm_xvalues, rm_tp, color=color, label=label, width=width)

# annotate the bars with the percentage of failed queries: #failed queries/(#queries - #failed queries because of vertex deletion)
idx = 0
for x, y in zip(rm_xvalues, rm_tp):
  if rm_failed[idx] > 0.005:
    label = "{:.02f}%".format(rm_failed[idx])
    ax.annotate(label, (x,y), textcoords="offset points", xytext=(0.5,-19), ha='center', annotation_clip=True, fontsize=6, rotation=90, color='#FFFFFF')
  idx += 1

color = "#000099"
label = "read intensive"

ri_xvalues = [val+0.5*width for val in ri_xvalues]
ax.bar(ri_xvalues, ri_tp, color=color, label=label, width=width)

# annotate the bars with the percentage of failed queries: #failed queries/(#queries - #failed queries because of vertex deletion)
idx = 0
for x, y in zip(ri_xvalues, ri_tp):
  if ri_failed[idx] > 0.005:
    label = "{:.02f}%".format(ri_failed[idx])
    ax.annotate(label, (x,y), textcoords="offset points", xytext=(0.5,-19), ha='center', annotation_clip=True, fontsize=6, rotation=90, color='#FFFFFF')
  idx += 1

plt.xticks(ticks=[8, 16, 24, 32, 40, 48, 56, 64], fontsize=9)
plt.yticks(fontsize=12)
ax.tick_params(axis='x', pad=2)
ax.tick_params(axis='y', pad=2)
plt.xlabel( "Servers", fontsize=9, labelpad=1)
plt.ylabel( "Million Queries/Second", fontsize=12, labelpad=-1)

# grid
ax.set_axisbelow(True) #draw grid behind the bars
ax.grid(True, which="both")
box = ax.get_position()
ax.set_position([box.x0, box.y0, box.width, box.height*0.8])

# legend
ax.legend( loc="upper center", bbox_to_anchor=(0.3,1.0), fontsize = 8.2, columnspacing = 0.4, labelspacing = 0.2, ncol=2 )

plt.savefig("throughput_read_strong_scaling_bar.pdf", bbox_inches='tight')
