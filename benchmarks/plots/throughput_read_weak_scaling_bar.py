# OLTP throughput for read mostly and read intensive workloads on graph with edge factor = 16 (weak scaling)

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
scale = 23
for i in ("0008", "0016", "0032", "0064", "0128", "0256", "0512", "1024", "2048"):
  ri_xvalues.append(scale)
  directory = "results/n" + i + "_s" + str(scale) + "_e16.oltp.ri/"
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
  scale += 1

# read mostly workload
scale = 23
for i in ("0008", "0016", "0032", "0064", "0128", "0256", "0512", "1024", "2048"):
  rm_xvalues.append(scale)
  directory = "results/n" + i + "_s" + str(scale) + "_e16.oltp.rm/"
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
  scale += 1

matplotlib.rc('pdf')
fig, ax = plt.subplots(1,1, figsize = (7.25,3.5))
plt.subplots_adjust(left=0.2, right = 0.95, top = 0.95, bottom = 0.15)

# plt.ylim(-0.5,24) # increase y range, so that the table doesn't overlap with the zero

width = 0.35
axis_xvalues = rm_xvalues

color = "#009900"
label = "read mostly"

rm_xvalues = [val-0.5*width for val in rm_xvalues]
ax.bar(rm_xvalues, rm_tp, color=color, label=label, width=width)

# annotate the bars with the percentage of failed queries: #failed queries/(#queries - #failed queries because of vertex deletion)
idx = 0
for x, y in zip(rm_xvalues, rm_tp):
  if rm_failed[idx] > 0.005:
    label = "{:.02f}%".format(rm_failed[idx])
    ax.annotate(label, (x,y), textcoords="offset points", xytext=(0.5,-13), ha='center', annotation_clip=True, fontsize=4, rotation=90, color='#FFFFFF')
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
    ax.annotate(label, (x,y), textcoords="offset points", xytext=(0.5,-13), ha='center', annotation_clip=True, fontsize=4, rotation=90, color='#FFFFFF')
  idx += 1

# rows = ["#servers", "#vertices", "#edges"]
# data = [[    "1",     "2",     "4",     "8",   "16",   "32",   "64",   "128",   "256",   "512", "1024",  "2048"],
#         [ "8.4M", "16.8M", "33.6M", "67.1M", "134M", "268M", "537M",  "1.1B",  "2.1B",  "4.3B", "8.6B", "17.2B"],
#         [ "134M",  "268M",  "537M",  "1.1B", "2.1B", "4.3B", "8.6B", "17.2B", "34.4B", "68.7B", "137B",  "275B"]]

# plt.xlim(22.5,34.5) # change x range, so that the table columns align with bars

# add a table at the bottom of the axes
# the_table = plt.table(cellText=data, rowLabels=rows, cellLoc='center', loc='bottom', edges='open')
# the_table.auto_set_font_size(False)
# the_table.set_fontsize(9)

# set ticks for the x axis, so that the grid works, but don't display them
plt.xticks(axis_xvalues)
# ax.tick_params(axis='x', which='both', bottom=False, labelbottom=False)
plt.xlabel( "Scale", fontsize=12, labelpad=1)

plt.yticks(fontsize=12)
ax.tick_params(axis='y', pad=2)
plt.ylabel( "Million Queries/Second", fontsize=12, labelpad=-1)

# grid
ax.set_axisbelow(True) #draw grid behind the bars
ax.grid(True, which="both")
box = ax.get_position()
ax.set_position([box.x0, box.y0, box.width, box.height*0.8])

# legend
ax.legend( loc="upper center", bbox_to_anchor=(0.4,1.0), fontsize = 8.2, columnspacing = 0.4, labelspacing = 0.2, ncol=2 )

plt.savefig("throughput_read_weak_scaling_bar.pdf", bbox_inches='tight')
