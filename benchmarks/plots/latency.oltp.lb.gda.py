# matplotlib code written by Nils Blach

import matplotlib
import matplotlib.pyplot as plt
import os

id_to_query = {
  0: "retrieve vertex", # get object (company_vertex, person_vertex, place_vertex, project vertex, resource vertex)
  1: "insert vertex", # insert object (vertex)
  2: "delete vertex", # delete object (vertex)
  3: "update vertex", # update object (vertex)
  4: "count edges", # count edges
  5: "retrieve edges", # get edges
  6: "add edges" # add edges
}

colors = {"1": "#009900", "2": "#000099", "4": "#990000", "8": "#990099"}

matplotlib.rc('pdf')
fig, axs = plt.subplots(7, sharex=True, sharey=True, figsize=(5, 10))

maximum = 5000

for i in ("8", "4", "2", "1"):
  plots_data = []
  for query_type in range(7):
    plots_data.append([])

  # read data
  directory = "results/n00" + i + "_s23_e16.oltp.lb.lat/"
  onlyfiles = [directory+f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f)) and f.startswith("lsb")]
  for filepath in onlyfiles:
    fp = open( filepath, "rt" )
    rdr = [list(filter(None, i.split(' '))) for i in list(filter(lambda row: row[0]!='#', fp))[1:]]
    for row in rdr:
      plots_data[int(row[0])].append(float(row[1]))
    fp.close()

  color = colors[i]
  label = "S$_{" + i + "}$"

  for subp, query_data in enumerate(plots_data):
    query_data = sorted(query_data)
    for i, data in enumerate(query_data):
      if data > maximum:
        query_data[i] = maximum+1
    axs[subp].hist(query_data, int(maximum/20), range=(0, maximum+11), density=False, color=color, label=label)

for subp in range(7):
  #grid
  axs[subp].set_axisbelow(True) #draw grid behind the bars
  axs[subp].grid(True, which="both", axis="y")
  axs[subp].set_title(id_to_query[subp], y=0.9, pad=-14)

axs[0].set_yscale('log', base=10)

axs[-1].set_xlabel("Time (μs)", size=12)
axs[3].set_ylabel("Query Count", size=12)

axs[0].legend(loc="lower center", bbox_to_anchor=(0.5, 1.0), prop={'size': 8.5}, markerscale=1.7, handletextpad=0.1, handlelength=1.0, columnspacing=1, labelspacing=0.2, ncol=4, framealpha=0.5)

plt.savefig("latency.oltp.lb.gda.pdf", bbox_inches='tight')
