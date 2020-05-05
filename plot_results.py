import pickle
import numpy
from spawn_forks import job_spec

folder  = "output/nid00021n_forks_100-max_concurrent_63-shell_True-worksize_64-reserve_host_True-import_mpi_False-use_affinity_True"
outfile = f"/projects/datascience/cadams/popen_fork_test//{folder}/output.pkl"
with open(outfile, 'rb') as _f:
    finished_jobs, CPU_USAGE, MEMORY = pickle.load(_f)


starts = [ j.starttime for j in finished_jobs ]

launch_start = min(starts)

times  = numpy.asarray([ j.runtime   for j in finished_jobs ] )
starts = numpy.asarray([ s - launch_start for s in starts ]   )
execs  = numpy.asarray([ j.exec_time for j in finished_jobs ] )
ends   = numpy.asarray([ j.endtime - launch_start for j in finished_jobs])

# Calculate and plot throughput:

throughput_marks = numpy.arange(min(starts), max(ends), ( max(ends) - min(starts) ) / 50 )


n_completed_throughput = [ numpy.sum(ends < t) for t in throughput_marks]

snapshot_times    = MEMORY.keys()
memory_percentage = [ MEMORY[key].percent for key in snapshot_times]
snapshot_times    = [t - launch_start for t in snapshot_times]



from matplotlib import pyplot as plt
# Create one big figure:
fig = plt.figure(1, figsize=(30,16))


# Plot the job completion times:
plt.subplot(221)

# Forking time as a function of start time:
plt.scatter(starts, times, label="With Forks")
plt.scatter(starts, execs, label="Just Application")
plt.xlabel("Start time")
plt.ylabel("Run time")
plt.legend()
plt.grid(True)
plt.title("Job Runtime")

# Throughput:
plt.subplot(222)
plt.plot(throughput_marks, n_completed_throughput, label="Completed jobs")
plt.legend()
ax2 = plt.gca().twinx()
ax2.plot(snapshot_times, memory_percentage, color='red', label="Memory Usage")
plt.ylim([0,100])
plt.grid()
plt.legend()
plt.title("Throughput")

# Plot the average fork times based on CPU

cpus = numpy.asarray([ j.used_cpu for j in finished_jobs ])


unique_cpus     = numpy.unique(cpus)
masks = [ cpus == c for c in unique_cpus ]
average_per_cpu = [ numpy.mean(times[m]) for m in masks ]
rms_per_cpu     = [ numpy.std(times[m]) for m in masks ]

locations = numpy.arange(len(unique_cpus))

plt.subplot(212)
plt.grid(zorder=0)
plt.bar(locations, average_per_cpu, yerr=rms_per_cpu, label="With Forks")
plt.xlabel("CPU ID")
plt.ylabel("Average Run time")
plt.xticks(ticks=locations, labels=unique_cpus)
plt.legend()


# Show everything
plt.show()
