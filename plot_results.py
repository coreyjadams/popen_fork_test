import pickle
from spawn_forks import job_spec


outfile = "/projects/datascience/cadams/popen_fork_test/debugcache_1forks_150total_4096/output.pkl"
with open(outfile, 'rb') as _f:
    finished_jobs = pickle.load(_f)


starts = [ j.starttime for j in finished_jobs ]

launch_start = min(starts)

times  = [ j.runtime   for j in finished_jobs ]
starts = [ s - launch_start for s in starts ]
execs  = [ j.exec_time for j in finished_jobs ]


from matplotlib import pyplot as plt 



plt.scatter(starts, times, label="With Forks")
plt.scatter(starts, execs, label="Just Application")
plt.xlabel("Start time")
plt.ylabel("Run time")
plt.legend()
plt.grid(True)
plt.show()
