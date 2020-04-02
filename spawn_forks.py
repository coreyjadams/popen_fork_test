import sys, os
import psutil
import pathlib
import uuid
import time
import pickle

from subprocess import Popen, STDOUT, TimeoutExpired



#################################################
#GLOBAL CONFIGURATION VARIABLES
N_FORKS         = 150
MAX_CONCURRENT  = 1
N_CPUS          = 64
SHELL           = True
WORKSIZE        = 4096
RESERVE_HOST    = False
DEFAULT_AFF     = []
TOP_PATH        = pathlib.Path(f"/projects/datascience/cadams/popen_fork_test/debugcache_{MAX_CONCURRENT}forks_{N_FORKS}total_{WORKSIZE}/")
#################################################





class job_spec(object):



    # Using state -1 = Nothing, 0 = running, 1 = done

    def __init__(self):
        object.__init__(self)
        self.used_cpu  = -1
        self.outfile   = None
        self.workdir   = None
        self.starttime = None
        self.endtime   = None
        self.runtime   = None
        self.state     = -1
        self.proc      = None
        self.exec_time = 0.0

# This script will run Popens in new directories and run the add_array.py script repeatedly

def prepare_directory(TOP_PATH):
    this_path = TOP_PATH / str(uuid.uuid1())
    this_path.mkdir()
    return this_path


def spawn_process(available_cpus, workdir, size=WORKSIZE):


    job = job_spec()


    job.used_cpu = available_cpus.pop()

    job.workdir = workdir
    
    # Set the outfile
    job.outfile = open(workdir / f"array_add_{size}.out", 'wb')


    # Create a process:
    _p = psutil.Process()

    # Set the affinity:
    _p.cpu_affinity([job.used_cpu,])

    args = [f'python /projects/datascience/cadams/popen_fork_test/array_add.py {size}']


    # Spawn a process:
    job.starttime = time.time()

    job.proc = Popen(args, stdout=job.outfile, stderr=STDOUT,
                     cwd=workdir, env=os.environ, shell=SHELL,)

    job.state = 0

    # Restore the affinity to the default
    _p.cpu_affinity(DEFAULT_AFF)

    return available_cpus, job

def check_active_jobs(active_jobs, available_cpus, timeout=1):

    local_finished_jobs = []
    for i, job in enumerate(active_jobs):
        try:
            retcode = job.proc.wait(timeout=timeout)
        except TimeoutExpired:
            retcode = None
            continue

        job.state = 1
        job.endtime = time.time()
        job.runtime = job.endtime - job.starttime
        # Close the output file
        job.outfile.close()


        # Read back the last line of the output file which prints the internal execution time
        with open(job.outfile.name, 'r') as _f:
            job.exec_time = float(_f.readlines()[-1])

        # Set the output file to just the name:
        job.outfile = job.outfile.name

        # Ensure the process is closed:
        job.proc.communicate()
        job.proc = None

        # Return the CPU to available CPUs:
        available_cpus.append(job.used_cpu)
        # if the job is finished, move it from one list to the other:
        local_finished_jobs.append(active_jobs.pop(i))



    return active_jobs, local_finished_jobs


def main():

    if len(sys.argv) > 1:
        output_file = sys.argv[-1]
    else:
        output_file = TOP_PATH / "output.pkl"

    # Make sure the top directory is available:
    TOP_PATH.mkdir(exist_ok=True)

    # create a list of available CPUs, starting with all of them:
    available_cpus = list(range(N_CPUS))

    if RESERVE_HOST:
        DEFAULT_AFF.append(available_cpus.pop(0))

    active_jobs = []
    finished_jobs = []

    launch_start = time.time()

    while len(finished_jobs) < N_FORKS:
        # Loop until we've completed N_FORKS.

        # Do two things: launch jobs to start processing, and check for finished jobs

        # Make sure enough jobs are running:
        while len(active_jobs) < MAX_CONCURRENT:
            workdir = prepare_directory(TOP_PATH)
            available_cpus, job = spawn_process(available_cpus, workdir)
            active_jobs.append(job)

        # Finalize finished jobs:
        active_jobs, these_finished_jobs = check_active_jobs(active_jobs, available_cpus)

        finished_jobs += these_finished_jobs
        print(f"Finished {len(finished_jobs)} jobs.")

    # save the job list into a file:
    with open(output_file, 'wb') as _f:
        pickle.dump(finished_jobs, _f)



if __name__ == "__main__":
    main()
