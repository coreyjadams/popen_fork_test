import sys, os
import psutil
import pathlib
import uuid
import time
import pickle
import socket
import argparse
import json

from signal import signal, SIGINT
from sys import exit

from subprocess import Popen, STDOUT, TimeoutExpired


parser = argparse.ArgumentParser(
    description     = 'Spawn forks for testing scaling',
    formatter_class = argparse.ArgumentDefaultsHelpFormatter)

parser.add_argument('--n-forks',
    type    = int,
    default = 256,
    help    = 'Total number of forks to spawn')

parser.add_argument('--max-concurrent',
    type    = int,
    default = 4,
    help    = 'Maximum number of running forks to use.')

parser.add_argument('--shell',
    type    = bool,
    default = True,
    help    = 'SHELL argument to subprocess')

parser.add_argument('--worksize',
    type    = int,
    default = 4096,
    help    = 'Size of problem to use in add_array')

parser.add_argument('--reserve-host',
    type    = bool,
    default = False,
    help    = 'If true, does not allow anything but the main loop run on CPU 0.')

parser.add_argument('--output-directory',
    type    = str,
    default = os.getcwd(),
    help    = 'Top directory to store data.')

parser.add_argument('--output-file',
    type    = str,
    default = 'output.pkl',
    help    = 'Name of file to dump statistics into.')

parser.add_argument('--import-mpi',
    type    = bool,
    default = False,
    help    = 'Import MPI and initialize, even though it isn\'t used.')

parser.add_argument('--use-affinity',
    type    = bool,
    default = True,
    help    = 'Use psutil to set affinity of forked jobs to an open CPU.')


args = parser.parse_args()

# Create the output folder name:
# output_name = socket.gethostname() +f"_{args.max_concurrent}forks_{args.n_forks}total_{args.worksize}"
s_list = []
for arg in vars(args):
    if "output" not in arg:
        s_list.append(f"{arg}_{getattr(args, arg)}")

output_name = socket.gethostname() + "-".join(s_list)


TOP_PATH    = pathlib.Path(args.output_directory)
TOP_PATH.mkdir(exist_ok=True)
TOP_PATH = TOP_PATH / output_name


N_CPUS   = psutil.cpu_count(logical=False)
DEFAULT_AFF = []

CPU_USAGE = {}
MEMORY    = {}



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


def spawn_process(available_cpus, workdir, size=args.worksize):


    job = job_spec()


    job.used_cpu = available_cpus.pop()

    job.workdir = workdir

    # Set the outfile
    job.outfile = open(workdir / f"array_add_{size}.out", 'wb')


    # Create a process:
    _p = psutil.Process()

    # Set the affinity:
    if args.use_affinity:
        _p.cpu_affinity([job.used_cpu,])

    job_args = [f'python /projects/datascience/cadams/popen_fork_test/array_add.py {size}']


    # Spawn a process:
    job.starttime = time.time()

    job.proc = Popen(job_args, stdout=job.outfile, stderr=STDOUT,
                     cwd=workdir, env=os.environ, shell=args.shell,)

    job.state = 0

    # Restore the affinity to the default
    if args.use_affinity:
        _p.cpu_affinity(DEFAULT_AFF)

    return available_cpus, job

def check_active_jobs(active_jobs, available_cpus, timeout=1):

    # The first thing done when checking jobs is to snapshot the CPU_USAGE
    t = time.time()
    CPU_USAGE[t] = psutil.cpu_times_percent(percpu=True)
    MEMORY[t]    = psutil.virtual_memory()

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


def dump_args(args):

    dump_location = TOP_PATH / "commandline_args.txt"
    with open(dump_location, 'w') as f:
        json.dump(args.__dict__, f, indent=2)



def main():


    if args.import_mpi:
        from mpi4py import MPI


    ##########################################################
    # Make this global to catch interrupts
    global output_file
    output_file = TOP_PATH / args.output_file
    global finished_jobs
    finished_jobs = []
    ##########################################################
    active_jobs = []


    # Make sure the top directory is available:
    TOP_PATH.mkdir(exist_ok=True)

    # Persist the argument:
    dump_args(args)

    # create a list of available CPUs, starting with all of them:
    # available_cpus = [4*i for i in range(64)]
    available_cpus = list(range(N_CPUS))

    if args.reserve_host:
        # Take the main cpu off the available list
        DEFAULT_AFF.append(available_cpus.pop(0))


    launch_start = time.time()
    previous_time = launch_start
    while len(finished_jobs) < args.n_forks:
        # Loop until we've completed args.n_forks.

        # Do two things: launch jobs to start processing, and check for finished jobs

        # Make sure enough jobs are running:
        while len(active_jobs) < args.max_concurrent:
            workdir = prepare_directory(TOP_PATH)
            available_cpus, job = spawn_process(available_cpus, workdir)
            active_jobs.append(job)

        # Finalize finished jobs:
        active_jobs, these_finished_jobs = check_active_jobs(active_jobs, available_cpus)

        finished_jobs += these_finished_jobs
        snapshot_time = time.time()
        throughput = len(these_finished_jobs) / (snapshot_time - previous_time)
        print(f"Finished {len(finished_jobs)} jobs ({throughput:.2f} jobs/second).")
        previous_time = snapshot_time

    snapshot_finished_jobs()

def snapshot_finished_jobs():
    # save the job list into a file:
    with open(output_file, 'wb') as _f:
        pickle.dump([finished_jobs,CPU_USAGE, MEMORY] , _f)


def handler(signal_received, frame):
    # Handle any cleanup here
    print('SIGINT or CTRL-C detected. Exiting gracefully')
    snapshot_finished_jobs()
    exit(0)


if __name__ == "__main__":
    signal(SIGINT, handler)
    main()
