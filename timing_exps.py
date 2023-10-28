import argparse
argparser = argparse.ArgumentParser()
argparser.add_argument('--N', type=int, default=1e-3)
argparser.add_argument('--M', type=int, default=1e-3)
argparser.add_argument('--n_jobs', type=int, default=32)
argparser.add_argument('--results_dir', type=str, default='results')

import os, math, datetime, itertools, csv
import numpy as np
from pyparfor import pyparfor

args = argparser.parse_args()
N = int(args.N)
M = int(args.M)
n_jobs = int(args.n_jobs)
results_dir = args.results_dir
if not os.path.exists(results_dir):
    os.makedirs(results_dir)
results_filepath = results_dir+'/timing_exps_results.csv'
with open(results_filepath, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["exec_type", "N", "M", "elapsed_time"])

## Sequential for loop
out_seq = np.zeros((N,M))
tic = datetime.datetime.now()
for i in range(N):
    for j in range(M):
        temp = i+j
        temp = temp**2
        temp = math.sqrt(temp)
        out_seq[i,j] = temp
elapsed_time = datetime.datetime.now() - tic
# print("[SEQUENTIAL] Time elapsed: ", datetime.datetime.now() - tic)
with open(results_filepath, 'a', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["sequential",N, M, elapsed_time])

## Parallelizing the outer for loop
def do_work(i, out_memmap:np.memmap = None):
    out = []
    for j in range(M):
        temp = i+j
        temp = temp**2
        temp = math.sqrt(temp)
        
        if out_memmap is None: out.append(temp)
        else: out_memmap[i,j] = temp
    return out

## Parallelizing both inner and outer for loop
def do_less_work(idxes, out_memmap:np.memmap = None):
    i,j = idxes
    out = []
    
    temp = i+j
    temp = temp**2
    temp = math.sqrt(temp)
    
    if out_memmap is None: out.append(temp)
    else: out_memmap[i,j] = temp
    return out

tic = datetime.datetime.now()
out_memmap = pyparfor(do_work, range(N), n_jobs=32, use_memmap=True, out_shape=(N,M))
elapsed_time = datetime.datetime.now() - tic
# print("[PARALLEL MEMMAP: SINGLE FOR] Time elapsed: ", datetime.datetime.now() - tic)
with open(results_filepath, 'a', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["single_parallel_memmap", N, M, elapsed_time])

tic = datetime.datetime.now()
out = pyparfor(do_work, range(N), n_jobs=32, use_memmap=False, out_shape=(N,M))
elapsed_time = datetime.datetime.now() - tic
# print("[PARALLEL: SINGLE FOR] Time elapsed: ", datetime.datetime.now() - tic)
with open(results_filepath, 'a', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["single_parallel", N, M, elapsed_time])

tic = datetime.datetime.now()
out_memmap = pyparfor(do_less_work, itertools.product(range(N), range(M)), n_jobs=32, use_memmap=True, out_shape=(N,M))
elapsed_time = datetime.datetime.now() - tic
# print("[PARALLEL MEMMAP: NESTED FOR] Time elapsed: ", datetime.datetime.now() - tic)
with open(results_filepath, 'a', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["nested_parallel_memmap", N, M, elapsed_time])

tic = datetime.datetime.now()
out = pyparfor(do_less_work, itertools.product(range(N), range(M)), n_jobs=32, use_memmap=False, out_shape=(N,M))
elapsed_time = datetime.datetime.now() - tic
# print("[PARALLEL: NESTED FOR] Time elapsed: ", datetime.datetime.now() - tic)
with open(results_filepath, 'a', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["nested_parallel", N, M, elapsed_time])