from operator import concat
from typing import Concatenate
import warnings
warnings.filterwarnings('ignore')
import ujson
import fsspec
import dask.bag as db
from dask.distributed import Client
from dask_jobqueue import SLURMCluster
from kerchunk.hdf import SingleHdf5ToZarr
from kerchunk.combine import MultiZarrToZarr
from datetime import datetime
from dask.distributed import LocalCluster
import sys




##############
# SLURM info #
##############


import argparse
parser = argparse.ArgumentParser(description="Kercunk GOES data on S3")
parser.add_argument('-p', '--product', required=True, type=str, default='ABI-L2-ACMC', help='GOES product to process (default ABI-L2-ACMC)')
# parser.add_argument('-f', '--file', required=False, type=str, default='*', help='File wildcard (default all files \"*\")')
parser.add_argument('-c', '--channel', required=False, type=int, default=None, help="L1b channel")
parser.add_argument('-m', '--mode', required=False, type=int, default=6, help="L1b mode")
parser.add_argument('-y', '--year', required=True, type=int, help="Year of data to process")
parser.add_argument('-d', '--days', required=True, type=int, nargs=2, help="Day range to process (e.g. \"-d 100 101\")")
# Optional SLURM arguments
parser.add_argument('-n', '--ncpu', type=int, default=12, help="Number of CPUs per SLURM job (default 12)")
parser.add_argument('-j', '--njobs', type=int, default=1, help="Number of SLURM jobs to submit (default 1)")
parser.add_argument('-q', '--queue', type=str, default='high', help='SLURM job queue to run on (default \"high\")')
parser.add_argument('-w', '--walltime', type=str, default='01:00:00', help='SLURM job walltime (default \"01:00:00\")')
args1 = parser.parse_args()

args=vars(args1)

product = args['product']
year = args['year']
day1 = args['days'][0]
day2 = args['days'][1]
chan = args['channel']
mode = args['mode']

ncpu = args['ncpu']
njobs = args['njobs']
queue = args['queue']
walltime = args['walltime']
# print(args)

################
# Do the work! #
################

t1 = datetime.now()

if __name__ == '__main__':
    # Set up s3 filesystem
    fs = fsspec.filesystem('s3', anon=True)

    if chan:
        file = f'OR_{product}-M{mode}C{chan:02}_*'
        # get list of files for given product, year, day
        print(f"Getting filelist for {product} (mode {mode}, channel {chan}) from {year} {day1:03}-{day2:03}")

    else:
        # get list of files for given product, year, day
        print(f"Getting filelist for {product} from {year} {day1:03}-{day2:03}")
        file = "*"

    flist = []
    for d in range(day1, day2+1):
        f = fs.glob(f"s3://noaa-goes16/{product}/{year}/{d:03}/*/{file}.nc")
        flist =concat(flist, f)

    print(f"Found {len(flist)} files")

    # Start a cluster on dask (use up a whole node with --exclusive)
    # this is where the actual computing of Kerchunk will happen
    print("Starting dask cluster")
    cluster = SLURMCluster(
        queue = queue,
        walltime = walltime, 
        cores = ncpu,
        n_workers=ncpu,
    )

    cluster.scale(jobs=njobs)
    client = Client(cluster)




    # Do the parallel computation

    def gen_ref(f):
        with fs.open(f) as inf:
            return SingleHdf5ToZarr(inf, f).translate()

    with cluster:
        with client:
            print("Setting up dask parallelization")
            #print(client)
            bag = db.from_sequence(flist, partition_size=1).map(gen_ref)

            with client:
                print("Kerchunking individual files in parallel")
                dicts = bag.compute()

     # Uncomment if you want to save each of the individual references to JSON

#    print("saving to json")
#    for i,d in enumerate(dicts):
#        name = f'{i:04}'
#        with open(f'jsons/{name}.json','w') as outf:
#            outf.write(ujson.dumps(d))

    # Combine all references into single file and write to disk
    print("Combining into single reference")
    mzz = MultiZarrToZarr(
        dicts,
        concat_dims='t',
        inline_threshold=0,
        remote_protocol='s3',
        remote_options={'anon':True}
    )

    mzz.translate(f"{product}-{year}-{day}.json")

    t2 = datetime.now()
    print(f"Done! This script took {(t2 - t1).total_seconds()} seconds")
