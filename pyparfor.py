import shutil, os
import numpy as np
from joblib import Parallel, delayed
from joblib.externals.loky import get_reusable_executor

def pyparfor(func, iterable, n_jobs=32, use_memmap=True, memmap_folder=None, out_shape=None):
    if use_memmap:
        if not memmap_folder:
            memmap_folder = os.getcwd()+"/memmap"
        if not os.path.exists(memmap_folder):
            os.makedirs(memmap_folder)
        out_memmap = np.memmap(memmap_folder+"/curr_memmap", dtype='float', mode='w+', shape=out_shape)
        print("Storing memmap in: ", memmap_folder)

        _ = Parallel(n_jobs=n_jobs, verbose=0)(delayed(func)(item, out_memmap) for item in iterable)
        get_reusable_executor().shutdown(wait=True)
        try:
            shutil.rmtree(memmap_folder)
        except:
            print('Could not clean-up automatically.')

        return out_memmap
    else:
        unstruct_out = Parallel(n_jobs=n_jobs, verbose=0)(delayed(func)(item) for item in iterable)
        get_reusable_executor().shutdown(wait=True)
        if out_shape:
            out = np.array(unstruct_out).reshape(out_shape)
        return out