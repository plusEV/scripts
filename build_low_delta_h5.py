from ml_utils import list_h5s
import pandas as pd
import numpy as np
import os,sys
sys.path.append(os.path.abspath("/home/jgreenwald/libs"))
from kit_kospi import liquid_underlying,wmid
from ml_kospi import kospi_implieds_enriched_features, make_kospi_implieds_relative


def build_prc_h5(lo,hi,md_root = "/home/jgreenwald/md/korea/"):
    all_data = []
    h5s = sorted(list_h5s(md_root,enriched=True))
    n = "prc_"+str(lo)+"_"+str(hi)

    save = pd.HDFStore('/home/jgreenwald/md/korea/'+n+'.h5','w')

    for f in h5s:
        print f
        d = f[:-3]
        try:
            store = pd.HDFStore(md_root+d+'.h5','r')
            hey = store['front']
            store.close()
            print hey.shape
            hey = hey.ix[np.logical_and(hey.ap0>lo,hey.ap0<hi),:]
            save[d] = hey
        except:
            store.close()
            continue
    save.close()

def main(argv):
    build_prc_h5(float(sys.argv[1]),float(sys.argv[2]))

if __name__ == "__main__":
    sys.exit(main(sys.argv))