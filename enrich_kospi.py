from os import listdir
from os.path import isfile, join
import sys,os
sys.path.append(os.path.abspath("/home/jgreenwald/libs"))
from kospi_utils import fit_vols
from kit_kospi import implieds_wide
from ml_utils import list_h5s
import pandas as pd

def main():
    md_root = "/home/jgreenwald/md/korea/"
    h5s = list_h5s(md_root)
    enriched = list_h5s(md_root,enriched=True)
    for f in h5s:
        d = f[:-3]
        if (d+"_enriched.h5") in enriched.values:
            continue
        try:
            store = pd.HDFStore(md_root+d+'.h5','r')
        except:
            print "Failed to open store at " + md_root+d+'.h5'
            store.close()
            raise
        try:
            save = pd.HDFStore(md_root+d+'_enriched.h5','w')
            hi = store['md']
            front, vols, und, raw, k, moneys, splined_vols, tick_info = fit_vols(hi,d)
            tick_theos,tick_deltas, tick_vegas, tick_vols, tick_futs = tick_info
            
            front['theo_bid'] = tick_theos[:,0]
            front['theo_ask'] = tick_theos[:,1]
            front['delta'] = tick_deltas
            front['vega'] = tick_vegas
            front['raw_vol'] = tick_vols
            
            front['imp_fut_bp'] = tick_futs[:,0]
            front['imp_fut_bz'] = tick_futs[:,1]
            front['imp_fut_ap'] = tick_futs[:,2]
            front['imp_fut_az'] = tick_futs[:,3]
            front['imp_fut_tp'] = tick_futs[:,4]
            front['imp_fut_tz'] = tick_futs[:,5]
            
            save['md'] = hi
            save['vols'] = vols
            save['und'] = und
            save['raw'] = raw
            save['k'] = k
            save['moneys'] = moneys
            save['splined_vols'] = splined_vols
            save['front'] = front
            save['implieds'] = pd.DataFrame(implieds_wide(front),index=front.index)
            save.close()
            store.close()
            print '...\n'
        except:
            print 'Failed to enrich: ' + d
            save.close()
            store.close()
            raise


if __name__ == "__main__":
    sys.exit(main())