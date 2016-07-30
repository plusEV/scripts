from os import listdir
from os.path import isfile, join
import sys,os
sys.path.append(os.path.abspath("/home/jgreenwald/libs"))
sys.path.append(os.path.abspath("/home/jgreenwald/scripts"))
import pandas as pd
import numpy as np
from ml_utils import list_h5s
from enrich_shfe import enrich_shfe
from tsutil import ts_returns

def daily_tick_report(enriched,d):
    save = pd.HDFStore('/home/jgreenwald/datasets/shfe_tick_report.h5')
    
    keys = save.keys()
    if keys:
        days_already_in_h5 = pd.Series(keys).str[1:].str.split('/').str.get(0).drop_duplicates()
    if keys and d in days_already_in_h5.values:
        save.close()
        return
    active = enriched.groupby('symbol')['volume'].max()
    active = active.ix[active>50000].index
    cols= ['symbol','volume','turnover','last','lastsize','bp0','bz0','ap0','az0','exchange_time','contig']
    tick_sizes = {
            'ag': 1,
            'al': 5,
            'au': .05,
            'bu': 2,
            'cu': 10,
            'fu': 1,
            'hc': 1,
            'ni': 10,
            'pb': 5,
            'rb': 1,
            'ru': 5,
            'sn': 10,
            'wr': 1,
            'zn': 5
        }

    for sym in active:
        df = enriched.ix[enriched.symbol == sym]
        df = df.ix[np.logical_and(df.bp0>0,df.ap0>0)]
        v = (df.index.values[1:] - df.index.values[:-1])<np.timedelta64(1,'m')
        df['contig'] = False
        df.contig.values[1:] = v
        prod_code = sym[:2]
        ts = tick_sizes[prod_code]
        uptick_sizes = (df.bp0 - df.ap0.shift()).ix[np.logical_and(df.bp0>df.ap0.shift(),df.contig==True)]
        downtick_sizes = (df.bp0.shift() - df.ap0).ix[np.logical_and(df.ap0<df.bp0.shift(),df.contig==True)]
        df = pd.DataFrame({'up': uptick_sizes, 'down': downtick_sizes})
        if len(df)<1:
            continue
        print "Adding " + sym + " with " + str(len(df)) + " total big ticks"
        for isym in active:
            hi = enriched.ix[enriched.symbol==isym]
            hi.index = hi.index.astype(long)
            save.put('/'+d+'/'+sym+'/moves/up/'+isym,ts_returns(hi,uptick_sizes.index,ts,mode='tick'))
            save.put('/'+d+'/'+sym+'/moves/down/'+isym,ts_returns(hi,downtick_sizes.index,ts,mode='tick'))
        save.put("/"+d+"/ticks/"+sym,df)
    save.close()

def main():
    md_root = "/home/jgreenwald/md/CHINA_SHFE/"
    h5s = list_h5s(md_root)

    for f in h5s:
        print 'Adding tick info for ' + f
        if f[0] != '2':
            continue
        try:
            store = pd.HDFStore(md_root+f,'r')
            md = store['md']
            store.close()
            enriched = enrich_shfe(md)
            daily_tick_report(enriched,f[:8])
        except:
            print f
            store.close()
            raise
if __name__ == "__main__":
    sys.exit(main())