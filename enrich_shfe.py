from os import listdir
from os.path import isfile, join
import sys,os
sys.path.append(os.path.abspath("/home/jgreenwald/libs"))
from ml_utils import list_h5s
from utils import wmid
import pandas as pd
import numpy as np

def enrich_shfe(md):
    tick_values = {
        'ag': 15,
        'al': 5,
        'au': 1000,
        'bu': 10,
        'cu': 5,
        'fu': 50,
        'hc': 10,
        'ni': 1,
        'pb': 5,
        'rb': 10,
        'ru': 10,
        'sn': 1,
        'wr': 10,
        'zn': 5
    }
    
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
    
    md['product'] = md.symbol.str[0:2]
    md['tick_size'] = md['product'].replace(tick_values)
    md['vwap'] = md.turnover / (md.volume) / md.tick_size
    md['wmid'] = map(lambda bp,bs,ap,az,product: wmid(bp,bs,ap,az,tick_sizes[product]),
                     md.bp0,md.bz0,md.ap0,md.az0,md['product'])
    return md

def liquid_symbols(md):
    groups = md.groupby(['product'])
    syms = []
    for g in groups:
        volumes = g[1].groupby('symbol')['volume'].max()
        volumes.sort(ascending=False)
        v = volumes[0]
        if v < 10000:
            continue
        syms.append(volumes.index[0])
        if volumes[1] > .1 * v:
            syms.append(volumes.index[1])
    return syms

def build_wide_frame(enriched):
    bucks = [-300000,-60000,-30000,-10000,-5000,-1000,0,5000,10000,30000,60000,300000]
    res = dict()
    st = enriched.index[0]
    enriched = enriched.drop_duplicates(subset=['exchange_time','symbol','turnover'])
    max_volume_syms = enriched.groupby('product').apply(lambda x: x.ix[x.volume == x.volume.max()].symbol.values[0])
    syms = liquid_symbols(enriched)
    for s in syms:
        temp = pd.Series(enriched.ix[enriched.symbol==s].wmid.values,
                           index=pd.Index(enriched.ix[enriched.symbol==s].exchange_time))
        temp.index = map(lambda t: t + np.timedelta64(24,'h') if t < st else t ,temp.index)
        temp.sort_index(inplace=True)
        dt = ((pd.Series(temp.index).diff().astype(long) == 0) * 500)
        temp.index = map(lambda x,t: t + np.timedelta64(x,'ms'),dt,temp.index)

        if s in max_volume_syms.values:
            res[s] = temp
        else:
            res[s+'$'] = temp
    resdf = pd.DataFrame(res)
    s = (pd.Series(resdf.index).diff() > np.timedelta64(20,'m'))
    breaks = [i for i in s.ix[s].index]
    breaks.append(len(s))
    
    data = []
    i = 0
    for br in breaks:
        df = resdf.iloc[i:br,:].fillna(method='ffill')
        i = br
        inner = dict()
        for b in bucks:
            inner[b] = df.reindex(df.index + np.timedelta64(b,'ms'),method='ffill')
            inner[b].index = df.index
        data.append(pd.Panel(inner).swapaxes(0,1))
    return data

def main():
    md_root = "/home/jgreenwald/md/CHINA_SHFE/"
    h5s = list_h5s(md_root)
    try:
        save = pd.HDFStore("/home/jgreenwald/md/CHINA_SHFE/data.h5",'w')
    except:
        save.close()
        raise
    for f in h5s:
        if f[0] != '2':
            continue
        print 'Adding ' + f + ' ...'
        try:
            store = pd.HDFStore(md_root+f,'r')
            md = store['md']
            store.close()
            enriched = enrich_shfe(md)
            d = enriched.index[0].strftime("%Y%m%d")
            data = build_wide_frame(enriched)
            for i,dat in enumerate(data):
                save.put(d+'/'+'session'+str(i),dat)
        except:
            store.close()
            save.close()
            raise
    save.close()

if __name__ == "__main__":
    sys.exit(main())