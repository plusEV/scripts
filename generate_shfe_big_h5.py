import pandas as pd
import numpy as np
import os,sys
sys.path.append(os.path.abspath("/home/jgreenwald/libs"))


def shfe_past_future(dat,session):
    syms = pd.Series(dat.minor_axis)
    leading_syms = syms.ix[syms.str[-1]!='$']

    p = []
    f = []
    n = []
    for s in leading_syms:
        df = dat.ix[:,:,s].T
        ref = df[0].ix[df[0].first_valid_index()]
        df /= ref
        ddf = pd.DataFrame(df[0],index=df.index,columns=[s[:2]])

        moves = pd.DataFrame(df.values - np.atleast_2d(df[0].values).T,index=df.index,columns=df.columns)
        t = moves.ix[:,[-300000,-60000,-30000,-10000,-5000,-1000]]
        t.columns = map(lambda x: s[:2] + str(x), t.columns)
        if s in leading_syms.values:
            p.append(t)
            n.append(ddf)
        t = moves.ix[:,[5000,10000,30000,60000,300000]]
        t.columns = map(lambda x: s[:2] + str(x), t.columns)
        f.append(t)
    past = pd.concat(p,axis=1)
    past['session'] = session
    future = pd.concat(f,axis=1)
    now = pd.concat(n,axis=1)
    return past,future,now

def read_shfe_day(d,store):
    sessions = ['session0','session1','session2']
    pasts = []
    futures = []
    nows = []
    keys =  store.keys()
    
    for i,sess in enumerate(sessions):
        k = '/'+d+'/session'+str(i)
        if k not in keys:
            continue
        data = store[k]
        try:
            p,f,n = shfe_past_future(data,i)
            pasts.append(p)
            futures.append(f)
            nows.append(n)
        except:
            continue
    daily_past = pd.concat(pasts,axis=0)
    daily_future = pd.concat(futures,axis=0)
    daily_now = pd.concat(nows,axis=0)
    daily_now['trade_date'] = d
    return daily_past,daily_future,daily_now

def main():
	try:
	    store = pd.HDFStore('/home/jgreenwald/md/CHINA_SHFE/data.h5','r')

	    keys =  store.keys()
	    days = pd.Series(keys).str[1:].str.split('/').str.get(0).drop_duplicates()

	    ps = []
	    fs = []
	    ns = []
	    for d in days:
	        p, f, n = read_shfe_day(d,store)
	        ps.append(p)
	        fs.append(f)
	        ns.append(n)
	    store.close()
	except:
	    store.close()
	    raise
	past = pd.concat(ps,axis=0)
	del ps
	future = pd.concat(fs,axis=0)
	del fs
	now = pd.concat(ns,axis=0)
	del ns
	save = pd.HDFStore('/home/jgreenwald/datasets/shfe_long.h5','w')
	save['past'] = past
	save['future'] = future
	save['now'] = now
	save.close()
if __name__ == "__main__":
    sys.exit(main())