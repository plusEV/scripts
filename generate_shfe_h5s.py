from os import listdir
from os.path import isfile, join
from subprocess import Popen,PIPE
import pandas as pd
import sys,os
import numpy as np
from kit_time import fix_timestamps


def sim_command(exec_path='/home/jgreenwald/tyche/build/sim/md_dump',
                config_path= '/home/jgreenwald/configs/md_config_SHFE'):
    return  exec_path + ' ' + config_path + ' 0'

def main(mypath = "/home/Dumps2/ProcessedData/", cmd = sim_command(),\
         cfg_path = '/home/jgreenwald/configs/MDConfig_SHFE.cfg'):
    
    dates = listdir(mypath)
    dd = pd.Series(dates)
    dates = dd.ix[dd.str[0]=="2"]
    md_root = "/home/jgreenwald/md/CHINA_SHFE/"

    s = pd.Series([f.strip() for f in listdir(md_root)])
    if s.empty:
        h5s = pd.Series([])
    else:
        h5s = s.ix[s.str[-2:]=='h5']
    #for d in dates:
    for d in dates:
        try:
            metadata_file = mypath + d + "/CHINA_SHFE_METADATA.dat"
            if (d+'.h5') in h5s.values or d < "20160501" or not os.path.isfile(metadata_file):
                continue
            update_cfg(cfg_path,d)
            p = Popen(cmd, stderr=PIPE, stdout=PIPE, shell=True)
            output, errors = p.communicate()
            if p.returncode ==0:
                print '...\n'
            else:
                print output
                print errors
            md = pd.read_csv(md_root+d+'.csv',sep=' ')
            md.local_time = pd.to_datetime(fix_timestamps(md.local_time.values)).tz_localize("UTC").tz_convert("Asia/Shanghai")
            backup = md.exchange_time
            try:
                md.exchange_time = pd.Index(pd.to_datetime(md.exchange_time*1e6)).tz_localize("UTC").tz_convert("Asia/Shanghai")
            except:
                md.exchange_time = backup
            md.index = md.local_time
            del md["local_time"]
            store = pd.HDFStore(md_root+d+'.h5','w',complevel=9,complib='blosc')
            store.append('md',md,)
            store.close()
            p = Popen("rm " + md_root+d+'.csv', stderr=PIPE, stdout=PIPE, shell=True)
        except:
            print 'Failed to parse: ' + d
            continue

def update_cfg(path,d):
    assert(len(d) == 8)
    with open(path,'r') as f:
        s = f.read()
    p1 = s.find('DATES = ')
    p3 = s.find('2', p1)
    ss =  s[:p3] + d + s[p3+8:]
    
    with open(path,'w') as f:
        f.write(ss)
if __name__ == "__main__":
    sys.exit(main())