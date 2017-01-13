"""
Usage:
  generate_kospi_h5s.py [--output-path=<path>] [--ktb] [--first-date=<d>]

Korea h5 generation

Options:
  --output-path=<path>    Path to store generated h5 file [default: /home/jgreenwald/md/korea/]
  --ktb                   Run on ktb data
  --first-date=<d>        Date to start on [default: 20170101]
  -h --help               Show this help message and exit
"""


from os import listdir
from os.path import isfile, join
from subprocess import Popen,PIPE
import pandas as pd
import sys,os
import numpy as np
from kit_time import fix_timestamps
from docopt import docopt

def dump_root():
    return '/home/jgreenwald/atm_dev/atm/run/app/dump/'
def cfg_path():
    return dump_root() + 'MDConfig_KRX.cfg'

def sim_command(exec_path='/home/jgreenwald/atm_dev/atm/run/app/dump/run'):
    return  exec_path + ' ' + cfg_path()

def ktb_id():
    return 'KOSPI_CNCFUT'

def option_ids():
    return 'KOSPI_FUT, KOSPI_CALL, KOSPI_PUT'

def md_root():
    return "/home/jgreenwald/md/korea/"

def ag_md_path():
    return "/home/Dumps2/ProcessedData/bs/"

def main(ktb = False, first_date = '20170101'):
    
    dates = listdir(ag_md_path())
    cmd = sim_command()
    s = pd.Series([f.strip() for f in listdir(md_root())])
    if s.empty:
        h5s = pd.Series([])
    else:
        h5s = s.ix[s.str[-2:]=='h5']

    if ktb:
        eid = ktb_id()
    else:
        eid = option_ids()

    last_possible_date = (pd.to_datetime('today') + np.timedelta64(1,'D')).strftime("%Y%m%d")

    for d in dates:
        try:
            if (d+'.h5') in h5s.values or d<first_date:
                continue
            update_cfg(cfg_path(),d,eid)
            p = Popen(cmd, stderr=PIPE, stdout=PIPE, shell=True)
            output, errors = p.communicate()
            if p.returncode ==0:
                print '...\n'
            else:
                print output
                print errors
            md = pd.read_csv(dump_root()+'md_dump.csv',sep=' ', index_col=False)
            md.local_time = pd.to_datetime(fix_timestamps(md.local_time.values)).tz_localize("UTC").tz_convert("Asia/Seoul")
            backup = md.exchange_time
            try:
                md.exchange_time = pd.Index(pd.to_datetime(md.exchange_time*1e6)).tz_localize("UTC").tz_convert("Asia/Seoul")
            except:
                md.exchange_time = backup
            md.index = md.local_time
            del md["local_time"]
            store = pd.HDFStore(md_root()+d+'.h5','w',complevel=9,complib='blosc')
            store.append('md',md,)
            store.close()
            p = Popen("rm " + dump_root()+'md_dump.csv', stderr=PIPE, stdout=PIPE, shell=True)
        except:
            print 'Failed to parse: ' + d
            continue



def update_cfg(path,d, exchid):
    assert(len(d) == 8)
    with open(path,'r') as f:
        s = f.read()
    p1 = s.find('DATES = ')
    p3 = s.find('2', p1)
    ss =  s[:p3] + d + s[p3+8:]
    
    p1 = ss.find('EXCHID = ')
    p3 = ss.find('\n',p1)
    
    v1 = 'EXCHID = ' + exchid
    
    sss = ss[:p1] + v1 + ss[p3:]
    with open(path,'w') as f:
        f.write(sss)

if __name__ == "__main__":
    try:
        arguments = docopt(__doc__, version='korea generate h5s 2.0')
        main(arguments['--ktb'] is not None, str(arguments['--first-date']))
    except:
        raise
    


