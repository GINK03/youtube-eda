
import re
import numpy as np

def km_parse(string):
    if 'K' in string:
        ret = re.search(r'(\d|\.){1,}', string).group(0)
        ret = int(float(ret) * 1000)
    elif 'M' in string:
        ret = re.search(r'(\d|\.){1,}', string).group(0)
        ret = int(float(ret) * 1000_000)
    else:
        ret = int(float(string))
    return ret

def parse_km_subs(df):
    rets = []
    for sub in df['subs']:
        rets.append(km_parse(sub))
    df['subs'] = np.array(rets)


def parse_estimate(df):
    rets = []
    for estimate in df['estimate']:
        minimam, maximam = map(lambda x:x.replace('$', ''), estimate.split(' - '))
        minimam, maximam = map(km_parse, [minimam, maximam])
        mean = (minimam + maximam)//2
        rets.append(mean * 108)

    df['estimate'] = np.array(rets)

        
