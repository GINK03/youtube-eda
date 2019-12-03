import pandas as pd
import numpy as np
import  re

def parse_num(x):
    xs = re.findall('\d', str(x))
    try:
        return int(''.join(xs))
    except ValueError:
        return 0

video_df = pd.read_csv('./vars/parse_each_videos.csv')
video_df['positive'] = video_df.positive.apply(parse_num)
video_df['negative'] = video_df.negative.apply(parse_num)

def get_channel_id(x):
    try:
        return x.split('/')[-1]
    except:
        return np.nan 
video_df['channel_id'] = video_df.owner.apply(get_channel_id)
#print(video_df.head())

channel_df = pd.read_csv('vars/channels.csv')
#print(channel_df.head())

df = pd.merge(video_df, channel_df, on=['channel_id'], how='left')

df = df[df.country == 'JP']
#print(df.date)
def sanitize_date(x):
    try:
        date = re.search(r'\d\d\d\d/\d\d/\d\d', x).group(0)
        return date
    except:
        return None
df['date'] = df.date.apply(sanitize_date)

def parse_view_count(x):
    try:
        return int(''.join(re.findall('\d', x)))
    except:
        return None
df['view_count'] = df.view_count.apply(parse_view_count)

def parse_subs(x):
    match = re.search(r'((\d|\.){1,}(K|M))', x)
    if match is None:
        return None
    raw = match.group(1)
    if 'K' in raw or 'M' in raw:
        suffix = raw[-1]
        prefix = raw[:-1]
        #print('siffx', suffix)
        if suffix == 'K':
            rate = 10**3
        elif suffix == 'M':
            rate = 10**6
        num = int(float(prefix) * rate)
        #print('test', num, rate)
    else:
        raw = raw.strip()
        num = int(raw)
    #print('parse sub',raw, num)
    return num

df['subs'] = df.subs.apply(parse_subs)
#print(df.head())

df.to_csv('vars/120_joined.csv', index=None)

def filter_channel_names():
    rets = []
    for cx, cy in zip(df.channel_name_x, df.channel_name_y):
        cy = cy.replace('…', '')[:-1]
        if cy not in cx:
            print(cx, cy)
            ret = False
        ret = True
        rets.append(ret)
    return rets

df = df[filter_channel_names()]
df.to_csv('vars/120_joined_filtered.csv', index=None)
