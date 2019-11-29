import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from pathlib import Path
import pickle
import gzip
import re
from urllib.parse import urlparse
from concurrent.futures import ProcessPoolExecutor as PPE

def ttrim(x):
    x = x.replace('\n', '')
    x = re.sub(r'\s{1,}', ' ', x)
    return x

pts = [pt for pt in Path().glob('./vars/htmls/*')]

def parallel(arg):
    pt = arg
    if 'url' not in pt.name:
        return
    url = pt.open().read()
    if not re.match(r'https://youtube.com/watch', url):
        #print('not', url)
        return

    pp = urlparse(url)
    dd = dict([tuple(pair.split('=')) for pair in pp.query.split('&')])
    if 'v' not in dd:
        return
    vid = dd['v']

    html_pt = Path(str(pt).replace('.url', '') + '.html')
    html = gzip.decompress(html_pt.open('rb').read()).decode()
    soup = BeautifulSoup(html, features="lxml")
    try:
        title = soup.find('title').text
        channel_name = ttrim(soup.find('div', {'class': 'ytd-channel-name'}).text)
        view_count = ttrim(soup.find('span', {'class': 'view-count'}).text)
        date = soup.find('div', {'id': 'date'}).text
        sub_count = soup.find('yt-formatted-string', {'id': 'owner-sub-count'}).text
        owner_referer = soup.find('a', {'class':'ytd-video-owner-renderer'}).get('href')
        #print(soup.find('div', {'id':'contents'}).text)
        #print(soup.find('div', {'class': 'ytd-comment-renderer'}))
        #print(len(soup.find_all('div', {'class': 'ytd-comment-renderer'})))
        #for xx in soup.find_all('div', {'class': 'ytd-comment-renderer'}):
        #    print('comment', xx)
        #print(len(soup.find_all('a', {'class':'ytd-toggle-button-renderer'})))
        positive, negative = [s.get('aria-label') for s in soup.find_all('yt-formatted-string', {'id':'text', 'aria-label':True})]
        obj = {'vid':vid, 
                'title':title,
                'channel_name':channel_name,
                'view_count':view_count,
                'date':date,
                'sub_count':sub_count,
                'positive':positive,
                'negative':negative, 
                'owner_referer':owner_referer
                }
        #print(obj)
    except Exception as exc:
        #print(exc)
        return 
    return obj

from tqdm import tqdm
import pandas as pd
objs = []
with PPE(max_workers=64) as exe:
    for ret in tqdm(exe.map(parallel, pts), total=len(pts)):
        if ret is None:
            continue
        objs.append(ret)

df = pd.DataFrame(objs)
df.to_csv('vars/parse_each_videos.csv', index=None)
