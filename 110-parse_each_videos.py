from tqdm import tqdm
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from pathlib import Path
import pickle
import gzip
import re
from urllib.parse import urlparse
from concurrent.futures import ProcessPoolExecutor as PPE
import multiprocessing 
import random
import sys
import os

def ttrim(x):
    x = x.replace('\n', '')
    x = re.sub(r'\s{1,}', ' ', x)
    return x

def parallel(arg):
    pt = arg

    try:
        url = pt.open().read()
        if not re.match(r'https://youtube.com/watch', url):
            #print('not', url)
            return
        name = pt.name 
        
        ## メモ化
        if Path(f'/tmp/{name}').exists():
            with open(f'/tmp/{name}', 'rb') as fp:
                return pickle.load(fp)

        pp = urlparse(url)
        try:
            dd = dict([tuple(pair.split('=')) for pair in pp.query.split('&')])
        except ValueError:
            simple_name = str(pt).replace('.url', '')
            for suffix in ['.url', '.html', '.hrefs']:
                Path(simple_name + suffix).unlink()
            return
        # print(dd)
        if 'v' not in dd:
            return
        vid = dd['v']

        html_pt = Path(str(pt).replace('.url', '') + '.html')
        html = gzip.decompress(html_pt.open('rb').read()).decode()
        soup = BeautifulSoup(html, features="lxml")
        try:
            title = soup.find('title').text
            channel_name = ttrim(
                soup.find('ytd-channel-name', {'class': 'ytd-video-owner-renderer'}).text)
            view_count = ttrim(soup.find('span', {'class': 'view-count'}).text)
            date = soup.find('div', {'id': 'date'}).text
            sub_count = soup.find('yt-formatted-string',
                                  {'id': 'owner-sub-count'}).text
            owner = soup.find(
                'ytd-channel-name', {'class': 'ytd-video-owner-renderer'}).find('a').get('href')
            #print(soup.find('div', {'id':'contents'}).text)
            #print(soup.find('div', {'class': 'ytd-comment-renderer'}))
            #print(len(soup.find_all('div', {'class': 'ytd-comment-renderer'})))
            # for xx in soup.find_all('div', {'class': 'ytd-comment-renderer'}):
            #    print('comment', xx)
            #print(len(soup.find_all('a', {'class':'ytd-toggle-button-renderer'})))
            positive, negative = [s.get('aria-label') for s in soup.find_all(
                'yt-formatted-string', {'id': 'text', 'aria-label': True})]
            obj = {
                'vid': vid,
                'title': title,
                'channel_name': channel_name,
                'view_count': view_count,
                'date': date,
                'sub_count': sub_count,
                'positive': positive,
                'negative': negative,
                'owner': owner
            }
            print(obj)
        except Exception as exc:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            #print(exc)
            simple_name = str(pt).replace('.url', '')
            for suffix in ['.url', '.html', '.hrefs']:
                Path(simple_name + suffix).unlink()
            return
            #return

        # メモ化
        with open(f'/tmp/{name}', 'wb') as fp:
            pickle.dump(obj, fp)
        return obj
    except Exception as exc:
        print('deep error', exc)
        return 

N = int(multiprocessing.cpu_count() * 1.4)
pts = [pt for pt in Path().glob('./vars/htmls/*.url')]
random.shuffle(pts)
objs = []
with PPE(max_workers=N) as exe:
    for ret in tqdm(exe.map(parallel, pts), total=len(pts)):
        if ret is None:
            continue
        objs.append(ret)

df = pd.DataFrame(objs)
df.to_csv('vars/parse_each_videos.csv', index=None)
